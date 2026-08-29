# frozen_string_literal: true

# Shared RS256 JWT verifier for the worker and the workspace server (ADR-015).
# Pure Ruby (stdlib + jwt gem) so the bare worker and the Rails app both require
# this same file.
#
# Security invariants:
#   * Algorithm is PINNED to RS256. The token's `alg` claim is checked but is
#     never used to select the verification algorithm — no alg-confusion.
#   * Keys are public-only, fetched from the JWKS endpoint; the pod never
#     holds the private signing key.
#
# Cache/backoff policy (cold-start fast retry, refresh, stale-serve ceiling,
# throttled unknown-kid refetch):
#   REFRESH_TTL     — a successful fetch stays "fresh" this long
#   COLD_RETRY      — retry interval while we have NO keys yet
#   REFETCH_THROTTLE— min interval between network fetches (bounds a forged-kid
#                     flood from hammering control)
#   STALE_CEILING   — past this, refuse to serve stale keys (fail closed)
#
require 'net/http'
require 'uri'
require 'json'
require 'openssl'
require 'base64'
require 'jwt'

class JwtVerifier
  REFRESH_TTL      = 5 * 60
  COLD_RETRY       = 15
  REFETCH_THROTTLE = 30
  STALE_CEILING    = 24 * 60 * 60

  class << self
    def configure(jwks_url:)
      @instance = new(jwks_url)
    end

    def verify(token)
      raise 'JwtVerifier not configured' unless @instance

      @instance.verify(token)
    end
  end

  def initialize(jwks_url)
    @jwks_url       = jwks_url
    @mutex          = Mutex.new
    @keys           = nil          # array of public JWK hashes, nil until first success
    @fetched_at     = nil          # monotonic time of last successful fetch
    @last_attempt_at = nil         # monotonic time of last network attempt
  end

  def verify(token)
    _payload, header = JWT.decode(token, nil, false)
    raise JWT::DecodeError, 'unexpected alg' unless header['alg'] == 'RS256'

    kid = header['kid']
    jwk = resolve_key(kid) or raise JWT::DecodeError, "no key for kid #{kid.inspect}"

    rsa = rsa_public_key(jwk)
    payload, = JWT.decode(token, rsa, true, { algorithm: 'RS256' })
    payload
  end

  private

  def resolve_key(kid)
    @mutex.synchronize do
      now  = monotonic_now
      keys = @keys

      if keys.nil?
        refetch(now) if due?(@last_attempt_at, now, COLD_RETRY)
        keys = @keys || []
      elsif now - @fetched_at >= STALE_CEILING
        refetch(now) if due?(@last_attempt_at, now, REFRESH_TTL)
        keys = []   # past ceiling: fail closed
      elsif now - @fetched_at >= REFRESH_TTL
        refetch(now)
        keys = @keys || []
      end

      found = keys.find { |j| j['kid'] == kid }
      return found if found

      # Unknown kid: throttled refetch, then fail THIS token (client retries;
      # the refetch is already scheduled and will serve the next attempt).
      refetch(now) if due?(@last_attempt_at, now, REFETCH_THROTTLE)
      (@keys || []).find { |j| j['kid'] == kid }
    end
  end

  def due?(last, now, interval)
    last.nil? || (now - last) >= interval
  end

  def refetch(now)
    @last_attempt_at = now
    body = fetch_jwks
    keys = JSON.parse(body)['keys'] || []
    @keys       = keys
    @fetched_at = now
  rescue => e
    # Keep whatever we had: nil on cold start (fail closed), stale on warm
    # (stale-serve until ceiling).
    warn "[jwt_verifier] JWKS fetch failed: #{e.class}: #{e.message}"
  end

  def fetch_jwks
    uri  = URI.parse(@jwks_url)
    http = Net::HTTP.new(uri.host, uri.port)
    http.open_timeout = 2
    http.read_timeout = 5
    http.use_ssl      = (uri.scheme == 'https')
    resp = http.get(uri.request_uri)
    raise "JWKS endpoint returned #{resp.code}" unless resp.is_a?(Net::HTTPSuccess)

    resp.body
  end

  def rsa_public_key(jwk)
    n = OpenSSL::BN.new(Base64.urlsafe_decode64(jwk['n']), 2)
    e = OpenSSL::BN.new(Base64.urlsafe_decode64(jwk['e']), 2)
    key = OpenSSL::PKey::RSA.new
    key.set_key(n, e, nil)
    key
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
