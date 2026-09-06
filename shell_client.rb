# ShellClient — the worker's side of the ADR-029 shell handle protocol.
#
# Replaces ProjectPod. The worker no longer creates, waits on, or deletes the
# shell pod, and no longer holds standing pods/exec on its own namespace. It
# asks control for a handle and gets back a pod name plus a short-lived token
# minted against a dedicated exec ServiceAccount.
#
# The inversion matters for more than tidiness: the pod is now a StatefulSet
# the operator owns, so it survives worker restarts, worker crashes, and pod
# rescheduling. The worker's opinion about whether it should exist is expressed
# as a refcount, not as a create/delete.
#
# Identity is the pod's projected ServiceAccount token, audience-scoped to
# carbide-control. Control TokenReviews it and derives the workspace from the
# token's namespace, so this client cannot act on a workspace that is not its
# own no matter what it puts in the URL.

require 'json'
require 'net/http'
require 'uri'
require 'time'
require 'fileutils'
require 'securerandom'
require 'tempfile'

class ShellClient
  # Projected by the operator with audience carbide-control. Re-read on every
  # request rather than cached: kubelet rotates projected tokens in place, and
  # a cached copy becomes a 401 somewhere north of an hour in.
  TOKEN_PATH = ENV.fetch('CARBIDE_CONTROL_TOKEN_PATH',
                         '/var/run/secrets/carbide/control/token').freeze

  CONTROL_URL = ENV.fetch('CONTROL_URL',
                          'http://control-plane.carbide-system.svc.cluster.local:3001').freeze

  WORKSPACE_ID = ENV.fetch('WORKSPACE_ID', '').freeze

  # Both are set unconditionally by the operator on every workspace pod
  # (ADR-029 §6, ADR-031). Kubernetes will happily start a pod without them,
  # and the readiness probe is Rails' /up, so a pod built wrong would run for
  # as long as it took someone to open a terminal. Fail here, where the log
  # line lands next to the deploy that caused it.
  if WORKSPACE_ID.empty?
    abort "[ShellClient] WORKSPACE_ID is not set; this pod was not built by the control operator"
  end
  unless File.readable?(TOKEN_PATH)
    abort "[ShellClient] control token not readable at #{TOKEN_PATH}; the projected volume is missing or mis-permissioned"
  end

  # In-cluster API server coordinates for the kubeconfig we hand to kubectl.
  API_SERVER = ENV.fetch('KUBERNETES_SERVICE_HOST', 'kubernetes.default.svc').freeze
  API_PORT   = ENV.fetch('KUBERNETES_SERVICE_PORT', '443').freeze
  CA_PATH    = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'.freeze

  OPEN_TIMEOUT = Integer(ENV.fetch('CARBIDE_CONTROL_OPEN_TIMEOUT', '5'))
  READ_TIMEOUT = Integer(ENV.fetch('CARBIDE_CONTROL_READ_TIMEOUT', '15'))

  # How long to keep asking while the shell is still coming up. The pod is
  # usually warm; this covers a cold start including an image pull.
  ACQUIRE_TIMEOUT = Integer(ENV.fetch('CARBIDE_SHELL_ACQUIRE_TIMEOUT', '90'))

  class NotReady < StandardError; end
  class Error < StandardError; end
  # A 5xx from control: retried inside acquire!'s deadline. 4xx stays Error.
  class ServerError < Error; end

  # A grant, valid until expires_at. Owns the kubeconfig file holding the
  # token, so the token never appears in argv where `ps` would show it to
  # every other process in the pod.
  class Handle
    attr_reader :namespace, :pod, :expires_at

    def initialize(namespace:, pod:, token:, expires_at:)
      @namespace  = namespace
      @pod        = pod
      @expires_at = expires_at
      @kubeconfig = write_kubeconfig(token)
    end

    # PTY.spawn runs this through /bin/sh -c. Login shell so /etc/profile and
    # ~/.profile load, matching what the user gets from a real terminal. The
    # image's WORKDIR is the mounted project directory, so no cd is needed.
    def exec_cmd
      "kubectl --kubeconfig #{@kubeconfig} exec -n #{@namespace} --tty --stdin #{@pod} -- bash -l"
    end

    # Only relevant when opening a NEW terminal: once the exec stream is
    # established the API server does not re-check the token, so an existing
    # terminal outlives the grant that started it. Skewed early so a handle
    # cannot expire between this check and kubectl's connect.
    def expired?
      return true if @expires_at.nil?

      Time.now >= (Time.parse(@expires_at.to_s) - 30)
    rescue ArgumentError
      true
    end

    # The kubeconfig outlives the exec on purpose: kubectl reads it once at
    # startup, but the file is the only place the token lives, and dropping it
    # while kubectl is still starting would race. Disposed when the terminal
    # that acquired it exits.
    def dispose
      File.unlink(@kubeconfig)
    rescue Errno::ENOENT
      nil
    end

    private

    def write_kubeconfig(token)
      dir = File.join(ENV.fetch('TMPDIR', '/tmp'), 'carbide-exec')
      FileUtils.mkdir_p(dir, mode: 0o700)
      path = File.join(dir, "kubeconfig-#{SecureRandom.hex(8)}")

      config = {
        'apiVersion' => 'v1',
        'kind' => 'Config',
        'clusters' => [{
          'name' => 'carbide',
          'cluster' => {
            'server' => "https://#{API_SERVER}:#{API_PORT}",
            'certificate-authority' => CA_PATH
          }
        }],
        'users' => [{ 'name' => 'exec', 'user' => { 'token' => token } }],
        'contexts' => [{
          'name' => 'carbide',
          'context' => { 'cluster' => 'carbide', 'user' => 'exec', 'namespace' => @namespace }
        }],
        'current-context' => 'carbide'
      }

      # 0600 before the token is written, not after.
      File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0o600) do |f|
        f.write(JSON.generate(config))
      end
      path
    end
  end

  class << self
    # Ask for a handle, retrying while control reports the shell is still
    # starting. Blocking -- callers must run this off the EM reactor thread.
    #
    # Idempotent on the control side, so retrying is a plain re-POST rather
    # than a separate poll endpoint.
    def acquire!
      deadline = Time.now + ACQUIRE_TIMEOUT
      delay    = 0.5
      last     = nil

      loop do
        body = begin
          post('shell')
        rescue ServerError => e
          # Control itself hiccupped (DB lock wait, kube read, mint). POST is
          # idempotent (ADR-029 §4), so inside the deadline this is "not yet",
          # not "never".
          raise Error, "control unavailable after #{ACQUIRE_TIMEOUT}s: #{e.message}" if Time.now >= deadline

          last = e.message
          sleep delay
          delay = [delay * 1.5, 3.0].min
          next
        end

        if body['ready']
          return Handle.new(
            namespace:  body['namespace'],
            pod:        body['shell_pod'],
            token:      body['exec_token'],
            expires_at: body['expires_at']
          )
        end

        last = body['reason'] || body['phase']

        # Control already distinguishes "still coming up" from "will never come
        # up" (ImagePullBackOff, CrashLoopBackOff, config errors). Waiting out
        # the full timeout on a terminal failure just delays the same message.
        raise Error, "shell cannot start: #{last}" if body['phase'] == 'Failed'
        raise NotReady, "shell not ready after #{ACQUIRE_TIMEOUT}s: #{last}" if Time.now >= deadline

        sleep delay
        delay = [delay * 1.5, 3.0].min
      end
    end

    # Report the live terminal count. This is the ONLY thing that keeps a lazy
    # shell up: control latches the falling edge to zero and idles the shell
    # down after the timeout. Called on every create/exit and periodically, so
    # a frozen worker stops reporting and its shell is reclaimed.
    def report!(terminals)
      post('shell/release', terminals: terminals.to_i)
      true
    rescue StandardError => e
      # Never fatal. A missed report is recovered by the next one, and the
      # worst case -- no reports at all -- is exactly the liveness signal the
      # sweep is watching for.
      warn "[ShellClient] report failed: #{e.class} #{e.message}"
      false
    end

    private

    def post(path, payload = nil)
      uri = URI.join(CONTROL_URL + '/', "api/v1/control/workspaces/#{WORKSPACE_ID}/#{path}")

      req = Net::HTTP::Post.new(uri)
      req['Authorization'] = "Bearer #{File.read(TOKEN_PATH).strip}"
      req['Content-Type']  = 'application/json'
      req.body = JSON.generate(payload || {})

      res = Net::HTTP.start(uri.hostname, uri.port,
                            use_ssl: uri.scheme == 'https',
                            open_timeout: OPEN_TIMEOUT,
                            read_timeout: READ_TIMEOUT) { |http| http.request(req) }

      return {} if res.is_a?(Net::HTTPNoContent)

      body = begin
        JSON.parse(res.body.to_s)
      rescue JSON::ParserError
        {}
      end

      unless res.is_a?(Net::HTTPSuccess)
        detail = body['error'] || body['reason'] || res.body.to_s[0, 200]
        klass  = res.is_a?(Net::HTTPServerError) ? ServerError : Error
        raise klass, "control #{path} returned #{res.code}: #{detail}"
      end

      body
    end
  end
end
