#!/usr/bin/env ruby
# Carbide2 worker — EventMachine WebSocket server
# Handles: terminal (PTY), chat, fs (file read). Protocol: { cs, cmd, payload }
$stdout.sync = true
$stderr.sync = true

# ---------------------------------------------------------------------------
# Persistent worker log (survives pod reaping)
# ---------------------------------------------------------------------------
# kubectl logs only retains output while the pod exists; a reaped/rolled pod
# takes its logs with it, so an overnight death leaves nothing to read. When
# CARBIDE_WORKER_LOG is set (the kube deployment points it at the project PVC),
# mirror stdout/stderr to that file with a timestamp on every line, emit a
# heartbeat so the time-of-death is known, and record the exit reason. The
# file lives on the persistent volume, so it outlives the container.
if (worker_log_path = ENV['CARBIDE_WORKER_LOG'].to_s) && !worker_log_path.empty?
  require 'fileutils'
  require 'time'
  begin
    FileUtils.mkdir_p(File.dirname(worker_log_path))
    # Roll if the previous run left a large file behind (the PVC also holds
    # project files; a runaway log must not fill it and cause ErrImagePull).
    if File.exist?(worker_log_path) && File.size(worker_log_path) > 16 * 1024 * 1024
      File.rename(worker_log_path, "#{worker_log_path}.1") rescue nil
    end
    _worker_log_io = File.open(worker_log_path, 'a')
    _worker_log_io.sync = true

    # Tee: write to the original stream AND the persistent file, prefixing
    # each line in the file with an ISO8601 timestamp. stdout/stderr that
    # kubectl reads stays raw so existing log parsing is unaffected.
    tee = Class.new do
      def initialize(orig, file) = (@orig, @file, @at_bol = orig, file, true)
      def write(str)
        @orig.write(str)
        s = str.to_s
        unless s.empty?
          stamped = +''
          s.each_char do |c|
            stamped << Time.now.utc.iso8601(3) << ' ' if @at_bol
            stamped << c
            @at_bol = (c == "\n")
          end
          @file.write(stamped)
        end
        str.to_s.bytesize
      end
      def puts(*a) = (a.empty? ? write("\n") : a.each { |x| write("#{x}\n") })
      def print(*a) = a.each { |x| write(x.to_s) }
      def printf(fmt, *a) = write(format(fmt, *a))
      def <<(x) = (write(x.to_s); self)
      def flush = (@orig.flush; @file.flush; self)
      def sync = true
      def sync=(v); v; end
      def fileno = @orig.fileno
      def tty? = @orig.tty?
      def respond_to_missing?(m, inc = false) = @orig.respond_to?(m, inc)
      def method_missing(m, *a, &b) = @orig.send(m, *a, &b)
    end
    $stdout = tee.new(STDOUT, _worker_log_io)
    $stderr = tee.new(STDERR, _worker_log_io)
    puts "[worker] persistent log opened at #{worker_log_path} (pid=#{Process.pid})"

    # Record why the worker exits — clean shutdown, signal, or crash. This is
    # the line that would have answered "why did the shell die overnight."
    at_exit do
      err = $! && $!.is_a?(Exception) ? "#{$!.class}: #{$!.message}" : nil
      puts "[worker] exiting (pid=#{Process.pid})#{err ? " due to #{err}" : ''}"
      if err && $!.backtrace
        $!.backtrace.first(20).each { |l| puts "[worker]   #{l}" }
      end
    end
    %w[TERM INT].each do |sig|
      trap(sig) do
        warn "[worker] received SIG#{sig} (pid=#{Process.pid}) — shutting down"
        exit(0)
      end
    end
  rescue => e
    warn "[worker] could not open persistent log #{worker_log_path}: #{e.class}: #{e.message}"
  end
end

require 'eventmachine'
require 'em-websocket'
require 'json'
require 'jwt'
require 'open3'
require 'pty'
require 'securerandom'
require 'io/console'
require 'uri'
require_relative 'terminal_instance'
require_relative 'terminal_recorder'
require_relative 'chat_room'
require_relative 'open_document'
require_relative 'document'
require_relative 'project_container'
require_relative 'project_pod'
require_relative 'session'
require_relative 'jwt_verifier'
require_relative 'ar_boot'
require_relative 'fs_store'
require_relative 'vfs_flusher'
require_relative 'vfs_watcher'
require_relative 'agent_tools'
require_relative 'agent_session'
require_relative 'command'
require_relative 'debug_stream'
require_relative 'handlers/term_handlers'
require_relative 'handlers/chat_handlers'
require_relative 'handlers/fs_handlers'
require_relative 'handlers/agent_handlers'
require_relative 'handlers/rtc_handlers'
require_relative 'handlers/session_handlers'
require 'set'

# ADR-015 asymmetric signing: the worker verifies control-minted RS256 tokens
# against public keys fetched from the JWKS endpoint. It holds NO signing secret.
JwtVerifier.configure(jwks_url: ENV.fetch('CONTROL_JWKS_URL') { 'http://control-plane.carbide-system.svc.cluster.local:3001/.well-known/jwks.json' })

# ADR-023 — first-message auth. An unauthenticated socket has
# AUTH_TIMEOUT_SECONDS to send system/auth before it is disconnected, and the
# worker holds at most MAX_PENDING_AUTH unauthenticated sockets at once.
AUTH_TIMEOUT_SECONDS = Integer(ENV.fetch('CARBIDE_AUTH_TIMEOUT_SECONDS', '10'))
MAX_PENDING_AUTH     = Integer(ENV.fetch('CARBIDE_MAX_PENDING_AUTH', '100'))

# A ?probe=true connection exists only to prove the WS upgrade (101) succeeded.
# Its command allowlist is empty today: probes may not route, auth, or promote.
# If we later want probes to answer a lightweight query (e.g. worker status),
# add the allowed [cs, cmd] pairs here — the onmessage probe branch consults
# this list.
PROBE_ALLOWED_COMMANDS = [].freeze

# ---------------------------------------------------------------------------
# Wire-protocol versioning
# ---------------------------------------------------------------------------
# Compatibility is decided by a single monotonic integer per side plus a floor,
# NOT a SemVer range matrix:
#   PROTOCOL   — the wire protocol this build speaks. Bump on ANY wire change.
#   MIN_CLIENT — the oldest client PROTOCOL this build still tolerates. Bump
#                ONLY on a breaking change.
# Two peers are compatible iff each is at or above the other's floor:
#   client.protocol >= server.MIN_CLIENT  AND  server.protocol >= client.min_server
# The check is advisory for now: on mismatch we warn (log + tell the client),
# but still serve the connection. Flip to a hard refuse later if needed.
# PROTOCOL 2: added the 'session' commandSet (server-side browser-session
# tracking). Additive — MIN_CLIENT stays 1 (clients that never send session
# frames are unaffected).
# PROTOCOL 3: session/delete + client_version on session create/list, and a
# stable per-terminal uuid in term/created + term/list. All additive.
# PROTOCOL 4: agent/user_turn broadcast (shared conversations, #80) + per-message
# author on agent user messages (#79). Additive — MIN_CLIENT stays 1.
# PROTOCOL 5: agent/stop + agent/stopping + agent/stopped (#83). Additive.
PROTOCOL   = 5
MIN_CLIENT = 1

# Load worker/carbide.yml if present; allows per-machine config without env vars.
_cfg_path = File.join(__dir__, 'carbide.yml')
_cfg      = File.exist?(_cfg_path) ? (require 'yaml'; YAML.load_file(_cfg_path, permitted_classes: []) || {}) : {}
PROJECT_ROOT = File.expand_path(
  ENV['PROJECT_ROOT'] || _cfg['project_root'].to_s.then { |p| p.empty? ? Dir.pwd : p }
).freeze
puts "[worker] PROJECT_ROOT = #{PROJECT_ROOT} (fallback only — overridden by project.root_path from DB)"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Validate a worker JWT. ADR-023: control-plane-only — the only accepted format
# is the control-minted one (iss=carbide-control, scope=workspace:rw). Signature
# is RS256, verified against the JWKS public keys (ADR-015) — never a shared
# secret. Identity is uuid-only (ADR-015); no integer claims.
#
# See JWT_CLAIMS.md for the wire format.
def validate_token(token)
  payload = JwtVerifier.verify(token)

  return nil unless payload['iss'] == 'carbide-control'

  # Audience guard: the token must name THIS workspace (uuid), preventing a
  # token minted for workspace A from being replayed against workspace B.
  expected = ENV['WORKSPACE_PROJECT_UUID'].to_s
  unless expected.empty? || payload['aud'] == "workspace:#{expected}"
    puts "[validate_token] aud mismatch: got #{payload['aud'].inspect}, want workspace:#{expected}"
    return nil
  end

  unless %w[workspace:rw].include?(payload['scope'])
    puts "[validate_token] unsupported scope: #{payload['scope'].inspect}"
    return nil
  end

  payload
rescue JWT::DecodeError => e
  puts "Invalid token: #{e}"
  nil
end

def send_msg(ws, cs, cmd, payload = {})
  ws.send({ cs: cs, cmd: cmd, payload: payload }.to_json) rescue nil
end

def broadcast(clients, cs, cmd, payload = {})
  # Serialise ONCE, and never on a path that can crash the reactor. A malformed
  # payload (e.g. invalid-UTF-8 PTY bytes that slipped past source scrubbing)
  # would otherwise raise here inside an EM.next_tick and take the worker down.
  # Drop the frame instead — one lost output chunk beats a dead reactor.
  begin
    msg = { cs: cs, cmd: cmd, payload: payload }.to_json
  rescue => e
    puts "[broadcast] payload not serialisable (cs=#{cs} cmd=#{cmd}): #{e.class} #{e.message}"
    return []
  end
  dead = []
  clients.each do |ws|
    ws.send(msg)
  rescue => e
    puts "[broadcast] send failed: #{e.class} #{e.message}"
    dead << ws
  end
  dead
end

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
TERMINALS           = {}        # terminal_id (int) => TerminalInstance
CHAT_ROOMS          = {}        # room_id (string)  => ChatRoom
OPEN_DOCUMENTS      = {}        # "#{project_id}:#{path}" => OpenDocument
DOCUMENTS           = {}        # srcpath => Document (in-memory rendered buffer)
SESSION_SUBSCRIBERS = {}        # browser_session uuid => { ws => {user_id:,name:,role:} }
PROJECT_CONTAINERS  = {}        # project_id (int)  => ProjectContainer
PROJECT_PODS        = {}        # project_id (int)  => ProjectPod
POD_REFCOUNTS       = Hash.new(0)  # project_id => live terminal count
SESSIONS_BY_PROJECT = {}        # project_id => [Session, ...]
VFS_FLUSH_SUPPRESS  = Set.new   # absolute paths being written by VfsFlusher
VFS_FLUSHERS        = {}        # project_id => VfsFlusher
VFS_WATCHERS        = {}        # project_id => VfsWatcher
PENDING_AUTH        = {}        # ws.object_id => Session (unauthenticated, awaiting system/auth)
AUTH_DEADLINES      = {}        # Session => EM::Timer (cancel on promote/close)

# ---------------------------------------------------------------------------
# First-message auth (ADR-023)
# ---------------------------------------------------------------------------
# A session is ALWAYS created unauthenticated at onopen. It becomes
# authenticated by exactly one path: the client's first frame, system/auth
# { token }. On success the ONE promotion path pins the principal, joins the
# project, cancels the auth deadline, and delivers the post-auth state. On
# failure the socket is closed.

# Resolve the LOCAL User mirror for a validated control token. The subject is
# typed (`sub: user:<uuid>`); fall back to email for pods that haven't stamped
# control_uuid yet. Find-only: the REST path is the creator/mirror-stamper.
def local_user_for(payload)
  sub   = payload['sub'].to_s
  uuid  = sub.start_with?('user:') ? sub.delete_prefix('user:') : ''
  email = payload['user_email'].to_s.downcase.strip
  return nil if email.empty?
  (uuid.empty? ? nil : User.find_by(control_uuid: uuid)) || User.find_by(email: email)
end

# Resolve the LOCAL project by its stable control-owned uuid (ADR-015). Under
# 1:1 the token's project_uuid is the workspace uuid, mirrored into the pod as
# WORKSPACE_PROJECT_UUID. Fall back to the single canonical project for local
# dev without control.
def local_project_for(payload)
  uuid = payload['project_uuid'].to_s.strip
  (uuid.empty? ? nil : Project.find_by(uuid: uuid)) || Project.canonical
end

# Pure: token -> principal-or-nil (validation + local identity resolution).
def establish!(token)
  return nil unless token.is_a?(String) && !token.empty?
  payload = validate_token(token)
  return nil unless payload
  # validate_token already enforced the token's iss/aud/scope.
  # Resolve the LOCAL user and LOCAL project by their stable uuids; neither
  # local id is the token's integer id.
  user    = local_user_for(payload)
  project = local_project_for(payload)
  return nil unless user && project
  payload.merge(
    'user_id'    => user.id,          # LOCAL users.id
    'user_email' => user.email,
    'project_id' => project.id        # LOCAL projects.id
  )
end

def handle_auth(session, payload)
  principal = establish!(payload['token'])
  principal ? promote(session, principal) : reject_auth(session)
end

def reject_auth(session)
  cancel_auth_deadline(session)
  PENDING_AUTH.delete(session.ws.object_id)
  send_msg(session.ws, 'system', 'error', { message: 'invalid or missing token' })
  session.ws.close_connection_after_writing
end

# The ONE promotion path: pin, join, cancel deadline, deliver post-auth state.
def promote(session, principal)
  session.pin_principal(principal)
  cancel_auth_deadline(session)
  PENDING_AUTH.delete(session.ws.object_id)

  SESSIONS_BY_PROJECT[session.project_id] ||= []
  SESSIONS_BY_PROJECT[session.project_id] << session

  send_msg(session.ws, 'system', 'connected', {
    user_id:    session.user_id,
    project_id: session.project_id,
    protocol:   PROTOCOL,
    min_client: MIN_CLIENT,
    token_exp:  session.token_exp
  })

  terminals = get_project_terminals(session.project_id)
  send_msg(session.ws, 'term', 'list', { project_id: session.project_id, terminals: terminals })

  puts "Client connected: user=#{session.user_id} project=#{session.project_id}"
end

def arm_auth_deadline(session)
  timer = EM.add_timer(AUTH_TIMEOUT_SECONDS) do
    if AUTH_DEADLINES.delete(session)
      puts '[auth] deadline exceeded before system/auth; disconnecting'
      session.ws.close_connection_after_writing
    end
  end
  AUTH_DEADLINES[session] = timer
end

def cancel_auth_deadline(session)
  timer = AUTH_DEADLINES.delete(session)
  EM.cancel_timer(timer) if timer
end

# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------
# Tiny handler module for the 'debug' commandSet — just (un)subscribes the
# caller to the DebugStream pub/sub. Events are pushed via DebugStream.emit
# from anywhere in the worker; see worker/debug_stream.rb.
module DebugHandlers
  def self.dispatch(cmd, session, payload)
    case cmd
    when 'subscribe'
      scope = payload['scope'] == 'all' ? :all : nil
      DebugStream.subscribe(session, scope: scope)
      send_msg(session.ws, 'debug', 'subscribed', { scope: (scope || session.project_id).to_s })
    when 'unsubscribe'
      DebugStream.unsubscribe(session)
      send_msg(session.ws, 'debug', 'unsubscribed', {})
    else
      send_msg(session.ws, 'system', 'error', { message: "unknown debug cmd: #{cmd}" })
    end
  end
end

# Connection liveness + in-band re-authentication for the client.
#   - ping:   echoed straight back as pong so the client can measure round-trip
#             latency and prove the socket is alive end-to-end (not just
#             TCP-open). Stateless.
#   - reauth: the client mints a fresh worker JWT over HTTP shortly before the
#             current one expires and presents it here, so a long-lived session
#             survives token rotation without ever dropping the socket. We
#             re-validate the token (same rules as the handshake) and, on
#             success, adopt its new expiry; on failure we tell the client and
#             let the normal expiry sweep close the socket.
module SystemHandlers
  def self.dispatch(cmd, session, payload)
    case cmd
    when 'auth'
      # system/auth is only valid on an unauthenticated session. Reaching here
      # means the session is already authenticated: refuse rather than
      # re-establishing (a second establishment would be a principal-change
      # path around system/reauth's identity pin).
      send_msg(session.ws, 'system', 'error', { message: 'already authenticated' })
    when 'ping'
      send_msg(session.ws, 'system', 'pong', { t: payload['t'] })
    when 'reauth'
      new_payload = validate_token(payload['token'])
      # Pin identity: a refreshed token must map to the same LOCAL user. Project
      # identity is invariant (one canonical project per pod), so only the user
      # needs checking here.
      new_user = new_payload && local_user_for(new_payload)
      same_identity = new_user && new_user.id.to_s == session.user_id.to_s
      if same_identity
        session.reauth(new_payload)
        send_msg(session.ws, 'system', 'reauth_ok', { exp: session.token_exp })
      else
        send_msg(session.ws, 'system', 'reauth_failed', { message: 'invalid or mismatched token' })
      end
    else
      send_msg(session.ws, 'system', 'error', { message: "unknown system cmd: #{cmd}" })
    end
  end
end

ROUTES = {
  'term'   => TermHandlers,
  'chat'   => ChatHandlers,
  'fs'     => FsHandlers,
  'agent'  => AgentHandlers,
  'rtc'    => RtcHandlers,
  'session' => SessionHandlers,
  'debug'  => DebugHandlers,
  'system' => SystemHandlers,
}.freeze

def route(session, msg_str)
  msg     = JSON.parse(msg_str)
  cs      = msg['cs']
  cmd     = msg['cmd']
  payload = msg['payload'] || {}

  handler = ROUTES[cs]
  if handler
    handler.dispatch(cmd, session, payload)
  else
    send_msg(session.ws, 'system', 'error', { message: "unknown commandSet: #{cs}" })
  end
rescue JSON::ParserError
  send_msg(session.ws, 'system', 'error', { message: 'invalid json' })
end

# Called once per terminal when its PTY reader hits EOF (shell exited, was
# killed, or the user clicked 'Destroy'). Prunes the global map, removes the
# id from every session's list, and re-broadcasts the project's terminal list
# so the UI removes the entry.
def on_terminal_exit(tid, project_id)
  return unless TERMINALS.delete(tid)
  (SESSIONS_BY_PROJECT[project_id] || []).each { |s| s.terminals.delete(tid) }
  broadcast_terminals_to_project(project_id)
  remaining = get_project_terminals(project_id).size
  puts "[on_terminal_exit] terminal=#{tid} project=#{project_id} pruned; #{remaining} remain"

  # Ref-count the kube-backed pod; tear it down when the last terminal exits.
  if ENV.fetch('CARBIDE_BACKEND', 'local') == 'kube' && PROJECT_PODS.key?(project_id)
    POD_REFCOUNTS[project_id] -= 1 if POD_REFCOUNTS[project_id] > 0
    if POD_REFCOUNTS[project_id] <= 0
      POD_REFCOUNTS.delete(project_id)
      pod = PROJECT_PODS.delete(project_id)
      Thread.new { pod.stop! } if pod  # don't block the EM reactor on kubectl delete
    end
  end
end

def get_project_terminals(project_id)
  TERMINALS.values.select { |t| t.project_id == project_id }.map(&:to_list_entry)
end

def broadcast_terminals_to_project(project_id)
  clients = (SESSIONS_BY_PROJECT[project_id] || []).map(&:ws)
  terminals = get_project_terminals(project_id)
  puts "[broadcast_terminals_to_project] project=#{project_id}, clients=#{clients.length}, terminals=#{terminals.length}"
  broadcast(clients, 'term', 'list', { project_id: project_id, terminals: terminals })
end

# ---------------------------------------------------------------------------
# HTTP API endpoint for Rails to create terminal instances
# Used by POST /api/projects/:id/terminals
# ---------------------------------------------------------------------------
def create_terminal(terminal_id, project_id:, cols: 80, rows: 24)
  term = TerminalInstance.new(terminal_id, project_id: project_id, cols: cols, rows: rows)
  TERMINALS[terminal_id] = term
  broadcast_terminals_to_project(project_id)
  terminal_id
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
EM.run do
  host = ENV.fetch('WORKER_HOST', '0.0.0.0')
  port = ENV.fetch('WORKER_PORT', '8080').to_i

  puts "Carbide2 worker starting on #{host}:#{port}"

  # Liveness heartbeat. With timestamped persistent logs, the heartbeat's last
  # line pins the time-of-death even when the worker was otherwise idle (an
  # overnight reap leaves no other output). Cheap: one line/minute.
  EM.add_periodic_timer(60) do
    puts "[worker] heartbeat pid=#{Process.pid} sessions=#{SESSIONS_BY_PROJECT.values.sum(&:size)} " \
         "terminals=#{(defined?(TERMINALS) ? TERMINALS.size : 0)} rss_kb=#{(File.read("/proc/#{Process.pid}/statm").split[1].to_i * 4 rescue 0)}"
  end
  puts "[worker] Docker container mode: #{ENV['CARBIDE_USE_DOCKER'] == '1' ? 'enabled' : 'disabled (set CARBIDE_USE_DOCKER=1 to enable)'}"
  puts "[worker] Shell backend: #{ENV.fetch('CARBIDE_BACKEND', 'local')} (image=#{ENV['CARBIDE_SHELL_IMAGE'] || 'n/a'} ns=#{ENV['CARBIDE_NAMESPACE'] || 'n/a'})"

  # Big-hammer orphan cleanup: any carbide2-shell pods left in this
  # workspace's namespace from a previous worker incarnation are dead to us
  # (POD_REFCOUNTS lives in memory). Their PTYs are gone, no client is bound,
  # and the pod is just consuming a node slot. Nuke them on startup. Other
  # workspaces have their own namespaces, so this is scoped.
  if ENV.fetch('CARBIDE_BACKEND', 'local') == 'kube'
    EM.defer do
      ns = ENV.fetch('CARBIDE_NAMESPACE') { ProjectPod.read_namespace }
      puts "[worker] pruning orphan carbide2-shell pods in ns=#{ns}"
      out, err, status = Open3.capture3(
        'kubectl', 'delete', 'pod', '-n', ns,
        '-l', 'app.kubernetes.io/name=carbide2-shell',
        '--ignore-not-found', '--wait=false', '--grace-period=5'
      )
      if status.success?
        puts "[worker] orphan prune: #{out.strip.empty? ? 'nothing to delete' : out.strip}"
      else
        warn "[worker] orphan prune failed: #{err.strip}"
      end
    end
  end

  # Stop all project containers and VFS watchers cleanly when the worker shuts down.
  EM.add_shutdown_hook do
    VFS_WATCHERS.each_value(&:stop)
    PROJECT_CONTAINERS.each_value(&:stop)
    puts '[worker] all project containers and VFS watchers stopped'
  end

  # Seed the filesystem for every project from its configured root on startup.
  # FS_PROJECT_ID still works as a single-project override; FS_ROOT only
  # applies in that single-project mode (it makes no sense to point every
  # project at the same directory). Disable entirely with FS_SKIP_LOAD=1.
  unless ENV['FS_SKIP_LOAD'] == '1'
    EM.defer do
      begin
        projects_root = ENV.fetch('PROJECTS_ROOT', '/srv/projects')
        single_id     = ENV['FS_PROJECT_ID']
        fs_root_env   = ENV['FS_ROOT'].presence

        project_ids =
          if single_id
            [Integer(single_id)]
          else
            Project.pluck(:id)
          end

        project_ids.each do |project_id|
          proj    = Project.find_by(id: project_id)
          next unless proj

          # Resolution order:
          #   1. project.project_setting.root_path
          #   2. FS_ROOT env (single-project mode only)
          #   3. PROJECTS_ROOT/<project_id>
          fs_root = File.expand_path(
            proj.project_setting&.root_path.presence ||
            (single_id ? fs_root_env : nil) ||
            File.join(projects_root, project_id.to_s)
          )
          FileUtils.mkdir_p(fs_root) rescue nil
          puts "[startup] Loading filesystem for project #{project_id} from #{fs_root}"
          stats = FsLoader.new(project_id: project_id, root_path: fs_root).load!
          puts "[startup] FS load complete (project #{project_id}) — " \
               "#{stats[:dirs]} dirs, #{stats[:files]} files, " \
               "#{stats[:existing]} skipped (already in DB)"

          # Start periodic flush (DB → disk) and inotify watcher (disk → DB)
          EM.next_tick do
            flusher = VfsFlusher.new(project_id: project_id, root_path: fs_root,
                                     suppress_set: VFS_FLUSH_SUPPRESS)
            VFS_FLUSHERS[project_id] = flusher
            EM.add_periodic_timer(VfsFlusher::POLL_INTERVAL) { flusher.flush! }

            watcher = VfsWatcher.new(project_id: project_id, root_path: fs_root,
                                     suppress_set: VFS_FLUSH_SUPPRESS)
            VFS_WATCHERS[project_id] = watcher
            watcher.start!(sessions_by_project: SESSIONS_BY_PROJECT,
                           broadcast_fn: method(:broadcast))
          end
        end
      rescue => e
        puts "[startup] FS load failed: #{e.class}: #{e.message}"
      end
    end
  end

  # Token-expiry sweep. A socket may not outlive its credential: the client is
  # expected to refresh in-band (system/reauth) before exp, but if it fails to,
  # we close the connection once the token lapses beyond a short grace window.
  # The grace absorbs clock skew and in-flight reauths. Closing triggers the
  # client's normal reconnect (which mints a fresh token), so a transient miss
  # self-heals; a truly expired upstream session ends up at the login screen.
  TOKEN_EXP_GRACE_SECONDS = 30
  EM.add_periodic_timer(15) do
    SESSIONS_BY_PROJECT.each_value do |sessions|
      sessions.dup.each do |s|
        next unless s.token_expired?(TOKEN_EXP_GRACE_SECONDS)
        puts "[token-sweep] closing expired session user=#{s.user_id} project=#{s.project_id}"
        send_msg(s.ws, 'system', 'token_expired', {})
        s.ws.close_connection_after_writing
      end
    end
  end

  EM::WebSocket.start(host: host, port: port) do |ws|
    session  = nil
    is_probe = false

    ws.onopen do |handshake|
      params = URI.decode_www_form(handshake.query_string || '').to_h
      probe  = (params['probe'] == 'true')

      if PENDING_AUTH.size >= MAX_PENDING_AUTH
        puts "[auth] max pending auth sessions (#{MAX_PENDING_AUTH}) reached; rejecting connection"
        ws.close_connection_after_writing
      elsif probe
        # Health-probe connection: the control plane only checks that the WS
        # upgrades (101). It never sends system/auth, so don't arm the auth
        # deadline and don't treat the absent proto/min_server as a mismatch.
        is_probe = true
        session  = Session.unauthenticated(ws)
      else
        # Wire-protocol handshake (advisory). proto/min_server remain in the
        # query string — wire versioning is ADR-019's concern, not ADR-023's.
        # No token rides the URI (ADR-023 hard cutover).
        client_proto      = params['proto'].to_i
        client_min_server = params['min_server'].to_i
        unless client_proto >= MIN_CLIENT && PROTOCOL >= client_min_server
          puts "[proto] version mismatch: client(proto=#{client_proto} min_server=#{client_min_server}) " \
               "server(proto=#{PROTOCOL} min_client=#{MIN_CLIENT}) — serving anyway (advisory)"
        end

        # A session is ALWAYS created unauthenticated. system/auth (the client's
        # first frame) is what promotes it.
        session = Session.unauthenticated(ws)
        PENDING_AUTH[ws.object_id] = session
        arm_auth_deadline(session)
      end
    end

    ws.onmessage do |msg|
      begin
        if is_probe
          # Probes may only send commands on PROBE_ALLOWED_COMMANDS (empty
          # today). They are never promotable to a real session.
          parsed = (JSON.parse(msg) rescue nil)
          allowed = parsed.is_a?(Hash) &&
                    PROBE_ALLOWED_COMMANDS.include?([parsed['cs'], parsed['cmd']])
          if allowed
            route(session, msg)
          else
            puts '[probe] frame not in probe allowlist; closing'
            ws.close_connection_after_writing
          end
        elsif session&.authenticated?
          route(session, msg)
        else
          # Unauthenticated (or a rejected connection where session is nil):
          # exactly one command is allowed — system/auth. Anything else closes;
          # retry belongs to the client's reconnect loop.
          parsed = (JSON.parse(msg) rescue nil)
          if session && parsed.is_a?(Hash) && parsed['cs'] == 'system' && parsed['cmd'] == 'auth'
            handle_auth(session, parsed['payload'] || {})
          else
            puts '[auth] frame before system/auth; closing'
            ws.close_connection_after_writing
          end
        end
      rescue => e
        puts "[route] error: #{e.class} #{e.message}\n#{e.backtrace.first(3).join("\n")}"
        send_msg(ws, 'system', 'error', { message: e.message })
      end
    end

    ws.onclose do
      cancel_auth_deadline(session) if session
      PENDING_AUTH.delete(ws.object_id)

      if session
        if session.authenticated?
          puts "Client disconnected: user=#{session.user_id}"

          # Drop any debug-stream subscription this session held
          DebugStream.unsubscribe(session)

          # Remove session from project tracking
          if SESSIONS_BY_PROJECT[session.project_id]
            SESSIONS_BY_PROJECT[session.project_id].delete(session)
          end

          session.cleanup
        else
          puts 'Client disconnected before authentication' unless is_probe
        end
        session = nil
      end
    end

    ws.onerror do |e|
      puts "WebSocket error: #{e.message}"
    end
  end
end
