#!/usr/bin/env ruby
# Carbide2 worker — EventMachine WebSocket server
# Handles: terminal (PTY), chat, fs (file read). Protocol: { cs, cmd, payload }
$stdout.sync = true
$stderr.sync = true
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
require_relative 'chat_room'
require_relative 'open_document'
require_relative 'project_container'
require_relative 'project_pod'
require_relative 'session'
require_relative 'ar_boot'
require_relative 'fs_store'
require_relative 'vfs_flusher'
require_relative 'vfs_watcher'
require_relative 'agent_tools'
require_relative 'agent_session'
require 'set'

WORKER_SECRET = ENV.fetch('WORKER_JWT_SECRET', 'replace_me')
ALGORITHM     = 'HS256'

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

def validate_token(token)
  payload, _ = JWT.decode(token, WORKER_SECRET, true, { algorithm: ALGORITHM })
  payload
rescue JWT::DecodeError => e
  puts "Invalid token: #{e}"
  nil
end

def send_msg(ws, cs, cmd, payload = {})
  ws.send({ cs: cs, cmd: cmd, payload: payload }.to_json) rescue nil
end

def broadcast(clients, cs, cmd, payload = {})
  msg  = { cs: cs, cmd: cmd, payload: payload }.to_json
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
PROJECT_CONTAINERS  = {}        # project_id (int)  => ProjectContainer
PROJECT_PODS        = {}        # project_id (int)  => ProjectPod
POD_REFCOUNTS       = Hash.new(0)  # project_id => live terminal count
SESSIONS_BY_PROJECT = {}        # project_id => [Session, ...]
VFS_FLUSH_SUPPRESS  = Set.new   # absolute paths being written by VfsFlusher
VFS_FLUSHERS        = {}        # project_id => VfsFlusher
VFS_WATCHERS        = {}        # project_id => VfsWatcher

# ---------------------------------------------------------------------------
# Message router
# ---------------------------------------------------------------------------
def route(session, msg_str)
  msg     = JSON.parse(msg_str)
  cs      = msg['cs']
  cmd     = msg['cmd']
  payload = msg['payload'] || {}

  case cs
  when 'term'
    handle_term(session, cmd, payload)
  when 'chat'
    handle_chat(session, cmd, payload)
  when 'fs'
    handle_fs(session, cmd, payload)
  when 'agent'
    handle_agent(session, cmd, payload)
  else
    send_msg(session.ws, 'system', 'error', { message: "unknown commandSet: #{cs}" })
  end
rescue JSON::ParserError
  send_msg(session.ws, 'system', 'error', { message: 'invalid json' })
end

def handle_term(session, cmd, payload)
  case cmd
  when 'create'
    begin
      # Create new terminal in current project — attach to (or start) the
      # project's persistent Docker container.
      terminal_id    = (TERMINALS.keys.map(&:to_i).max || 0) + 1
      requested_name = payload['name']
      puts "[handle_term] creating terminal #{terminal_id} for project #{session.project_id}"

      proj = Project.find_by(id: session.project_id)

      case ENV.fetch('CARBIDE_BACKEND', 'local')
      when 'kube'
        pod = PROJECT_PODS[session.project_id] ||= ProjectPod.new(session.project_id)
        pod.ensure_running!
        POD_REFCOUNTS[session.project_id] += 1
        term = TerminalInstance.new(
          terminal_id,
          project_id: session.project_id,
          cols: 80, rows: 24,
          name: requested_name,
          cmd:  pod.exec_cmd,
          cwd:  nil
        )
      when 'docker'
        container = PROJECT_CONTAINERS[session.project_id] ||=
          ProjectContainer.new(session.project_id, root_path: proj&.project_setting&.root_path.presence)
        container.ensure_running!
        term = TerminalInstance.new(
          terminal_id,
          project_id: session.project_id,
          cols: 80, rows: 24,
          name: requested_name,
          cmd:  container.exec_cmd,
          cwd:  nil   # cwd is handled by the container's -w flag
        )
      else
        cwd  = proj&.project_setting&.root_path.presence || PROJECT_ROOT
        term = TerminalInstance.new(
          terminal_id,
          project_id: session.project_id,
          cols: 80, rows: 24,
          name: requested_name,
          cwd:  cwd
        )
      end
      TERMINALS[terminal_id] = term
      term.on_exit { |tid| on_terminal_exit(tid, session.project_id) }
      puts "[handle_term] sending 'created' to client"
      send_msg(session.ws, 'term', 'created', { terminal_id: terminal_id, name: term.name })
      puts "[handle_term] broadcasting terminal list to project"
      broadcast_terminals_to_project(session.project_id)
      puts "[handle_term] done"
    rescue => e
      puts "[handle_term] ERROR: #{e.class} #{e.message}\n#{e.backtrace.first(5).join("\n")}"
      send_msg(session.ws, 'system', 'error', { message: "Failed to create terminal: #{e.message}" })
    end

  when 'join'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id
      term.add_client(session.ws)
      session.terminals << tid unless session.terminals.include?(tid)
      send_msg(session.ws, 'term', 'joined', { terminal_id: tid, rows: term.rows, cols: term.cols })
    else
      # Terminal is gone (shell exited, destroyed, or never existed). Reply
      # with 'exit' rather than 'system/error' so the client treats it as a
      # closed session ([session ended] in the pane) instead of surfacing a
      # red banner — useful for tabs that were left open after the shell
      # exited and are clicked again later.
      send_msg(session.ws, 'term', 'exit', { terminal_id: tid, code: nil, reason: 'gone' })
    end

  when 'input'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    term.write_input(payload['data'].to_s) if term

  when 'resize'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term
      term.apply_winsize(payload['rows'], payload['cols'])
      broadcast(term.clients.values, 'term', 'resized', {
        terminal_id: tid,
        rows: term.rows,
        cols: term.cols,
        user_id: session.user_id
      })
    end

  when 'rename'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id && term.rename(payload['name'])
      send_msg(session.ws, 'term', 'renamed', { terminal_id: tid, name: term.name })
      broadcast_terminals_to_project(session.project_id)
    else
      send_msg(session.ws, 'system', 'error', { message: "terminal #{tid} rename failed" })
    end

  when 'leave'
    tid = payload['terminal_id'].to_i
    TERMINALS[tid]&.remove_client(session.ws)
    session.terminals.delete(tid)

  when 'destroy'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id
      puts "[handle_term] destroy requested for tid=#{tid}"
      term.destroy!
      # destroy! triggers the PTY reader's EOF path, which fires on_exit and
      # broadcasts 'term' 'exit'. As a belt-and-braces fallback in case the
      # reader thread is wedged, prune + broadcast here too.
      EM.add_timer(0.5) { on_terminal_exit(tid, session.project_id) if TERMINALS.key?(tid) }
    else
      send_msg(session.ws, 'system', 'error', { message: "terminal #{tid} not found or access denied" })
    end
  end
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

def handle_chat(session, cmd, payload)
  case cmd
  when 'join'
    cid = Integer(payload['channel_id']) rescue nil
    return send_msg(session.ws, 'system', 'error', { message: 'chat join requires channel_id' }) unless cid
    rid  = "project_#{session.project_id}_channel_#{cid}"
    room = CHAT_ROOMS[rid] ||= ChatRoom.new(rid, channel_id: cid)
    already_joined = room.member?(session.ws)
    room.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.rooms << rid unless session.rooms.include?(rid)
    send_msg(session.ws, 'chat', 'joined', { channel_id: cid, room_id: rid, already_joined: already_joined })

  when 'message'
    cid = Integer(payload['channel_id']) rescue nil
    return send_msg(session.ws, 'system', 'error', { message: 'chat message requires channel_id' }) unless cid
    rid  = "project_#{session.project_id}_channel_#{cid}"
    room = CHAT_ROOMS[rid]
    unless room && room.member?(session.ws)
      return send_msg(session.ws, 'system', 'error', { message: 'not joined to channel' })
    end
    room.handle_message(session.ws, payload['text'].to_s)

  when 'typing'
    cid = Integer(payload['channel_id']) rescue nil
    return unless cid
    rid  = "project_#{session.project_id}_channel_#{cid}"
    room = CHAT_ROOMS[rid]
    room&.handle_typing(session.ws)

  when 'leave'
    cid = Integer(payload['channel_id']) rescue nil
    return send_msg(session.ws, 'system', 'error', { message: 'chat leave requires channel_id' }) unless cid
    rid = "project_#{session.project_id}_channel_#{cid}"
    room = CHAT_ROOMS[rid]
    unless room && room.member?(session.ws)
      return send_msg(session.ws, 'system', 'error', { message: 'not joined to channel' })
    end
    room.remove_client(session.ws)
    session.rooms.delete(rid)
    send_msg(session.ws, 'chat', 'left', { channel_id: cid, room_id: rid })
  end
end

# ---------------------------------------------------------------------------
# Filesystem handler — database-backed via FsStore
# ---------------------------------------------------------------------------
def handle_fs(session, cmd, payload)
  FsStore.handle(
    session, cmd, payload,
    SESSIONS_BY_PROJECT,
    method(:send_msg),
    method(:broadcast)
  )
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
# Agent — LLM tool-call loop. Per-conversation state lives in AgentSession.
# Commands:
#   agent/list           -> agent/list  { agents: [{slug, name, role, ...}] }
#   agent/ask  { agent_slug, conversation_id?, message } ->
#     emits agent/tool_call, agent/tool_result, agent/stream*, agent/done
# Heavy-lifting (HTTP to model server) runs in EM.defer so the reactor stays
# free for other clients.
# ---------------------------------------------------------------------------
def handle_agent(session, cmd, payload)
  case cmd
  when 'list'
    agents = Agent.enabled.order(:role, :name).map do |a|
      {
        slug:        a.slug,
        name:        a.name,
        description: a.description,
        role:        a.role,
        model:       a.model,
        tools:       a.allowed_tool_slugs,
      }
    end
    send_msg(session.ws, 'agent', 'list', { agents: agents })

  when 'ask'
    slug = payload['agent_slug'].to_s
    msg  = payload['message'].to_s
    conv = payload['conversation_id'].to_s
    conv = SecureRandom.uuid if conv.empty?

    if msg.empty?
      send_msg(session.ws, 'system', 'error', { message: 'agent/ask: message is required' })
      return
    end

    agent = Agent.enabled.find_by(slug: slug)
    unless agent
      send_msg(session.ws, 'system', 'error', { message: "agent/ask: no enabled agent with slug=#{slug}" })
      return
    end

    sess = AgentSession.find(conv) ||
           AgentSession.start(session: session, agent: agent,
                              project_id: session.project_id,
                              conversation_id: conv)
    # Ack immediately so the UI can show the conversation id.
    send_msg(session.ws, 'agent', 'started',
             { conversation_id: conv, agent: agent.slug })

    EM.defer { sess.ask(msg) }

  else
    send_msg(session.ws, 'system', 'error', { message: "unknown agent cmd: #{cmd}" })
  end
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
EM.run do
  host = ENV.fetch('WORKER_HOST', '0.0.0.0')
  port = ENV.fetch('WORKER_PORT', '8080').to_i

  puts "Carbide2 worker starting on #{host}:#{port}"
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

  EM::WebSocket.start(host: host, port: port) do |ws|
    session = nil

    ws.onopen do |handshake|
      params = URI.decode_www_form(handshake.query_string || '').to_h
      token  = params['token']

      payload = validate_token(token)
      if payload
        session = Session.new(ws, payload)
        
        # Track session by project for terminal broadcasts
        SESSIONS_BY_PROJECT[session.project_id] ||= []
        SESSIONS_BY_PROJECT[session.project_id] << session
        
        send_msg(ws, 'system', 'connected', {
          user_id:    session.user_id,
          project_id: session.project_id
        })
        
        # Send initial terminal list
        terminals = get_project_terminals(session.project_id)
        send_msg(ws, 'term', 'list', { project_id: session.project_id, terminals: terminals })
        
        puts "Client connected: user=#{session.user_id} project=#{session.project_id}"
      else
        send_msg(ws, 'system', 'error', { message: 'invalid or missing token' })
        ws.close_connection_after_writing
      end
    end

    ws.onmessage do |msg|
      begin
        route(session, msg) if session
      rescue => e
        puts "[route] error: #{e.class} #{e.message}\n#{e.backtrace.first(3).join("\n")}"
        send_msg(ws, 'system', 'error', { message: e.message })
      end
    end

    ws.onclose do
      if session
        puts "Client disconnected: user=#{session.user_id}"
        
        # Remove session from project tracking
        if SESSIONS_BY_PROJECT[session.project_id]
          SESSIONS_BY_PROJECT[session.project_id].delete(session)
        end
        
        session.cleanup
        session = nil
      end
    end

    ws.onerror do |e|
      puts "WebSocket error: #{e.message}"
    end
  end
end
