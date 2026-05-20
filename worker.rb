#!/usr/bin/env ruby
# Carbide2 worker — EventMachine WebSocket server
# Handles: terminal (PTY), chat, fs (file read). Protocol: { cs, cmd, payload }
$stdout.sync = true
$stderr.sync = true
require 'eventmachine'
require 'em-websocket'
require 'json'
require 'jwt'
require 'pty'
require 'io/console'
require 'uri'
require_relative 'terminal_instance'
require_relative 'chat_room'
require_relative 'open_document'
require_relative 'session'
require_relative 'ar_boot'
require_relative 'fs_store'

WORKER_SECRET = ENV.fetch('WORKER_JWT_SECRET', 'replace_me')
ALGORITHM     = 'HS256'
PROJECT_ROOT  = File.expand_path(ENV.fetch('PROJECT_ROOT', Dir.pwd)).freeze

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
TERMINALS       = {}  # terminal_id (int) => TerminalInstance
CHAT_ROOMS      = {}  # room_id (string)  => ChatRoom
OPEN_DOCUMENTS  = {}  # "#{project_id}:#{path}" => OpenDocument
SESSIONS_BY_PROJECT = {}  # project_id => [Session, ...]

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
      # Create new terminal in current project
      terminal_id = (TERMINALS.keys.map(&:to_i).max || 0) + 1
      requested_name = payload['name']
      puts "[handle_term] creating terminal #{terminal_id} for project #{session.project_id}"
      term = TerminalInstance.new(terminal_id, project_id: session.project_id, cols: 80, rows: 24, name: requested_name)
      TERMINALS[terminal_id] = term
      puts "[handle_term] sending 'created' to client"
      send_msg(session.ws, 'term', 'created', { terminal_id: terminal_id })
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
      send_msg(session.ws, 'system', 'error', { message: "terminal #{tid} not found or access denied" })
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
# Main
# ---------------------------------------------------------------------------
EM.run do
  host = ENV.fetch('WORKER_HOST', '0.0.0.0')
  port = ENV.fetch('WORKER_PORT', '8080').to_i

  puts "Carbide2 worker starting on #{host}:#{port}"

  # Seed the filesystem for project 1 from the default directory on startup.
  # Override with FS_ROOT env var; disable entirely with FS_SKIP_LOAD=1.
  unless ENV['FS_SKIP_LOAD'] == '1'
    EM.defer do
      begin
        project_id = Integer(ENV.fetch('FS_PROJECT_ID', '1'))
        fs_root    = File.expand_path(ENV.fetch('FS_ROOT', '~/repos/carbide2-server'))
        puts "[startup] Loading filesystem for project #{project_id} from #{fs_root}"
        stats = FsLoader.new(project_id: project_id, root_path: fs_root).load!
        puts "[startup] FS load complete — #{stats[:dirs]} dirs, #{stats[:files]} files, #{stats[:existing]} skipped (already in DB)"
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
