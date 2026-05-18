#!/usr/bin/env ruby
# Carbide2 worker — EventMachine WebSocket server
# Handles: terminal (PTY), chat. Protocol: { cs, cmd, payload }
require 'eventmachine'
require 'em-websocket'
require 'json'
require 'jwt'
require 'pty'
require 'io/console'
require 'uri'

WORKER_SECRET = ENV.fetch('WORKER_JWT_SECRET', 'replace_me')
ALGORITHM     = 'HS256'

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
  msg = { cs: cs, cmd: cmd, payload: payload }.to_json
  clients.each { |ws| ws.send(msg) rescue nil }
end

# ---------------------------------------------------------------------------
# TerminalInstance — owns one PTY, broadcasts output to subscribed sockets
# ---------------------------------------------------------------------------
class TerminalInstance
  attr_reader :terminal_id, :master, :slave, :pid, :clients, :cols, :rows

  def initialize(terminal_id, cols: 80, rows: 24, cmd: '/bin/bash')
    @terminal_id = terminal_id
    @clients     = []
    @cols        = cols
    @rows        = rows
    @master, @slave, @pid = PTY.spawn(cmd)
    apply_winsize(@rows, @cols)
    start_reader
  end

  def start_reader
    Thread.new do
      loop do
        data = @master.readpartial(4096)
        broadcast(@clients, 'term', 'output', { terminal_id: @terminal_id, data: data })
      end
    rescue EOFError, Errno::EIO
      broadcast(@clients, 'term', 'exit', { terminal_id: @terminal_id, code: 0 })
    end
  end

  def write_input(data)
    @master.write(data)
  end

  def apply_winsize(rows, cols)
    @rows = rows.to_i
    @cols = cols.to_i
    @slave.winsize = [@rows, @cols] if @slave.respond_to?(:winsize=)
    Process.kill('SIGWINCH', @pid) rescue nil
  end

  def add_client(ws)
    @clients << ws unless @clients.include?(ws)
  end

  def remove_client(ws)
    @clients.delete(ws)
  end
end

# ---------------------------------------------------------------------------
# ChatRoom — IRC-style room, broadcasts to all members
# ---------------------------------------------------------------------------
class ChatRoom
  attr_reader :room_id, :clients

  def initialize(room_id)
    @room_id = room_id
    @clients = {}  # ws => { user_id:, name: }
  end

  def add_client(ws, user_id:, name:)
    @clients[ws] = { user_id: user_id, name: name }
    broadcast_to_others(ws, 'user_join', { room_id: @room_id, user_id: user_id, name: name })
    send_msg(ws, 'chat', 'user_list', { room_id: @room_id, users: user_list })
  end

  def remove_client(ws)
    info = @clients.delete(ws)
    return unless info
    broadcast_all('user_leave', { room_id: @room_id, user_id: info[:user_id], name: info[:name] })
  end

  def handle_message(ws, text)
    info = @clients[ws]
    return unless info
    broadcast_all('message', {
      room_id:   @room_id,
      user_id:   info[:user_id],
      name:      info[:name],
      text:      text,
      timestamp: Time.now.utc.iso8601
    })
  end

  def user_list
    @clients.values.map { |c| { user_id: c[:user_id], name: c[:name] } }
  end

  private

  def broadcast_all(cmd, payload)
    broadcast(@clients.keys, 'chat', cmd, payload)
  end

  def broadcast_to_others(ws, cmd, payload)
    broadcast(@clients.keys.reject { |s| s == ws }, 'chat', cmd, payload)
  end
end

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
TERMINALS  = {}  # terminal_id (int) => TerminalInstance
CHAT_ROOMS = {}  # room_id (string)  => ChatRoom

# ---------------------------------------------------------------------------
# Per-connection session state
# ---------------------------------------------------------------------------
class Session
  attr_reader :ws, :user_id, :name, :project_id, :terminals, :rooms

  def initialize(ws, payload)
    @ws         = ws
    @user_id    = payload['user']
    @name       = payload['name'] || "user_#{@user_id}"
    @project_id = payload['project']
    @terminals  = []  # terminal_ids joined
    @rooms      = []  # room_ids joined
  end

  def cleanup
    @terminals.each do |tid|
      TERMINALS[tid]&.remove_client(@ws)
    end
    @rooms.each do |rid|
      CHAT_ROOMS[rid]&.remove_client(@ws)
    end
  end
end

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
  else
    send_msg(session.ws, 'system', 'error', { message: "unknown commandSet: #{cs}" })
  end
rescue JSON::ParserError
  send_msg(session.ws, 'system', 'error', { message: 'invalid json' })
end

def handle_term(session, cmd, payload)
  case cmd
  when 'join'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    unless term
      send_msg(session.ws, 'term', 'error', { message: "terminal #{tid} not found" })
      return
    end
    term.add_client(session.ws)
    session.terminals << tid unless session.terminals.include?(tid)
    send_msg(session.ws, 'term', 'joined', { terminal_id: tid })

  when 'input'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    term.write_input(payload['data'].to_s) if term

  when 'resize'
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    term.apply_winsize(payload['rows'], payload['cols']) if term

  when 'leave'
    tid = payload['terminal_id'].to_i
    TERMINALS[tid]&.remove_client(session.ws)
    session.terminals.delete(tid)
  end
end

def handle_chat(session, cmd, payload)
  case cmd
  when 'join'
    rid  = "project_#{session.project_id}"
    room = CHAT_ROOMS[rid] ||= ChatRoom.new(rid)
    room.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.rooms << rid unless session.rooms.include?(rid)

  when 'message'
    rid  = "project_#{session.project_id}"
    room = CHAT_ROOMS[rid]
    room.handle_message(session.ws, payload['text'].to_s) if room

  when 'leave'
    rid = "project_#{session.project_id}"
    CHAT_ROOMS[rid]&.remove_client(session.ws)
    session.rooms.delete(rid)
  end
end

# ---------------------------------------------------------------------------
# HTTP API endpoint for Rails to create terminal instances
# Used by POST /api/projects/:id/terminals
# ---------------------------------------------------------------------------
def create_terminal(terminal_id, cols: 80, rows: 24)
  TERMINALS[terminal_id] ||= TerminalInstance.new(terminal_id, cols: cols, rows: rows)
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
EM.run do
  host = ENV.fetch('WORKER_HOST', '0.0.0.0')
  port = ENV.fetch('WORKER_PORT', '8080').to_i

  puts "Carbide2 worker starting on #{host}:#{port}"

  EM::WebSocket.start(host: host, port: port) do |ws|
    session = nil

    ws.onopen do |handshake|
      params = URI.decode_www_form(handshake.query_string || '').to_h
      token  = params['token']

      payload = validate_token(token)
      if payload
        session = Session.new(ws, payload)
        send_msg(ws, 'system', 'connected', {
          user_id:    session.user_id,
          project_id: session.project_id
        })
        puts "Client connected: user=#{session.user_id} project=#{session.project_id}"
      else
        send_msg(ws, 'system', 'error', { message: 'invalid or missing token' })
        ws.close_connection_after_writing
      end
    end

    ws.onmessage do |msg|
      route(session, msg) if session
    end

    ws.onclose do
      if session
        puts "Client disconnected: user=#{session.user_id}"
        session.cleanup
        session = nil
      end
    end

    ws.onerror do |e|
      puts "WebSocket error: #{e.message}"
    end
  end
end
