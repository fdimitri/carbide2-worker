#!/usr/bin/env ruby
require 'eventmachine'
require 'em-websocket'
require 'json'
require 'jwt'
require 'pty'
require 'io/console'

WORKER_SECRET = ENV.fetch('WORKER_JWT_SECRET', 'replace_me')
ALGORITHM = 'HS256'

class TerminalInstance
  attr_reader :terminal_id, :master, :slave, :pid, :clients, :cols, :rows

  def initialize(terminal_id, cols: 80, rows: 24, cmd: '/bin/bash')
    @terminal_id = terminal_id
    @master, @slave, @pid = PTY.spawn(cmd)
    @clients = []
    @cols = cols
    @rows = rows
    apply_winsize(@slave, rows, cols)
    start_reader
  end

  def start_reader
    Thread.new do
      begin
        loop do
          data = @master.readpartial(1024)
          broadcast({ type: 'output', terminal_id: terminal_id, data: data })
        end
      rescue EOFError
        broadcast({ type: 'exit', terminal_id: terminal_id })
      end
    end
  end

  def broadcast(msg)
    payload = msg.to_json
    @clients.each do |ws|
      ws.send payload rescue nil
    end
  end

  def write_input(data)
    @master.write data
  end

  def apply_winsize(io, rows, cols)
    if io.respond_to?(:winsize=)
      io.winsize = [rows, cols]
      Process.kill('SIGWINCH', @pid)
      @rows = rows
      @cols = cols
    end
  end

  def add_client(ws)
    @clients << ws
  end

  def remove_client(ws)
    @clients.delete(ws)
  end
end

TERMINALS = {}

def validate_token(token)
  begin
    payload, _ = JWT.decode(token, WORKER_SECRET, true, { algorithm: ALGORITHM })
    payload
  rescue JWT::DecodeError => e
    puts "Invalid token: #{e}"
    nil
  end

EM.run do
  host = '0.0.0.0'
  port = 8080

  puts "Starting worker websocket server on #{host}:#{port}"

  EM::WebSocket.start(host: host, port: port) do |ws|
    ws.onopen do |handshake|
      # expect token in query string: ws://host:port/?token=...
      params = URI.decode_www_form(handshake.query_string || '').to_h
      token = params['token']

      payload = validate_token(token)
      if payload && payload['scopes']&.include?('terminal:connect')
        terminal_id = payload['terminal']
        cols = payload['cols'] || 80
        rows = payload['rows'] || 24

        term = TERMINALS[terminal_id] ||= TerminalInstance.new(terminal_id, cols: cols, rows: rows)
        term.add_client(ws)

        ws.send({ type: 'connected', terminal_id: terminal_id }.to_json)
      else
        ws.send({ type: 'error', message: 'invalid token' }.to_json)
        ws.close_connection_after_writing
      end
    end

    ws.onmessage do |msg|
      begin
        data = JSON.parse(msg)
        case data['type']
        when 'input'
          term = TERMINALS[data['terminal_id']]
          term.write_input(data['data']) if term
        when 'resize'
          term = TERMINALS[data['terminal_id']]
          if term
            term.apply_winsize(term.slave, data['rows'].to_i, data['cols'].to_i)
            term.broadcast({ type: 'resize', terminal_id: term.terminal_id, cols: term.cols, rows: term.rows })
          end
        else
          # ignore
        end
      rescue JSON::ParserError
        ws.send({ type: 'error', message: 'invalid json' }.to_json)
      end
    end

    ws.onclose do
      TERMINALS.each_value do |term|
        term.remove_client(ws)
      end
    end
  end
end
