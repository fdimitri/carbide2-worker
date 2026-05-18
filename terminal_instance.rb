# TerminalInstance owns one PTY and broadcasts output to subscribed sockets.
class TerminalInstance
  attr_reader :terminal_id, :project_id, :master, :slave, :pid, :clients, :cols, :rows, :name

  def initialize(terminal_id, project_id:, cols: 80, rows: 24, cmd: '/bin/bash', name: nil)
    @terminal_id = terminal_id
    @project_id  = project_id
    @name        = name.to_s.strip
    @name        = "terminal-#{terminal_id}" if @name.empty?
    # Track subscribers by object identity so separate browser windows are never collapsed.
    @clients     = {}
    @cols        = cols
    @rows        = rows
    @scrollback  = +''
    @scrollback_limit = 200_000
    begin
      @master, @slave, @pid = PTY.spawn(cmd)
    rescue => e
      puts "[PTY] ERROR spawning #{cmd}: #{e.class} #{e.message}"
      raise e
    end
    apply_winsize(@rows, @cols)
    start_reader
  end

  def start_reader
    Thread.new do
      puts "[PTY:#{@terminal_id}] reader thread started, reading from master"
      loop do
        data = @master.readpartial(4096)
        puts "[PTY:#{@terminal_id}] read #{data.bytes.size} bytes: #{data.inspect[0..50]}"
        append_scrollback(data)
        EM.next_tick do
          broadcast(@clients.values, 'term', 'output', { terminal_id: @terminal_id, data: data })
        end
      end
    rescue EOFError, Errno::EIO => e
      puts "[PTY:#{@terminal_id}] reader EOF: #{e.class}"
      EM.next_tick do
        broadcast(@clients.values, 'term', 'exit', { terminal_id: @terminal_id, code: 0 })
      end
    rescue => e
      puts "[PTY:#{@terminal_id}] reader error: #{e.class} #{e.message}"
    end
  end

  def write_input(data)
    puts "[PTY:#{@terminal_id}] writing #{data.bytes.size} bytes: #{data.inspect[0..50]}"
    @slave.write(data)
  rescue Errno::EIO, Errno::EPIPE, IOError => e
    puts "[PTY:#{@terminal_id}] write failed: #{e.class} #{e.message}"
  end

  def apply_winsize(rows, cols)
    @rows = rows.to_i
    @cols = cols.to_i
    @slave.winsize = [@rows, @cols] if @slave.respond_to?(:winsize=)
  end

  def add_client(ws)
    @clients[ws.object_id] = ws
    replay_to_client(ws)
  end

  def remove_client(ws)
    @clients.delete(ws.object_id)
  end

  def to_list_entry
    {
      id: @terminal_id,
      name: @name,
      status: 'active',
      cols: @cols,
      rows: @rows
    }
  end

  def rename(new_name)
    candidate = new_name.to_s.strip
    return false if candidate.empty?
    @name = candidate
    true
  end

  def append_scrollback(data)
    @scrollback << data
    if @scrollback.bytesize > @scrollback_limit
      @scrollback = @scrollback.byteslice(-@scrollback_limit, @scrollback_limit) || +''
    end
  end

  def replay_to_client(ws)
    return if @scrollback.empty?
    send_msg(ws, 'term', 'output', { terminal_id: @terminal_id, data: @scrollback })
  rescue => e
    puts "[PTY:#{@terminal_id}] replay failed: #{e.class} #{e.message}"
  end
end
