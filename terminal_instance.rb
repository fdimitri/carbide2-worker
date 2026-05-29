# TerminalInstance owns one PTY and broadcasts output to subscribed sockets.
#
# Agent integration
# -----------------
# A terminal can be flagged @agent_accessible (toggleable at runtime).
# When an agent calls shell_exec(terminal_id), the AgentSession invokes
# claim_for_agent!(timeout_s) which:
#   - rejects if @agent_busy is already set
#   - sets @agent_busy true with an EM-driven auto-release after timeout_s
#   - returns a "tap" — a callable that yields each chunk of PTY output to
#     a buffer the caller controls, until release.
# While @agent_busy is true, write_input from regular clients is dropped
# (their UI shows a lock badge). The agent writes via agent_write(data),
# which bypasses the gate. The auto-release timeout is mandatory to
# prevent a wedged agent from locking the user out forever; the upper
# bound is project_settings.agent_shell_busy_timeout_s.
class TerminalInstance
  attr_reader :terminal_id, :project_id, :master, :slave, :pid, :clients, :cols, :rows, :name
  attr_reader :agent_accessible, :agent_busy, :agent_busy_until_ms

  def initialize(terminal_id, project_id:, cols: 80, rows: 24, cmd: '/bin/bash',
                 name: nil, cwd: nil, agent_accessible: false)
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
    @agent_accessible    = !!agent_accessible
    @agent_busy          = false
    @agent_busy_until_ms = nil
    @agent_output_taps   = []   # array of procs called with each raw chunk
    @agent_release_timer = nil  # EM timer object for auto-release
    @prompt_marker_installed = false
    @state_change_cb     = nil  # set by worker for badge updates
    begin
      spawn_cmd = build_spawn_cmd(cmd, cwd)
      @master, @slave, @pid = PTY.spawn(spawn_cmd)
    rescue => e
      puts "[PTY] ERROR spawning #{cmd}: #{e.class} #{e.message}"
      raise e
    end
    apply_winsize(@rows, @cols)
    start_reader
    # If this terminal was created already-agent-accessible, install the
    # OSC marker after a short delay so bash has time to print its first
    # prompt and consume our export line on the same prompt cycle (sending
    # too early can race with shell initialization and land mid-PS1).
    if @agent_accessible
      EM.add_timer(0.5) { install_agent_prompt_marker! }
    end
  end

  def start_reader
    Thread.new do
      puts "[PTY:#{@terminal_id}] reader thread started, reading from master"
      loop do
        data = @master.readpartial(4096)
        puts "[PTY:#{@terminal_id}] read #{data.bytes.size} bytes: #{data.inspect[0..50]}"
        append_scrollback(data)
        EM.next_tick do
          # Broadcast to UI clients as always — agent runs are visible live.
          dead = broadcast(@clients.values, 'term', 'output', { terminal_id: @terminal_id, data: data })
          dead.each { |ws| @clients.delete(ws.object_id) }
          # Tee to any agent output taps. Errors in a tap must not kill the
          # reader; the tap is in untrusted territory (closures over agent
          # state) so we isolate it.
          @agent_output_taps.each do |tap|
            begin
              tap.call(data)
            rescue => e
              puts "[PTY:#{@terminal_id}] agent tap raised: #{e.class} #{e.message}"
            end
          end
        end
      end
    rescue EOFError, Errno::EIO => e
      puts "[PTY:#{@terminal_id}] reader EOF: #{e.class}"
      EM.next_tick do
        dead = broadcast(@clients.values, 'term', 'exit', { terminal_id: @terminal_id, code: 0 })
        dead.each { |ws| @clients.delete(ws.object_id) }
        # Wake any agent currently blocked on this terminal so the tool call
        # returns instead of hanging until timeout.
        release_from_agent!(reason: 'terminal exited')
        @on_exit&.call(@terminal_id)
      end
    rescue => e
      puts "[PTY:#{@terminal_id}] reader error: #{e.class} #{e.message}"
    end
  end

  # User-side input. Dropped silently while an agent owns the terminal —
  # the client UI shows a lock overlay so the user understands why their
  # keystrokes vanish. (Silent rather than echoing back an error frame:
  # the user might be typing fast and a dozen "denied" frames per second
  # would be worse than a still terminal.)
  def write_input(data)
    if @agent_busy
      puts "[PTY:#{@terminal_id}] user input dropped (agent_busy)"
      return
    end
    puts "[PTY:#{@terminal_id}] writing #{data.bytes.size} bytes: #{data.inspect[0..50]}"
    @slave.write(data)
  rescue Errno::EIO, Errno::EPIPE, IOError => e
    puts "[PTY:#{@terminal_id}] write failed: #{e.class} #{e.message}"
  end

  # Agent-side input. Bypasses the busy gate (the agent IS the lock holder).
  # Caller MUST have first claim_for_agent!()'d the terminal.
  def agent_write(data)
    raise 'terminal not claimed by agent' unless @agent_busy
    @slave.write(data)
  rescue Errno::EIO, Errno::EPIPE, IOError => e
    puts "[PTY:#{@terminal_id}] agent_write failed: #{e.class} #{e.message}"
  end

  # Run a block with the slave PTY's ECHO disabled so anything we write via
  # agent_write isn't echoed back to xterm character-by-character. Used by
  # the prompt-marker installer to hide a one-time bash export line; no
  # longer needed by shell_exec itself now that we use OSC 633 markers.
  def with_agent_echo_off
    require 'io/console'
    @slave.noecho { yield }
  rescue NotImplementedError, Errno::ENOTTY => e
    # Fallback for environments where the slave doesn't support termios
    # (shouldn't happen for a real PTY, but don't crash shell_exec if so).
    puts "[PTY:#{@terminal_id}] noecho unavailable: #{e.class}"
    yield
  end

  # Install a bash PROMPT_COMMAND that emits a private OSC 633 ; D ; <exit>
  # marker after every command. This is the same shell-integration trick
  # VS Code's terminal uses to know when commands finish and what their
  # exit code was — xterm renders OSC sequences as nothing, so the user
  # never sees the marker, but our PTY tap reads raw bytes and can parse
  # the exit code without typing a sentinel printf into the user's shell.
  #
  # Idempotent: subsequent calls re-install identically (PROMPT_COMMAND
  # gets prefixed once per call, but the marker only contributes one byte
  # sequence per prompt anyway). For safety against double-prefix we guard
  # via @prompt_marker_installed.
  #
  # Visible side-effect: the export line itself is invisible (echo off)
  # and the trailing \e[1A\r\e[2K wipes the otherwise-blank line bash
  # leaves behind, so the user sees no disruption — just their prompt.
  def install_agent_prompt_marker!
    return if @prompt_marker_installed
    line = <<~BASH.gsub("\n", '')
      __c2_rc=$?;
      export PROMPT_COMMAND='__c2_rc=$?; printf "\\e]633;D;%s\\a" "$__c2_rc"; '"${PROMPT_COMMAND:+$PROMPT_COMMAND}";
      printf '\\e[1A\\r\\e[2K';
    BASH
    begin
      require 'io/console'
      @slave.noecho { @slave.write(line + "\n") }
      @prompt_marker_installed = true
      puts "[PTY:#{@terminal_id}] installed OSC 633 prompt marker"
    rescue => e
      puts "[PTY:#{@terminal_id}] prompt-marker install failed: #{e.class} #{e.message}"
    end
  end

  def agent_prompt_marker_installed?
    @prompt_marker_installed
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

  def on_exit(&block)
    @on_exit = block
  end

  # Worker sets this so toggling agent_accessible / agent_busy can rebroadcast
  # the project's terminal list (badge / lock updates land in the UI).
  def on_state_change(&block)
    @state_change_cb = block
  end

  # ─── Agent gate ──────────────────────────────────────────────────────────

  # Toggle whether this terminal may be claimed by an agent. Returns the
  # new value. If false-ing during a busy window, also releases the lock
  # (so the user can recover input by un-toggling). Enabling lazily
  # installs the OSC 633 prompt marker so shell_exec can parse exit codes
  # without typing a sentinel into the user's shell.
  def set_agent_accessible(flag)
    @agent_accessible = !!flag
    release_from_agent!(reason: 'agent access revoked') if @agent_busy && !@agent_accessible
    install_agent_prompt_marker! if @agent_accessible && !@prompt_marker_installed
    fire_state_change
    @agent_accessible
  end

  # Try to claim the terminal for an agent shell_exec call. Returns a tap
  # callable on success, or nil if the claim was rejected. The tap is a
  # Proc that accepts a block; the block runs on EM thread for each chunk
  # of PTY output until release_from_agent! is called.
  def claim_for_agent!(timeout_s:)
    return nil unless @agent_accessible
    return nil if @agent_busy
    @agent_busy          = true
    @agent_busy_until_ms = (Time.now.to_f + timeout_s) * 1000
    @agent_release_timer = EM.add_timer(timeout_s) {
      if @agent_busy
        puts "[PTY:#{@terminal_id}] agent_busy auto-release after #{timeout_s}s"
        release_from_agent!(reason: 'timeout')
      end
    }
    fire_state_change
    proc do |&block|
      tap = ->(data) { block.call(data) }
      @agent_output_taps << tap
      tap
    end
  end

  # Detach a previously installed tap. Safe to call multiple times.
  def detach_tap(tap)
    @agent_output_taps.delete(tap)
  end

  def release_from_agent!(reason: 'released')
    return unless @agent_busy
    @agent_busy          = false
    @agent_busy_until_ms = nil
    @agent_output_taps.clear
    if @agent_release_timer
      EM.cancel_timer(@agent_release_timer) rescue nil
      @agent_release_timer = nil
    end
    puts "[PTY:#{@terminal_id}] released (#{reason})"
    fire_state_change
  end

  def to_list_entry
    {
      id:               @terminal_id,
      name:             @name,
      status:           'active',
      cols:             @cols,
      rows:             @rows,
      agent_accessible: @agent_accessible,
      agent_busy:       @agent_busy,
      agent_busy_until_ms: @agent_busy_until_ms,
    }
  end

  def rename(new_name)
    candidate = new_name.to_s.strip
    return false if candidate.empty?
    @name = candidate
    true
  end

  # Force-kill the underlying process and close the PTY. Safe to call multiple
  # times; the reader thread's EOF path will broadcast 'term' 'exit'.
  def destroy!
    begin
      Process.kill('TERM', @pid) if @pid
      sleep 0.05
      Process.kill('KILL', @pid) if @pid && process_alive?(@pid)
    rescue Errno::ESRCH, Errno::EPERM
      # already gone
    end
    @master.close rescue nil
    @slave.close  rescue nil
  end

  private

  def fire_state_change
    return unless @state_change_cb
    EM.next_tick { @state_change_cb.call(self) rescue nil }
  end

  def process_alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH, Errno::EPERM
    false
  end

  # Build the shell command. If cwd is given, cd into it before exec-ing the shell.
  def build_spawn_cmd(cmd, cwd)
    return cmd if cwd.nil? || cwd.strip.empty?
    safe_dir = cwd.strip.gsub("'", "'\\\\''")
    "cd '#{safe_dir}' && exec #{cmd}"
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
