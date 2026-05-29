# AgentTools — worker-side registry of tools an Agent may invoke.
#
# Each tool is:
#   - a JSON schema definition (sent to the LLM in tool_choice/tools)
#   - a Ruby block that takes (session:, project_id:, args:) and returns a
#     hash suitable for JSON serialization.
#
# Tools intentionally route through existing worker code paths (FsStore,
# DirectoryEntry, etc.) so they inherit the same authorization the user has
# over the project. Never add a tool that bypasses the session's project_id
# scope.
#
# Safety posture: tools added here are *capabilities*. Each Agent row picks
# which subset it's allowed to call via the allowed_tools column. That means
# a "safety-guard" agent can be wired with chat-only and zero tools, while a
# "coder" agent gets read_file + list_dir + (later) propose_patch.
module AgentTools
  # ---------------------------------------------------------------------
  # Registry. Each entry:
  #   slug => {
  #     schema:   { ...OpenAI tools[i] payload... },
  #     callable: ->(session:, project_id:, args:) { ...returns Hash... }
  #   }
  # ---------------------------------------------------------------------
  REGISTRY = {}

  def self.register(slug, schema:, &callable)
    raise ArgumentError, "tool #{slug} already registered" if REGISTRY.key?(slug)
    REGISTRY[slug] = { schema: schema, callable: callable }
  end

  def self.openai_tools_for(allowed_slugs)
    allowed_slugs.filter_map { |s| REGISTRY.dig(s, :schema) }
  end

  # Invoke a tool by name. Returns the tool's result (Hash). Raises
  # ArgumentError if the tool isn't registered or isn't in allowed_slugs.
  # Any exception inside the tool is caught and returned as { error: ... }
  # so the model can read it and retry rather than killing the loop.
  #
  # `agent:` is the Agent record — passed to callables that need per-agent
  # capability gates beyond the allowed_slugs list (currently: shell_exec
  # also requires agent.shell_exec_enabled).
  def self.invoke(slug, allowed_slugs:, session:, project_id:, args:, agent: nil)
    unless allowed_slugs.include?(slug)
      raise ArgumentError, "tool #{slug.inspect} not allowed for this agent"
    end
    entry = REGISTRY[slug] or raise ArgumentError, "unknown tool #{slug.inspect}"
    begin
      entry[:callable].call(session: session, project_id: project_id,
                            args: args, agent: agent)
    rescue => e
      { error: "#{e.class}: #{e.message}" }
    end
  end

  # ---------------------------------------------------------------------
  # read_file(path) — return current text content of a VFS file.
  # ---------------------------------------------------------------------
  register('read_file',
    schema: {
      type: 'function',
      function: {
        name: 'read_file',
        description: 'Read the current contents of a single file in the ' \
                     "user's project filesystem. Path is the VFS path " \
                     "(absolute, starting with '/').",
        parameters: {
          type: 'object',
          required: ['path'],
          properties: {
            path: { type: 'string', description: "VFS path, e.g. '/README.md'" }
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    path  = args['path'].to_s
    entry = DirectoryEntry.find_by_project_and_path(project_id, path)
    if entry.nil?
      { error: "no such path: #{path}" }
    elsif entry.ftype != 'file'
      { error: "not a file: #{path} (ftype=#{entry.ftype})" }
    else
      content = entry.calc_current
      # Cap returned content so a 5 MB log doesn't blow up the prompt.
      truncated = content.length > 64_000
      {
        path: path,
        bytes: content.bytesize,
        truncated: truncated,
        content: truncated ? content.byteslice(0, 64_000) : content,
      }
    end
  end

  # ---------------------------------------------------------------------
  # list_dir(path) — list immediate children of a VFS directory.
  # ---------------------------------------------------------------------
  register('list_dir',
    schema: {
      type: 'function',
      function: {
        name: 'list_dir',
        description: 'List the immediate children (files and folders) of a ' \
                     'directory in the project VFS.',
        parameters: {
          type: 'object',
          required: ['path'],
          properties: {
            path: { type: 'string', description: "VFS path, e.g. '/' or '/src'" }
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    path  = args['path'].to_s
    entry = DirectoryEntry.find_by_project_and_path(project_id, path)
    if entry.nil?
      { error: "no such path: #{path}" }
    elsif entry.ftype != 'folder' && path != '/'
      { error: "not a directory: #{path}" }
    else
      children = DirectoryEntry.where(project_id: project_id, owner_id: entry.id).order(:cur_name)
      {
        path: path,
        entries: children.map { |c| { name: c.cur_name, type: c.ftype } },
      }
    end
  end

  # ---------------------------------------------------------------------
  # list_terminals() — enumerate agent-accessible terminals in the project.
  #
  # The model uses this to discover which terminal_id to pass to shell_exec.
  # Terminals where agent_accessible is false are intentionally hidden so
  # the model can't be tricked into trying them.
  # ---------------------------------------------------------------------
  register('list_terminals',
    schema: {
      type: 'function',
      function: {
        name: 'list_terminals',
        description: 'List terminals in the current project that have been ' \
                     'marked agent-accessible by the user. Returns id, name, ' \
                     'and busy state for each. Use the returned id with ' \
                     'shell_exec.',
        parameters: { type: 'object', properties: {}, additionalProperties: false },
      },
    }
  ) do |session:, project_id:, args:, **_|
    terms = TERMINALS.values.select { |t| t.project_id == project_id && t.agent_accessible }
    {
      terminals: terms.map { |t|
        { id: t.terminal_id, name: t.name, busy: t.agent_busy }
      },
    }
  end

  # ---------------------------------------------------------------------
  # shell_exec(terminal_id, command, timeout_s?) — run a single command in
  # a user-designated terminal and capture exit status + output.
  #
  # Implementation: write the command followed by a sentinel printf to the
  # PTY, tap the master stream until we see the sentinel, parse the trailing
  # exit code. The user watches the command stream live in their terminal
  # UI (this is the entire point of using their existing PTY rather than
  # spawning a hidden one). While running, the terminal's input is locked
  # to user keystrokes — shown as a badge in the client.
  #
  # Limits:
  #   - timeout_s defaults to 60s, capped by min(arg, 300, project_setting).
  #   - output buffer capped at 16 KB; truncated flag in result.
  #   - Caller must have agent.shell_exec_enabled AND the terminal must
  #     be flagged agent_accessible.
  # ---------------------------------------------------------------------
  SHELL_EXEC_OUTPUT_CAP   = 16_000   # bytes returned to the model
  SHELL_EXEC_DEFAULT_TO   = 60       # seconds
  SHELL_EXEC_MAX_TO       = 300      # hard ceiling
  SHELL_EXEC_POLL_S       = 0.05     # tap-buffer poll interval
  SHELL_EXEC_QUIET_S      = 0.2      # no-more-OSC-D window = "command done"

  # Strip ANSI / OSC / common control sequences from captured output before
  # returning to the model. xterm renders these correctly for the human but
  # they're noise (and tokens) for an LLM.
  SHELL_EXEC_ANSI_RX = /
    \e\][^\a\e]*(?:\a|\e\\)            # OSC ... BEL or ST
  | \e\[[0-?]*[ -\/]*[@-~]              # CSI ... final byte
  | \e[@-_]                             # 2-byte ESC sequences
  | \r                                  # carriage returns (PTY artifact)
  /xn

  register('shell_exec',
    schema: {
      type: 'function',
      function: {
        name: 'shell_exec',
        description: 'Run a single shell command in an agent-accessible ' \
                     "terminal and return its exit code and captured output. " \
                     "The user sees the command stream live. Use list_terminals " \
                     "first to find a terminal_id.",
        parameters: {
          type: 'object',
          required: ['terminal_id', 'command'],
          properties: {
            terminal_id: { type: 'integer',
                           description: 'ID from list_terminals' },
            command:     { type: 'string',
                           description: 'Shell command to run. Newlines are ' \
                                        'allowed but the call returns when the ' \
                                        'compound command finishes.' },
            timeout_s:   { type: 'integer',
                           description: "Seconds before giving up (default 60, max 300)." },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, agent:|
    # Two-layer gate: allowed_slugs already passed (we're inside the block);
    # also require the per-agent boolean.
    unless agent&.shell_exec_enabled
      next { error: 'shell_exec is not enabled for this agent' }
    end

    tid = args['terminal_id'].to_i
    cmd = args['command'].to_s
    if cmd.empty?
      next { error: 'command is required' }
    end

    term = TERMINALS[tid]
    unless term && term.project_id == project_id
      next { error: "terminal #{tid} not found in this project" }
    end
    unless term.agent_accessible
      next { error: "terminal #{tid} is not agent-accessible" }
    end

    # Resolve timeout. Project setting acts as an additional ceiling so the
    # user keeps the final say over how long the busy lock can hold.
    proj_setting = Project.find_by(id: project_id)&.project_setting
    user_cap     = proj_setting&.agent_shell_busy_timeout_s || SHELL_EXEC_DEFAULT_TO
    req          = args['timeout_s']&.to_i
    timeout_s    = [req && req > 0 ? req : SHELL_EXEC_DEFAULT_TO,
                    SHELL_EXEC_MAX_TO, user_cap].min

    factory = term.claim_for_agent!(timeout_s: timeout_s)
    unless factory
      next { error: "terminal #{tid} is already busy with another agent call" }
    end

    # The terminal's PROMPT_COMMAND emits OSC 633 ; D ; <exit> BEL after
    # every command. We watch the tap stream for that marker. For compound
    # commands ("a; b; c") bash fires it after the LAST statement, but a
    # newline-separated multi-line command fires once per line — so we
    # take the *last* OSC D seen after a short quiet window (no more Ds
    # for SHELL_EXEC_QUIET_S) as the finish signal and use that exit code.
    osc_rx = /\e\]633;D;(-?\d+)\a/

    buffer      = +''
    last_exit   = nil
    last_osc_at = nil
    cap_hit     = false
    mutex       = Mutex.new

    tap = factory.call do |chunk|
      mutex.synchronize do
        if buffer.bytesize < SHELL_EXEC_OUTPUT_CAP * 4
          buffer << chunk
        else
          cap_hit = true
        end
        # Scan all OSC Ds; latest wins. scan returns [["code"], ...].
        chunk.scan(osc_rx) do |(code)|
          last_exit   = code.to_i
          last_osc_at = Time.now
        end
      end
    end

    begin
      # Send the command verbatim. No sentinel, no echo gymnastics — the
      # PROMPT_COMMAND we installed at agent-enable time handles
      # completion detection invisibly.
      payload = cmd
      payload += "\n" unless payload.end_with?("\n")
      term.agent_write(payload)

      deadline  = Time.now + timeout_s
      timed_out = true
      while Time.now < deadline
        sleep SHELL_EXEC_POLL_S
        done = mutex.synchronize do
          last_osc_at && (Time.now - last_osc_at) >= SHELL_EXEC_QUIET_S
        end
        if done
          timed_out = false
          break
        end
      end

      result_buf, hit, exit_code = mutex.synchronize { [buffer.dup, cap_hit, last_exit] }

      # Strip ANSI / OSC noise from what we hand the model. The user sees
      # the styled output in xterm; the LLM gets a clean text slice.
      clean = result_buf.gsub(SHELL_EXEC_ANSI_RX, '')
      # Drop a trailing PS1 line if bash redrew its prompt before we
      # captured (best-effort: strip the last line if it has no newline).
      if (idx = clean.rindex("\n"))
        tail = clean.byteslice(idx + 1, clean.bytesize - idx - 1) || ''
        # If the tail looks like a PS1 (short, ends with $ or # or >), trim it.
        if tail.length < 200 && tail =~ /[#\$>]\s*\z/
          clean = clean.byteslice(0, idx)
        end
      end
      clean = clean.strip

      truncated = false
      if clean.bytesize > SHELL_EXEC_OUTPUT_CAP
        clean = clean.byteslice(0, SHELL_EXEC_OUTPUT_CAP)
        truncated = true
      end
      truncated ||= hit

      if timed_out
        { terminal_id: tid, exit_code: nil, output: clean,
          truncated: truncated, timed_out: true,
          error: "command did not finish within #{timeout_s}s" }
      else
        { terminal_id: tid, exit_code: exit_code, output: clean,
          truncated: truncated, timed_out: false }
      end
    ensure
      term.detach_tap(tap) if tap
      term.release_from_agent!(reason: 'shell_exec done')
    end
  end
end
