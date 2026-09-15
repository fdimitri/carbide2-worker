# AgentTools — worker-side registry of tools an Agent may invoke.
#
# Each tool is:
#   - a JSON schema definition (sent to the LLM in tool_choice/tools)
#   - a Ruby block that takes (session:, project_id:, args:) and returns a
#     hash suitable for JSON serialization.
#
# Tools intentionally route through existing worker code paths (ProjectFs /
# the project's DbfsV2::Store, etc.) so they inherit the same authorization the user has
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

  # Metadata for every registered tool, so a client can build its per-agent
  # allowlist UI from the live registry instead of a hardcoded list. slug is
  # the registry key; name/description come from the OpenAI schema. This is the
  # authoritative set of tools the worker can expose. See fdimitri/carbide2#73.
  def self.catalog
    REGISTRY.map do |slug, entry|
      fn = entry.dig(:schema, :function) || {}
      { slug: slug.to_s, name: (fn[:name] || slug).to_s, description: fn[:description].to_s }
    end
  end

  # Invoke a tool by name. Returns the tool's result (Hash).
  #
  # An unknown tool, or one not in this agent's allowlist, is NOT raised — it
  # returns { error: ... } so the model receives a tool result it can read and
  # recover from (inventing a tool name must not kill the conversation). Any
  # exception inside the tool is likewise caught and returned as { error: ... }.
  #
  # `agent:` is the Agent record — passed to callables that need per-agent
  # capability gates beyond the allowed_slugs list (currently: shell_exec
  # also requires agent.shell_exec_enabled).
  def self.invoke(slug, allowed_slugs:, session:, project_id:, args:, agent: nil,
                  cancel_check: nil)
    unless allowed_slugs.include?(slug)
      return { error: "tool #{slug.inspect} is not allowed for this agent" }
    end
    entry = REGISTRY[slug]
    return { error: "unknown tool #{slug.inspect}" } unless entry
    begin
      entry[:callable].call(session: session, project_id: project_id,
                            args: args, agent: agent, cancel_check: cancel_check)
    rescue => e
      { error: "#{e.class}: #{e.message}" }
    end
  end

  # =====================================================================
  # Write helpers — shared by file_edit_anchored and file_write_lines.
  #
  # Agent edits are lowered to the SAME positional change ops the editors,
  # DBFS replay, and both bridges already understand (deleteData*/
  # insertData*). The anchor/line window is resolved HERE, worker-side,
  # against the authoritative content, and only concrete positional deltas
  # travel the wire — so no client or bridge needs a new change type, and
  # no consumer re-runs a match against a possibly-stale copy.
  #
  # Coordinates are 0-based (line, char) with CHARACTER (not byte) columns,
  # matching DbfsV2::Buffer's Ruby-string replay and Monaco's columns for BMP
  # text. setContents is intentionally never used for a replace.
  # =====================================================================

  # Convert a CHARACTER offset within `str` into a 0-based (line, char) pair,
  # LF-delimited.
  def self.char_offset_to_line_char(str, char_off)
    char_off = [char_off.to_i, 0].max
    head    = str[0, char_off].to_s
    line    = head.count("\n")
    last_nl = head.rindex("\n")
    char    = last_nl ? (head.length - last_nl - 1) : head.length
    [line, char]
  end

  # Number of leading characters `a` and `b` share.
  def self.common_prefix_len(a, b)
    n = [a.length, b.length].min
    i = 0
    i += 1 while i < n && a[i] == b[i]
    i
  end

  # Number of trailing characters `a` and `b` share.
  def self.common_suffix_len(a, b)
    n = [a.length, b.length].min
    i = 0
    i += 1 while i < n && a[a.length - 1 - i] == b[b.length - 1 - i]
    i
  end

  # Append the delta(s) that replace buf[off...endoff] with `new_text`, with
  # coordinates computed against `buf` (the state a client will be in when
  # this delta is applied). Returns the new buffer.
  #
  # The replaced span is narrowed to the minimal changed region by keeping any
  # shared prefix/suffix in place, so a pure insertion emits only an insert
  # (and a pure deletion only a delete) instead of deleting then re-inserting
  # identical text — one revision, half the fs/change
  # broadcast for the common "add a line" edit.
  def self.push_replace_spec(buf, off, endoff, new_text, specs)
    old_text = buf[off...endoff].to_s
    pfx = common_prefix_len(old_text, new_text)
    sfx = common_suffix_len(old_text[pfx..].to_s, new_text[pfx..].to_s)

    del_start = off + pfx
    del_end   = endoff - sfx
    ins_text  = new_text[pfx...(new_text.length - sfx)].to_s

    sl, sc = char_offset_to_line_char(buf, del_start)
    if del_end > del_start
      el, ec = char_offset_to_line_char(buf, del_end)
      specs << {
        change_type: (sl == el ? 'deleteDataSingleLine' : 'deleteDataMultiLine'),
        change_data: { startLine: sl, startChar: sc, endLine: el, endChar: ec },
        start_line: sl, start_char: sc, end_line: el, end_char: ec,
      }
    end
    unless ins_text.empty?
      specs << {
        change_type: (ins_text.include?("\n") ? 'insertDataMultiLine' : 'insertDataSingleLine'),
        change_data: { startLine: sl, startChar: sc, data: ins_text },
        start_line: sl, start_char: sc, end_line: nil, end_char: nil,
      }
    end
    buf[0, off].to_s + new_text + buf[endoff..].to_s
  end

  # Sequentially resolve `edits` against an in-memory copy of `buffer`.
  # All-or-nothing: returns { error: } on the first edit that fails its
  # match constraint (nothing is written by the caller in that case), else
  # { buffer:, specs:, applied: }. Each edit:
  #   old_string (req), new_string (req),
  #   replace_all  — replace every occurrence
  #   replace_first — replace only the first occurrence
  #   (neither)    — require a unique match, else error.
  def self.compute_anchored_edits(buffer, edits)
    specs = []
    applied = 0
    buf = buffer.dup
    edits.each_with_index do |ed, i|
      old_s = ed['old_string'].to_s
      new_s = ed['new_string'].to_s
      return { error: "edit ##{i}: old_string must not be empty" } if old_s.empty?

      offsets = []
      from = 0
      while (idx = buf.index(old_s, from))
        offsets << idx
        from = idx + old_s.length   # non-overlapping
      end
      count = offsets.size

      if count.zero?
        return { error: "edit ##{i}: old_string not found", edit_index: i, occurrences: 0 }
      end

      unless ed['expected_count'].nil?
        want = ed['expected_count'].to_i
        if count != want
          return { error: "edit ##{i}: expected #{want} occurrence(s) of old_string but found #{count}",
                   edit_index: i, occurrences: count, expected_count: want }
        end
      end

      if ed['fail_on_multiple'] && count > 1
        return { error: "edit ##{i}: old_string matched #{count} times and fail_on_multiple is set",
                 edit_index: i, occurrences: count }
      end

      if count > 1 && !ed['replace_all'] && !ed['replace_first']
        return { error: "edit ##{i}: old_string matched #{count} times; set replace_all " \
                        'or replace_first, or add surrounding context to make it unique',
                 edit_index: i, occurrences: count }
      end

      targets = ed['replace_all'] ? offsets : [offsets.first]
      shift = 0
      targets.each do |orig_off|
        off    = orig_off + shift
        endoff = off + old_s.length
        buf    = push_replace_spec(buf, off, endoff, new_s, specs)
        shift += new_s.length - old_s.length
        applied += 1
      end
    end
    { buffer: buf, specs: specs, applied: applied }
  end

  # Persist accumulated change specs as DBFS revisions (one transaction),
  # broadcast each persisted revision to EVERY client with the file open (the
  # agent, not any single editor, is the origin), and nudge the VFS flusher so
  # the on-disk mirror is rewritten. Mirrors FsStore.commit_and_broadcast.
  #
  # The specs were computed one after another against the content at
  # `base_revision_id`, so the batch is anchored there: if the file moved in
  # between, the batch is auto-branched at that revision and rebased onto main
  # (ProjectFs.write_batch!); an edit overlapping a concurrent replace raises
  # BranchConflict and the batch stays on the branch. Returns the batch's own persisted Revisions.
  def self.commit_changes!(project_id:, node:, specs:, base_revision_id:, user_id:)
    return [] if specs.empty?

    deltas = specs.map { |ch| DbfsV2::Delta.new(ch[:change_type], ch[:change_data]) }
    result = ProjectFs.write_batch!(ProjectFs.store(project_id), node.path, deltas,
                                    base_revision_id: base_revision_id, user_id: user_id)

    doc   = defined?(OPEN_DOCUMENTS) ? OPEN_DOCUMENTS["#{project_id}:#{node.path}"] : nil
    peers = doc ? doc.clients.keys : []
    unless peers.empty?
      ProjectFs.batch_peer_frames(node.path, result, node, user_id: user_id).each do |cmd, frame|
        msg = { cs: 'fs', cmd: cmd, payload: frame }.to_json
        peers.each { |ws| ws.send(msg) rescue nil }
      end
    end

    bytes = result.revisions.sum { |r| r.change_data.to_s.bytesize }
    VFS_FLUSHERS[project_id]&.record_write(node.id, bytes) if defined?(VFS_FLUSHERS)
    result.revisions
  end

  # Look up a live text file for an agent tool. Returns [node, target, error]:
  # `target` is the symlink-resolved node, `error` a tool result Hash or nil.
  def self.text_file(project_id, path, verb: 'edit')
    node = ProjectFs.store(project_id).find(path)
    return [nil, nil, { error: "no such path: #{path}" }] if node.nil?
    return [node, nil, { error: "not a file: #{path} (ftype=#{node.ftype})" }] if node.ftype != 'file'
    target = node.resolve
    return [node, nil, { error: "dangling symlink: #{path}" }] if target.nil?
    return [node, target, { error: "cannot #{verb} binary file: #{path}" }] if target.binary?
    [node, target, nil]
  end

  # Stale-base check shared by the write tools. Returns an error Hash or nil.
  def self.stale_base_error(base_rev, cur_rev)
    return nil if base_rev.nil? || base_rev.to_s == cur_rev.to_s
    { error: "stale base_revision: you have #{base_rev}, current is #{cur_rev}. " \
             'Re-read the file and retry.', revision: cur_rev, stale: true }
  end

  # A write that lost a race between resolving the edit and committing it.
  def self.concurrent_write_error(project_id, target, e)
    err = { error: "the file changed while this edit was being applied and the edits overlap (#{e.message}). " \
                   'Re-read the file and retry.',
            revision: ProjectFs.head_revision_id(target.reload), stale: true }
    err[:branch] = e.branch if e.respond_to?(:branch)
    err
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
                     "(absolute, starting with '/'). The returned `revision` " \
                     'is the file version stamp (an opaque id) — pass it back as ' \
                     '`base_revision` to file_edit_anchored / file_write_lines ' \
                     'so the edit fails if the file changed since you read it.',
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
    path = args['path'].to_s
    node, target, err = text_file(project_id, path, verb: 'read')
    next err if err

    content = ProjectFs.store(project_id).read(node.path).to_s
    # Cap returned content so a 5 MB log doesn't blow up the prompt.
    truncated = content.length > 64_000
    {
      path: node.path,
      bytes: content.bytesize,
      truncated: truncated,
      revision: ProjectFs.head_revision_id(target),
      content: truncated ? content.byteslice(0, 64_000) : content,
    }
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
    store = ProjectFs.store(project_id)
    node  = store.find(path)
    if node.nil?
      { error: "no such path: #{path}" }
    elsif node.ftype != 'folder'
      { error: "not a directory: #{path}" }
    else
      {
        path: node.path,
        entries: store.list(node.path).map { |c| { name: c.cur_name, type: c.ftype } },
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
  ) do |session:, project_id:, args:, agent:, cancel_check:|
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
      interrupted = false
      while Time.now < deadline
        if cancel_check&.call
          term.agent_interrupt!
          interrupted = true
          break
        end
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

      if interrupted
        { terminal_id: tid, exit_code: nil, output: clean,
          truncated: truncated, interrupted: true }
      elsif timed_out
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

  # ---------------------------------------------------------------------
  # shell_peek_buffer(terminal_id, tail_bytes?, strip_ansi?) — read-only
  # snapshot of a terminal's scrollback for the agent (#70).
  #
  # Does NOT claim the terminal, does NOT set @agent_busy, and does NOT lock
  # the user out. It returns the current scrollback tail as text — the raw
  # literal buffer, optionally ANSI-stripped. No rendered screen grid: that
  # is the separate shell_get_rendered_screen (#71).
  #
  # Gate: the terminal must be agent_accessible (peeking still exposes what
  # the user typed/ran), but it does NOT require agent.shell_exec_enabled —
  # reading is strictly less privileged than executing.
  #
  # Default tail: project_settings.agent_shell_peek_tail_bytes (seeded, always
  # present); an explicit tail_bytes always wins. No code constant, no env
  # fallback.
  # ---------------------------------------------------------------------
  register('shell_peek_buffer',
    schema: {
      type: 'function',
      function: {
        name: 'shell_peek_buffer',
        description: 'Return a read-only snapshot of the most recent output ' \
                     'on an agent-accessible terminal. Use list_terminals first ' \
                     "to find a terminal_id. This does not run a command and " \
                     'does not lock the user out; it only reads what is already ' \
                     'on screen. Returns the project-configured default tail ' \
                     'of scrollback, with ANSI control sequences stripped.',
        parameters: {
          type: 'object',
          required: ['terminal_id'],
          properties: {
            terminal_id: { type: 'integer',
                           description: 'ID from list_terminals' },
            tail_bytes:  { type: 'integer',
                           description: 'Number of trailing bytes to return ' \
                                        "(overrides the project default; 0 = whole buffer)." },
            strip_ansi:  { type: 'boolean',
                           description: 'Strip ANSI/OSC/control sequences ' \
                                        '(default true).' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    tid = args['terminal_id'].to_i
    term = TERMINALS[tid]
    unless term && term.project_id == project_id
      next { error: "terminal #{tid} not found in this project" }
    end
    unless term.agent_accessible
      next { error: "terminal #{tid} is not agent-accessible" }
    end

    proj_setting = Project.find_by(id: project_id)&.project_setting
    next { error: "project #{project_id} has no project settings" } unless proj_setting

    tail = args['tail_bytes']
    tail = proj_setting.agent_shell_peek_tail_bytes if tail.nil?
    data = term.scrollback_snapshot(tail_bytes: tail.to_i)

    strip = args.key?('strip_ansi') ? !!args['strip_ansi'] : true
    clean = strip ? data.gsub(SHELL_EXEC_ANSI_RX, '') : data

    {
      terminal_id: tid,
      bytes:       clean.bytesize,
      content:     clean,
    }
  end

  # ---------------------------------------------------------------------
  # file_edit_anchored(path, edits[], base_revision?) — anchored find/replace.
  #
  # Each edit locates old_string in the current file content and swaps it for
  # new_string. The anchor IS the version stamp (content-keyed CAS): if the
  # surrounding text moved, the anchor won't match and the edit fails rather
  # than corrupting the file. Matching is resolved worker-side and lowered to
  # positional deleteData*/insertData* deltas the editors + bridges already
  # apply — never setContents.
  # ---------------------------------------------------------------------
  register('file_edit_anchored',
    schema: {
      type: 'function',
      function: {
        name: 'file_edit_anchored',
        description: 'Edit a text file by anchored find-and-replace. Each edit ' \
                     'finds old_string and replaces it with new_string. By ' \
                     'default old_string must match exactly once (otherwise the ' \
                     'edit fails — add surrounding context to disambiguate, or ' \
                     'set replace_all / replace_first). Edits are applied in ' \
                     'order and are all-or-nothing: if any edit fails to match, ' \
                     'nothing is written. Optionally pass base_revision (from ' \
                     'read_file) to abort if the file changed meanwhile.',
        parameters: {
          type: 'object',
          required: ['path', 'edits'],
          properties: {
            path: { type: 'string', description: "VFS path, e.g. '/src/app.rb'" },
            edits: {
              type: 'array',
              description: 'Ordered list of anchored replacements.',
              items: {
                type: 'object',
                required: ['old_string', 'new_string'],
                properties: {
                  old_string:    { type: 'string',
                                   description: 'Exact text to find (include ' \
                                                'enough context to be unique).' },
                  new_string:    { type: 'string',
                                   description: 'Replacement text (empty string ' \
                                                'deletes the match).' },
                  replace_all:   { type: 'boolean',
                                   description: 'Replace every occurrence.' },
                  replace_first: { type: 'boolean',
                                   description: 'Replace only the first occurrence.' },
                  fail_on_multiple: { type: 'boolean',
                                      description: 'Error if old_string matches ' \
                                                   'more than once (even with ' \
                                                   'replace_all/replace_first).' },
                  expected_count: { type: 'integer',
                                    description: 'Assert old_string matches ' \
                                                 'exactly this many times; error ' \
                                                 'otherwise. Checked before replacing.' },
                },
                additionalProperties: false,
              },
            },
            base_revision: { type: 'string',
                             description: 'Revision from read_file; edit is ' \
                                          'rejected if the file has changed.' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    path  = args['path'].to_s
    edits = args['edits']
    node, target, err = text_file(project_id, path)
    next err if err
    next { error: 'edits must be a non-empty array' } unless edits.is_a?(Array) && !edits.empty?

    # Resolve against the content AT a known head, and anchor the write there.
    cur_rev = ProjectFs.head_revision_id(target)
    stale = stale_base_error(args['base_revision'], cur_rev)
    next stale if stale

    content = ProjectFs.content_at(target, cur_rev)
    result = compute_anchored_edits(content, edits)
    next result if result[:error]

    begin
      stored = commit_changes!(project_id: project_id, node: target, specs: result[:specs],
                               base_revision_id: cur_rev, user_id: session.user_id)
    rescue DbfsV2::ConflictError => e
      next concurrent_write_error(project_id, target, e)
    end
    {
      path:          node.path,
      applied:       true,
      edits_applied: result[:applied],
      changes:       stored.size,
      revision:      ProjectFs.head_revision_id(target.reload),
    }
  end

  # ---------------------------------------------------------------------
  # file_write_lines(path, start_line, line_count, lines[], base_revision?)
  #
  # Replace `line_count` lines starting at `start_line` (0-based) with the
  # given lines. line_count: 0 pure-inserts before start_line. Each entry in
  # `lines` should include its trailing newline (matches read_lines output).
  # Lowered to the same positional delete+insert delta as anchored edits.
  # ---------------------------------------------------------------------
  register('file_write_lines',
    schema: {
      type: 'function',
      function: {
        name: 'file_write_lines',
        description: 'Replace a range of lines in a text file. Replaces ' \
                     'line_count lines starting at start_line (0-based) with ' \
                     'the provided lines. Use line_count 0 to insert before ' \
                     'start_line without deleting. Each line should include its ' \
                     'trailing newline, matching how files are read. Optionally ' \
                     'pass base_revision (from read_file) to abort if the file ' \
                     'changed meanwhile.',
        parameters: {
          type: 'object',
          required: ['path', 'start_line', 'line_count', 'lines'],
          properties: {
            path:       { type: 'string', description: "VFS path, e.g. '/src/app.rb'" },
            start_line: { type: 'integer', description: '0-based first line to replace.' },
            line_count: { type: 'integer', description: 'Number of lines to replace (0 = insert).' },
            lines:      { type: 'array', items: { type: 'string' },
                          description: 'Replacement lines (include trailing newlines).' },
            base_revision: { type: 'string',
                             description: 'Revision from read_file; write is ' \
                                          'rejected if the file has changed.' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    path = args['path'].to_s
    node, target, err = text_file(project_id, path)
    next err if err

    cur_rev = ProjectFs.head_revision_id(target)
    stale = stale_base_error(args['base_revision'], cur_rev)
    next stale if stale

    start_line = [args['start_line'].to_i, 0].max
    line_count = [args['line_count'].to_i, 0].max
    lines      = Array(args['lines']).map(&:to_s)

    buffer    = ProjectFs.content_at(target, cur_rev)
    cur_lines = buffer.lines
    total     = cur_lines.length
    next { error: "start_line #{start_line} is beyond EOF (#{total} lines)" } if start_line > total

    head_char = cur_lines[0, start_line].to_a.sum(&:length)
    del_char  = cur_lines[start_line, line_count].to_a.sum(&:length)
    new_text  = lines.join

    specs = []
    push_replace_spec(buffer, head_char, head_char + del_char, new_text, specs)
    begin
      stored = commit_changes!(project_id: project_id, node: target, specs: specs,
                               base_revision_id: cur_rev, user_id: session.user_id)
    rescue DbfsV2::ConflictError => e
      next concurrent_write_error(project_id, target, e)
    end
    {
      path:           node.path,
      applied:        true,
      lines_replaced: [line_count, total - start_line].min,
      changes:        stored.size,
      revision:       ProjectFs.head_revision_id(target.reload),
    }
  end

  # ---------------------------------------------------------------------
  # file_pcre_search(pattern, path?, ignore_case?, dotall?, max_results?)
  #
  # Regex search across the project's text files (or a file/subtree under
  # `path`). Returns per-line matches with 0-based line numbers. Binary
  # files are skipped. Read-only.
  # ---------------------------------------------------------------------
  PCRE_SEARCH_DEFAULT_MAX = 200
  PCRE_SEARCH_HARD_MAX    = 1000

  register('file_pcre_search',
    schema: {
      type: 'function',
      function: {
        name: 'file_pcre_search',
        description: 'Search the project text files with a Ruby/PCRE regular ' \
                     'expression (pattern is the regex source, no delimiters). ' \
                     'Returns matching lines with their file path and 0-based ' \
                     'line number. Optionally restrict to a single file or a ' \
                     'subtree via path.',
        parameters: {
          type: 'object',
          required: ['pattern'],
          properties: {
            pattern:     { type: 'string', description: 'Regex source, e.g. "def\\s+\\w+".' },
            path:        { type: 'string',
                           description: "Optional VFS file or folder to limit the search (default: whole project)." },
            ignore_case: { type: 'boolean', description: 'Case-insensitive match.' },
            dotall:      { type: 'boolean', description: 'Let . match newlines (Ruby /m).' },
            max_results: { type: 'integer',
                           description: "Cap on matches returned (default 200, max 1000)." },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    pattern = args['pattern'].to_s
    next { error: 'pattern is required' } if pattern.empty?

    flags = 0
    flags |= Regexp::IGNORECASE if args['ignore_case']
    flags |= Regexp::MULTILINE  if args['dotall']
    rx = begin
      Regexp.new(pattern, flags)
    rescue RegexpError => e
      next { error: "invalid regex: #{e.message}" }
    end

    max_results = args['max_results'].to_i
    max_results = PCRE_SEARCH_DEFAULT_MAX if max_results <= 0
    max_results = PCRE_SEARCH_HARD_MAX    if max_results > PCRE_SEARCH_HARD_MAX

    scope = args['path'].to_s
    store = ProjectFs.store(project_id)
    nodes = FileNode.live.where(project_id: project_id, ftype: 'file', binary: false, symlink_target: nil)
    if !scope.empty? && scope != '/'
      base  = scope.chomp('/')
      nodes = nodes.where('path = ? OR starts_with(path, ?)', base, "#{base}/")
    end

    matches       = []
    files_scanned = 0
    truncated     = false
    nodes.order(:path).pluck(:path).each do |npath|
      break if matches.size >= max_results
      files_scanned += 1
      store.read(npath).to_s.each_line.with_index do |line, ln|
        next unless line.match?(rx)
        matches << { path: npath, line: ln, text: line.chomp[0, 500] }
        if matches.size >= max_results
          truncated = true
          break
        end
      end
    end

    { matches: matches, count: matches.size, files_scanned: files_scanned, truncated: truncated }
  end

  # =====================================================================
  # AGENTS.md + agent memory
  #
  # AGENTS.md is a conventional per-project instruction file at the VFS root.
  # It is auto-injected directly behind the system prompt on every inference
  # turn (see AgentSession#outgoing_messages) — the tools below just let an
  # agent read and rewrite it.
  #
  # Memories are markdown files under MEMORY_DIR. They are pull-based: an
  # agent stores durable notes with memory_write and later retrieves them
  # with memory_list / memory_read. Both live in the same VFS as the rest of
  # the project, so they persist, mirror to disk, and are user-visible.
  # =====================================================================
  AGENTS_MD_PATH   = '/AGENTS.md'
  MEMORY_DIR       = '/.carbide/memories'
  MEMORY_NAME_RX   = /\A[a-z0-9][a-z0-9._-]*\z/

  # Slugify a memory name into a safe '/.carbide/memories/<slug>.md' path, or
  # nil if the name can't be reduced to a valid slug (empty / traversal).
  def self.memory_path(name)
    slug = name.to_s.strip.downcase
    slug = slug.sub(/\.md\z/, '')
    slug = slug.gsub(/[^a-z0-9._-]+/, '-').gsub(/\A[-.]+|[-.]+\z/, '')
    return nil if slug.empty? || slug.include?('..') || !slug.match?(MEMORY_NAME_RX)
    "#{MEMORY_DIR}/#{slug}.md"
  end

  # Tell every session in the project about a newly created entry so the file
  # explorer picks it up without a manual refresh. Mirrors the fs/created
  # broadcast FsStore.handle_create_file sends for interactive creates.
  def self.broadcast_created!(project_id:, node:)
    return unless defined?(SESSIONS_BY_PROJECT)
    msg = { cs: 'fs', cmd: 'created',
            payload: { path: node.path, type: node.ftype, id: node.id } }.to_json
    (SESSIONS_BY_PROJECT[project_id] || []).each { |s| s.ws&.send(msg) rescue nil }
  end

  # Create `srcpath` with `content` if it's missing, otherwise replace its
  # whole content with a minimal delta. New files broadcast fs/created and are
  # flushed to disk; edits go through commit_changes! (fs/change + flush) like
  # every other agent edit. Returns a result Hash.
  def self.write_whole_file!(project_id:, srcpath:, content:, user_id:)
    content = content.to_s
    store   = ProjectFs.store(project_id)
    node    = store.find(srcpath)
    if node.nil?
      node = ProjectFs.ensure_file!(store, srcpath, content: content, user_id: user_id)
      broadcast_created!(project_id: project_id, node: node)
      VFS_FLUSHERS[project_id]&.record_write(node.id, content.bytesize) if defined?(VFS_FLUSHERS)
      return { path: node.path, created: true, revision: ProjectFs.head_revision_id(node) }
    end
    _node, target, err = text_file(project_id, srcpath, verb: 'write')
    return err if err

    cur_rev = ProjectFs.head_revision_id(target)
    current = ProjectFs.content_at(target, cur_rev)
    specs = []
    push_replace_spec(current, 0, current.length, content, specs)
    begin
      stored = commit_changes!(project_id: project_id, node: target, specs: specs,
                               base_revision_id: cur_rev, user_id: user_id)
    rescue DbfsV2::ConflictError => e
      return concurrent_write_error(project_id, target, e)
    end
    { path: node.path, created: false, changes: stored.size, revision: ProjectFs.head_revision_id(target.reload) }
  end

  # ---------------------------------------------------------------------
  # agents_md_read() — return the current project AGENTS.md.
  #
  # AGENTS.md is already injected behind the system prompt every turn, so an
  # agent rarely needs to read it — but this lets it fetch the exact current
  # text before rewriting it with agents_md_write.
  # ---------------------------------------------------------------------
  register('agents_md_read',
    schema: {
      type: 'function',
      function: {
        name: 'agents_md_read',
        description: 'Read the project AGENTS.md — the always-in-effect ' \
                     'instruction file for this project. Returns empty content ' \
                     'with exists=false if no AGENTS.md has been created yet.',
        parameters: { type: 'object', properties: {}, additionalProperties: false },
      },
    }
  ) do |session:, project_id:, args:, **_|
    node, target, err = text_file(project_id, AGENTS_MD_PATH, verb: 'read')
    if node.nil? || node.ftype != 'file'
      { path: AGENTS_MD_PATH, exists: false, content: '' }
    elsif err
      err
    else
      { path: node.path, exists: true, revision: ProjectFs.head_revision_id(target),
        content: ProjectFs.store(project_id).read(node.path).to_s }
    end
  end

  # ---------------------------------------------------------------------
  # agents_md_write(content) — create or replace the project AGENTS.md.
  #
  # Overwrites the whole file. Because AGENTS.md is re-read and injected on
  # every subsequent turn, the new guidance takes effect immediately — no
  # need to restate it in chat.
  # ---------------------------------------------------------------------
  register('agents_md_write',
    schema: {
      type: 'function',
      function: {
        name: 'agents_md_write',
        description: 'Create or replace the project AGENTS.md with the given ' \
                     'markdown. This file is injected directly behind the ' \
                     'system prompt on every turn, so keep it concise and ' \
                     'durable (build/test commands, conventions, constraints). ' \
                     'Overwrites the entire file.',
        parameters: {
          type: 'object',
          required: ['content'],
          properties: {
            content: { type: 'string', description: 'Full markdown body of AGENTS.md.' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    write_whole_file!(project_id: project_id, srcpath: AGENTS_MD_PATH,
                      content: args['content'].to_s, user_id: session.user_id)
  end

  # ---------------------------------------------------------------------
  # memory_list() — enumerate stored memories.
  # ---------------------------------------------------------------------
  register('memory_list',
    schema: {
      type: 'function',
      function: {
        name: 'memory_list',
        description: 'List the agent memories stored for this project. Each ' \
                     'memory is a markdown note under ' + MEMORY_DIR + '. ' \
                     'Returns each memory name plus its first heading/line as a ' \
                     'preview. Use memory_read to fetch a full memory.',
        parameters: { type: 'object', properties: {}, additionalProperties: false },
      },
    }
  ) do |session:, project_id:, args:, **_|
    store = ProjectFs.store(project_id)
    dir   = store.find(MEMORY_DIR)
    files = dir && dir.ftype == 'folder' ? store.list(MEMORY_DIR).select { |c| c.ftype == 'file' } : []
    memories = files.map do |c|
      name    = File.basename(c.path, '.md')
      target  = c.resolve
      preview = target.nil? || target.binary? ? '' : store.read(c.path).to_s.lines.find { |l| !l.strip.empty? }.to_s.strip[0, 120]
      { name: name, preview: preview }
    end
    { dir: MEMORY_DIR, count: memories.size, memories: memories }
  end

  # ---------------------------------------------------------------------
  # memory_read(name) — fetch one memory's markdown.
  # ---------------------------------------------------------------------
  register('memory_read',
    schema: {
      type: 'function',
      function: {
        name: 'memory_read',
        description: 'Read a single stored memory by name (as listed by ' \
                     'memory_list). Returns exists=false if there is no memory ' \
                     'with that name.',
        parameters: {
          type: 'object',
          required: ['name'],
          properties: {
            name: { type: 'string', description: 'Memory name, e.g. "build-commands".' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    srcpath = memory_path(args['name'])
    next { error: "invalid memory name: #{args['name'].inspect}" } if srcpath.nil?
    node, target, err = text_file(project_id, srcpath, verb: 'read')
    if node.nil? || node.ftype != 'file'
      { name: File.basename(srcpath, '.md'), exists: false, content: '' }
    elsif err
      err
    else
      { name: File.basename(srcpath, '.md'), exists: true, path: node.path,
        revision: ProjectFs.head_revision_id(target),
        content: ProjectFs.store(project_id).read(node.path).to_s }
    end
  end

  # ---------------------------------------------------------------------
  # memory_write(name, content) — create or replace a memory.
  # ---------------------------------------------------------------------
  register('memory_write',
    schema: {
      type: 'function',
      function: {
        name: 'memory_write',
        description: 'Store a durable memory as markdown under ' + MEMORY_DIR +
                     '. Creates the memory if new, otherwise overwrites it ' \
                     'entirely. Use a short kebab-case name that describes the ' \
                     'note (e.g. "build-commands", "api-conventions").',
        parameters: {
          type: 'object',
          required: ['name', 'content'],
          properties: {
            name:    { type: 'string', description: 'Memory name, e.g. "build-commands".' },
            content: { type: 'string', description: 'Full markdown body of the memory.' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    srcpath = memory_path(args['name'])
    next { error: "invalid memory name: #{args['name'].inspect}" } if srcpath.nil?
    res = write_whole_file!(project_id: project_id, srcpath: srcpath,
                            content: args['content'].to_s, user_id: session.user_id)
    res.is_a?(Hash) && res[:path] ? res.merge(name: File.basename(srcpath, '.md')) : res
  end

  # ---------------------------------------------------------------------
  # rehydrate_ttl(tool_call_id) — renew a tool result's lease before it expires.
  #
  # ADR-033 phase 2: tool results carry a turn-budget TTL (expires_at_turn).
  # The agent calls this on a tool_call_id it still needs, which clears the
  # expiry (keeps the result in the prompt). This is lease renewal BEFORE
  # eviction — a tombstoned result is a flag, not a delete, so un-flagging is
  # still the restore path; this tool is for keeping content that is merely
  # *about to* expire.
  # ---------------------------------------------------------------------
  register('rehydrate_ttl',
    schema: {
      type: 'function',
      function: {
        name: 'rehydrate_ttl',
        description: 'Renew the expiry on a tool result the agent still ' \
                     'needs, identified by its tool_call_id. Tool results are ' \
                     'evicted from the prompt after a turn budget to save ' \
                     'tokens; call this on any tool_call_id whose result you ' \
                     'will need again so it is not removed.',
        parameters: {
          type: 'object',
          required: ['tool_call_id'],
          properties: {
            tool_call_id: { type: 'string',
                            description: 'The tool_call_id of the result to keep.' },
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:, **_|
    tid = args['tool_call_id'].to_s
    next { error: 'tool_call_id is required' } if tid.empty?
    msgs = AgentMessage.joins(:agent_conversation)
                       .where(agent_conversations: { project_id: project_id })
                       .where(tool_call_id: tid, role: 'tool')
    next { error: "no tool result with tool_call_id #{tid.inspect} in this project" } if msgs.empty?

    msgs.each { |m| m.update_column(:expires_at_turn, nil) }
    { tool_call_id: tid, rehydrated: msgs.size }
  end
end
