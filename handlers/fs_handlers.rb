# worker/handlers/fs_handlers.rb
#
# Filesystem commandSet handlers, migrated out of FsStore's case statement
# into the Command::Dispatcher registry (ADR-019 §5 / ADR-017 §7). Each
# command is a registered (session, payload) method; the bodies use the
# worker's top-level send_msg/broadcast helpers and SESSIONS_BY_PROJECT
# directly rather than receiving them as injected dependencies.
#
# The dispatch override preserves FsStore's error contract: unknown cmd and
# handler raises surface as `fs/error` (not `system/error`), and
# ActiveRecord::RecordNotFound becomes a "not found" envelope.

require 'base64'
require 'fileutils'
require 'open3'

module FsHandlers
  extend Command::Dispatcher
  namespace 'fs'

  # Preserve the fs/error envelope. A handler raise must never escape into
  # the EM reactor; RecordNotFound gets the friendlier "not found" shape.
  def self.dispatch(cmd, session, payload)
    m = handlers[cmd.to_s]
    unless m
      send_msg(session.ws, 'fs', 'error', { message: "unknown fs cmd: #{cmd}" })
      return
    end
    send(m, session, payload)
  rescue ActiveRecord::RecordNotFound => e
    send_msg(session.ws, 'fs', 'error', { message: "not found: #{e.message}" })
  rescue => e
    warn "[fs/#{cmd}] ERROR: #{e.class} #{e.message}\n  " \
         "#{e.backtrace.first(5).join("\n  ")}"
    send_msg(session.ws, 'fs', 'error', { message: e.message })
  end

  # -------------------------------------------------------------------------
  # Handlers
  # -------------------------------------------------------------------------

  def self.handle_tree(session, _payload)
    tree = DirectoryEntry.tree_for_project(session.project_id)
    send_msg(session.ws, 'fs', 'tree', { tree: tree })
  end
  register 'tree', :handle_tree

  def self.handle_read(session, payload)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'is binary — use read_binary' }) if entry.binary?

    content = entry.get_content
    send_msg(session.ws, 'fs', 'content', {
      path:    entry.srcpath,
      content: content
    })
  end
  register 'read', :handle_read

  # read_binary — stream a chunk of a binary file from disk.
  # Payload: { path:, offset: 0, length: 65536 }
  # Reply:   { path:, offset:, length: (actual), size: (total), eof: bool, data: base64 }
  def self.handle_read_binary(session, payload)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

    flusher   = VFS_FLUSHERS[session.project_id]
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'no disk root configured' }) unless flusher
    disk_path = File.join(flusher.root_path, entry.srcpath)
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'not present on disk' }) unless File.file?(disk_path)

    offset = [payload['offset'].to_i, 0].max
    # Cap a single chunk at 1 MB to keep WS frames sane. The client should
    # loop until eof for whole-file reads (e.g. download).
    length = payload['length'].to_i
    length = 64 * 1024 if length <= 0
    length = [length, 1024 * 1024].min

    total = File.size(disk_path)
    bytes = ''
    if offset < total
      File.open(disk_path, 'rb') do |f|
        f.seek(offset)
        bytes = f.read(length).to_s
      end
    end
    send_msg(session.ws, 'fs', 'binary_chunk', {
      path:   entry.srcpath,
      offset: offset,
      length: bytes.bytesize,
      size:   total,
      eof:    offset + bytes.bytesize >= total,
      data:   Base64.strict_encode64(bytes)
    })
  end
  register 'read_binary', :handle_read_binary

  # stat — metadata snapshot for the explorer Properties panel (#5).
  def self.handle_stat(session, payload)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    send_msg(session.ws, 'fs', 'stat', entry.stat_hash)
  end
  register 'stat', :handle_stat

  # open — register this session as viewing a file; receive its peer viewer list
  def self.handle_open(session, payload)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

    norm = entry.srcpath
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key] ||= OpenDocument.new(session.project_id, norm)
    doc.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.open_file(norm)

    send_msg(session.ws, 'fs', 'opened', { path: norm, viewers: doc.viewers })
  end
  register 'open', :handle_open

  # close — unregister this session from a file
  def self.handle_close(session, payload)
    path = payload['path'].to_s.strip
    norm = path.start_with?('/') ? path : "/#{path}"
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key]
    return unless doc

    doc.remove_client(session.ws)
    session.close_file(norm)
    OPEN_DOCUMENTS.delete(key) if doc.empty?
  end
  register 'close', :handle_close

  # cursor — update this session's cursor position and broadcast to co-viewers
  def self.handle_cursor(session, payload)
    path = payload['path'].to_s.strip
    norm = path.start_with?('/') ? path : "/#{path}"
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key]
    return unless doc&.member?(session.ws)

    line = payload['line'].to_i
    char = payload['char'].to_i
    doc.update_cursor(session.ws, line: line, char: char)

    broadcast(doc.others(session.ws), 'fs', 'cursor', {
      path:    norm,
      user_id: session.user_id,
      name:    session.name,
      line:    line,
      char:    char
    })
  end
  register 'cursor', :handle_cursor

  # write — accepts { path:, changes: [...] }
  # Each change in the array: { change_type:, change_data:, start_line:, start_char:, end_line:, end_char: }
  def self.handle_write(session, payload)
    path    = payload['path'].to_s.strip
    changes = Array(payload['changes'])
    return send_msg(session.ws, 'fs', 'error', { message: 'no changes provided' }) if changes.empty?

    entry = find_entry!(session.project_id, path)
    return send_msg(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

    # Hydrate the live buffer BEFORE persisting so a cache miss replays only
    # pre-write rows. apply! then advances it by exactly the rows just written.
    # (Order matters: hydrate-after-persist would replay the new rows during
    # cache construction and then apply! them a second time.)
    cached = Document.for(entry)

    stored = ActiveRecord::Base.transaction do
      changes.map do |ch|
        FileChange.append!(
          directory_entry_id: entry.id,
          user_id:            session.user_id,
          change_type:        ch['change_type'].to_s,
          change_data:        ch['change_data'].is_a?(Hash) ? ch['change_data'].to_json : ch['change_data'].to_s,
          start_line:         ch['start_line'].to_i,
          start_char:         ch['start_char'].to_i,
          end_line:           ch['end_line'],
          end_char:           ch['end_char']
        )
      end
    end

    send_msg(session.ws, 'fs', 'written', {
      path:      entry.srcpath,
      revisions: stored.map(&:revision)
    })

    # Advance the in-memory buffer with the same changes we just persisted.
    changes.each { |ch| cached&.apply!(ch['change_type'], ch['change_data']) }

    # Broadcast only to clients that have this file open
    key   = "#{session.project_id}:#{entry.srcpath}"
    doc   = OPEN_DOCUMENTS[key]
    peers = doc ? doc.others(session.ws) : []
    changes.each_with_index do |ch, i|
      broadcast(peers, 'fs', 'change', {
        path:        entry.srcpath,
        change_type: ch['change_type'],
        change_data: ch['change_data'],
        start_line:  ch['start_line'],
        start_char:  ch['start_char'],
        end_line:    ch['end_line'],
        end_char:    ch['end_char'],
        revision:    stored[i].revision,
        user_id:     session.user_id
      })
    end

    data_bytes = changes.sum { |ch| ch['change_data'].to_s.bytesize }
    VFS_FLUSHERS[session.project_id]&.record_write(entry.id, data_bytes)
  end
  register 'write', :handle_write

  def self.handle_set_contents(session, payload)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s
    entry   = find_entry!(session.project_id, path)

    # Hydrate the live buffer BEFORE persisting (see handle_write for why).
    cached = Document.for(entry)

    fc = ActiveRecord::Base.transaction do
      FileChange.append!(
        directory_entry_id: entry.id,
        user_id:            session.user_id,
        change_type:        'setContents',
        change_data:        content.encode('UTF-8', invalid: :replace, undef: :replace, replace: ''),
        start_line:         0,
        start_char:         0
      )
    end

    send_msg(session.ws, 'fs', 'written', { path: entry.srcpath, revisions: [fc.revision] })
    cached&.apply!('setContents', content)
    key   = "#{session.project_id}:#{entry.srcpath}"
    doc   = OPEN_DOCUMENTS[key]
    peers = doc ? doc.others(session.ws) : []
    broadcast(peers, 'fs', 'set_contents', {
      path:     entry.srcpath,
      content:  content,
      revision: fc.revision,
      user_id:  session.user_id
    })

    VFS_FLUSHERS[session.project_id]&.record_write(entry.id, content.bytesize)
  end
  register 'set_contents', :handle_set_contents

  def self.handle_create_file(session, payload)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s
    entry   = DirectoryEntry.create_file!(
      project_id: session.project_id,
      srcpath:    path,
      user_id:    session.user_id,
      data:       content,
      mkdirp:     !!payload['mkdirp']
    )

    send_msg(session.ws, 'fs', 'created', { path: entry.srcpath, type: 'file', id: entry.id })
    peers = other_project_sessions(session)
    broadcast(peers, 'fs', 'created', {
      path:    entry.srcpath,
      type:    'file',
      id:      entry.id,
      user_id: session.user_id
    })
  end
  register 'create_file', :handle_create_file

  def self.handle_create_dir(session, payload)
    path  = payload['path'].to_s.strip
    entry = DirectoryEntry.mkdir_p!(project_id: session.project_id, srcpath: path, user_id: session.user_id)

    send_msg(session.ws, 'fs', 'created', { path: entry.srcpath, type: 'folder', id: entry.id })
    peers = other_project_sessions(session)
    broadcast(peers, 'fs', 'created', {
      path:    entry.srcpath,
      type:    'folder',
      id:      entry.id,
      user_id: session.user_id
    })
  end
  register 'create_dir', :handle_create_dir

  def self.handle_rename(session, payload)
    path     = payload['path'].to_s.strip
    new_name = payload['new_name'].to_s.strip
    entry    = find_entry!(session.project_id, path)
    old_path = entry.srcpath
    entry.rename!(new_name)
    Document.forget(old_path)
    Document.forget(entry.srcpath)

    send_msg(session.ws, 'fs', 'renamed', { old_path: old_path, new_path: entry.srcpath, id: entry.id })
    peers = other_project_sessions(session)
    broadcast(peers, 'fs', 'renamed', {
      old_path: old_path,
      new_path: entry.srcpath,
      id:       entry.id,
      user_id:  session.user_id
    })
  end
  register 'rename', :handle_rename

  def self.handle_delete(session, payload)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    entry_path = entry.srcpath

    # Collect every descendant srcpath BEFORE destroying the DB row. After
    # destroy, ActiveRecord's dependent: :destroy has already cascaded, so we
    # need the list in advance to (a) forget each live Document buffer the DB
    # cascade never touches, and (b) suppress each resulting inotify :delete as
    # an external mutation. Fixes #12 in May30-Questions.md + Cursor PR #5.
    descendant_paths =
      DirectoryEntry.where(project_id: session.project_id)
        .where('srcpath = ? OR srcpath LIKE ?', entry_path, "#{entry_path.chomp('/')}/%")
        .pluck(:srcpath)

    flusher = VFS_FLUSHERS[session.project_id]
    root    = flusher&.root_path

    entry.destroy!
    descendant_paths.each { |p| Document.forget(p) }

    if root
      descendant_paths.each do |p|
        abs_path = File.join(root, p)
        flusher.suppress_set&.add(abs_path)
        begin
          FileUtils.rm_rf(abs_path)
        rescue => e
          puts "[fs] disk delete failed for #{abs_path}: #{e.class}: #{e.message}"
        ensure
          EM.add_timer(1) { flusher.suppress_set&.delete(abs_path) }
        end
      end
    end

    send_msg(session.ws, 'fs', 'deleted', { path: entry_path })
    peers = other_project_sessions(session)
    broadcast(peers, 'fs', 'deleted', { path: entry_path, user_id: session.user_id })
    DebugStream.emit(:fs, level: :info,
      message: "deleted #{entry_path}", project_id: session.project_id,
      meta: { path: entry_path, user_id: session.user_id, source: 'ws' }) if defined?(DebugStream)
  end
  register 'delete', :handle_delete

  # -------------------------------------------------------------------------
  # Import a public git repo into this project's on-disk root, then walk the
  # result into the DBFS.
  #
  # The clone + walk is blocking, so it runs on EM's deferred thread pool; the
  # completion callback runs back on the reactor thread where touching the
  # WebSocket connections is safe.
  def self.handle_import_git(session, payload)
    git_url = payload['git_url'].to_s.strip
    git_ref = payload['git_ref'].to_s.strip
    git_ref = nil if git_ref.empty?

    if git_url.empty? || !git_url.match?(/\A(https?:\/\/|git@)[^\s]+\z/)
      return send_msg(session.ws, 'fs', 'error',
                      { message: 'git_url must be an http(s):// or git@ URL' })
    end
    unless DirectoryEntry.project_empty?(session.project_id)
      return send_msg(session.ws, 'fs', 'error',
                      { message: 'project is not empty; refusing to import' })
    end

    root = VFS_FLUSHERS[session.project_id]&.root_path ||
           Project.find(session.project_id).project_setting&.root_path
    unless root
      return send_msg(session.ws, 'fs', 'error',
                      { message: 'no root_path configured for project' })
    end

    project_id = session.project_id
    user_id    = session.user_id
    send_msg(session.ws, 'fs', 'import_started', { git_url: git_url, git_ref: git_ref })
    puts "[fs] import_git project=#{project_id} url=#{git_url} ref=#{git_ref || '(default)'} -> #{root}"

    EM.defer(
      proc do
        ActiveRecord::Base.connection_pool.with_connection do
          do_import_git(project_id, user_id, root, git_url, git_ref)
        end
      end,
      proc do |result|
        if result[:ok]
          all = (SESSIONS_BY_PROJECT[project_id] || []).map(&:ws)
          broadcast(all, 'fs', 'created', { path: '/', reason: 'import_git' })
          send_msg(session.ws, 'fs', 'import_done', { stats: result[:stats] })
          puts "[fs] import_git project=#{project_id} done: #{result[:stats].inspect}"
        else
          send_msg(session.ws, 'fs', 'error', { message: result[:error] })
          puts "[fs] import_git project=#{project_id} failed: #{result[:error]}"
        end
      end
    )
  end
  register 'import_git', :handle_import_git

  # Blocking worker for handle_import_git. Runs on a deferred thread with its
  # own AR connection checked out. Returns a result hash.
  def self.do_import_git(project_id, user_id, root, git_url, git_ref)
    FileUtils.mkdir_p(root)
    unless Dir.empty?(root)
      return { ok: false, error: 'project root is not empty on disk' }
    end

    out, ok = clone_repo(git_url, git_ref, root)
    unless ok
      # Wipe the partial clone so the user can retry from a clean slate.
      FileUtils.rm_rf(Dir.glob(File.join(root, '*')) + Dir.glob(File.join(root, '.[!.]*')))
      return { ok: false, error: "clone failed: #{out.to_s.lines.last&.strip || out}" }
    end

    stats = FsLoader.new(project_id: project_id, root_path: root,
                         user_id: user_id, verbose: false).load!
    { ok: true, stats: stats }
  rescue => e
    { ok: false, error: "#{e.class}: #{e.message}" }
  end

  # Run `git clone --depth 1` with a hard timeout, killing the child if it
  # hangs. Returns [combined_output, success_bool].
  def self.clone_repo(git_url, git_ref, root)
    timeout = Integer(ENV.fetch('IMPORT_FROM_GIT_TIMEOUT_S', '1800'))
    cmd = ['git', 'clone', '--depth', '1']
    cmd += ['--branch', git_ref] if git_ref
    cmd += ['--', git_url, root]

    out_buf = +''
    _stdin, stdout_err, wait_thr = Open3.popen2e(*cmd)
    _stdin.close
    reader = Thread.new { stdout_err.each_line { |line| out_buf << line } }

    start = Time.now
    while wait_thr.alive?
      if Time.now - start > timeout
        Process.kill('TERM', wait_thr.pid) rescue nil
        sleep 2
        Process.kill('KILL', wait_thr.pid) rescue nil
        out_buf << "\n[clone_repo] timeout after #{timeout}s\n"
        break
      end
      sleep 0.5
    end
    reader.join(5)
    [out_buf, !!wait_thr.value&.success?]
  ensure
    stdout_err&.close
  end

  # -------------------------------------------------------------------------
  private_class_method

  def self.find_entry!(project_id, path)
    normalized = path.start_with?('/') ? path : "/#{path}"
    entry = DirectoryEntry.find_by_project_and_path(project_id, normalized)
    raise ActiveRecord::RecordNotFound, path unless entry
    entry
  end

  def self.other_project_sessions(session)
    (SESSIONS_BY_PROJECT[session.project_id] || [])
      .reject { |s| s.ws == session.ws }
      .map(&:ws)
  end
end
