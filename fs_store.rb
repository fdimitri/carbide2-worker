# FsStore — the 'fs' commandSet over WebSocket, on DBFS v2.
#
# Reads come from the project's DbfsV2::Store (served from the in-memory
# DocumentCache when warm). Writes append revisions to the file's DAG — OT
# transforms an edit whose base is behind the head — and the revisions AS
# PERSISTED are broadcast to co-viewers. Deletes are tombstones: the node is
# hidden, its history is kept, and recreating the path resurrects it.
#
# Supported commands (cs: 'fs'):
#   tree         — full file tree for the session's project
#   read         — current text content (+ head `revision`) for a file
#   read_binary  — base64 chunk of a file's live bytes on disk
#   stat         — stat-style metadata for a single node
#   open/close   — register/unregister as a viewer of a file
#   cursor       — broadcast this session's cursor to co-viewers
#   write        — apply one or more change operations to a file
#   set_contents — replace file content (diffed against the base, mergeable)
#   create_file  — create a file
#   create_dir   — create a directory (mkdir -p)
#   rename       — rename a file or directory
#   delete       — tombstone a node (and subtree), remove it from disk
#   import_git   — clone a repo into an empty project and load it
#
# `revision` values on the wire are revision UUID strings (PROTOCOL 6).

require 'base64'
require 'fileutils'
require 'open3'

module FsStore
  # Entry point — called by worker route() for cs == 'fs'
  def self.handle(session, cmd, payload, sessions_by_project, send_fn, broadcast_fn)
    case cmd
    when 'tree'
      handle_tree(session, send_fn)
    when 'read'
      handle_read(session, payload, send_fn)
    when 'read_binary'
      handle_read_binary(session, payload, send_fn)
    when 'stat'
      handle_stat(session, payload, send_fn)
    when 'open'
      handle_open(session, payload, send_fn)
    when 'close'
      handle_close(session, payload)
    when 'cursor'
      handle_cursor(session, payload, broadcast_fn)
    when 'write'
      handle_write(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'set_contents'
      handle_set_contents(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'create_file'
      handle_create_file(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'create_dir'
      handle_create_dir(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'rename'
      handle_rename(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'delete'
      handle_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'import_git'
      handle_import_git(session, payload, sessions_by_project, send_fn, broadcast_fn)
    else
      send_fn.call(session.ws, 'fs', 'error', { message: "unknown fs cmd: #{cmd}" })
    end
  rescue ActiveRecord::RecordNotFound => e
    send_fn.call(session.ws, 'fs', 'error', { message: "not found: #{e.message}" })
  rescue => e
    puts "[FsStore] ERROR #{e.class}: #{e.message}\n#{e.backtrace.first(3).join("\n")}"
    send_fn.call(session.ws, 'fs', 'error', { message: e.message })
  end

  # -------------------------------------------------------------------------
  # Handlers
  # -------------------------------------------------------------------------

  def self.handle_tree(session, send_fn)
    send_fn.call(session.ws, 'fs', 'tree', { tree: ProjectFs.tree_json(session.project_id) })
  end

  def self.handle_read(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    target = node.resolve
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'dangling symlink' }) unless target
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is binary — use read_binary' }) if target.binary?

    send_fn.call(session.ws, 'fs', 'content', {
      path:     node.path,
      content:  store_for(session).read(node.path),
      revision: ProjectFs.head_revision_id(target)
    })
  end

  # read_binary — stream a chunk of a file's live bytes from the working tree.
  # (The PVC is authoritative for binaries; archived revisions are served by the
  # REST blob endpoint with ?revision=.)
  # Payload: { path:, offset: 0, length: 65536 }
  # Reply:   { path:, offset:, length: (actual), size: (total), eof: bool, data: base64 }
  def self.handle_read_binary(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    flusher = VFS_FLUSHERS[session.project_id]
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'no disk root configured' }) unless flusher
    disk_path = ProjectFs.disk_path(flusher.root_path, node.path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'not present on disk' }) unless File.file?(disk_path)

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
    send_fn.call(session.ws, 'fs', 'binary_chunk', {
      path:   node.path,
      offset: offset,
      length: bytes.bytesize,
      size:   total,
      eof:    offset + bytes.bytesize >= total,
      data:   Base64.strict_encode64(bytes)
    })
  end

  # stat — metadata snapshot for the explorer Properties panel (#5).
  def self.handle_stat(session, payload, send_fn)
    path = payload['path'].to_s.strip
    find_node!(session.project_id, path)
    send_fn.call(session.ws, 'fs', 'stat', store_for(session).stat(path))
  end

  # open — register this session as viewing a file; receive its peer viewer list
  def self.handle_open(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    norm = node.path
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key] ||= OpenDocument.new(session.project_id, norm)
    doc.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.open_file(norm)

    send_fn.call(session.ws, 'fs', 'opened', { path: norm, viewers: doc.viewers })
  end

  # close — unregister this session from a file
  def self.handle_close(session, payload)
    norm = normalize(payload['path'])
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key]
    return unless doc

    doc.remove_client(session.ws)
    session.close_file(norm)
    OPEN_DOCUMENTS.delete(key) if doc.empty?
  end

  # cursor — update this session's cursor position and broadcast to co-viewers
  def self.handle_cursor(session, payload, broadcast_fn)
    norm = normalize(payload['path'])
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key]
    return unless doc&.member?(session.ws)

    line = payload['line'].to_i
    char = payload['char'].to_i
    doc.update_cursor(session.ws, line: line, char: char)

    broadcast_fn.call(doc.others(session.ws), 'fs', 'cursor', {
      path:    norm,
      user_id: session.user_id,
      name:    session.name,
      line:    line,
      char:    char
    })
  end

  # write — { path:, changes: [...], base_revision_id: (optional) }
  # Each change: { change_type:, change_data:, start_line:, start_char:, end_line:, end_char: }
  # change_data is the JSON payload DbfsV2::Delta parses ({startLine, startChar, ...}).
  #
  # Without base_revision_id every change is a blind append at the head (what
  # today's client sends). With it, the batch is anchored and chained; see
  # ProjectFs.write_batch!.
  def self.handle_write(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    changes = Array(payload['changes'])
    return send_fn.call(session.ws, 'fs', 'error', { path: path, message: 'no changes provided' }) if changes.empty?

    node = find_node!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    deltas =
      begin
        changes.map do |ch|
          data = ch['change_data']
          DbfsV2::Delta.parse(ch['change_type'].to_s, data.is_a?(Hash) ? data : data.to_s)
        end
      rescue JSON::ParserError => e
        return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: "bad change_data: #{e.message}", resync: true })
      end
    commit_and_broadcast(session, node.path, deltas, payload['base_revision_id'].presence, send_fn, broadcast_fn)
  end

  def self.handle_set_contents(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s.dup.force_encoding('UTF-8')
    content = content.scrub('') unless content.valid_encoding?
    node    = find_node!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    delta = DbfsV2::Delta.new('setContents', { data: content })
    commit_and_broadcast(session, node.path, [delta], payload['base_revision_id'].presence, send_fn, broadcast_fn)
  end

  # Persist `deltas`, reply fs/written to the author, broadcast each persisted
  # revision to the file's other viewers, and nudge the flusher.
  #
  # A ConflictError (an edit overlapping a concurrent write — decisions #7/#16)
  # or an ArgumentError (coordinates out of range for the base) commits nothing;
  # the author gets fs/error with conflict/resync set so it can re-read.
  def self.commit_and_broadcast(session, path, deltas, base_revision_id, send_fn, broadcast_fn)
    store = store_for(session)
    revs =
      begin
        ProjectFs.write_batch!(store, path, deltas, base_revision_id: base_revision_id, user_id: session.user_id)
      rescue DbfsV2::ConflictError => e
        return send_fn.call(session.ws, 'fs', 'error', { path: path, error: e.message, conflict: true, resync: true })
      rescue ArgumentError, JSON::ParserError, RegexpError, ActiveRecord::RecordInvalid => e
        return send_fn.call(session.ws, 'fs', 'error', { path: path, error: e.message, resync: true })
      end

    send_fn.call(session.ws, 'fs', 'written', { path: path, revisions: revs.map(&:id) })

    doc   = OPEN_DOCUMENTS["#{session.project_id}:#{path}"]
    peers = doc ? doc.others(session.ws) : []
    revs.each do |rev|
      cmd, frame = ProjectFs.revision_frame(path, rev, user_id: session.user_id)
      broadcast_fn.call(peers, 'fs', cmd, frame)
    end

    node = store.find(path)
    VFS_FLUSHERS[session.project_id]&.record_write(node.id, revs.sum { |r| r.change_data.to_s.bytesize }) if node
  end

  def self.handle_create_file(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = normalize(payload['path'])
    content = payload['content'].to_s
    store   = store_for(session)

    unless payload['mkdirp'] || store.find(File.dirname(path))
      return send_fn.call(session.ws, 'fs', 'error', { path: path, error: "Parent directory #{File.dirname(path)} does not exist" })
    end

    node = ProjectFs.ensure_file!(store, path, content: content, user_id: session.user_id)
    VFS_FLUSHERS[session.project_id]&.record_write(node.id, content.bytesize)

    send_fn.call(session.ws, 'fs', 'created', { path: node.path, type: 'file', id: node.id })
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'created', {
      path: node.path, type: 'file', id: node.id, user_id: session.user_id
    })
  end

  def self.handle_create_dir(session, payload, sessions_by_project, send_fn, broadcast_fn)
    node = ProjectFs.ensure_folder!(store_for(session), normalize(payload['path']), user_id: session.user_id)
    flusher = VFS_FLUSHERS[session.project_id]
    if flusher
      abs = ProjectFs.disk_path(flusher.root_path, node.path)
      flusher.suppress(abs) { FileUtils.mkdir_p(abs) }
    end

    send_fn.call(session.ws, 'fs', 'created', { path: node.path, type: 'folder', id: node.id })
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'created', {
      path: node.path, type: 'folder', id: node.id, user_id: session.user_id
    })
  end

  # rename — { path:, new_name: } (a basename, same parent). Files and folders:
  # DBFS v2 moves a folder by rewriting descendant paths; revisions stay keyed
  # to the same node ids, so history survives the rename.
  def self.handle_rename(session, payload, sessions_by_project, send_fn, broadcast_fn)
    new_name = payload['new_name'].to_s.strip
    if new_name.empty? || new_name.include?('/')
      return send_fn.call(session.ws, 'fs', 'error', { path: payload['path'], error: 'new_name must be a single path segment' })
    end

    node     = find_node!(session.project_id, payload['path'].to_s.strip)
    old_path = node.path
    new_path = File.join(File.dirname(old_path), new_name)
    moved    = store_for(session).move(old_path, new_path, user_id: session.user_id)

    flusher = VFS_FLUSHERS[session.project_id]
    if flusher
      from = ProjectFs.disk_path(flusher.root_path, old_path)
      to   = ProjectFs.disk_path(flusher.root_path, new_path)
      if File.exist?(from) || File.symlink?(from)
        flusher.suppress(from, to) { File.rename(from, to) }
      end
    end

    send_fn.call(session.ws, 'fs', 'renamed', { old_path: old_path, new_path: moved.path, id: moved.id })
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'renamed', {
      old_path: old_path, new_path: moved.path, id: moved.id, user_id: session.user_id
    })
  rescue RuntimeError => e
    send_fn.call(session.ws, 'fs', 'error', { path: payload['path'], error: e.message })
  end

  # delete — tombstone the node and its subtree (history kept), then remove it
  # from disk with the watcher suppressed for every descendant path.
  def self.handle_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    node = find_node!(session.project_id, payload['path'].to_s.strip)
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'cannot delete root' }) if node.root?

    entry_path = node.path
    prefix     = "#{entry_path.chomp('/')}/"
    descendant_paths =
      FileNode.live.where(project_id: session.project_id)
              .where('path = ? OR starts_with(path, ?)', entry_path, prefix)
              .pluck(:path)

    store_for(session).delete(entry_path, user_id: session.user_id)

    flusher = VFS_FLUSHERS[session.project_id]
    if flusher
      abs_paths = descendant_paths.map { |p| ProjectFs.disk_path(flusher.root_path, p) }
      flusher.suppress(*abs_paths) do
        FileUtils.rm_rf(ProjectFs.disk_path(flusher.root_path, entry_path))
      rescue => e
        puts "[FsStore] disk delete failed for #{entry_path}: #{e.class}: #{e.message}"
      end
    end

    send_fn.call(session.ws, 'fs', 'deleted', { path: entry_path })
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'deleted',
                      { path: entry_path, user_id: session.user_id })
    DebugStream.emit(:fs, level: :info,
      message: "deleted #{entry_path}", project_id: session.project_id,
      meta: { path: entry_path, user_id: session.user_id, source: 'ws' }) if defined?(DebugStream)
  end

  # -------------------------------------------------------------------------
  # Import a public git repo into this project's on-disk root, then walk the
  # result into the DBFS.
  #
  # This lives in the worker (not Rails) on purpose: the worker already owns
  # the project's on-disk root, the FsLoader, and the broadcast path, so it can
  # clone, ingest, and notify connected clients all in-process. A git clone is
  # a write-heavy burst that the inotify watcher cannot reliably keep up with
  # (children get written before the new recursive watch is registered), so we
  # do an authoritative FsLoader walk after the clone rather than trusting the
  # live event stream, then broadcast a single tree refresh.
  #
  # The clone + walk is blocking, so it runs on EM's deferred thread pool; the
  # completion callback runs back on the reactor thread where touching the
  # WebSocket connections is safe.
  def self.handle_import_git(session, payload, sessions_by_project, send_fn, broadcast_fn)
    git_url = payload['git_url'].to_s.strip
    git_ref = payload['git_ref'].to_s.strip
    git_ref = nil if git_ref.empty?

    if git_url.empty? || !git_url.match?(/\A(https?:\/\/|git@)[^\s]+\z/)
      return send_fn.call(session.ws, 'fs', 'error',
                          { message: 'git_url must be an http(s):// or git@ URL' })
    end
    unless ProjectFs.store(session.project_id).project_empty?
      return send_fn.call(session.ws, 'fs', 'error',
                          { message: 'project is not empty; refusing to import' })
    end

    root = VFS_FLUSHERS[session.project_id]&.root_path ||
           ProjectFs.root_path(Project.find(session.project_id))
    unless root
      return send_fn.call(session.ws, 'fs', 'error',
                          { message: 'no root_path configured for project' })
    end

    project_id = session.project_id
    user_id    = session.user_id
    send_fn.call(session.ws, 'fs', 'import_started', { git_url: git_url, git_ref: git_ref })
    puts "[FsStore] import_git project=#{project_id} url=#{git_url} ref=#{git_ref || '(default)'} -> #{root}"

    EM.defer(
      proc do
        ActiveRecord::Base.connection_pool.with_connection do
          do_import_git(project_id, user_id, root, git_url, git_ref)
        end
      end,
      proc do |result|
        if result[:ok]
          all = (sessions_by_project[project_id] || []).map(&:ws)
          broadcast_fn.call(all, 'fs', 'created', { path: '/', reason: 'import_git' })
          send_fn.call(session.ws, 'fs', 'import_done', { stats: result[:stats] })
          puts "[FsStore] import_git project=#{project_id} done: #{result[:stats].inspect}"
        else
          send_fn.call(session.ws, 'fs', 'error', { message: result[:error] })
          puts "[FsStore] import_git project=#{project_id} failed: #{result[:error]}"
        end
      end
    )
  end

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

    # Attributed to the system user like every import from the working tree;
    # the clone itself is the user's action.
    stats = FsLoader.new(project_id: project_id, root_path: root, verbose: false).load!
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

  def self.store_for(session)
    ProjectFs.store(session.project_id)
  end

  def self.normalize(path)
    p = path.to_s.strip
    p = "/#{p}" unless p.start_with?('/')
    p = p.chomp('/')
    p.empty? ? '/' : p
  end

  def self.find_node!(project_id, path)
    node = ProjectFs.store(project_id).find(path)
    raise ActiveRecord::RecordNotFound, path unless node
    node
  end

  def self.other_project_sessions(session, sessions_by_project)
    (sessions_by_project[session.project_id] || [])
      .reject { |s| s.ws == session.ws }
      .map(&:ws)
  end
end
