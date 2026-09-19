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
#   read         — current text content (+ head `revision`) for a file on a
#                  branch, or (`revision_id`) the content at one revision, pinned
#   read_binary  — base64 chunk of a file's live bytes on disk
#   stat         — stat-style metadata for a single node
#   open/close   — register/unregister as a viewer of a file on a branch
#   cursor       — broadcast this session's cursor to co-viewers of that branch
#   write        — apply one or more change operations to a file on a branch
#   set_contents — replace file content (diffed against the base, mergeable)
#   branches     — the file's branches with their heads
#   branch_create— fork a new branch of a file from another branch's head
#   branch_delete— drop a branch of a file (not main; not while others view it)
#   merge        — auto-merge one branch of a file into another
#   dag          — the file's revision DAG condensed for display (history rail)
#   create_file  — create a file
#   create_dir   — create a directory (mkdir -p)
#   rename       — rename a file or directory
#   delete       — tombstone a node (and subtree), remove it from disk
#   import_git   — clone a repo into an empty project and load it
#
# `revision` values on the wire are revision UUID strings (PROTOCOL 6).
#
# Branches are per file (DbfsV2: a Branch row per file_node). Every command
# that names a file takes an optional `branch` (default main), and a viewer is
# subscribed to one (path, branch): edits on a branch are only broadcast to
# viewers of that branch. Only main is flushed to disk (VfsFlusher), so branch
# edits live in the store until merged.

require 'base64'
require 'fileutils'
require 'open3'

module FsStore
  # Replies to recent fs/write batches, by "project:user:batch_id", so a
  # client retrying after a dropped socket gets the original answer.
  module RecentBatches
    TTL_S = 600
    MAX   = 2000
    @entries = {}
    @lock = Mutex.new

    def self.get(key)
      @lock.synchronize do
        e = @entries[key]
        e && Time.now - e[0] < TTL_S ? e[1] : nil
      end
    end

    def self.put(key, value)
      @lock.synchronize do
        @entries[key] = [Time.now, value]
        if @entries.size > MAX
          cutoff = Time.now - TTL_S
          @entries.delete_if { |_, (t, _)| t < cutoff }
          @entries.shift while @entries.size > MAX
        end
      end
    end
  end

  # Entry point — called by worker route() for cs == 'fs'
  def self.handle(session, cmd, payload, sessions_by_project, send_fn, broadcast_fn)
    case cmd
    when 'tree'
      handle_tree(session, payload, send_fn)
    when 'read'
      handle_read(session, payload, send_fn)
    when 'read_binary'
      handle_read_binary(session, payload, send_fn)
    when 'stat'
      handle_stat(session, payload, send_fn)
    when 'open'
      handle_open(session, payload, send_fn)
    when 'close'
      handle_close(session, payload, broadcast_fn)
    when 'cursor'
      handle_cursor(session, payload, broadcast_fn)
    when 'write'
      handle_write(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'set_contents'
      handle_set_contents(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'branches'
      handle_branches(session, payload, send_fn)
    when 'branch_create'
      handle_branch_create(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'branch_delete'
      handle_branch_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'project_branches'
      handle_project_branches(session, send_fn)
    when 'project_branch_create'
      handle_project_branch_create(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'project_branch_delete'
      handle_project_branch_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'project_branch_materialize'
      handle_project_branch_materialize(session, payload, sessions_by_project, send_fn, broadcast_fn)
    when 'project_dag'
      handle_project_dag(session, payload, send_fn)
    when 'project_merge_preview'
      handle_project_merge(session, payload, sessions_by_project, send_fn, broadcast_fn, dry_run: true)
    when 'project_merge'
      handle_project_merge(session, payload, sessions_by_project, send_fn, broadcast_fn, dry_run: false)
    when 'merge'
      handle_merge(session, payload, send_fn, broadcast_fn)
    when 'merge_preview'
      handle_merge_preview(session, payload, send_fn)
    when 'merge_resolve'
      handle_merge_resolve(session, payload, send_fn, broadcast_fn)
    when 'dag'
      handle_dag(session, payload, send_fn)
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
    send_fn.call(session.ws, 'fs', 'error', error_frame(cmd, payload, "not found: #{e.message}"))
  rescue => e
    puts "[FsStore] ERROR #{e.class}: #{e.message}\n#{e.backtrace.first(3).join("\n")}"
    send_fn.call(session.ws, 'fs', 'error', error_frame(cmd, payload, e.message))
  end

  WRITE_CMDS = %w[write set_contents].freeze

  # An error the client can attribute: it echoes the path, branch and batch_id
  # of the command that failed. For a write it also says resync — nothing was
  # committed and the client's queue is based on a view it should re-read.
  def self.error_frame(cmd, payload, message)
    frame = { message: message, error: message }
    if payload.is_a?(Hash)
      frame[:path]     = payload['path'] if payload['path']
      frame[:branch]   = branch_of(payload) if payload['path']
      frame[:batch_id] = payload['batch_id'] if payload['batch_id']
    end
    frame[:resync] = true if WRITE_CMDS.include?(cmd)
    frame
  end

  # -------------------------------------------------------------------------
  # Handlers
  # -------------------------------------------------------------------------

  # tree — { branch? } the whole tree as main (tree_json) or a project branch
  # (its index) has it. The reply names the branch so an explorer can ignore a
  # tree for a view it is not showing.
  def self.handle_tree(session, payload, send_fn)
    store  = store_for(session)
    branch = view_branch(store, payload)
    tree   = branch == Branch::MAIN ? ProjectFs.tree_json(session.project_id) : store.tree('/', branch: branch)
    send_fn.call(session.ws, 'fs', 'tree', { tree: tree, branch: branch })
  end

  def self.handle_read(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    target = node.resolve
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'dangling symlink' }) unless target
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is binary — use read_binary' }) if target.binary?

    branch = branch_of(payload)
    # A project branch reads a file it has not written from its pin: no
    # per-file row yet is fine there.
    unless store_for(session).project_branch(branch) || target.branches.exists?(name: branch)
      return send_fn.call(session.ws, 'fs', 'error', { path: node.path, branch: branch, error: "no branch #{branch} on #{node.path}" })
    end

    # A pinned read: the content AT one revision (history view). Not a branch
    # head, so the reply says `pinned` and a client must not base edits on it.
    if (rev = payload['revision_id'].presence)
      unless Revision.exists?(id: rev, file_node_id: target.id)
        return send_fn.call(session.ws, 'fs', 'error', { path: node.path, branch: branch, error: "no revision #{rev} of #{node.path}" })
      end
      return send_fn.call(session.ws, 'fs', 'content', {
        path: node.path, branch: branch, pinned: true, revision: rev,
        content: store_for(session).read(node.path, revision_id: rev, branch: branch)
      })
    end

    send_fn.call(session.ws, 'fs', 'content', {
      path:     node.path,
      branch:   branch,
      content:  store_for(session).read(node.path, branch: branch),
      revision: ProjectFs.head_revision_id(target, branch)
    })
  end

  # read_binary — stream a chunk of a file's live bytes from the working tree.
  # (The PVC is authoritative for binaries; archived revisions are served by the
  # REST blob endpoint with ?revision=.)
  # Payload: { path:, offset: 0, length: 65536 }
  # Reply:   { path:, offset:, length: (actual), size: (total), eof: bool, data: base64 }
  def self.handle_read_binary(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    flusher = flusher_for(session.project_id, branch_of(payload))
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
    find_node!(session.project_id, path, branch_of(payload))
    send_fn.call(session.ws, 'fs', 'stat', store_for(session).stat(path, branch: branch_of(payload)))
  end

  # open — register this session as viewing a file on a branch; receive that
  # branch's peer viewer list
  def self.handle_open(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    norm   = node.path
    branch = branch_of(payload)
    key    = doc_key(session.project_id, norm, branch)
    doc    = OPEN_DOCUMENTS[key] ||= OpenDocument.new(session.project_id, norm, branch)
    doc.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.open_file(key)

    send_fn.call(session.ws, 'fs', 'opened', { path: norm, branch: branch, viewers: doc.viewers })
  end

  # close — unregister this session from a file on a branch. The remaining
  # viewers are told (fs/viewer_left) so they drop this session's cursor.
  def self.handle_close(session, payload, broadcast_fn)
    key = doc_key(session.project_id, normalize(payload['path']), branch_of(payload))
    doc = OPEN_DOCUMENTS[key]
    return unless doc

    leave_document(session, key, doc, broadcast_fn)
  end

  # Shared by fs/close and Session#cleanup (disconnect).
  def self.leave_document(session, key, doc, broadcast_fn)
    return unless doc.member?(session.ws)

    doc.remove_client(session.ws)
    session.close_file(key)
    if doc.empty?
      OPEN_DOCUMENTS.delete(key)
    elsif broadcast_fn
      broadcast_fn.call(doc.clients.keys, 'fs', 'viewer_left', {
        path: doc.path, branch: doc.branch, user_id: session.user_id, name: session.name
      })
    end
  end

  # cursor — update this session's cursor position and broadcast to co-viewers
  # of the same branch
  def self.handle_cursor(session, payload, broadcast_fn)
    norm   = normalize(payload['path'])
    branch = branch_of(payload)
    doc    = OPEN_DOCUMENTS[doc_key(session.project_id, norm, branch)]
    return unless doc&.member?(session.ws)

    line = payload['line'].to_i
    char = payload['char'].to_i
    doc.update_cursor(session.ws, line: line, char: char)

    broadcast_fn.call(doc.others(session.ws), 'fs', 'cursor', {
      path:    norm,
      branch:  branch,
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
  # Without base_revision_id every change is a blind append at the head (older
  # clients). With it, the batch is based on that revision: appended if it is
  # still the head, otherwise auto-branched there and rebased onto main; see
  # ProjectFs.write_batch!.
  def self.handle_write(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    changes = Array(payload['changes'])
    return send_fn.call(session.ws, 'fs', 'error', { path: path, message: 'no changes provided' }) if changes.empty?

    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    deltas =
      begin
        changes.map do |ch|
          data = ch['change_data']
          DbfsV2::Delta.parse(ch['change_type'].to_s, data.is_a?(Hash) ? data : data.to_s)
        end
      rescue JSON::ParserError => e
        return send_fn.call(session.ws, 'fs', 'error', { path: node.path, branch: branch_of(payload), batch_id: payload['batch_id'],
                                                         error: "bad change_data: #{e.message}", resync: true })
      end
    commit_and_broadcast(session, node.path, deltas, payload['base_revision_id'].presence, send_fn, broadcast_fn,
                         batch_id: payload['batch_id'].presence, branch: branch_of(payload))
  end

  def self.handle_set_contents(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s.dup.force_encoding('UTF-8')
    content = content.scrub('') unless content.valid_encoding?
    node    = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    delta = DbfsV2::Delta.new('setContents', { data: content })
    commit_and_broadcast(session, node.path, [delta], payload['base_revision_id'].presence, send_fn, broadcast_fn,
                         batch_id: payload['batch_id'].presence, branch: branch_of(payload))
  end

  # Persist `deltas` on `branch` (ProjectFs.write_batch!: blind, anchored
  # append, or auto-branch + rebase), reply fs/written to the author, send the
  # other viewers of that branch one frame per revision that landed, and — on
  # main only — nudge the flusher.
  #
  # Failures commit nothing on the branch and come back as fs/error with resync
  # set: conflict: true when an anchored batch overlapped a concurrent replace
  # (its revisions are kept on `auto_branch`), or when the base is unknown.
  #
  # `batch_id` (client-chosen, optional) makes a retried batch idempotent: a
  # client that lost its connection before the reply resends the same batch_id,
  # and gets the original reply instead of the batch being applied twice. The
  # memory is per worker process (RecentBatches), so it covers socket drops, not
  # a worker restart.
  def self.commit_and_broadcast(session, path, deltas, base_revision_id, send_fn, broadcast_fn,
                                batch_id: nil, branch: Branch::MAIN)
    key = batch_id && "#{session.project_id}:#{session.user_id}:#{batch_id}"
    if key && (replay = RecentBatches.get(key))
      return send_fn.call(session.ws, 'fs', replay[0], replay[1])
    end
    reply = lambda do |cmd, frame|
      frame = frame.merge(batch_id: batch_id) if batch_id
      RecentBatches.put(key, [cmd, frame]) if key
      send_fn.call(session.ws, 'fs', cmd, frame)
    end

    store = store_for(session)
    node  = store.resolve(path)
    result =
      begin
        ProjectFs.write_batch!(store, path, deltas, base_revision_id: base_revision_id, user_id: session.user_id,
                               branch: branch)
      rescue ProjectFs::BranchConflict => e
        return reply.call('error', { path: path, branch: branch, error: e.message, conflict: true, resync: true,
                                     auto_branch: e.branch, auto_branch_head: e.branch_head })
      rescue DbfsV2::ConflictError => e
        return reply.call('error', { path: path, branch: branch, error: e.message, conflict: true, resync: true })
      rescue ArgumentError, JSON::ParserError, RegexpError, ActiveRecord::RecordInvalid, ActiveRecord::RecordNotFound => e
        return reply.call('error', { path: path, branch: branch, error: e.message, resync: true })
      end

    reply.call('written', ProjectFs.batch_ack(path, result, node))

    doc   = OPEN_DOCUMENTS[doc_key(session.project_id, path, branch)]
    peers = doc ? doc.others(session.ws) : []
    unless peers.empty?
      ProjectFs.batch_peer_frames(path, result, node, user_id: session.user_id).each do |cmd, frame|
        broadcast_fn.call(peers, 'fs', cmd, frame)
      end
    end

    flusher_for(session.project_id, branch)&.record_write(node.id, result.revisions.sum { |r| r.change_data.to_s.bytesize })
  end

  # branches — { path } -> fs/branches { path, branches: [{ name, head }] }
  def self.handle_branches(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    send_fn.call(session.ws, 'fs', 'branches', { path: node.path, branches: store_for(session).branches(node.path, branch: branch_of(payload)) })
  end

  BRANCH_NAME = %r{\A(?!auto/)[A-Za-z0-9][A-Za-z0-9._/-]{0,127}\z}

  # branch_create — { path, name, from?, at_revision? } forks `name` at `from`'s
  # head (default main), or at `at_revision` (any revision of the file: the
  # history rail's "branch from here"). Idempotent on an existing name (its
  # head is returned, not moved).
  # Replies fs/branch_created to the caller and tells every other session of
  # the project, so open pickers for the file can refresh.
  def self.handle_branch_create(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path = payload['path'].to_s.strip
    name = payload['name'].to_s.strip
    from = payload['from'].presence || Branch::MAIN
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'
    unless BRANCH_NAME.match?(name)
      return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: "bad branch name #{name.inspect}" })
    end

    b = store_for(session).branch(node.path, name, from: from, at_revision: payload['at_revision'].presence,
                                  branch: branch_of(payload))
    frame = { path: node.path, name: b.name, head: b.head_revision_id, from: from, user_id: session.user_id }
    send_fn.call(session.ws, 'fs', 'branch_created', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'branch_created', frame)
  end

  # branch_delete — { path, name } drops a branch of the file. main cannot be
  # deleted, nor a branch another session is viewing (they would be editing a
  # branch that no longer exists); the caller's own view of it is fine — it
  # moves to main when fs/branch_deleted arrives. History is kept (the store
  # re-homes the branch's revisions to main). Replies fs/branch_deleted to the
  # caller and every other session of the project.
  def self.handle_branch_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path = payload['path'].to_s.strip
    name = payload['name'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: "cannot delete #{Branch::MAIN}" }) if name == Branch::MAIN

    doc    = OPEN_DOCUMENTS[doc_key(session.project_id, node.path, name)]
    others = doc ? doc.others(session.ws) : []
    unless others.empty?
      return send_fn.call(session.ws, 'fs', 'error', {
        path: node.path, error: "branch #{name} is open by #{others.size} other viewer#{'s' if others.size != 1}"
      })
    end

    store_for(session).delete_branch(node.path, name, branch: branch_of(payload))
    frame = { path: node.path, name: name, user_id: session.user_id }
    send_fn.call(session.ws, 'fs', 'branch_deleted', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'branch_deleted', frame)
  end

  # dag — { path, gap_ms?, auto? } -> fs/dag: DbfsV2::Graph.condense's
  # { path, gap_ms, auto, heads, nodes, edges } plus `users` { id => display
  # name } for the user_ids that appear. gap_ms (default 3000; 0 = topology
  # only) splits one user's run at a pause; auto: true shows the auto-branches
  # a rebase leaves behind instead of folding them (debugging the rebase path).
  DAG_DEFAULT_GAP_MS = 3000

  def self.handle_dag(session, payload, send_fn)
    path = payload['path'].to_s.strip
    node = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'

    gap  = payload.key?('gap_ms') ? payload['gap_ms'].to_i : DAG_DEFAULT_GAP_MS
    auto = payload['auto'] == true
    g    = store_for(session).dag_condensed(node.path, gap_ms: gap, auto: auto, branch: branch_of(payload))
    ids  = g[:nodes].map { |n| n[:user_id] }.compact.uniq
    g[:users] = User.where(id: ids).to_h { |u| [u.id, u.display_name] }
    send_fn.call(session.ws, 'fs', 'dag', g)
  end

  # --- project branches (ADR-042) ---------------------------------------------
  #
  # project_branches -> fs/project_branches { branches: [{ id, name,
  # forked_from, fork_seq, seq, deleted, materialized }] }, main first.
  def self.handle_project_branches(session, send_fn)
    send_fn.call(session.ws, 'fs', 'project_branches',
                 { branches: store_for(session).project_branches.map { |b| branch_h(b) } })
  end

  # A branch on the wire: ProjectBranch#to_h plus `disk`, its directory
  # relative to the project root when materialized (nil otherwise).
  def self.branch_h(pb)
    pb.to_h.merge(disk: BranchMirrors.relative_dir(pb))
  end

  # project_branch_create — { name, from? } forks a project branch off `from`
  # (default main): a full copy of its tree, content pinned at the fork.
  # Replies fs/project_branch_created { branch } and tells the project.
  def self.handle_project_branch_create(session, payload, sessions_by_project, send_fn, broadcast_fn)
    name = payload['name'].to_s.strip
    from = payload['from'].presence || Branch::MAIN
    unless BRANCH_NAME.match?(name) && name != Branch::MAIN
      return send_fn.call(session.ws, 'fs', 'error', { error: "bad branch name #{name.inspect}" })
    end
    store = store_for(session)
    if store.project_branch(name)
      return send_fn.call(session.ws, 'fs', 'error', { error: "project branch #{name} exists" })
    end

    pb = store.create_project_branch(name, from: from, user_id: session.user_id)
    frame = { branch: branch_h(pb), user_id: session.user_id }
    send_fn.call(session.ws, 'fs', 'project_branch_created', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'project_branch_created', frame)
  rescue ActiveRecord::RecordNotFound
    send_fn.call(session.ws, 'fs', 'error', { error: "no project branch #{from}" })
  end

  # project_branch_delete — { name } tombstones the branch (history kept, name
  # free). Replies fs/project_branch_deleted { name, id } and tells the project.
  def self.handle_project_branch_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    name = payload['name'].to_s.strip
    return send_fn.call(session.ws, 'fs', 'error', { error: "cannot delete #{Branch::MAIN}" }) if name == Branch::MAIN
    BranchMirrors.stop!(session.project_id, name)
    pb = store_for(session).delete_project_branch(name)
    frame = { name: name, id: pb.id, user_id: session.user_id }
    send_fn.call(session.ws, 'fs', 'project_branch_deleted', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'project_branch_deleted', frame)
  rescue ActiveRecord::RecordNotFound
    send_fn.call(session.ws, 'fs', 'error', { error: "no project branch #{name}" })
  end

  # project_branch_materialize — { name, on: true|false } puts a branch's tree
  # on disk at <project root>/.branches/<branch uuid> with its own
  # flusher/watcher pair (BranchMirrors), or takes it off again (the directory
  # is removed; the DBFS keeps everything). Replies — once the pair is live —
  # fs/project_branch_materialized { branch, on, user_id } and tells the
  # project. Main is always on disk.
  def self.handle_project_branch_materialize(session, payload, sessions_by_project, send_fn, broadcast_fn)
    name = payload['name'].to_s.strip
    on   = payload['on'] != false
    return send_fn.call(session.ws, 'fs', 'error', { error: "#{Branch::MAIN} is always on disk" }) if name == Branch::MAIN
    store = store_for(session)
    pb    = store.project_branch(name)
    return send_fn.call(session.ws, 'fs', 'error', { error: "no project branch #{name}" }) unless pb

    announce = lambda do
      pb.reload
      frame = { branch: branch_h(pb), on: pb.materialized?, user_id: session.user_id }
      send_fn.call(session.ws, 'fs', 'project_branch_materialized', frame)
      broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'project_branch_materialized', frame)
    end

    if on
      project = Project.find(session.project_id)
      BranchMirrors.start!(project, pb,
                           sessions_by_project: sessions_by_project, broadcast_fn: broadcast_fn,
                           suppress_set: defined?(VFS_FLUSH_SUPPRESS) ? VFS_FLUSH_SUPPRESS : nil,
                           on_ready: lambda do |_mirror, err|
                             if err
                               send_fn.call(session.ws, 'fs', 'error', { error: "materialize #{name}: #{err.message}" })
                             else
                               announce.call
                             end
                           end)
    else
      BranchMirrors.stop!(session.project_id, name)
      announce.call
    end
  rescue Project::WorkspaceUuidMissing => e
    send_fn.call(session.ws, 'fs', 'error', { error: e.message })
  end

  # project_dag — { gap_ms? } the project's branches as a rail graph
  # (ProjectGraph.build): fs/project_dag { gap_ms, heads, branches, nodes,
  # edges, users }.
  def self.handle_project_dag(session, payload, send_fn)
    gap = payload.key?('gap_ms') ? payload['gap_ms'].to_i : DAG_DEFAULT_GAP_MS
    g   = store_for(session).project_graph(gap_ms: gap)
    ids = g[:nodes].map { |n| n[:user_id] }.compact.uniq
    g[:users] = User.where(id: ids).to_h { |u| [u.id, u.display_name] }
    send_fn.call(session.ws, 'fs', 'project_dag', g)
  end

  # project_merge_preview / project_merge — { source, target?, resolutions? }
  # merges project branch `source` into `target` (default main; either may be
  # the other's parent). Preview plans and applies in a transaction that
  # rolls back; merge commits. Reply fs/project_merge_preview |
  # fs/project_merged { merged, source, target, base: { branch, seq }, seq,
  # actions: [{ kind: delete|move|add|content, ... }], conflicts: [{ id, kind,
  # ours: { path, revision_id }, theirs: {...}, detail }] }. `resolutions` is
  # { node_id => { action: ours|theirs|path, path? } } for identity
  # conflicts; content conflicts are settled with fs/merge_resolve first. A
  # committed merge is announced to the project (fs/project_merged, so
  # explorers on the target refresh), open viewers of changed files on the
  # target get fs/set_contents, and main's disk mirror follows.
  def self.handle_project_merge(session, payload, sessions_by_project, send_fn, broadcast_fn, dry_run:)
    store  = store_for(session)
    source = payload['source'].to_s.strip
    target = payload['target'].presence || Branch::MAIN
    res    = store.merge_branches(source: source, target: target, resolutions: payload['resolutions'] || {},
                                  user_id: session.user_id, dry_run: dry_run)
    if dry_run
      return send_fn.call(session.ws, 'fs', 'project_merge_preview', res)
    end

    frame = res.merge(user_id: session.user_id)
    send_fn.call(session.ws, 'fs', 'project_merged', frame)
    return unless res[:merged]

    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'project_merged', frame)
    announce_project_merge(session, store, res, broadcast_fn)
  rescue ArgumentError => e
    send_fn.call(session.ws, 'fs', 'error', { source: source, target: target, error: e.message })
  end

  # After a committed project merge: viewers of files whose content moved on
  # the target hear the new text; when the target is on disk (main, or a
  # materialized branch) disk follows the applied actions in order.
  def self.announce_project_merge(session, store, res, broadcast_fn)
    target  = res[:target]
    flusher = flusher_for(session.project_id, target)
    state   = store.state(branch: target)

    res[:actions].each do |a|
      case a[:kind]
      when 'delete'
        next unless flusher
        abs = ProjectFs.disk_path(flusher.root_path, a[:path])
        flusher.suppress(abs) { FileUtils.rm_rf(abs) }
      when 'move'
        next unless flusher
        from = ProjectFs.disk_path(flusher.root_path, a[:from])
        to   = ProjectFs.disk_path(flusher.root_path, a[:to])
        next unless File.exist?(from) || File.symlink?(from)
        flusher.suppress(from, to) { FileUtils.mkdir_p(File.dirname(to)); File.rename(from, to) }
      when 'add'
        next unless flusher
        if a[:ftype] == 'folder'
          abs = ProjectFs.disk_path(flusher.root_path, a[:path])
          flusher.suppress(abs) { FileUtils.mkdir_p(abs) }
        else
          flusher.record_write(a[:id], 0)
        end
      when 'content'
        entry = state.entries[a[:id]]
        next unless entry
        flusher&.record_write(a[:id], 0)
        doc = OPEN_DOCUMENTS[doc_key(session.project_id, entry.path, target)]
        next unless doc && !doc.empty?
        broadcast_fn.call(doc.clients.keys, 'fs', 'set_contents', {
          path: entry.path, branch: target, content: store.read(entry.path, branch: target),
          revision: a[:head], parent: a[:ours_revision], user_id: session.user_id,
          source: "merge #{res[:source]}"
        })
      end
    end
  end

  # merge — { path, source, target? } auto-merges `source` into `target`
  # (default main): fast-forward, replay of the source's edits as OT, or a
  # three-way content merge; conflicts are refused. Replies fs/merged
  # { path, source, target, merged, head?, reason?, conflicts? }. When something
  # landed, viewers of the target get one set_contents frame chained to the head
  # they held, and main's flusher is nudged.
  def self.handle_merge(session, payload, send_fn, broadcast_fn)
    path   = payload['path'].to_s.strip
    source = payload['source'].to_s.strip
    target = payload['target'].presence || Branch::MAIN
    node   = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'source and target are the same branch' }) if source == target

    store    = store_for(session)
    resolved = node.resolve || node
    old_head = ProjectFs.head_revision_id(resolved, target)
    res      = store.merge(node.path, target: target, source: source, auto: true, user_id: session.user_id,
                           branch: branch_of(payload))

    reply = { path: node.path, source: source, target: target, merged: res[:merged] == true }
    unless reply[:merged]
      reply[:reason]    = res[:reason]
      reply[:error]     = res[:error] if res[:error]
      reply[:conflicts] = res[:conflicts] if res[:conflicts]
      return send_fn.call(session.ws, 'fs', 'merged', reply)
    end

    announce_merge(session, store, node, resolved, source, target, old_head, reply, send_fn, broadcast_fn)
  end

  # merge_preview — { path, source, target? } -> fs/merge_preview: the
  # three-way view for a merge tab (DbfsV2::Merge.preview): base / ours
  # (target) / theirs (source) with their revisions, the regions auto-merge
  # refused on, `clean`, and `merged` — the exact auto-merge result when clean,
  # else a diff3 text with markers plus `conflict_blocks` locating them.
  def self.handle_merge_preview(session, payload, send_fn)
    path   = payload['path'].to_s.strip
    source = payload['source'].to_s.strip
    target = payload['target'].presence || Branch::MAIN
    node   = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'source and target are the same branch' }) if source == target

    p = store_for(session).merge_preview(node.path, target: target, source: source, branch: branch_of(payload))
    send_fn.call(session.ws, 'fs', 'merge_preview', p.merge(path: node.path))
  end

  # merge_resolve — { path, source, target?, content, expected_head,
  # expected_source_head } commits a human's resolution as a merge commit on
  # target with source's head as second parent (ADR-036). Both heads are
  # pinned to what the preview showed: if either advanced since, nothing is
  # committed and the reply is fs/merged { merged: false, reason: 'stale',
  # error } so the client re-previews. On success the reply and viewer
  # broadcast are the same as fs/merge.
  def self.handle_merge_resolve(session, payload, send_fn, broadcast_fn)
    path   = payload['path'].to_s.strip
    source = payload['source'].to_s.strip
    target = payload['target'].presence || Branch::MAIN
    node   = find_node!(session.project_id, path, branch_of(payload))
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if node.ftype == 'folder'
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'source and target are the same branch' }) if source == target
    unless payload.key?('content') && payload['expected_head'].present?
      return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'content and expected_head are required' })
    end

    store    = store_for(session)
    resolved = node.resolve || node
    old_head = payload['expected_head']
    reply    = { path: node.path, source: source, target: target }
    begin
      store.merge(node.path, target: target, source: source, resolved: payload['content'].to_s,
                             user_id: session.user_id, expected_head: old_head,
                             expected_source_head: payload['expected_source_head'].presence,
                             branch: branch_of(payload))
    rescue DbfsV2::ConflictError => e
      return send_fn.call(session.ws, 'fs', 'merged', reply.merge(merged: false, reason: 'stale', error: e.message))
    end

    announce_merge(session, store, node, resolved, source, target, old_head, reply.merge(merged: true), send_fn, broadcast_fn)
  end

  # After something landed on `target`: reply fs/merged with the new head,
  # give the target's viewers one set_contents chained to the head they held,
  # and nudge main's flusher.
  def self.announce_merge(session, store, node, resolved, source, target, old_head, reply, send_fn, broadcast_fn)
    head = ProjectFs.head_revision_id(resolved, target)
    reply[:head] = head
    send_fn.call(session.ws, 'fs', 'merged', reply)

    doc = OPEN_DOCUMENTS[doc_key(session.project_id, node.path, target)]
    if doc && !doc.empty? && head != old_head
      broadcast_fn.call(doc.clients.keys, 'fs', 'set_contents', {
        path: node.path, branch: target, content: store.read(node.path, branch: target),
        revision: head, parent: old_head, user_id: session.user_id, source: "merge #{source}"
      })
    end

    flusher_for(session.project_id, target)&.record_write(resolved.id, 0)
  end

  # Existence ops take `branch` (a project branch; default main). Disk follows
  # for main and for any branch the user has materialized (BranchMirrors);
  # otherwise a branch's tree lives in the DBFS alone.
  def self.handle_create_file(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = normalize(payload['path'])
    content = payload['content'].to_s
    store   = store_for(session)
    branch  = view_branch(store, payload)

    unless payload['mkdirp'] || store.find(File.dirname(path), branch: branch)
      return send_fn.call(session.ws, 'fs', 'error', { path: path, branch: branch, error: "Parent directory #{File.dirname(path)} does not exist" })
    end

    node = ProjectFs.ensure_file!(store, path, content: content, user_id: session.user_id, branch: branch)
    flusher_for(session.project_id, branch)&.record_write(node.id, content.bytesize)

    frame = { path: node.path, type: 'file', id: node.id, branch: branch }
    send_fn.call(session.ws, 'fs', 'created', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'created', frame.merge(user_id: session.user_id))
  end

  def self.handle_create_dir(session, payload, sessions_by_project, send_fn, broadcast_fn)
    store  = store_for(session)
    branch = view_branch(store, payload)
    node   = ProjectFs.ensure_folder!(store, normalize(payload['path']), user_id: session.user_id, branch: branch)
    flusher = flusher_for(session.project_id, branch)
    if flusher
      abs = ProjectFs.disk_path(flusher.root_path, node.path)
      flusher.suppress(abs) { FileUtils.mkdir_p(abs) }
    end

    frame = { path: node.path, type: 'folder', id: node.id, branch: branch }
    send_fn.call(session.ws, 'fs', 'created', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'created', frame.merge(user_id: session.user_id))
  end

  # rename — { path:, new_name: } (a basename, same parent). Files and folders:
  # DBFS v2 moves a folder by rewriting descendant paths; revisions stay keyed
  # to the same node ids, so history survives the rename.
  def self.handle_rename(session, payload, sessions_by_project, send_fn, broadcast_fn)
    new_name = payload['new_name'].to_s.strip
    if new_name.empty? || new_name.include?('/')
      return send_fn.call(session.ws, 'fs', 'error', { path: payload['path'], error: 'new_name must be a single path segment' })
    end

    store    = store_for(session)
    branch   = view_branch(store, payload)
    node     = find_node!(session.project_id, payload['path'].to_s.strip, branch)
    old_path = node.path
    new_path = File.join(File.dirname(old_path), new_name)
    moved    = store.move(old_path, new_path, user_id: session.user_id, branch: branch)
    rekey_open_documents!(session.project_id, old_path, moved.path, branch, sessions_by_project)

    flusher = flusher_for(session.project_id, branch)
    if flusher
      from = ProjectFs.disk_path(flusher.root_path, old_path)
      to   = ProjectFs.disk_path(flusher.root_path, new_path)
      if File.exist?(from) || File.symlink?(from)
        flusher.suppress(from, to) { File.rename(from, to) }
      end
    end

    frame = { old_path: old_path, new_path: moved.path, id: moved.id, branch: branch }
    send_fn.call(session.ws, 'fs', 'renamed', frame)
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'renamed', frame.merge(user_id: session.user_id))
  rescue RuntimeError => e
    send_fn.call(session.ws, 'fs', 'error', { path: payload['path'], error: e.message })
  end

  # delete — tombstone the node and its subtree (history kept), then remove it
  # from disk with the watcher suppressed for every descendant path.
  def self.handle_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    store  = store_for(session)
    branch = view_branch(store, payload)
    node   = find_node!(session.project_id, payload['path'].to_s.strip, branch)
    return send_fn.call(session.ws, 'fs', 'error', { path: node.path, error: 'cannot delete root' }) if node.root?

    entry_path = node.path
    descendant_paths = flatten_tree(store.tree(entry_path, branch: branch))

    store.delete(entry_path, user_id: session.user_id, branch: branch)

    flusher = flusher_for(session.project_id, branch)
    if flusher
      abs_paths = descendant_paths.map { |p| ProjectFs.disk_path(flusher.root_path, p) }
      flusher.suppress(*abs_paths) do
        FileUtils.rm_rf(ProjectFs.disk_path(flusher.root_path, entry_path))
      rescue => e
        puts "[FsStore] disk delete failed for #{entry_path}: #{e.class}: #{e.message}"
      end
    end

    send_fn.call(session.ws, 'fs', 'deleted', { path: entry_path, branch: branch })
    broadcast_fn.call(other_project_sessions(session, sessions_by_project), 'fs', 'deleted',
                      { path: entry_path, branch: branch, user_id: session.user_id })
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
        begin
          ActiveRecord::Base.connection_pool.with_connection do
            do_import_git(project_id, user_id, root, git_url, git_ref)
          end
        ensure
          worker_release_db! if defined?(worker_release_db!)
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

  # The flusher mirroring (project, branch) to disk: main's, a materialized
  # branch's, or nil when that branch's tree is not on disk. `branch` may
  # also be a per-file content branch name (merge targets) — those are only
  # on disk when they are a materialized project branch of the same name.
  def self.flusher_for(project_id, branch)
    b = branch.to_s.presence || Branch::MAIN
    return VFS_FLUSHERS[project_id] if b == Branch::MAIN
    BranchMirrors.flusher_for(project_id, b)
  end

  def self.flatten_tree(t)
    return [] unless t
    [t[:path]] + (t[:children] || []).flat_map { |c| flatten_tree(c) }
  end

  def self.normalize(path)
    p = path.to_s.strip
    p = "/#{p}" unless p.start_with?('/')
    p = p.chomp('/')
    p.empty? ? '/' : p
  end

  # The branch a command names, main when it doesn't.
  def self.branch_of(payload)
    payload.is_a?(Hash) ? (payload['branch'].to_s.strip.presence || Branch::MAIN) : Branch::MAIN
  end

  # OPEN_DOCUMENTS key: one viewer set per (project, path, branch). Path is the
  # location; identity of the file is FileNode UUID (stable across rename).
  def self.doc_key(project_id, path, branch = Branch::MAIN)
    "#{project_id}:#{path}@#{branch}"
  end

  # A rename keeps the FileNode id and the viewer set; only the path in the
  # key (and on the OpenDocument) moves, including descendants of a folder.
  def self.rekey_open_documents!(project_id, old_path, new_path, branch, sessions_by_project)
    old_path = normalize(old_path)
    new_path = normalize(new_path)
    return if old_path == new_path
    mapping = {}
    OPEN_DOCUMENTS.keys.each do |key|
      doc = OPEN_DOCUMENTS[key]
      next unless doc && doc.project_id == project_id && doc.branch == branch
      next unless doc.path == old_path || doc.path.start_with?("#{old_path}/")
      newp = doc.path == old_path ? new_path : "#{new_path}#{doc.path.delete_prefix(old_path)}"
      new_key = doc_key(project_id, newp, branch)
      next if new_key == key
      mapping[key] = new_key
      doc.relocate(newp)
      OPEN_DOCUMENTS.delete(key)
      OPEN_DOCUMENTS[new_key] = doc
    end
    return if mapping.empty?
    (sessions_by_project[project_id] || []).each do |s|
      s.open_files.map! { |k| mapping[k] || k }
    end
  end

  # The node `path` names as `branch` sees it: a project branch's index when
  # `branch` is one, else main's (a detached per-file branch name lives on
  # main's tree).
  def self.find_node!(project_id, path, branch = Branch::MAIN)
    node = ProjectFs.store(project_id).find(path, branch: branch)
    raise ActiveRecord::RecordNotFound, path unless node
    node
  end

  # The project branch an existence op (tree/create/rename/delete) acts on:
  # `branch` when it names a live project branch, else main.
  def self.view_branch(store, payload)
    b = branch_of(payload)
    store.project_branch(b) ? b : Branch::MAIN
  end

  def self.other_project_sessions(session, sessions_by_project)
    (sessions_by_project[session.project_id] || [])
      .reject { |s| s.ws == session.ws }
      .map(&:ws)
  end
end
