# FsStore — database-backed filesystem handler for the EventMachine worker.
#
# Handles the 'fs' commandSet over WebSocket.  All reads go through
# DirectoryEntry#calc_current (full replay).  Writes append FileChange rows
# and broadcast the operation to other connected clients in the same project.
#
# Supported commands (cs: 'fs'):
#   tree        — return full file tree for the session's project
#   read        — return current content for a single file
#   write       — append one or more change operations to a file
#   set_contents— replace file content entirely (setContents)
#   create_file — create a new file entry
#   create_dir  — create a directory (mkdir -p)
#   rename      — rename a file entry
#   delete      — delete an entry (and children)

module FsStore
  # Entry point — called by worker route() for cs == 'fs'
  def self.handle(session, cmd, payload, sessions_by_project, send_fn, broadcast_fn)
    case cmd
    when 'tree'
      handle_tree(session, send_fn)
    when 'read'
      handle_read(session, payload, send_fn)
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
    tree = DirectoryEntry.tree_for_project(session.project_id)
    send_fn.call(session.ws, 'fs', 'tree', { tree: tree })
  end

  def self.handle_read(session, payload, send_fn)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

    content = entry.calc_current
    send_fn.call(session.ws, 'fs', 'content', {
      path:    entry.srcpath,
      content: content
    })
  end

  # open — register this session as viewing a file; receive its peer viewer list
  def self.handle_open(session, payload, send_fn)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

    norm = entry.srcpath
    key  = "#{session.project_id}:#{norm}"
    doc  = OPEN_DOCUMENTS[key] ||= OpenDocument.new(session.project_id, norm)
    doc.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.open_file(norm)

    send_fn.call(session.ws, 'fs', 'opened', { path: norm, viewers: doc.viewers })
  end

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

  # cursor — update this session's cursor position and broadcast to co-viewers
  def self.handle_cursor(session, payload, broadcast_fn)
    path = payload['path'].to_s.strip
    norm = path.start_with?('/') ? path : "/#{path}"
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

  # write — accepts { path:, changes: [...] }
  # Each change in the array: { change_type:, change_data:, start_line:, start_char:, end_line:, end_char: }
  def self.handle_write(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    changes = Array(payload['changes'])
    return send_fn.call(session.ws, 'fs', 'error', { message: 'no changes provided' }) if changes.empty?

    entry = find_entry!(session.project_id, path)
    return send_fn.call(session.ws, 'fs', 'error', { path: path, error: 'is a directory' }) if entry.ftype == 'folder'

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

    send_fn.call(session.ws, 'fs', 'written', {
      path:      entry.srcpath,
      revisions: stored.map(&:revision)
    })

    # Broadcast only to clients that have this file open
    key   = "#{session.project_id}:#{entry.srcpath}"
    doc   = OPEN_DOCUMENTS[key]
    peers = doc ? doc.others(session.ws) : []
    changes.each_with_index do |ch, i|
      broadcast_fn.call(peers, 'fs', 'change', {
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
  end

  def self.handle_set_contents(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s
    entry   = find_entry!(session.project_id, path)

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

    send_fn.call(session.ws, 'fs', 'written', { path: entry.srcpath, revisions: [fc.revision] })
    key   = "#{session.project_id}:#{entry.srcpath}"
    doc   = OPEN_DOCUMENTS[key]
    peers = doc ? doc.others(session.ws) : []
    broadcast_fn.call(peers, 'fs', 'set_contents', {
      path:     entry.srcpath,
      content:  content,
      revision: fc.revision,
      user_id:  session.user_id
    })
  end

  def self.handle_create_file(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path    = payload['path'].to_s.strip
    content = payload['content'].to_s
    entry   = DirectoryEntry.create_file!(
      project_id: session.project_id,
      srcpath:    path,
      user_id:    session.user_id,
      data:       content,
      mkdirp:     !!payload['mkdirp']
    )

    send_fn.call(session.ws, 'fs', 'created', { path: entry.srcpath, type: 'file', id: entry.id })
    peers = other_project_sessions(session, sessions_by_project)
    broadcast_fn.call(peers, 'fs', 'created', {
      path:    entry.srcpath,
      type:    'file',
      id:      entry.id,
      user_id: session.user_id
    })
  end

  def self.handle_create_dir(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path  = payload['path'].to_s.strip
    entry = DirectoryEntry.mkdir_p!(project_id: session.project_id, srcpath: path, user_id: session.user_id)

    send_fn.call(session.ws, 'fs', 'created', { path: entry.srcpath, type: 'folder', id: entry.id })
    peers = other_project_sessions(session, sessions_by_project)
    broadcast_fn.call(peers, 'fs', 'created', {
      path:    entry.srcpath,
      type:    'folder',
      id:      entry.id,
      user_id: session.user_id
    })
  end

  def self.handle_rename(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path     = payload['path'].to_s.strip
    new_name = payload['new_name'].to_s.strip
    entry    = find_entry!(session.project_id, path)
    old_path = entry.srcpath
    entry.rename!(new_name)

    send_fn.call(session.ws, 'fs', 'renamed', { old_path: old_path, new_path: entry.srcpath, id: entry.id })
    peers = other_project_sessions(session, sessions_by_project)
    broadcast_fn.call(peers, 'fs', 'renamed', {
      old_path: old_path,
      new_path: entry.srcpath,
      id:       entry.id,
      user_id:  session.user_id
    })
  end

  def self.handle_delete(session, payload, sessions_by_project, send_fn, broadcast_fn)
    path  = payload['path'].to_s.strip
    entry = find_entry!(session.project_id, path)
    entry.destroy!

    send_fn.call(session.ws, 'fs', 'deleted', { path: path })
    peers = other_project_sessions(session, sessions_by_project)
    broadcast_fn.call(peers, 'fs', 'deleted', { path: path, user_id: session.user_id })
  end

  # -------------------------------------------------------------------------
  private_class_method

  def self.find_entry!(project_id, path)
    normalized = path.start_with?('/') ? path : "/#{path}"
    entry = DirectoryEntry.find_by_project_and_path(project_id, normalized)
    raise ActiveRecord::RecordNotFound, path unless entry
    entry
  end

  def self.other_project_sessions(session, sessions_by_project)
    (sessions_by_project[session.project_id] || [])
      .reject { |s| s.ws == session.ws }
      .map(&:ws)
  end
end
