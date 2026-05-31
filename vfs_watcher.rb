# VfsWatcher — inotify watcher that syncs external disk changes into the VFS DB.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Watches root_path recursively for close_write / moved_to events. When a
# tracked file changes outside of Carbide (e.g. edited in a terminal), reads
# the new content, appends a setContents FileChange, and broadcasts to all
# connected sessions so editors update in real time.
#
# Integrates with EventMachine via EM.watch on the inotify file descriptor
# (non-blocking; process is only called when events are ready).
#
# Usage (inside EM.run, after FsLoader completes):
#   watcher = VfsWatcher.new(project_id: 1, root_path: '/srv/project',
#                            suppress_set: VFS_FLUSH_SUPPRESS)
#   watcher.start!(sessions_by_project: SESSIONS_BY_PROJECT,
#                  broadcast_fn: method(:broadcast))
#   # On shutdown:
#   watcher.stop!
require 'rb-inotify'

class VfsWatcher
  def initialize(project_id:, root_path:, suppress_set: nil)
    @project_id   = project_id
    @root_path    = root_path.to_s.chomp('/')
    @suppress_set = suppress_set
    @notifier     = nil
    @em_conn      = nil
  end

  def start!(sessions_by_project:, broadcast_fn:)
    @sessions_by_project = sessions_by_project
    @broadcast_fn        = broadcast_fn

    @notifier = INotify::Notifier.new

    add_watches_recursive(@root_path)

    # Attach to EM's event loop: notify_readable fires only when events are ready
    notifier_ref = @notifier
    handler = Module.new { define_method(:notify_readable) { notifier_ref.process } }
    @em_conn = EM.watch(@notifier.to_io, handler)
    @em_conn.notify_readable = true

    puts "[VfsWatcher:#{@project_id}] watching #{@root_path}"
  rescue => e
    puts "[VfsWatcher:#{@project_id}] start! failed: #{e.class}: #{e.message}"
  end

  def stop!
    @em_conn&.detach rescue nil
    @notifier&.close rescue nil
    puts "[VfsWatcher:#{@project_id}] stopped"
  end

  private

  def add_watches_recursive(dir)
    add_watch(dir)
    Dir.glob("#{dir}/**/*/").each { |d| add_watch(d.chomp('/')) }
  end

  def add_watch(dir)
    @notifier.watch(dir, :close_write, :moved_to, :create, :moved_from, :delete) do |event|
      handle_event(event)
    end
  rescue Errno::ENOENT, Errno::EACCES
    # directory vanished or unreadable — skip silently
  end

  def handle_event(event)
    abs_path = event.absolute_name

    # Deletions / moves-out — drop the DBFS entry (and any subtree) and notify.
    # Note: inotify on the parent dir is what carries :delete for children,
    # so we don't strictly need to scope these to abs_path inside @root_path.
    if event.flags.include?(:delete) || event.flags.include?(:moved_from)
      return unless abs_path.start_with?(@root_path + '/')
      srcpath = path_to_srcpath(abs_path)
      entry = DirectoryEntry.find_by(project_id: @project_id, srcpath: srcpath)
      return unless entry
      # destroy! cascades children via has_many :dependent => :destroy
      entry.destroy!
      sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
      @broadcast_fn.call(sessions, 'fs', 'deleted', { path: srcpath, source: 'inotify' })
      puts "[VfsWatcher:#{@project_id}] external delete: #{srcpath}"
      DebugStream.emit(:watcher, level: :info,
        message: "deleted #{srcpath}", project_id: @project_id,
        meta: { path: srcpath, source: 'inotify' }) if defined?(DebugStream)
      return
    end

    # Directory appeared: add watches AND make a DBFS entry so it shows up
    # in the explorer. Fixes #2 (May30-Questions.md) for new directories.
    if event.flags.include?(:isdir)
      if (event.flags.include?(:create) || event.flags.include?(:moved_to)) && File.directory?(abs_path)
        add_watches_recursive(abs_path)
        ensure_dir_entry(abs_path) if abs_path.start_with?(@root_path + '/')
      end
      return
    end

    # Only act on file-write events
    return unless event.flags.include?(:close_write) || event.flags.include?(:moved_to)

    # Skip paths written by VfsFlusher to prevent feedback loops
    return if @suppress_set&.include?(abs_path)
    return if File.directory?(abs_path)
    return unless abs_path.start_with?(@root_path + '/')

    srcpath = path_to_srcpath(abs_path)

    entry = DirectoryEntry.find_by(project_id: @project_id, srcpath: srcpath)
    if entry.nil?
      # File created outside Carbide (e.g. `touch foo.txt` in the terminal).
      # Import it into the DBFS so the explorer picks it up without a manual
      # rescan. Fixes #2 in May30-Questions.md.
      import_new_file(abs_path, srcpath)
      return
    end
    return unless File.file?(abs_path)

    # Binary entries: content lives on disk only. Just refresh stat metadata
    # (size/mtime/mode) so the explorer Properties panel stays accurate, and
    # broadcast an fs/changed event so open viewers can re-fetch the blob.
    if entry.binary?
      entry.refresh_disk_stat!(abs_path)
      sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
      @broadcast_fn.call(sessions, 'fs', 'changed', {
        path:   srcpath,
        size:   File.size(abs_path),
        binary: true,
        source: 'inotify'
      })
      puts "[VfsWatcher:#{@project_id}] external change (binary): #{srcpath}"
      return
    end

    # First 8KB null-byte check — a previously-text file may have been replaced
    # with binary content. Promote the entry to binary so we don't dump bytes
    # through the text replay pipeline.
    raw_head = File.binread(abs_path, [File.size(abs_path), 8192].min)
    if raw_head.include?("\x00")
      entry.update_columns(binary: true, updated_at: Time.current)
      entry.refresh_disk_stat!(abs_path)
      sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
      @broadcast_fn.call(sessions, 'fs', 'changed', {
        path:   srcpath,
        size:   File.size(abs_path),
        binary: true,
        source: 'inotify'
      })
      puts "[VfsWatcher:#{@project_id}] external change (now binary): #{srcpath}"
      return
    end

    content = File.read(abs_path, encoding: 'UTF-8', invalid: :replace, undef: :replace, replace: '')
    current = entry.calc_current
    if content == current
      entry.refresh_disk_stat!(abs_path)
      return  # no net text change — stat refresh is still useful for mtime
    end

    fc = ActiveRecord::Base.transaction do
      FileChange.append!(
        directory_entry_id: entry.id,
        user_id:            nil,
        change_type:        'setContents',
        change_data:        content,
        start_line:         0,
        start_char:         0
      )
    end
    entry.refresh_disk_stat!(abs_path)

    sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
    @broadcast_fn.call(sessions, 'fs', 'set_contents', {
      path:     srcpath,
      content:  content,
      revision: fc.revision,
      user_id:  nil,
      source:   'inotify'
    })
    puts "[VfsWatcher:#{@project_id}] external change: #{srcpath} (rev #{fc.revision})"
    DebugStream.emit(:watcher, level: :info,
      message: "changed #{srcpath} (rev #{fc.revision})", project_id: @project_id,
      meta: { path: srcpath, rev: fc.revision, source: 'inotify' }) if defined?(DebugStream)
  rescue => e
    puts "[VfsWatcher:#{@project_id}] handle_event error: #{e.class}: #{e.message}"
  end

  # Convert an absolute path under @root_path to a leading-slash srcpath.
  def path_to_srcpath(abs_path)
    sp = abs_path[@root_path.length..]
    sp.start_with?('/') ? sp : "/#{sp}"
  end

  # Idempotently create a DBFS folder entry for an externally-created
  # directory and broadcast fs/created. mkdir_p ensures intermediate dirs
  # also exist in the DBFS — covers `mkdir -p a/b/c` in one inotify event.
  def ensure_dir_entry(abs_path)
    srcpath = path_to_srcpath(abs_path)
    existing = DirectoryEntry.find_by(project_id: @project_id, srcpath: srcpath)
    if existing
      existing.refresh_disk_stat!(abs_path)
      return existing
    end
    DirectoryEntry.mkdir_p!(project_id: @project_id, srcpath: srcpath, user_id: nil)
    entry = DirectoryEntry.find_by(project_id: @project_id, srcpath: srcpath)
    entry&.refresh_disk_stat!(abs_path)
    sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
    @broadcast_fn.call(sessions, 'fs', 'created', {
      path:   srcpath,
      type:   'folder',
      source: 'inotify'
    })
    puts "[VfsWatcher:#{@project_id}] external mkdir: #{srcpath}"
    DebugStream.emit(:watcher, level: :info,
      message: "mkdir #{srcpath}", project_id: @project_id,
      meta: { path: srcpath, type: 'folder', source: 'inotify' }) if defined?(DebugStream)
    entry
  rescue => e
    puts "[VfsWatcher:#{@project_id}] ensure_dir_entry error: #{e.class}: #{e.message}"
    nil
  end

  # Import a brand-new on-disk file into the DBFS. Picks the text/binary path
  # by sniffing for null bytes (same heuristic as FsLoader/ArchiveImporter).
  # Broadcasts fs/created so the explorer refreshes without a manual rescan.
  def import_new_file(abs_path, srcpath)
    return unless File.file?(abs_path)
    size = File.size(abs_path)
    head = size.zero? ? ''.b : File.binread(abs_path, [size, 8192].min)
    binary = head.include?("\x00")

    if binary
      entry = DirectoryEntry.create_file!(
        project_id: @project_id, srcpath: srcpath,
        user_id: nil, mkdirp: true, binary: true
      )
      entry.update_columns(last_size: size, updated_at: Time.current) if entry.last_size != size
      entry.refresh_disk_stat!(abs_path)
    else
      data = File.read(abs_path, encoding: 'UTF-8', invalid: :replace, undef: :replace, replace: '')
      entry = DirectoryEntry.create_file!(
        project_id: @project_id, srcpath: srcpath,
        user_id: nil, data: data, mkdirp: true
      )
      entry.refresh_disk_stat!(abs_path)
    end

    sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
    @broadcast_fn.call(sessions, 'fs', 'created', {
      path:   srcpath,
      type:   'file',
      binary: binary,
      size:   size,
      source: 'inotify'
    })
    puts "[VfsWatcher:#{@project_id}] external create (#{binary ? 'binary' : 'text'}): #{srcpath} (#{size}B)"
    DebugStream.emit(:watcher, level: :info,
      message: "new #{binary ? 'binary' : 'text'} file #{srcpath} (#{size}B)",
      project_id: @project_id,
      meta: { path: srcpath, type: 'file', binary: binary, size: size, source: 'inotify' }) if defined?(DebugStream)
  rescue => e
    puts "[VfsWatcher:#{@project_id}] import_new_file error for #{srcpath}: #{e.class}: #{e.message}"
  end
end
