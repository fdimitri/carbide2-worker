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

    # Directory appeared: add watches so we track files created inside it
    if event.flags.include?(:isdir)
      if (event.flags.include?(:create) || event.flags.include?(:moved_to)) && File.directory?(abs_path)
        add_watches_recursive(abs_path)
      end
      return
    end

    # Only act on file-write events
    return unless event.flags.include?(:close_write) || event.flags.include?(:moved_to)

    # Skip paths written by VfsFlusher to prevent feedback loops
    return if @suppress_set&.include?(abs_path)
    return if File.directory?(abs_path)
    return unless abs_path.start_with?(@root_path + '/')

    srcpath = abs_path[@root_path.length..]
    srcpath = "/#{srcpath}" unless srcpath.start_with?('/')

    entry = DirectoryEntry.find_by(project_id: @project_id, srcpath: srcpath)
    return unless entry  # file not tracked in VFS — ignore
    return unless File.file?(abs_path)

    content = File.read(abs_path, encoding: 'UTF-8', invalid: :replace, undef: :replace, replace: '')
    current = entry.calc_current
    return if content == current  # no net change

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

    sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
    @broadcast_fn.call(sessions, 'fs', 'set_contents', {
      path:     srcpath,
      content:  content,
      revision: fc.revision,
      user_id:  nil,
      source:   'inotify'
    })
    puts "[VfsWatcher:#{@project_id}] external change: #{srcpath} (rev #{fc.revision})"
  rescue => e
    puts "[VfsWatcher:#{@project_id}] handle_event error: #{e.class}: #{e.message}"
  end
end
