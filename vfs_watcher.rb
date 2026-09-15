# VfsWatcher — inotify watcher that folds external disk changes into DBFS v2.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Watches root_path recursively. When something outside Carbide (a terminal, a
# build, git) changes the working tree, the change is absorbed into DBFS and
# broadcast to connected sessions so editors and the explorer update live.
#
# This is the EventMachine adapter. The absorb rules themselves are
# DbfsV2::Watcher's (lib/dbfs_v2/watcher.rb), shared rather than re-implemented:
#
#   * text   — DbfsV2::Watcher#ingest_text: a setContents revision (loose sync),
#              broadcast as fs/set_contents. The flusher's own writes are
#              recognised by digest and ignored; a genuine external edit is
#              anchored to the revision last flushed to that path, so OT merges
#              it with editor writes that landed since (or raises a conflict)
#              rather than reverting them.
#   * binary — DbfsV2::Watcher#ingest_binary -> DbfsV2::Ingest, broadcast as
#              fs/changed (or fs/created). Runs OFF the reactor, one file at a
#              time, with a real read guard: while a file's bytes are being
#              copied, a per-file inotify watch (IN_MODIFY / IN_CLOSE_WRITE /
#              move / delete) is held, and any event on it discards the read so
#              the newer state is ingested instead (decisions #28). The
#              prototype's own guard is fed by its synchronous pump and never
#              sees IN_MODIFY, so it cannot fire in an event loop; this is the
#              adapter that makes #28 true here.
#   * delete — tombstone (Store#delete); history is kept.
#   * over ProjectFs::MAX_FILE_SIZE — tracked as a metadata-only binary node.
#
# Directory creation, pruning, dot-dirs, watch-exhaustion handling and the
# debounced reconcile sweep are unchanged from the DBFS v1 watcher.
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
require 'digest'

class VfsWatcher
  # Debounced reconcile: coalesce a burst of inotify events (e.g. a shell
  # `git submodule update`) and run one idempotent FsLoader sweep once the
  # burst goes quiet, so files written before their directory's watch went
  # live still make it into the DBFS. See fdimitri/carbide2#72.
  RECONCILE_DEBOUNCE  = 1.5   # seconds of quiescence before sweeping
  RECONCILE_MAX_DELAY = 10.0  # never defer a pending sweep longer than this

  # Read guard for one binary ingest (see header). Events arrive on the reactor
  # thread; the copy runs on an EM.defer thread, hence the mutex.
  class FileGuard
    WATCH_FLAGS = %i[modify close_write move_self delete_self].freeze

    def initialize(notifier, abs)
      @mutex   = Mutex.new
      @changed = false
      @watch   = notifier.watch(abs, *WATCH_FLAGS) { mark! }
    rescue SystemCallError
      # The file vanished before we could watch it; the copy will fail on its
      # own and the delete event is already queued.
      @watch = nil
    end

    def mark!
      @mutex.synchronize { @changed = true }
    end

    # DbfsV2::Ingest guard protocol.
    def on_read_start
      @mutex.synchronize { @changed = false }
    end

    def changed?
      @mutex.synchronize { @changed }
    end

    def close
      @watch&.close
    rescue SystemCallError
      nil # already removed by the kernel (file deleted)
    end
  end

  def initialize(project_id:, root_path:, suppress_set: nil)
    @project_id   = project_id
    @root_path    = root_path.to_s.chomp('/')
    @suppress_set = suppress_set
    @store        = ProjectFs.store(project_id)
    @absorber     = DbfsV2::Watcher.new(@store, @root_path, cache: ProjectFs.blob_cache(project_id))
    @notifier     = nil
    @em_conn      = nil
    @dirty_dirs      = {}
    @reconcile_timer = nil
    @dirty_since     = nil
    @reconciling     = false
    @ingest_queue = []   # abs paths awaiting binary ingest, FIFO, deduped
    @ingesting    = nil  # abs path whose binary ingest is running off-reactor
    @rerun        = {}   # abs => true when an event arrived for @ingesting mid-read
    @guard        = nil
  end

  # Returns true on success, false when the watcher could not be started. On
  # failure it releases the notifier (and any kernel watches already allocated)
  # so the caller can decline to register it (#101) instead of leaving a
  # half-initialized watcher that holds fds/watch budget while never consuming
  # events.
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
    true
  rescue => e
    puts "[VfsWatcher:#{@project_id}] start! failed: #{e.class}: #{e.message}"
    if e.is_a?(Errno::ENOSPC)
      puts "[VfsWatcher:#{@project_id}] inotify watch limit reached — raise " \
           "fs.inotify.max_user_watches on the NODE (node sysctl, not pod-tunable) and restart the worker"
    end
    stop!
    false
  end

  def stop!
    EM.cancel_timer(@reconcile_timer) if @reconcile_timer
    @reconcile_timer = nil
    @guard&.close
    @em_conn&.detach rescue nil
    @notifier&.close rescue nil
    puts "[VfsWatcher:#{@project_id}] stopped"
  end

  private

  def add_watches_recursive(dir)
    add_watch(dir)
    # Dotmatch-aware: hidden directories (.carbide, .gnupg, ...) are
    # first-class workspace paths and must get watches too (#77).
    #
    # Mirror FsLoader's prune set so the two can't drift: .git / node_modules /
    # .bundle are excluded by the loader and therefore must not be watched (and
    # watching .git's object store would otherwise risk exhausting inotify
    # watches).
    #
    # Guarded on both the enumeration and the per-child stat (#101): a directory
    # that vanishes or becomes unreadable mid-walk must skip that subtree, not
    # kill the entire recursive walk.
    begin
      entries = Dir.children(dir)
    rescue Errno::ENOENT, Errno::EACCES, Errno::ENAMETOOLONG
      return
    end

    entries.each do |name|
      next if FsLoader::PRUNE_DIR_NAMES.include?(name)
      sub = File.join(dir, name)

      begin
        next if File.symlink?(sub)          # don't follow/loop; inotify won't traverse them
        next unless File.directory?(sub)
      rescue Errno::ENOENT, Errno::EACCES, Errno::ENAMETOOLONG
        next
      end

      add_watches_recursive(sub)
    end
  end

  def add_watch(dir)
    @notifier.watch(dir, :close_write, :moved_to, :create, :moved_from, :delete) do |event|
      handle_event(event)
    end
  rescue Errno::ENOENT, Errno::EACCES
    # directory vanished or unreadable — skip silently
  end

  def handle_event(event)
    if event.flags.include?(:q_overflow)
      puts "[VfsWatcher:#{@project_id}] inotify queue overflow — scheduling full reconcile"
      mark_dirty(@root_path)
      return
    end

    abs_path = event.absolute_name

    # Deletions / moves-out — tombstone the node (and subtree) and notify.
    if event.flags.include?(:delete) || event.flags.include?(:moved_from)
      return unless abs_path.start_with?(@root_path + '/')
      return if @suppress_set&.include?(abs_path)
      srcpath = path_to_srcpath(abs_path)
      return unless @store.find(srcpath)

      @store.delete(srcpath)
      broadcast('deleted', { path: srcpath, source: 'inotify' })
      puts "[VfsWatcher:#{@project_id}] external delete: #{srcpath}"
      DebugStream.emit(:watcher, level: :info,
        message: "deleted #{srcpath}", project_id: @project_id,
        meta: { path: srcpath, source: 'inotify' }) if defined?(DebugStream)
      return
    end

    # Directory appeared: add watches AND make a DBFS node so it shows up in
    # the explorer.
    if event.flags.include?(:isdir)
      if (event.flags.include?(:create) || event.flags.include?(:moved_to)) && File.directory?(abs_path)
        in_root = abs_path.start_with?(@root_path + '/')
        # Arm the #72 reconcile sweep BEFORE the walk: it's a free timer-arm and
        # a walk failure must not skip recovery (#101).
        mark_dirty(abs_path) if in_root
        # Runtime prune: a directory created AFTER startup (git init, npm install)
        # must respect the same .git / node_modules / .bundle exclusion as the
        # startup traversal, or we watch pruned trees and can exhaust inotify (#6).
        unless FsLoader::PRUNE_DIR_NAMES.include?(File.basename(abs_path))
          add_watches_recursive(abs_path)
        end
        # Create the DBFS node AFTER the walk: its DB write + broadcast must not
        # delay watch registration (#101).
        ensure_dir_entry(abs_path) if in_root
      end
      return
    end

    # Only act on file-write events
    return unless event.flags.include?(:close_write) || event.flags.include?(:moved_to)

    # Skip paths FsStore is moving/creating itself (its rename lands here as
    # moved_to). Flusher writes are recognised by digest in absorb_text.
    return if @suppress_set&.include?(abs_path)
    return unless abs_path.start_with?(@root_path + '/')

    absorb_path(abs_path)
  rescue => e
    puts "[VfsWatcher:#{@project_id}] handle_event error: #{e.class}: #{e.message}"
  end

  # Decide how a changed file on disk enters DBFS. Re-entered after a binary
  # ingest when more events arrived for the same path while it ran.
  def absorb_path(abs_path)
    return unless File.file?(abs_path)
    srcpath = path_to_srcpath(abs_path)

    # One writer per path: an event for a file that is being (or waiting to be)
    # ingested is folded into a rerun instead of racing it.
    if @ingesting == abs_path
      @rerun[abs_path] = true
      return
    end
    return if @ingest_queue.include?(abs_path)

    size = File.size(abs_path)
    if size > ProjectFs::MAX_FILE_SIZE
      existed = !@store.find(srcpath).nil?
      ProjectFs.track_oversized!(@store, srcpath, abs_path)
      broadcast(existed ? 'changed' : 'created',
                { path: srcpath, type: 'file', size: size, binary: true, source: 'inotify' })
      puts "[VfsWatcher:#{@project_id}] tracked without content (#{size}B > cap): #{srcpath}"
      return
    end

    if ProjectFs.binary_file?(abs_path)
      enqueue_binary(abs_path)
    else
      absorb_text(abs_path, srcpath)
    end
  rescue Errno::ENOENT
    nil # gone between the event and the stat; its delete event follows
  end

  # --- text ----------------------------------------------------------------

  def absorb_text(abs_path, srcpath, retried: false)
    # Our own flush coming back: the bytes on disk are exactly what the flusher
    # last wrote here. Ignore it — even if DBFS has moved on since, because
    # folding it in would diff those newer writes away.
    flushed = VFS_FLUSHERS[@project_id]&.flushed_state(abs_path) if defined?(VFS_FLUSHERS)
    return if flushed && Digest::SHA256.file(abs_path).hexdigest == flushed.digest

    # A genuine external edit, anchored to the revision the file was last
    # flushed from (when that revision still belongs to this node).
    node = @store.find(srcpath)
    base = flushed&.head
    base = nil unless base && node && Revision.exists?(id: base, file_node_id: (node.resolve || node).id)

    res = @absorber.ingest_text(abs_path, srcpath, base_revision_id: base)
    ProjectFs.record_disk_stat!(res[:node], abs_path)

    case res[:status]
    when :created
      node = res[:node]
      adopt_if_identical(node, abs_path, ProjectFs.head_revision_id(node), @store.read(srcpath).to_s)
      size = File.size(abs_path) rescue 0
      broadcast('created', { path: srcpath, type: 'file', binary: false, size: size, source: 'inotify' })
      puts "[VfsWatcher:#{@project_id}] external create (text): #{srcpath} (#{size}B)"
      DebugStream.emit(:watcher, level: :info,
        message: "new text file #{srcpath} (#{size}B)", project_id: @project_id,
        meta: { path: srcpath, type: 'file', binary: false, size: size, source: 'inotify' }) if defined?(DebugStream)
    when :changed
      rev  = res[:revisions].last
      head = @store.read(srcpath).to_s
      broadcast('set_contents', {
        path: srcpath, content: head, revision: rev&.id, user_id: nil, source: 'inotify'
      })
      adopt_if_identical(res[:node], abs_path, rev, head)
      puts "[VfsWatcher:#{@project_id}] external change: #{srcpath} (rev #{rev&.id})"
      DebugStream.emit(:watcher, level: :info,
        message: "changed #{srcpath} (rev #{rev&.id})", project_id: @project_id,
        meta: { path: srcpath, rev: rev&.id, source: 'inotify' }) if defined?(DebugStream)
    end
  rescue ActiveRecord::RecordNotUnique
    # A reconcile sweep created the node between our find and create; the
    # second pass sees it and takes the change path.
    retry_once = !retried
    absorb_text(abs_path, srcpath, retried: true) if retry_once
  rescue DbfsV2::ConflictError => e
    puts "[VfsWatcher:#{@project_id}] external change to #{srcpath} conflicts with a concurrent edit: #{e.message}"
    DebugStream.emit(:watcher, level: :warn,
      message: "external change conflicted: #{srcpath}", project_id: @project_id,
      meta: { path: srcpath, error: e.message, source: 'inotify' }) if defined?(DebugStream)
  end

  # When the merged head is byte-identical to the disk file, tell the flusher
  # the disk is already current. (After an OT merge with concurrent editor
  # writes it isn't, and the next sweep writes the merge out.)
  def adopt_if_identical(node, abs_path, rev_or_id, head_content)
    flusher = defined?(VFS_FLUSHERS) && VFS_FLUSHERS[@project_id]
    return unless flusher && node && rev_or_id

    disk = File.binread(abs_path)
    return unless disk == head_content.b

    head_id = rev_or_id.respond_to?(:id) ? rev_or_id.id : rev_or_id
    flusher.adopt_disk_state(node.id, abs_path, head_id, Digest::SHA256.hexdigest(disk))
  rescue Errno::ENOENT
    nil
  end

  # --- binary --------------------------------------------------------------

  def enqueue_binary(abs_path)
    @ingest_queue << abs_path
    pump_ingest
  end

  def pump_ingest
    return if @ingesting || @ingest_queue.empty?

    abs_path   = @ingest_queue.shift
    srcpath    = path_to_srcpath(abs_path)
    @ingesting = abs_path
    @rerun.delete(abs_path)
    # Watch the file BEFORE the copy starts (reactor thread; rb-inotify's
    # registry is not thread-safe).
    @guard = FileGuard.new(@notifier, abs_path)
    guard  = @guard
    store, absorber = @store, @absorber

    work = proc do
      ActiveRecord::Base.connection_pool.with_connection do
        existed = !store.find(srcpath).nil?
        # Create the node here, race-tolerantly (a reconcile sweep may be
        # importing the same path), before the absorber looks for it.
        ProjectFs.ensure_file!(store, srcpath, binary: true) unless existed
        res = absorber.ingest_binary(abs_path, srcpath, guard_factory: ->(_) { guard })
        ProjectFs.record_disk_stat!(store.find(srcpath), abs_path)
        [res, existed]
      end
    rescue => e
      [{ status: :error, error: "#{e.class}: #{e.message}" }, true]
    end

    done = proc do |(res, existed)|
      guard.close
      @guard = nil
      @ingesting = nil
      report_binary(srcpath, res, existed)
      absorb_path(abs_path) if @rerun.delete(abs_path)
      pump_ingest
    end

    EM.defer(work, done)
  end

  def report_binary(srcpath, res, existed)
    case res && res[:status]
    when :committed
      rev = res[:revision]
      broadcast(existed ? 'changed' : 'created', {
        path: srcpath, type: 'file', binary: true, size: res[:size], revision: rev&.id, source: 'inotify'
      })
      puts "[VfsWatcher:#{@project_id}] external #{existed ? 'change' : 'create'} (binary): #{srcpath} (#{res[:size]}B)"
      DebugStream.emit(:watcher, level: :info,
        message: "#{existed ? 'changed' : 'new'} binary file #{srcpath} (#{res[:size]}B)",
        project_id: @project_id,
        meta: { path: srcpath, binary: true, size: res[:size], rev: rev&.id, source: 'inotify' }) if defined?(DebugStream)
    when :noop
      broadcast('created', { path: srcpath, type: 'file', binary: true, source: 'inotify' }) unless existed
    when :discarded
      # Still being written after every attempt; its next close_write reruns it.
      puts "[VfsWatcher:#{@project_id}] binary ingest discarded (file kept changing): #{srcpath}"
    when :error
      puts "[VfsWatcher:#{@project_id}] binary ingest error for #{srcpath}: #{res[:error]}"
      DebugStream.emit(:watcher, level: :error,
        message: "binary ingest failed: #{srcpath}", project_id: @project_id,
        meta: { path: srcpath, error: res[:error], source: 'inotify' }) if defined?(DebugStream)
    end
  end

  # --- helpers -------------------------------------------------------------

  def broadcast(cmd, payload)
    sessions = (@sessions_by_project[@project_id] || []).map(&:ws)
    @broadcast_fn.call(sessions, 'fs', cmd, payload)
  end

  # Convert an absolute path under @root_path to a leading-slash srcpath.
  def path_to_srcpath(abs_path)
    sp = abs_path[@root_path.length..]
    sp.start_with?('/') ? sp : "/#{sp}"
  end

  # Idempotently create a DBFS folder node for an externally-created directory
  # and broadcast fs/created. mkdir -p semantics cover `mkdir -p a/b/c` in one
  # inotify event.
  def ensure_dir_entry(abs_path)
    srcpath  = path_to_srcpath(abs_path)
    existing = @store.find(srcpath)
    if existing
      ProjectFs.record_disk_stat!(existing, abs_path)
      return existing
    end
    node = ProjectFs.ensure_folder!(@store, srcpath)
    ProjectFs.record_disk_stat!(node, abs_path)
    broadcast('created', { path: srcpath, type: 'folder', source: 'inotify' })
    puts "[VfsWatcher:#{@project_id}] external mkdir: #{srcpath}"
    DebugStream.emit(:watcher, level: :info,
      message: "mkdir #{srcpath}", project_id: @project_id,
      meta: { path: srcpath, type: 'folder', source: 'inotify' }) if defined?(DebugStream)
    node
  rescue => e
    puts "[VfsWatcher:#{@project_id}] ensure_dir_entry error: #{e.class}: #{e.message}"
    nil
  end

  # --- Debounced reconcile (fdimitri/carbide2#72) --------------------------
  #
  # Mark a directory as needing a sweep and (re)arm the debounce timer. The
  # actual sweep runs off the reactor once events go quiet.
  def mark_dirty(disk_dir)
    return unless disk_dir && disk_dir.start_with?(@root_path)
    @dirty_dirs[disk_dir] = true
    arm_reconcile_timer
  end

  def arm_reconcile_timer(force: false)
    @dirty_since ||= Time.now
    # Debounce, but don't keep deferring past RECONCILE_MAX_DELAY from the
    # first dirty event of this burst — let the pending timer fire.
    if @reconcile_timer && !force && (Time.now - @dirty_since) >= RECONCILE_MAX_DELAY
      return
    end
    EM.cancel_timer(@reconcile_timer) if @reconcile_timer
    @reconcile_timer = EM.add_timer(RECONCILE_DEBOUNCE) { reconcile! }
  end

  # Collapse the dirty set to its minimal roots (drop any path that is a
  # descendant of another) so a bulk checkout sweeps a few top-level dirs
  # instead of every directory it created.
  def minimal_roots(dirs)
    roots = []
    dirs.sort.each do |d|
      next if roots.any? { |r| d == r || d.start_with?(r + '/') }
      roots << d
    end
    roots
  end

  # Runs on the reactor when the debounce fires. Sweeps the dirty roots via
  # FsLoader off the reactor thread, then broadcasts a single tree refresh.
  def reconcile!
    @reconcile_timer = nil
    return if @dirty_dirs.empty?
    if @reconciling
      # A sweep is already in flight; try again shortly.
      arm_reconcile_timer(force: true)
      return
    end

    roots = minimal_roots(@dirty_dirs.keys)
    @dirty_dirs  = {}
    @dirty_since = nil
    @reconciling = true

    project_id = @project_id
    root_path  = @root_path

    work = proc do
      imported = 0
      begin
        ActiveRecord::Base.connection_pool.with_connection do
          roots.each do |disk_dir|
            next unless Dir.exist?(disk_dir)
            stats = FsLoader.new(project_id: project_id, root_path: root_path,
                                 user_id: nil, verbose: false).load_dir!(disk_dir)
            imported += stats[:dirs].to_i + stats[:files].to_i
          end
        end
      rescue => e
        puts "[VfsWatcher:#{project_id}] reconcile error: #{e.class}: #{e.message}"
      end
      imported
    end

    done = proc do |imported|
      @reconciling = false
      if imported.to_i.positive?
        # Client refetches the whole tree on any fs/created (ExplorerPane).
        broadcast('created', { path: '/', type: 'folder', source: 'inotify-reconcile' })
        puts "[VfsWatcher:#{@project_id}] reconcile imported #{imported} entries"
        DebugStream.emit(:watcher, level: :info,
          message: "reconciled #{imported} entries", project_id: @project_id,
          meta: { imported: imported, source: 'inotify-reconcile' }) if defined?(DebugStream)
      end
      # Anything that arrived while we swept? Go again.
      arm_reconcile_timer(force: true) unless @dirty_dirs.empty?
    end

    EM.defer(work, done)
  end
end
