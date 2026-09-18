# VfsFlusher — writes DBFS v2 text heads back to the working tree on disk.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Two flush triggers:
#   1. Periodic sweep (default 800 ms) — any text file whose main-branch head
#      moved since this flusher last wrote it.
#   2. Byte threshold (default 20 bytes) — immediate flush triggered by FsStore
#      / AgentTools via record_write() when accumulated unflushed bytes >= threshold.
#
# Dirty tracking is DbfsV2's: a branch head is an immutable revision id that
# only moves forward, so "head != last flushed head" is the whole check — one
# query per sweep for the project, no counts, no replay.
#
# Binaries are never flushed (decisions #28): their live copy is the PVC, and a
# second writer to the working area is exactly what v2 rules out. The actual
# write (path safety, mode/owner, folders) is DbfsV2::Flusher#flush_file.
#
# Settings are read from ProjectSetting (DB) with a 5-second cache so changes
# made via PATCH /api/projects/:id/settings take effect quickly at runtime.
# Env var fallbacks apply when no ProjectSetting row exists:
#   CARBIDE_FLUSH_INTERVAL=1.5   # seconds (default 0.8)
#   CARBIDE_FLUSH_BYTES=512      # bytes   (default 20)
#
# The worker timer fires every POLL_INTERVAL seconds (0.1 s); the flusher
# internally decides whether enough time has elapsed for a full sweep.
require 'fileutils'
require 'digest'

class VfsFlusher
  POLL_INTERVAL          = 0.1   # seconds — EM timer granularity (fixed)
  DEFAULT_INTERVAL_S     = Float(ENV.fetch('CARBIDE_FLUSH_INTERVAL', '0.8'))
  DEFAULT_BYTE_THRESHOLD = Integer(ENV.fetch('CARBIDE_FLUSH_BYTES',  '20'))
  SETTINGS_CACHE_TTL     = 5.0   # seconds between DB re-reads
  SUPPRESS_HOLD_S        = 1     # how long a path we wrote stays invisible to the watcher

  # Sentinel for "never flushed by this process". Distinct from nil, which is a
  # real head value (a text file created empty has no revisions yet).
  NEVER = Object.new.freeze

  attr_reader :root_path, :project_id, :suppress_set, :branch

  # What this flusher last put on disk at a path: the head it wrote and the
  # SHA-256 of the bytes. The watcher uses it to recognise its own echo by
  # content (not by a time window, which also swallowed real edits that landed
  # inside it) and to anchor a genuine external edit to the revision the
  # external writer saw.
  Flushed = Struct.new(:head, :digest)

  # `branch:` — the project branch this flusher mirrors (default main). A
  # branch's store view (DbfsV2::BranchView) makes every read and head query
  # below branch-scoped without further plumbing.
  def initialize(project_id:, root_path:, suppress_set: nil, branch: Branch::MAIN)
    @project_id      = project_id
    @branch          = branch.to_s
    @root_path       = root_path.to_s.chomp('/')
    @suppress_set    = suppress_set
    @store           = ProjectFs.store(project_id, branch: @branch)
    @writer          = DbfsV2::Flusher.new(@store, @root_path)
    @last_head       = Hash.new(NEVER)  # file_node_id => head revision id at last flush
    @on_disk         = {}               # abs path => Flushed
    @unflushed_bytes = Hash.new(0)      # file_node_id => bytes accumulated since last flush
    @last_flush_at   = 0.0
    @settings_cached_at    = 0.0
    @cached_interval_s     = DEFAULT_INTERVAL_S
    @cached_byte_threshold = DEFAULT_BYTE_THRESHOLD
  end

  # Called after every DBFS write that should reach disk. Triggers an immediate
  # flush for this node when the byte threshold is crossed.
  #
  # Agent tool calls write from EM.defer threads; the flusher's bookkeeping and
  # its EM timers belong to the reactor, so off-reactor callers are marshalled
  # onto it.
  def record_write(file_node_id, byte_count)
    unless !EM.reactor_running? || EM.reactor_thread?
      return EM.schedule { record_write(file_node_id, byte_count) }
    end

    @unflushed_bytes[file_node_id] += byte_count
    flush_node_by_id!(file_node_id) if @unflushed_bytes[file_node_id] >= @cached_byte_threshold
  end

  def flushed_state(abs_path)
    @on_disk[abs_path]
  end

  # The watcher absorbed an external edit and DBFS's head content is now
  # byte-identical to the file on disk: record that, so the next sweep doesn't
  # rewrite the file it just read (an mtime bump and a pointless echo).
  def adopt_disk_state(file_node_id, abs_path, head, digest)
    return unless !EM.reactor_running? || EM.reactor_thread?

    @last_head[file_node_id] = head
    @on_disk[abs_path] = Flushed.new(head, digest)
    @unflushed_bytes[file_node_id] = 0
  end

  # Mark absolute paths as touched-by-us for the duration of the block and a
  # short hold after it, so the watcher ignores the delete/move/mkdir events
  # FsStore's own disk operations produce. (Content writes don't need this; see
  # Flushed.)
  def suppress(*abs_paths)
    abs_paths.each { |p| @suppress_set&.add(p) }
    yield
  ensure
    release = -> { abs_paths.each { |p| @suppress_set&.delete(p) } }
    EM.reactor_running? ? EM.add_timer(SUPPRESS_HOLD_S, &release) : release.call
  end

  # Called by the EM timer every POLL_INTERVAL seconds.
  # Only performs a full sweep when the configured interval has elapsed.
  def flush!
    refresh_settings_cache!
    now = EM.current_time
    return unless now - @last_flush_at >= @cached_interval_s
    @last_flush_at = now

    flushed = 0
    text_heads.each do |id, path, head|
      next if @last_head[id] == head
      flush_single(id, path, head) && flushed += 1
    end

    puts "[VfsFlusher:#{tag}] sweep: flushed #{flushed} file(s)" if flushed > 0
  rescue => e
    puts "[VfsFlusher:#{tag}] flush! error: #{e.class}: #{e.message}"
  end

  private

  # [file_node_id, path, head] for every live, non-symlink text file on the
  # branch (Store#text_heads: main's heads, or a branch's heads/pins).
  def text_heads(node_ids = nil)
    @store.text_heads(node_ids: node_ids)
  end

  def tag = @branch == Branch::MAIN ? @project_id.to_s : "#{@project_id}@#{@branch}"

  def refresh_settings_cache!
    now = EM.current_time
    return unless now - @settings_cached_at >= SETTINGS_CACHE_TTL
    @settings_cached_at = now
    setting = ProjectSetting.find_by(project_id: @project_id)
    @cached_interval_s     = setting&.flush_interval_s || DEFAULT_INTERVAL_S
    @cached_byte_threshold = setting&.flush_bytes      || DEFAULT_BYTE_THRESHOLD
  rescue => e
    puts "[VfsFlusher:#{tag}] settings refresh error: #{e.message}"
  end

  def flush_node_by_id!(file_node_id)
    row = text_heads([file_node_id]).first
    return unless row
    _id, path, head = row
    return if @last_head[file_node_id] == head
    flush_single(file_node_id, path, head)
  rescue => e
    puts "[VfsFlusher:#{tag}] flush_node_by_id! error: #{e.class}: #{e.message}"
  end

  def flush_single(id, path, head)
    abs = @writer.disk_path(path)
    first_time = @last_head[id].equal?(NEVER)

    # The first time this process sees a node, skip the write when the disk
    # already holds exactly the head content: a worker restart must not rewrite
    # every file in the tree and bump every mtime (which makes build tools
    # rebuild everything). After that, head movement alone decides.
    content = @store.read(path).to_s
    if first_time && File.file?(abs) && File.size(abs) == content.bytesize && File.binread(abs) == content.b
      @last_head[id] = head
      @on_disk[abs] = Flushed.new(head, Digest::SHA256.hexdigest(content.b))
      @unflushed_bytes[id] = 0
      return false
    end

    # No suppress window: the watcher recognises this write by digest.
    @writer.flush_file(path)
    written = File.binread(abs)
    bytes = written.bytesize
    @on_disk[abs] = Flushed.new(head, Digest::SHA256.hexdigest(written))
    @last_head[id] = head
    @unflushed_bytes[id] = 0
    puts "[VfsFlusher:#{tag}] flushed #{path}"
    DebugStream.emit(:flusher, level: :info,
      message: "flushed #{path}", project_id: @project_id,
      meta: { path: path, bytes: bytes, rev: head }) if defined?(DebugStream)
    true
  rescue => e
    puts "[VfsFlusher:#{tag}] write error #{abs}: #{e.class}: #{e.message}"
    DebugStream.emit(:flusher, level: :error,
      message: "write error #{path}: #{e.message}", project_id: @project_id,
      meta: { path: path, error: e.class.to_s }) if defined?(DebugStream)
    false
  end
end
