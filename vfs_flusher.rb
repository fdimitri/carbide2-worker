# VfsFlusher — writes changed VFS (DB) file content back to disk.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Two flush triggers:
#   1. Periodic sweep (default 800 ms) — any file with pending changes is written.
#   2. Byte threshold (default 20 bytes) — immediate flush triggered by FsStore
#      via record_write() when accumulated unflushed bytes >= threshold.
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

class VfsFlusher
  POLL_INTERVAL          = 0.1   # seconds — EM timer granularity (fixed)
  DEFAULT_INTERVAL_S     = Float(ENV.fetch('CARBIDE_FLUSH_INTERVAL', '0.8'))
  DEFAULT_BYTE_THRESHOLD = Integer(ENV.fetch('CARBIDE_FLUSH_BYTES',  '20'))
  SETTINGS_CACHE_TTL     = 5.0   # seconds between DB re-reads

  def initialize(project_id:, root_path:, suppress_set: nil)
    @project_id      = project_id
    @root_path       = root_path.to_s.chomp('/')
    @suppress_set    = suppress_set
    @last_rev        = {}   # entry_id => max revision at last flush
    @unflushed_bytes = {}   # entry_id => bytes accumulated since last flush
    @last_flush_at   = 0.0  # monotonic time of last sweep
    # Settings cache
    @settings_cached_at    = 0.0
    @cached_interval_s     = DEFAULT_INTERVAL_S
    @cached_byte_threshold = DEFAULT_BYTE_THRESHOLD
  end

  # Called by FsStore after every write/set_contents to track unflushed bytes.
  # Triggers an immediate flush for this entry when the threshold is exceeded.
  def record_write(entry_id, byte_count)
    @unflushed_bytes[entry_id] = (@unflushed_bytes[entry_id] || 0) + byte_count
    flush_entry_by_id!(entry_id) if @unflushed_bytes[entry_id] >= @cached_byte_threshold
  end

  # Called by the EM timer every POLL_INTERVAL seconds.
  # Only performs a full sweep when the configured interval has elapsed.
  def flush!
    refresh_settings_cache!
    now = EM.current_time
    return unless now - @last_flush_at >= @cached_interval_s
    @last_flush_at = now

    rows = DirectoryEntry
      .joins(:file_changes)
      .where(project_id: @project_id, ftype: 'file')
      .group('directory_entries.id')
      .select('directory_entries.id, directory_entries.srcpath, MAX(file_changes.revision) AS max_rev')

    flushed = 0
    rows.each do |row|
      next if @last_rev[row.id] == row.max_rev.to_i
      entry = DirectoryEntry.find(row.id)
      flush_single(entry, row.max_rev.to_i) && flushed += 1
    end

    puts "[VfsFlusher:#{@project_id}] sweep: flushed #{flushed} file(s)" if flushed > 0
  rescue => e
    puts "[VfsFlusher:#{@project_id}] flush! error: #{e.class}: #{e.message}"
  end

  private

  def refresh_settings_cache!
    now = EM.current_time
    return unless now - @settings_cached_at >= SETTINGS_CACHE_TTL
    @settings_cached_at = now
    setting = ProjectSetting.find_by(project_id: @project_id)
    @cached_interval_s     = setting&.flush_interval_s || DEFAULT_INTERVAL_S
    @cached_byte_threshold = setting&.flush_bytes      || DEFAULT_BYTE_THRESHOLD
  rescue => e
    puts "[VfsFlusher:#{@project_id}] settings refresh error: #{e.message}"
  end

  def flush_entry_by_id!(entry_id)
    entry = DirectoryEntry.find_by(id: entry_id, project_id: @project_id, ftype: 'file')
    return unless entry
    max_rev = FileChange.where(directory_entry_id: entry_id).maximum(:revision).to_i
    return if @last_rev[entry_id] == max_rev
    flush_single(entry, max_rev)
  rescue => e
    puts "[VfsFlusher:#{@project_id}] flush_entry_by_id! error: #{e.class}: #{e.message}"
  end

  def flush_single(entry, max_rev)
    abs_path = File.join(@root_path, entry.srcpath)
    content  = entry.calc_current
    @suppress_set&.add(abs_path)
    begin
      FileUtils.mkdir_p(File.dirname(abs_path))
      File.write(abs_path, content)
      @last_rev[entry.id]        = max_rev
      @unflushed_bytes[entry.id] = 0
      puts "[VfsFlusher:#{@project_id}] flushed #{entry.srcpath}"
      true
    rescue => e
      puts "[VfsFlusher:#{@project_id}] write error #{abs_path}: #{e.message}"
      false
    ensure
      EM.add_timer(1) { @suppress_set&.delete(abs_path) }
    end
  end
end
