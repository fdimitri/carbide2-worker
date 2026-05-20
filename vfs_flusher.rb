# VfsFlusher — writes changed VFS (DB) file content back to disk.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Two flush triggers:
#   1. Periodic timer (default 800 ms) — flushes any file with pending changes.
#   2. Byte threshold (default 20 bytes) — an immediate flush is triggered from
#      FsStore via record_write() when accumulated unflushed bytes >= threshold.
#
# Override defaults:
#   CARBIDE_FLUSH_INTERVAL=1.5   # seconds between periodic sweeps
#   CARBIDE_FLUSH_BYTES=512      # byte threshold for immediate flush
#
# suppress_set: shared Set of absolute paths currently being written by the
# flusher; VfsWatcher skips these to prevent disk→DB→disk feedback loops.
require 'fileutils'

class VfsFlusher
  INTERVAL_S     = Float(ENV.fetch('CARBIDE_FLUSH_INTERVAL',  '0.8'))
  BYTE_THRESHOLD = Integer(ENV.fetch('CARBIDE_FLUSH_BYTES',   '20'))

  def initialize(project_id:, root_path:, suppress_set: nil)
    @project_id      = project_id
    @root_path       = root_path.to_s.chomp('/')
    @suppress_set    = suppress_set
    @last_rev        = {}  # entry_id => max revision at last flush
    @unflushed_bytes = {}  # entry_id => bytes accumulated since last flush
  end

  # Called by FsStore after every write/set_contents to track unflushed bytes.
  # Triggers an immediate flush for this entry if the threshold is exceeded.
  def record_write(entry_id, byte_count)
    @unflushed_bytes[entry_id] = (@unflushed_bytes[entry_id] || 0) + byte_count
    flush_entry_by_id!(entry_id) if @unflushed_bytes[entry_id] >= BYTE_THRESHOLD
  end

  # Periodic sweep — flushes every entry that has changed since last flush.
  def flush!
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

    puts "[VfsFlusher:#{@project_id}] periodic: flushed #{flushed} file(s)" if flushed > 0
  rescue => e
    puts "[VfsFlusher:#{@project_id}] flush! error: #{e.class}: #{e.message}"
  end

  private

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
