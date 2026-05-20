# VfsFlusher — periodically writes changed VFS (DB) file content back to disk.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Tracks the last-flushed revision per entry in memory. On each tick only files
# whose max FileChange revision exceeds the last-flushed value are written.
#
# Usage (inside EM.run):
#   flusher = VfsFlusher.new(project_id: 1, root_path: '/srv/project',
#                            suppress_set: VFS_FLUSH_SUPPRESS)
#   EM.add_periodic_timer(VfsFlusher::INTERVAL) { flusher.flush! }
#
# suppress_set: a Set of absolute paths being written right now; the inotify
# watcher skips these paths to avoid re-importing our own flush writes.
require 'fileutils'

class VfsFlusher
  # Override with CARBIDE_FLUSH_INTERVAL env var (seconds). Default: 30.
  INTERVAL = Integer(ENV.fetch('CARBIDE_FLUSH_INTERVAL', '30'))

  def initialize(project_id:, root_path:, suppress_set: nil)
    @project_id   = project_id
    @root_path    = root_path.to_s.chomp('/')
    @suppress_set = suppress_set
    @last_rev     = {}  # entry_id => max revision at last flush
  end

  def flush!
    rows = DirectoryEntry
      .joins(:file_changes)
      .where(project_id: @project_id, ftype: 'file')
      .group('directory_entries.id')
      .select('directory_entries.id, directory_entries.srcpath, MAX(file_changes.revision) AS max_rev')

    flushed = 0
    rows.each do |row|
      max_rev = row.max_rev.to_i
      next if @last_rev[row.id] == max_rev

      abs_path = File.join(@root_path, row.srcpath)
      entry    = DirectoryEntry.find(row.id)
      content  = entry.calc_current

      @suppress_set&.add(abs_path)
      begin
        FileUtils.mkdir_p(File.dirname(abs_path))
        File.write(abs_path, content)
        @last_rev[row.id] = max_rev
        flushed += 1
      rescue => e
        puts "[VfsFlusher:#{@project_id}] write error #{abs_path}: #{e.message}"
      ensure
        # Hold the suppression for 1s to cover the inotify close_write event.
        EM.add_timer(1) { @suppress_set&.delete(abs_path) }
      end
    end

    puts "[VfsFlusher:#{@project_id}] flushed #{flushed} file(s)" if flushed > 0
  rescue => e
    puts "[VfsFlusher:#{@project_id}] flush! error: #{e.class}: #{e.message}"
  end
end
