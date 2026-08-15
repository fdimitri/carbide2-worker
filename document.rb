# Document — in-memory rendered mirror of a file's current contents.
#
# The FileChange log in Postgres is the persistent source of truth, but
# replaying it (DirectoryEntry#calc_current) on every read is O(history) plus a
# JSON parse per row — the original CARBIDE kept a live copy in memory instead.
# This restores that: a live FsDocument per file, hydrated once from
# calc_current, then advanced by feeding each subsequent change through the same
# applier (DirectoryEntry.apply_change!). Reads become O(1); writes stay
# coherent because every write path calls #apply! after persisting.
#
# One project per pod, so the global DOCUMENTS store is keyed by srcpath alone.
# Eviction is intentionally not implemented yet — buffers live for the pod's
# lifetime.
#
# Thread-safety: client fs writes run on the EM reactor thread while agent tool
# calls run on EM.defer worker threads, so both the registry lookup and each
# buffer mutation are guarded. A single lock is plenty — there's one project per
# pod and edits are infrequent relative to the reactor's other work.
class Document
  LOCK = Mutex.new

  attr_reader :path

  # Fetch (hydrating on first touch) the live Document for a text file entry.
  # Returns nil for folders / binary entries, which have no replayable buffer.
  def self.for(entry)
    return nil unless entry && entry.ftype == 'file' && !entry.binary?

    LOCK.synchronize { DOCUMENTS[entry.srcpath] ||= new(entry) }
  end

  # Drop the cached buffer for a path (rename, delete, or an external
  # overwrite we'd rather re-hydrate than patch in place).
  def self.forget(path)
    LOCK.synchronize { DOCUMENTS.delete(path) }
  end

  def initialize(entry)
    @path     = entry.srcpath
    @doc      = FsDocument.new
    @doc.set_contents(entry.calc_current)
    @revision = entry.file_changes.count
  end

  # Current revision stamp (== file_changes.count).
  def revision
    LOCK.synchronize { @revision }
  end

  # Current rendered contents — O(1), no DB replay.
  def content
    LOCK.synchronize { @doc.get_contents }
  end

  # Advance the buffer by one already-persisted change and bump the revision
  # (one FileChange row == one revision). `change_data` must be in the same
  # form calc_current sees it (a JSON string or already-parsed Hash). Call this
  # exactly once per persisted change so @revision stays equal to
  # file_changes.count.
  def apply!(change_type, change_data)
    LOCK.synchronize do
      DirectoryEntry.apply_change!(@doc, change_type, change_data)
      @revision += 1
    end
    self
  end
end
