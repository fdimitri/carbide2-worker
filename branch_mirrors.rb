# BranchMirrors — a materialized project branch is a branch whose tree is on
# disk, with its own VfsFlusher (DB → disk) and VfsWatcher (disk → DB) pair,
# exactly like main's. Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Layout: main is the project root (/srv/projects/<project-uuid>); a branch is
# <root>/.branches/<branch-uuid>. The uuid, not the name, so a re-used name of
# a tombstoned branch never lands in a stale directory. `.branches` is in
# FsLoader::PRUNE_DIR_NAMES so main's loader and watcher never descend into it.
#
# Materializing is the user's call (ProjectBranch#materialized): on turns it
# on — full flush of the branch's tree off the reactor, then the pair starts —
# and off stops the pair and removes the directory (the DBFS is authoritative;
# the directory is a mirror). Materialized branches come back on worker boot.
require 'fileutils'

module BranchMirrors
  DIR = '.branches'

    Mirror = Struct.new(:project_id, :branch_id, :branch, :root, :flusher, :watcher, :timer, :state, :ready)

  @mirrors = {} # [project_id, branch_name] => Mirror

  class << self
    def all = @mirrors.values

    def for(project_id, branch) = @mirrors[[project_id, branch.to_s]]

    def flusher_for(project_id, branch) = self.for(project_id, branch)&.flusher

    def root_for(project, pb) = File.join(File.expand_path(project.default_root_path), DIR, pb.id.to_s)

    # Path of a branch's directory relative to the project root, or nil when
    # it is not materialized — what fs/project_branches reports as `disk`.
    def relative_dir(pb) = pb.materialized? ? File.join(DIR, pb.id.to_s) : nil

    # Turn on: flush the whole tree, start the pair, then set the DB flag.
    # `on_ready` runs on the reactor when the pair is live (or with an
    # exception). A second call while still flushing waits for that flush
    # rather than claiming the mirror is already live.
    def start!(project, pb, sessions_by_project:, broadcast_fn:, suppress_set: nil, on_ready: nil)
      key = [project.id, pb.name]
      if (m = @mirrors[key])
        if m.state == :live
          on_ready&.call(m, nil)
        else
          (m.ready ||= []) << on_ready if on_ready
        end
        return m
      end

      root = root_for(project, pb)
      m = Mirror.new(project.id, pb.id, pb.name, root, nil, nil, nil, :flushing, [])
      m.ready << on_ready if on_ready
      @mirrors[key] = m

      work = proc do
        begin
          ActiveRecord::Base.connection_pool.with_connection do
            FileUtils.mkdir_p(root)
            n = DbfsV2::Flusher.new(ProjectFs.store(project.id, branch: pb.name), root).flush_all
            puts "[BranchMirrors:#{project.id}@#{pb.name}] materialized #{n} entr#{n == 1 ? 'y' : 'ies'} at #{root}"
            n
          end
        ensure
          worker_release_db! if defined?(worker_release_db!)
        end
      end

      fire = lambda do |mirror, err|
        cbs = (mirror&.ready || [])
        mirror.ready = [] if mirror
        cbs.each { |cb| cb&.call(err ? nil : mirror, err) }
      end

      done = proc do |_n|
        begin
          raise 'mirror stopped while flushing' unless @mirrors[key].equal?(m)
          flusher = VfsFlusher.new(project_id: project.id, root_path: root, suppress_set: suppress_set, branch: pb.name)
          m.flusher = flusher
          m.timer   = EM.add_periodic_timer(VfsFlusher::POLL_INTERVAL) { flusher.flush! }
          watcher = VfsWatcher.new(project_id: project.id, root_path: root, suppress_set: suppress_set, branch: pb.name)
          if watcher.start!(sessions_by_project: sessions_by_project, broadcast_fn: broadcast_fn)
            m.watcher = watcher
          else
            puts "[BranchMirrors:#{project.id}@#{pb.name}] live sync DISABLED — DB→disk flush still active"
          end
          m.state = :live
          pb.update!(materialized: true) unless pb.materialized?
          fire.call(m, nil)
        rescue => e
          puts "[BranchMirrors:#{project.id}@#{pb.name}] start failed: #{e.class}: #{e.message}"
          stop!(project.id, pb.name, remove: false, flag: false)
          fire.call(m, e)
        end
      end

      if EM.reactor_running?
        EM.defer(work, done, ->(e) {
          EM.schedule {
            stop!(project.id, pb.name, remove: false, flag: false)
            fire.call(m, e)
          }
        })
      else
        done.call(work.call)
      end
      m
    end

    # Turn off: stop the pair, clear the flag, drop the directory. Safe when
    # nothing is mirrored (a branch flagged in the DB whose pair never came up).
    def stop!(project_id, branch, remove: true, flag: true)
      m = @mirrors.delete([project_id, branch.to_s])
      m&.timer&.cancel
      m&.watcher&.stop!
      if flag
        pb = ProjectBranch.live.find_by(project_id: project_id, name: branch.to_s)
        pb&.update!(materialized: false)
      end
      if remove
        root = m&.root || (pb && Project.find_by(id: project_id)&.then { |p| root_for(p, pb) })
        FileUtils.rm_rf(root) if root && root.include?("/#{DIR}/")
      end
      puts "[BranchMirrors:#{project_id}@#{branch}] stopped#{remove ? ' and removed' : ''}" if m
      m
    end

    # Boot: bring back every branch the user left materialized.
    def start_all!(project, sessions_by_project:, broadcast_fn:, suppress_set: nil)
      ProjectBranch.live.where(project_id: project.id, materialized: true).where.not(name: Branch::MAIN).each do |pb|
        start!(project, pb, sessions_by_project: sessions_by_project, broadcast_fn: broadcast_fn, suppress_set: suppress_set)
      end
    end

    def stop_all!
      @mirrors.each_value { |m| m.timer&.cancel; m.watcher&.stop! }
      @mirrors.clear
    end
  end
end
