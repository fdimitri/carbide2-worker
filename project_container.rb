# ProjectContainer — manages one persistent Docker container per project.
#
# Each project gets a single container running `sleep infinity`.  Terminals
# attach via `docker exec -it <name> bash` so they share the same filesystem
# namespace and any background processes the user leaves running.
#
# Lifecycle:
#   ensure_running! — start the container if not already up (idempotent)
#   stop            — stop and remove the container
#   exec_cmd        — returns the shell string for PTY.spawn
#
# The container name is deterministic: "carbide2-project-<id>" so a restart of
# the worker can re-adopt a container that survived the crash.
require 'shellwords'
require 'open3'
require 'fileutils'

class ProjectContainer
  # Override with CARBIDE_SHELL_IMAGE env var to use a custom image.
  SHELL_IMAGE = ENV.fetch('CARBIDE_SHELL_IMAGE', 'ubuntu:24.04').freeze

  # When the worker runs inside docker compose, all project files live on a
  # shared named volume mounted at PROJECTS_ROOT in the worker container.
  # Spawned shell containers mount the same volume with a per-project subpath
  # so they see exactly the same files without any host-path coupling.
  PROJECTS_ROOT   = ENV.fetch('PROJECTS_ROOT', '').freeze
  PROJECTS_VOLUME = ENV.fetch('CARBIDE_PROJECTS_VOLUME', '').freeze

  attr_reader :project_id, :name

  def initialize(project_id, root_path: nil)
    @project_id = project_id
    @root_path  = root_path.to_s.strip
    @name       = "carbide2-project-#{project_id}"
  end

  # Ensure the container is running.  Starts it if necessary; re-starts it if
  # Docker reports it as stopped/exited.  Raises on Docker failure.
  def ensure_running!
    if running?
      puts "[ProjectContainer:#{@name}] already up"
      return self
    end
    start!
    self
  end

  # Command string to pass as `cmd:` to TerminalInstance / PTY.spawn.
  # -i  keep stdin open  (required for I/O through our PTY)
  # -t  allocate a PTY inside the container so bash gets job control / readline
  def exec_cmd
    "docker exec -it #{@name} bash -l"
  end

  # Stop and remove the container.  Safe to call even if it is not running.
  def stop
    puts "[ProjectContainer:#{@name}] stopping"
    system("docker stop #{Shellwords.escape(@name)} >/dev/null 2>&1")
    system("docker rm   #{Shellwords.escape(@name)} >/dev/null 2>&1")
  end

  # ── Helpers ────────────────────────────────────────────────────────────────

  def running?
    out = `docker inspect --format='{{.State.Running}}' #{Shellwords.escape(@name)} 2>/dev/null`.strip
    out == 'true'
  end

  # Absolute path the *worker* uses for this project's files (for FsLoader,
  # VfsFlusher, VfsWatcher).  When PROJECTS_ROOT is set, derived from the
  # project id; otherwise falls back to the ProjectSetting root_path.
  def worker_path
    if !PROJECTS_ROOT.empty?
      File.join(PROJECTS_ROOT, @project_id.to_s)
    else
      @root_path
    end
  end

  private

  def start!
    # Remove any stale stopped/exited container before creating a fresh one.
    system("docker rm -f #{Shellwords.escape(@name)} >/dev/null 2>&1")

    args = ['docker', 'run', '-d', '--name', @name]

    mount_args = build_mount_args
    args.concat(mount_args) unless mount_args.empty?
    args.push('-w', '/workspace') unless mount_args.empty?

    args.push(SHELL_IMAGE, 'sleep', 'infinity')

    puts "[ProjectContainer:#{@name}] #{args.join(' ')}"
    out, err, status = Open3.capture3(*args)
    out = out.strip
    unless status.success? && !out.empty?
      raise "docker run failed (exit #{status.exitstatus}): #{err.strip}"
    end

    puts "[ProjectContainer:#{@name}] up (#{out[0, 12]})"
  end

  # Decide how to expose project files to the shell container:
  #   1. CARBIDE_PROJECTS_VOLUME set → mount the named volume with subpath
  #      <project_id>.  Worker also pre-creates the subdir under
  #      PROJECTS_ROOT so the subpath exists before docker stats it.
  #   2. PROJECTS_ROOT set (no volume) → bind-mount the per-project subdir
  #      from PROJECTS_ROOT (worker and shell must see the same host path).
  #   3. Legacy: bind-mount @root_path directly.
  def build_mount_args
    if !PROJECTS_VOLUME.empty?
      FileUtils.mkdir_p(worker_path) unless worker_path.empty?
      [
        '--mount',
        "type=volume,source=#{PROJECTS_VOLUME},target=/workspace,volume-subpath=#{@project_id}"
      ]
    elsif !PROJECTS_ROOT.empty?
      FileUtils.mkdir_p(worker_path)
      ['-v', "#{worker_path}:/workspace"]
    elsif !@root_path.empty?
      ['-v', "#{@root_path}:/workspace"]
    else
      []
    end
  end
end

