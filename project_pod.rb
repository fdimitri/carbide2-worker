# ProjectPod — manages one Kubernetes Pod per project for user shells.
#
# Equivalent to ProjectContainer but uses `kubectl` against the workspace pod's
# in-cluster ServiceAccount instead of the host Docker socket. Required RBAC
# (pods, pods/exec) is granted by charts/workspace/templates/rbac.yaml.
#
# Lifecycle is reference-counted by worker.rb:
#   ensure_running!  — create pod if absent, wait for Ready (idempotent)
#   exec_cmd         — returns the shell string for PTY.spawn
#   stop!            — delete the pod (called when last terminal closes)
#
# The pod name is deterministic ("proj-<id>-shell") so a worker restart can
# re-adopt an existing pod instead of orphaning it.

require 'shellwords'
require 'open3'
require 'json'

class ProjectPod
  def self.read_namespace
    File.read('/var/run/secrets/kubernetes.io/serviceaccount/namespace').strip
  rescue Errno::ENOENT
    'default'
  end

  SHELL_IMAGE       = ENV.fetch('CARBIDE_SHELL_IMAGE', 'carbide2-shell:dev').freeze
  IMAGE_PULL_POLICY = ENV.fetch('CARBIDE_SHELL_PULL_POLICY', 'IfNotPresent').freeze
  NAMESPACE         = ENV.fetch('CARBIDE_NAMESPACE') { read_namespace }.freeze
  PVC_NAME          = ENV.fetch('CARBIDE_PROJECTS_PVC', '').freeze
  READY_TIMEOUT     = Integer(ENV.fetch('CARBIDE_POD_READY_TIMEOUT', '60'))

  attr_reader :project_id, :name

  def initialize(project_id)
    @project_id = project_id
    @name       = "proj-#{project_id}-shell"
  end

  def ensure_running!
    if pod_phase == 'Running'
      puts "[ProjectPod:#{@name}] already running"
      return self
    end
    delete_if_terminal!
    create!
    wait_ready!
    self
  end

  # PTY.spawn invokes /bin/sh -c with this string. `--tty --stdin` give us a
  # PTY allocation inside the pod. login shell so /etc/profile + ~/.profile load.
  def exec_cmd
    "kubectl exec -n #{NAMESPACE} --tty --stdin #{@name} -- bash -l"
  end

  def stop!
    puts "[ProjectPod:#{@name}] deleting"
    system('kubectl', 'delete', 'pod', '-n', NAMESPACE,
           '--ignore-not-found', '--wait=false', '--grace-period=5',
           @name, out: '/dev/null', err: '/dev/null')
  end

  # ─────────────────────────────────────────────────────────────────────────

  private

  def pod_phase
    out, _err, status = Open3.capture3(
      'kubectl', 'get', 'pod', '-n', NAMESPACE, @name,
      '-o', 'jsonpath={.status.phase}'
    )
    status.success? ? out.strip : nil
  end

  # The container's waiting reason, if any (e.g. ImagePullBackOff, ErrImagePull,
  # InvalidImageName, ContainerCreating). Empty string when not waiting. Lets
  # wait_ready! fail fast on an unrecoverable image problem instead of burning
  # the full READY_TIMEOUT while the pod sits in Pending.
  def container_waiting_reason
    out, _err, status = Open3.capture3(
      'kubectl', 'get', 'pod', '-n', NAMESPACE, @name,
      '-o', 'jsonpath={.status.containerStatuses[0].state.waiting.reason}'
    )
    status.success? ? out.strip : ''
  end

  # Pods in Failed/Succeeded can't be re-used; delete before recreating.
  def delete_if_terminal!
    phase = pod_phase
    return if phase.nil? || %w[Pending Running].include?(phase)
    puts "[ProjectPod:#{@name}] terminal phase=#{phase}, deleting before recreate"
    stop!
    sleep 1
  end

  def create!
    spec = pod_spec
    puts "[ProjectPod:#{@name}] creating pod (image=#{SHELL_IMAGE} pvc=#{PVC_NAME.empty? ? 'none' : PVC_NAME})"
    out, err, status = Open3.capture3(
      'kubectl', 'apply', '-n', NAMESPACE, '-f', '-',
      stdin_data: JSON.generate(spec)
    )
    unless status.success?
      raise "kubectl apply failed (exit #{status.exitstatus}): #{err.strip}\n#{out}"
    end
  end

  def wait_ready!
    deadline = Time.now + READY_TIMEOUT
    loop do
      phase = pod_phase
      case phase
      when 'Running'
        return
      when 'Failed', 'Unknown', nil
        raise "pod #{@name} entered phase=#{phase || 'missing'} before ready"
      end
      # Fail fast on an unrecoverable image problem rather than blocking the
      # caller for the full READY_TIMEOUT. With IfNotPresent + a local-only
      # image that isn't on the node, kubelet falls back to docker.io and sits
      # in ImagePullBackOff forever — there's nothing to wait for.
      reason = container_waiting_reason
      if %w[ImagePullBackOff ErrImagePull InvalidImageName ErrImageNeverPull].include?(reason)
        raise "pod #{@name} cannot start: image #{SHELL_IMAGE} #{reason} " \
              "(import it into the node: `k3d image import #{SHELL_IMAGE}`)"
      end
      if Time.now >= deadline
        raise "pod #{@name} not Ready after #{READY_TIMEOUT}s (phase=#{phase})"
      end
      sleep 0.5
    end
  end

  def pod_spec
    spec = {
      apiVersion: 'v1',
      kind: 'Pod',
      metadata: {
        name: @name,
        namespace: NAMESPACE,
        labels: {
          'app.kubernetes.io/name'      => 'carbide2-shell',
          'app.kubernetes.io/component' => 'project-shell',
          'carbide2.dev/project-id'     => @project_id.to_s,
        },
      },
      spec: {
        restartPolicy: 'Never',
        terminationGracePeriodSeconds: 5,
        # Run the shell pod as the non-root `carbide` user baked into
        # Dockerfile.shell. fsGroup ensures the PVC subPath ends up
        # writable by gid 1000 on first provision (k8s chowns the volume
        # at mount time when fsGroup is set).
        securityContext: {
          runAsUser:  1000,
          runAsGroup: 1000,
          fsGroup:    1000,
        },
        # The k3d default `local-path` provisioner creates PV directories as
        # root:root 0755, and kubelet's fsGroup recursive chown does not
        # reliably apply to subPath mounts — so /workspace lands root-owned
        # and the non-root `carbide` (uid 1000) cannot write. Fix ownership
        # up front with a root initContainer before the unprivileged shell
        # starts. No-op when there is no PVC (PVC_NAME empty).
        initContainers: pvc_mounts.empty? ? [] : [{
          name: 'fix-workspace-perms',
          image: SHELL_IMAGE,
          imagePullPolicy: IMAGE_PULL_POLICY,
          command: ['sh', '-c', 'chown 1000:1000 /workspace && chmod 0775 /workspace'],
          securityContext: { runAsUser: 0, runAsGroup: 0 },
          volumeMounts: pvc_mounts,
        }],
        containers: [{
          name: 'shell',
          image: SHELL_IMAGE,
          imagePullPolicy: IMAGE_PULL_POLICY,
          command: ['sleep', 'infinity'],
          workingDir: '/workspace',
          tty: true,
          stdin: true,
          resources: {
            requests: { cpu: '50m', memory: '128Mi' },
            limits:   { cpu: '1',   memory: '1Gi'   },
          },
          volumeMounts: pvc_mounts,
        }],
        volumes: pvc_volumes,
      },
    }
    spec
  end

  def pvc_mounts
    return [] if PVC_NAME.empty?
    [{ name: 'files', mountPath: '/workspace', subPath: @project_id.to_s }]
  end

  def pvc_volumes
    return [] if PVC_NAME.empty?
    [{ name: 'files', persistentVolumeClaim: { claimName: PVC_NAME } }]
  end
end
