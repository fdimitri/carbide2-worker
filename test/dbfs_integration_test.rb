# DBFS v2 worker integration test: real Postgres, real inotify, a real
# EventMachine reactor and a temp working tree. Exercises FsLoader, FsStore
# handlers, VfsFlusher, VfsWatcher (text echo detection and anchored merge,
# binary ingest with the read guard, deletes, oversized files), AgentTools file
# tools, and ProjectFs.write_binary!.
#
# Needs:
#   DBFS_IT_DATABASE_URL  postgres://user:pass@host/<name ending in _test>
#                         (DROPPED and recreated by this test)
#   CARBIDE_SERVER_DIR    carbide2-server checkout (default: ../carbide2-server
#                         beside this repo, or .. when run from /app/worker)
#   gems: activerecord, pg, eventmachine, rb-inotify, minitest
#
#   DBFS_IT_DATABASE_URL=postgres://carbide:carbide@127.0.0.1/carbide2_dbfs_it_test \
#     ruby test/dbfs_integration_test.rb
require_relative 'support/dbfs_boot'

worker_dir = WORKER_DIR
require 'eventmachine'
require 'set'
require 'digest'
require 'minitest/autorun'

OPEN_DOCUMENTS      = {}
SESSIONS_BY_PROJECT = {}
VFS_FLUSH_SUPPRESS  = Set.new
VFS_FLUSHERS        = {}
require File.join(worker_dir, 'open_document')
require File.join(worker_dir, 'fs_store')
require File.join(worker_dir, 'vfs_flusher')
require File.join(worker_dir, 'vfs_watcher')
require File.join(worker_dir, 'agent_tools')

class FakeWS
  attr_reader :frames
  def initialize = @frames = []
  def send(msg) = @frames << JSON.parse(msg)
  def of(cmd) = @frames.select { |f| f['cmd'] == cmd }
end

FakeSession = Struct.new(:ws, :project_id, :user_id, :name) do
  def open_file(_p) = nil
  def close_file(_p) = nil
end

def send_msg(ws, cs, cmd, payload = {}) = ws.send({ cs: cs, cmd: cmd, payload: payload }.to_json)
def broadcast(clients, cs, cmd, payload = {})
  clients.each { |ws| send_msg(ws, cs, cmd, payload) }
  []
end

class WorkerDbfsIntegrationTest < Minitest::Test
  i_suck_and_my_tests_are_order_dependent!

  # One project + reactor for the whole class, driven step by step.
  def self.world = @world ||= {}

  def w = self.class.world

  # One reactor for the whole run, on its own thread. Anything that touches
  # worker state runs on it via on_reactor; the test thread only waits.
  def self.start_reactor!
    return if @reactor
    ready = Queue.new
    @reactor = Thread.new { EM.run { ready << true } }
    ready.pop
  end

  def on_reactor
    q = Queue.new
    EM.schedule do
      q << [:ok, yield]
    rescue Exception => e # rubocop:disable Lint/RescueException
      q << [:err, e]
    end
    kind, v = q.pop
    raise v if kind == :err
    v
  end

  def wait_until(timeout = 5)
    started = Time.now
    until (ok = yield)
      return false if Time.now - started > timeout
      sleep 0.05
    end
    ok
  end

  def fs(session, cmd, payload = {})
    on_reactor do
      FsStore.handle(session, cmd, JSON.parse(payload.to_json), SESSIONS_BY_PROJECT, method(:send_msg), method(:broadcast))
    end
  end

  def store = ProjectFs.store(w[:project].id)
  def disk(p) = File.join(w[:root], p.sub(%r{\A/}, ''))

  def test_01_loader_imports_the_tree
    project = Project.create!(name: 'it', uuid: SecureRandom.uuid)
    root = project.default_root_path
    FileUtils.mkdir_p(File.join(root, 'sub/dir'))
    FileUtils.mkdir_p(File.join(root, '.git/objects'))
    File.write(File.join(root, 'a.txt'), "alpha\n")
    File.write(File.join(root, 'run.sh'), "#!/bin/sh\necho hi\n")
    File.chmod(0o755, File.join(root, 'run.sh'))
    File.write(File.join(root, 'sub/dir/nested.txt'), "nested\n")
    File.binwrite(File.join(root, 'img.bin'), "\x89PNG\x00\x01\x02".b)
    File.binwrite(File.join(root, 'big.dat'), 'x' * (ProjectFs::MAX_FILE_SIZE + 1))
    File.write(File.join(root, '.git/objects/o'), 'nope')
    w[:project] = project
    w[:root] = root

    stats = FsLoader.new(project_id: project.id, root_path: root, verbose: false).load!
    assert_equal "alpha\n", store.read('/a.txt')
    assert_equal "nested\n", store.read('/sub/dir/nested.txt')
    assert_nil store.find('/.git'), '.git is pruned'
    assert_equal 0o755, store.find('/run.sh').posix_mode & 0o777, 'mode captured from disk'
    img = store.find('/img.bin')
    assert img.binary?
    assert_equal Digest::SHA256.file(File.join(root, 'img.bin')).hexdigest, store.head_blob_digest('/img.bin')
    big = store.find('/big.dat')
    assert big.binary?, 'oversized file is tracked as binary'
    assert_nil ProjectFs.head_revision_id(big), 'oversized file has no archived revision'
    assert_equal ProjectFs::MAX_FILE_SIZE + 1, big.reload.last_size
    assert_operator stats[:files], :>=, 4
    system_id = User.system.id
    assert_equal system_id, User.find_by!(control_uuid: User::SYSTEM_UUID).id
    loaded = FileNode.where(project_id: project.id)
    assert loaded.all? { |n| n.created_by == system_id }, "imports from the working tree are the system user: #{loaded.reject { |n| n.created_by == system_id }.map { |n| [n.path, n.created_by] }}"
    assert Revision.where(file_node_id: loaded.select(:id)).all? { |r| r.user_id == system_id }

    # Second load is idempotent: no new revisions anywhere.
    before = Revision.joins(:file_node).where(file_nodes: { project_id: project.id }).count
    FsLoader.new(project_id: project.id, root_path: root, verbose: false).load!
    assert_equal before, Revision.joins(:file_node).where(file_nodes: { project_id: project.id }).count

    tree = ProjectFs.tree_json(project.id)
    assert_equal '/', tree[:path]
    assert_equal 'sub', tree[:children].first[:name], 'folders first'
    assert tree[:children].none? { |c| c[:type] == 'file' && c.key?(:children) }
  end

  def test_02_start_flusher_and_watcher
    pid = w[:project].id
    a = FakeSession.new(FakeWS.new, pid, 101, 'alice')
    b = FakeSession.new(FakeWS.new, pid, 102, 'bob')
    SESSIONS_BY_PROJECT[pid] = [a, b]
    w[:a] = a
    w[:b] = b
    flusher = VfsFlusher.new(project_id: pid, root_path: w[:root], suppress_set: VFS_FLUSH_SUPPRESS)
    VFS_FLUSHERS[pid] = flusher
    w[:flusher] = flusher
    mtime_before = File.mtime(disk('/a.txt'))
    sleep 1.1 # coarse-mtime filesystems

    self.class.start_reactor!
    on_reactor do
      EM.add_periodic_timer(VfsFlusher::POLL_INTERVAL) { flusher.flush! }
      watcher = VfsWatcher.new(project_id: pid, root_path: w[:root], suppress_set: VFS_FLUSH_SUPPRESS)
      raise 'watcher failed to start' unless watcher.start!(sessions_by_project: SESSIONS_BY_PROJECT, broadcast_fn: method(:broadcast))
      w[:watcher] = watcher
    end
    sleep 1.2
    assert_equal mtime_before, File.mtime(disk('/a.txt')), 'first sweep must not rewrite an unchanged file'
  end

  def em_with_flusher(timeout = 5, &blk) = wait_until(timeout, &blk)

  def test_03_read_open_write_broadcast_flush
    a, b = w[:a], w[:b]
    fs(a, 'read', path: '/a.txt')
    content = a.ws.of('content').last['payload']
    assert_equal "alpha\n", content['content']
    assert_match(/\A[0-9a-f-]{36}\z/, content['revision'])

    fs(a, 'open', path: '/a.txt')
    fs(b, 'open', path: '/a.txt')
    change = { change_type: 'insertDataSingleLine', change_data: { startLine: 0, startChar: 5, data: '!' }.to_json }
    fs(a, 'write', path: '/a.txt', changes: [change])
    written = a.ws.of('written').last['payload']
    assert_equal 1, written['revisions'].size
    peer = b.ws.of('change').last['payload']
    assert_equal 'insertDataSingleLine', peer['change_type']
    assert_equal written['revisions'].first, peer['revision']
    assert_empty a.ws.of('change'), 'author does not receive its own change'
    assert_equal "alpha!\n", store.read('/a.txt')

    assert em_with_flusher { File.read(disk('/a.txt')) == "alpha!\n" }, 'flushed to disk'
    assert_empty b.ws.of('set_contents'), 'our own flush is not echoed back by the watcher'
  end

  def test_04_edit_preserves_disk_mode
    a = w[:a]
    fs(a, 'write', path: '/run.sh', changes: [{ change_type: 'insertDataSingleLine',
                                                change_data: { startLine: 1, startChar: 0, data: '# ' }.to_json }])
    assert em_with_flusher { File.read(disk('/run.sh')).include?('# echo') }
    assert_equal 0o755, File.stat(disk('/run.sh')).mode & 0o777
  end

  def test_05_bad_coordinates_fail_closed_with_resync
    a = w[:a]
    before = store.read('/a.txt')
    fs(a, 'write', path: '/a.txt', changes: [{ change_type: 'deleteDataSingleLine',
                                               change_data: { startLine: 9, startChar: 0, endChar: 3 }.to_json }])
    err = a.ws.of('error').last['payload']
    assert err['resync']
    assert_equal before, store.read('/a.txt')
  end

  def test_06_external_text_edit_and_create
    b = w[:b]
    File.write(disk('/a.txt'), "alpha!\nfrom shell\n")
    assert em_with_flusher { b.ws.of('set_contents').any? }
    sc = b.ws.of('set_contents').last['payload']
    assert_equal "alpha!\nfrom shell\n", sc['content']
    assert_equal "alpha!\nfrom shell\n", store.read('/a.txt')

    File.write(disk('/sub/new.txt'), "fresh\n")
    assert em_with_flusher { store.find('/sub/new.txt') }
    assert b.ws.of('created').any? { |f| f['payload']['path'] == '/sub/new.txt' }

    system_id = User.system.id
    assert_equal system_id, Revision.find(sc['revision']).user_id, 'external edit is the system user'
    created = store.find('/sub/new.txt')
    assert_equal system_id, created.created_by
    assert Revision.where(file_node_id: created.id).all? { |r| r.user_id == system_id }
  end

  def test_06b_external_edit_merges_with_unflushed_editor_writes
    a, b = w[:a], w[:b]
    flusher = w[:flusher]
    assert wait_until { File.read(disk('/a.txt')) == store.read('/a.txt') }
    sleep 1.0 # let the watcher drain the create/flush events above
    # Hold the flusher so the editor's keystroke stays unflushed.
    on_reactor do
      flusher.instance_variable_set(:@cached_interval_s, 3600.0)
      flusher.instance_variable_set(:@settings_cached_at, EM.current_time + 3600)
      flusher.instance_variable_set(:@cached_byte_threshold, 1_000_000)
    end
    sleep 0.2
    flushes_before = File.mtime(disk('/a.txt'))
    fs(a, 'write', path: '/a.txt', changes: [{ change_type: 'insertDataSingleLine',
                                               change_data: { startLine: 0, startChar: 0, data: 'X' }.to_json }])
    assert_equal "Xalpha!\nfrom shell\n", store.read('/a.txt')
    assert_equal flushes_before, File.mtime(disk('/a.txt')), 'keystroke not flushed yet'

    b.ws.frames.clear
    File.write(disk('/a.txt'), "alpha!\nfrom shell\ntail\n")
    assert wait_until { b.ws.of('set_contents').any? }
    assert_equal "Xalpha!\nfrom shell\ntail\n", store.read('/a.txt'), 'both the keystroke and the external append survive'
  ensure
    on_reactor do
      flusher.instance_variable_set(:@cached_interval_s, VfsFlusher::DEFAULT_INTERVAL_S)
      flusher.instance_variable_set(:@settings_cached_at, 0.0)
      flusher.instance_variable_set(:@cached_byte_threshold, VfsFlusher::DEFAULT_BYTE_THRESHOLD)
    end
    assert wait_until { File.read(disk('/a.txt')) == store.read('/a.txt') }, 'merge flushed back to disk'
  end

  def test_06c_ot_with_base_revision_and_stale_batches
    a, b = w[:a], w[:b]
    fs(a, 'read', path: '/a.txt')
    base = a.ws.of('content').last['payload']['revision']
    # b writes first (blind)...
    fs(b, 'write', path: '/a.txt', changes: [{ change_type: 'insertDataSingleLine',
                                               change_data: { startLine: 0, startChar: 0, data: 'B' }.to_json }])
    # ...a's single edit based on the older revision is transformed past it.
    before = store.read('/a.txt')
    a.ws.frames.clear
    fs(a, 'write', path: '/a.txt', base_revision_id: base,
       changes: [{ change_type: 'insertDataSingleLine', change_data: { startLine: 1, startChar: 0, data: 'A' }.to_json }])
    assert a.ws.of('written').any?, a.ws.frames.inspect
    lines = store.read('/a.txt').lines
    assert lines[0].start_with?('B'), before.inspect
    assert lines[1].start_with?('A')

    # A stale multi-change batch is auto-branched at its base and rebased onto
    # main: the author gets the edits from its own view to the head, the peer
    # one frame per rebased revision, chained from its head.
    fs(a, 'read', path: '/a.txt')
    stale = a.ws.of('content').last['payload']['revision']
    fs(b, 'write', path: '/a.txt', changes: [{ change_type: 'insertDataSingleLine',
                                               change_data: { startLine: 0, startChar: 0, data: 'b' }.to_json }])
    b_head = b.ws.of('written').last['payload']['head']
    author_view = a.ws.of('content').last['payload']['content'].lines
    author_view[2] = "12#{author_view[2]}"
    b.ws.frames.clear
    fs(a, 'write', path: '/a.txt', base_revision_id: stale, changes: [
      { change_type: 'insertDataSingleLine', change_data: { startLine: 2, startChar: 0, data: '1' }.to_json },
      { change_type: 'insertDataSingleLine', change_data: { startLine: 2, startChar: 1, data: '2' }.to_json }
    ])
    ack = a.ws.of('written').last['payload']
    assert_equal 'rebased', ack['mode'], a.ws.frames.last.inspect
    assert_equal 'main', ack['branch']
    assert ack['auto_branch'].start_with?('auto/')
    merged = store.read('/a.txt')
    assert merged.start_with?('b'), merged.inspect
    assert_equal '12', merged.lines[2][0, 2]
    assert_equal ack['head'], ProjectFs.head_revision_id(store.find('/a.txt'))
    assert_equal merged, apply_frames(author_view.join, ack['changes']), 'author applies changes to its own view'

    frames = b.ws.frames.select { |f| %w[change set_contents].include?(f['cmd']) }.map { |f| f['payload'] }
    assert_equal b_head, frames.first['parent']
    assert_equal ack['head'], frames.last['revision']
    b_view = DbfsV2::Content.at(store.find('/a.txt'), b_head)
    frames.each { |f| b_view = apply_frames(b_view, [f]) }
    assert_equal merged, b_view, 'peer applies the rebased revisions to its head'
  end

  def apply_frames(text, changes)
    buf = DbfsV2::Buffer.new(text)
    changes.each { |c| buf.apply(DbfsV2::Delta.parse(c['change_type'], c['change_data'])) }
    buf.to_s
  end

  def test_07_external_binary_ingest
    b = w[:b]
    bytes = ("\x00" + SecureRandom.random_bytes(4096)).b
    File.binwrite(disk('/blob.bin'), bytes)
    assert em_with_flusher { store.head_blob_digest('/blob.bin') == Digest::SHA256.hexdigest(bytes) }
    assert em_with_flusher { b.ws.of('created').any? { |f| f['payload']['path'] == '/blob.bin' && f['payload']['binary'] } }
    node = store.find('/blob.bin')
    assert_equal User.system.id, node.created_by
    assert Revision.where(file_node_id: node.id).all? { |r| r.user_id == User.system.id }, 'binary ingest is the system user'
  end

  # decisions #28 in an event loop: a write landing while the ingest copy is in
  # flight must discard that copy; only the final content is committed.
  def test_08_binary_read_guard_discards_a_torn_read
    v1 = ("\x00" + 'A' * 8192).b
    v2 = ("\x00" + 'B' * 8192).b
    path = disk('/guarded.bin')
    copies = 0
    orig = DbfsV2::Ingest.method(:copy_hash)
    DbfsV2::Ingest.define_singleton_method(:copy_hash) do |src, dst|
      res = orig.call(src, dst)
      copies += 1
      if copies == 1
        # The file is rewritten while this (first) copy is "still reading".
        File.binwrite(src, v2)
        sleep 0.3
      end
      res
    end
    begin
      File.binwrite(path, v1)
      assert em_with_flusher(8) { store.head_blob_digest('/guarded.bin') == Digest::SHA256.hexdigest(v2) && w[:watcher].instance_variable_get(:@ingesting).nil? }
    ensure
      DbfsV2::Ingest.define_singleton_method(:copy_hash, orig)
    end
    node = store.find('/guarded.bin')
    digests = Revision.where(file_node_id: node.id, change_type: 'writeBinary').map { |r| r.payload['sha256'] }
    refute_includes digests, Digest::SHA256.hexdigest(v1), 'the torn/overwritten read was committed'
    assert_operator copies, :>=, 2
  end

  def test_09_upload_path_write_binary_is_not_double_ingested
    bytes = ("\x00upload" * 100).b
    ProjectFs.write_binary!(w[:project], store, '/up/loaded.bin', bytes, user_id: 101)
    node = store.find('/up/loaded.bin')
    assert_equal Digest::SHA256.hexdigest(bytes), store.head_blob_digest('/up/loaded.bin')
    em_with_flusher(1.5) { false } # let the watcher see the rename
    assert_equal 1, Revision.where(file_node_id: node.id).count, 'watcher trailing event is a no-op by digest'
    refute Dir.children(ProjectFs.staging_dir(w[:project])).any?, 'staging is clean'
  end

  def test_10_ws_delete_tombstones_and_is_not_echoed
    a, b = w[:a], w[:b]
    node = store.find('/sub/new.txt')
    revs = Revision.where(file_node_id: node.id).count
    b.ws.frames.clear
    fs(a, 'delete', path: '/sub/new.txt')
    refute File.exist?(disk('/sub/new.txt'))
    assert_nil store.find('/sub/new.txt')
    assert store.find_any('/sub/new.txt').deleted?
    assert_equal revs, Revision.where(file_node_id: node.id).count
    em_with_flusher(1.0) { false }
    assert_equal 1, b.ws.of('deleted').size, 'one deleted frame (FsStore), none from inotify'
  end

  def test_11_external_delete_and_resurrect
    b = w[:b]
    id = store.find('/sub/dir/nested.txt').id
    File.delete(disk('/sub/dir/nested.txt'))
    assert em_with_flusher { store.find('/sub/dir/nested.txt').nil? }
    assert b.ws.of('deleted').any? { |f| f['payload']['path'] == '/sub/dir/nested.txt' }
    File.write(disk('/sub/dir/nested.txt'), "back\n")
    assert em_with_flusher { store.find('/sub/dir/nested.txt') }
    assert_equal id, store.find('/sub/dir/nested.txt').id, 'same node, history intact'
  end

  def test_12_rename_folder_keeps_history
    a = w[:a]
    node_id = store.find('/sub/dir/nested.txt').id
    fs(a, 'rename', path: '/sub/dir', new_name: 'renamed')
    assert a.ws.of('renamed').any?
    assert File.exist?(disk('/sub/renamed/nested.txt'))
    refute File.exist?(disk('/sub/dir'))
    moved = store.find('/sub/renamed/nested.txt')
    assert_equal node_id, moved.id
    em_with_flusher(1.0) { false }
    refute_nil store.find('/sub/renamed/nested.txt'), 'watcher did not tombstone the renamed file'
    assert_nil store.find('/sub/dir')
  end

  def test_13_agent_tools
    pid = w[:project].id
    a, b = w[:a], w[:b]
    b.ws.frames.clear
    invoke = ->(*a, **k) { on_reactor { AgentTools.invoke(*a, **k) } }
    r = invoke.('read_file', allowed_slugs: ['read_file'], session: a, project_id: pid, args: { 'path' => '/a.txt' })
    rev = r[:revision]
    edit = invoke.('file_edit_anchored', allowed_slugs: ['file_edit_anchored'], session: a, project_id: pid,
                             args: { 'path' => '/a.txt', 'base_revision' => rev,
                                     'edits' => [{ 'old_string' => 'from shell', 'new_string' => 'from agent' }] })
    assert edit[:applied], edit.inspect
    assert_includes store.read('/a.txt'), "from agent\n"
    refute_includes store.read('/a.txt'), 'from shell' 
    assert b.ws.of('change').any?, 'open viewer received the agent edit'

    stale = invoke.('file_write_lines', allowed_slugs: ['file_write_lines'], session: a, project_id: pid,
                              args: { 'path' => '/a.txt', 'start_line' => 0, 'line_count' => 1, 'lines' => ["x\n"], 'base_revision' => rev })
    assert stale[:stale]

    mem = invoke.('memory_write', allowed_slugs: ['memory_write'], session: a, project_id: pid,
                            args: { 'name' => 'build-commands', 'content' => "# Build\nrake\n" })
    assert mem[:created], mem.inspect
    list = invoke.('memory_list', allowed_slugs: ['memory_list'], session: a, project_id: pid, args: {})
    assert_equal ['build-commands'], list[:memories].map { |m| m[:name] }
    mem2 = invoke.('memory_write', allowed_slugs: ['memory_write'], session: a, project_id: pid,
                             args: { 'name' => 'build-commands', 'content' => "# Build\nrake test\n" })
    refute mem2[:created]
    assert_equal "# Build\nrake test\n", store.read('/.carbide/memories/build-commands.md')

    search = invoke.('file_pcre_search', allowed_slugs: ['file_pcre_search'], session: a, project_id: pid,
                               args: { 'pattern' => 'agent', 'path' => '/a.txt' })
    assert_equal 1, search[:count]
    ls = invoke.('list_dir', allowed_slugs: ['list_dir'], session: a, project_id: pid, args: { 'path' => '/' })
    assert ls[:entries].any? { |e| e[:name] == 'a.txt' }
  end

  # A viewer on a branch: its edits land on the branch only, reach only that
  # branch's viewers, never hit disk, and a merge brings them to main's viewers
  # as one set_contents chained to the head they held.
  def test_15_branches_on_the_wire
    a, b = w[:a], w[:b]
    a.ws.frames.clear
    b.ws.frames.clear
    main_before = store.read('/a.txt')
    disk_before = File.read(disk('/a.txt'))

    fs(a, 'branch_create', path: '/a.txt', name: 'topic')
    created = a.ws.of('branch_created').last['payload']
    assert_equal 'topic', created['name']
    assert_equal ProjectFs.head_revision_id(store.find('/a.txt')), created['head']
    assert_equal 'topic', b.ws.of('branch_created').last['payload']['name'], 'other sessions hear of the branch'

    fs(a, 'branches', path: '/a.txt')
    names = a.ws.of('branches').last['payload']['branches'].map { |x| x['name'] }
    assert_includes names, 'main'
    assert_includes names, 'topic'

    fs(a, 'read', path: '/a.txt', branch: 'nope')
    assert_match(/no branch nope/, a.ws.of('error').last['payload']['error'])

    # a moves to topic; b stays on main.
    fs(a, 'close', path: '/a.txt')
    fs(a, 'open', path: '/a.txt', branch: 'topic')
    assert_equal 'topic', a.ws.of('opened').last['payload']['branch']
    fs(a, 'read', path: '/a.txt', branch: 'topic')
    content = a.ws.of('content').last['payload']
    assert_equal 'topic', content['branch']
    assert_equal main_before, content['content']

    b.ws.frames.clear
    fs(a, 'write', path: '/a.txt', branch: 'topic', base_revision_id: content['revision'], batch_id: 'bt1',
                   changes: [{ change_type: 'insertDataSingleLine', change_data: { startLine: 0, startChar: 0, data: 'T' }.to_json }])
    ack = a.ws.of('written').last['payload']
    assert_equal 'topic', ack['branch']
    assert_equal 'append', ack['mode']
    assert_equal "T#{main_before}", store.read('/a.txt', branch: 'topic')
    assert_equal main_before, store.read('/a.txt'), 'main untouched'
    assert_empty b.ws.of('change'), 'main viewers do not see branch edits'
    sleep VfsFlusher::POLL_INTERVAL * 2
    assert_equal disk_before, File.read(disk('/a.txt')), 'branch edits are not flushed'

    main_head = ProjectFs.head_revision_id(store.find('/a.txt'))
    fs(a, 'merge', path: '/a.txt', source: 'topic')
    merged = a.ws.of('merged').last['payload']
    assert_equal true, merged['merged'], merged.inspect
    assert_equal "T#{main_before}", store.read('/a.txt')
    sc = b.ws.of('set_contents').last['payload']
    assert_equal 'main', sc['branch']
    assert_equal main_head, sc['parent'], 'chained to the head main viewers held'
    assert_equal merged['head'], sc['revision']
    assert_equal "T#{main_before}", sc['content']
    assert em_with_flusher { File.read(disk('/a.txt')) == "T#{main_before}" }, 'the merge is flushed'

    fs(a, 'merge', path: '/a.txt', source: 'topic')
    assert_equal false, a.ws.of('merged').last['payload']['merged'], 'nothing left to merge'

    # A write to a missing branch is a resync error the client can attribute.
    fs(a, 'write', path: '/a.txt', branch: 'nope', batch_id: 'bt2',
                   changes: [{ change_type: 'insertDataSingleLine', change_data: { startLine: 0, startChar: 0, data: 'x' }.to_json }])
    err = a.ws.of('error').last['payload']
    assert_equal '/a.txt', err['path']
    assert_equal 'bt2', err['batch_id']
    assert err['resync']

    # b joins a on topic, then leaves: a is told, so it can drop b's cursor.
    fs(b, 'open', path: '/a.txt', branch: 'topic')
    a.ws.frames.clear
    fs(b, 'close', path: '/a.txt', branch: 'topic')
    left = a.ws.of('viewer_left').last['payload']
    assert_equal ['/a.txt', 'topic', b.user_id], [left['path'], left['branch'], left['user_id']]

    # Deleting a branch someone else is viewing is refused; once they leave,
    # it goes, everyone hears, and main's history is intact.
    fs(b, 'open', path: '/a.txt', branch: 'topic')
    fs(a, 'branch_delete', path: '/a.txt', name: 'topic')
    assert_match(/open by 1 other viewer/, a.ws.of('error').last['payload']['error'])
    fs(a, 'branch_delete', path: '/a.txt', name: 'main')
    assert_match(/cannot delete main/, a.ws.of('error').last['payload']['error'])
    fs(b, 'close', path: '/a.txt', branch: 'topic')
    b.ws.frames.clear
    fs(a, 'branch_delete', path: '/a.txt', name: 'topic')
    assert_equal 'topic', a.ws.of('branch_deleted').last['payload']['name']
    assert_equal 'topic', b.ws.of('branch_deleted').last['payload']['name']
    # Earlier tests left server-made auto/… branches on this file; they are not what is under test here.
    assert_equal %w[main], store.branches('/a.txt').map { |x| x[:name] }.reject { |n| n.start_with?('auto/') }
    assert_equal "T#{main_before}", store.read('/a.txt'), 'main survives deleting the branch it fast-forwarded to'

    fs(a, 'close', path: '/a.txt', branch: 'topic')
    fs(a, 'open', path: '/a.txt')

    # The history rail's data: keystrokes collapsed into runs, heads named,
    # every edge between present nodes, authors resolved.
    fs(a, 'dag', path: '/a.txt')
    dag = a.ws.of('dag').last['payload']
    assert_equal 3000, dag['gap_ms']
    assert_equal false, dag['auto']
    assert_equal ['main'], dag['heads'].map { |h| h['branch'] }
    ids = dag['nodes'].map { |n| n['id'] }
    assert_includes ids, dag['heads'][0]['revision']
    assert(dag['edges'].all? { |e| ids.include?(e['from']) && ids.include?(e['to']) })
    assert(dag['nodes'].all? { |n| n['count'] >= 1 && n['branch'] })
    assert dag['users'].is_a?(Hash), 'authors resolved to names (101/102 are not real users here)'
    fs(a, 'dag', path: '/a.txt', gap_ms: 0, auto: true)
    assert_equal true, a.ws.of('dag').last['payload']['auto']

    # "Load into editor": a pinned read is the content AT a revision, and a
    # branch can be forked there.
    oldest = dag['nodes'].first
    fs(a, 'read', path: '/a.txt', revision_id: oldest['first'])
    pinned = a.ws.of('content').last['payload']
    assert_equal true, pinned['pinned']
    assert_equal oldest['first'], pinned['revision']
    assert_equal DbfsV2::Content.at(store.find('/a.txt'), oldest['first']), pinned['content']
    fs(a, 'read', path: '/a.txt', revision_id: SecureRandom.uuid)
    assert_match(/no revision/, a.ws.of('error').last['payload']['error'])
    fs(a, 'branch_create', path: '/a.txt', name: 'from-history', at_revision: oldest['first'])
    assert_equal oldest['first'], a.ws.of('branch_created').last['payload']['head']
    assert_equal pinned['content'], store.read('/a.txt', branch: 'from-history')

    # A conflict, and the human path through it: preview shows all three
    # sides and a marked start text; a resolution pinned at the previewed
    # heads commits as a merge; a stale pin is refused.
    store.create_file('/c.txt', content: "one\ntwo\nthree\n", user_id: a.user_id)
    store.branch('/c.txt', 'topic')
    store.write('/c.txt', DbfsV2::Delta.new('setContents', { data: "one\nTHEIRS\nthree\n" }), branch: 'topic', user_id: b.user_id)
    store.write('/c.txt', DbfsV2::Delta.new('setContents', { data: "one\nOURS\nthree\n" }), user_id: a.user_id)
    fs(a, 'merge', path: '/c.txt', source: 'topic')
    refused = a.ws.of('merged').last['payload']
    assert_equal [false, 'conflict'], [refused['merged'], refused['reason']]

    fs(a, 'merge_preview', path: '/c.txt', source: 'topic')
    pv = a.ws.of('merge_preview').last['payload']
    assert_equal false, pv['clean']
    assert_equal ["one\ntwo\nthree\n", "one\nOURS\nthree\n", "one\nTHEIRS\nthree\n"], pv.values_at('base', 'ours', 'theirs')
    assert_includes pv['merged'], "<<<<<<< main\nOURS\n=======\nTHEIRS\n>>>>>>> topic\n"
    assert_equal 1, pv['conflict_count']
    assert_equal ProjectFs.head_revision_id(store.find('/c.txt')), pv['target_head']

    fs(a, 'merge_resolve', path: '/c.txt', source: 'topic', content: "one\nBOTH\nthree\n",
                           expected_head: SecureRandom.uuid, expected_source_head: pv['source_head'])
    stale = a.ws.of('merged').last['payload']
    assert_equal [false, 'stale'], [stale['merged'], stale['reason']]
    assert_equal "one\nOURS\nthree\n", store.read('/c.txt'), 'a stale pin commits nothing'

    fs(b, 'open', path: '/c.txt')
    b.ws.frames.clear
    fs(a, 'merge_resolve', path: '/c.txt', source: 'topic', content: "one\nBOTH\nthree\n",
                           expected_head: pv['target_head'], expected_source_head: pv['source_head'])
    done = a.ws.of('merged').last['payload']
    assert_equal true, done['merged'], done.inspect
    assert_equal "one\nBOTH\nthree\n", store.read('/c.txt')
    rev = Revision.find(done['head'])
    assert_equal [pv['target_head'], pv['source_head']], [rev.parent_id, rev.second_parent_id]
    sc = b.ws.of('set_contents').last['payload']
    assert_equal ["one\nBOTH\nthree\n", pv['target_head'], done['head']], sc.values_at('content', 'parent', 'revision')
    fs(b, 'close', path: '/c.txt')
  end

  # Project branches on the wire: a fork sees main's tree, edits its own copy,
  # and main and disk are untouched by it.
  def test_16_project_branches_on_the_wire
    a, b = w[:a], w[:b]
    a.ws.frames.clear
    b.ws.frames.clear
    store.create_file('/pb.txt', content: "pb1\n", user_id: a.user_id)
    fs(a, 'project_branch_create', name: 'feature')
    created = a.ws.of('project_branch_created').last['payload']['branch']
    assert_equal ['feature', 'main'], created.values_at('name', 'forked_from')
    assert_equal 'feature', b.ws.of('project_branch_created').last['payload']['branch']['name'], 'the project heard'
    fs(a, 'project_branches')
    assert_equal %w[main feature], a.ws.of('project_branches').last['payload']['branches'].map { |x| x['name'] }

    fs(a, 'tree', branch: 'feature')
    t = a.ws.of('tree').last['payload']
    assert_equal 'feature', t['branch']
    assert_includes flat_paths(t['tree']), '/pb.txt'

    # A read on the branch before any write: the pin, with main's head as revision.
    fs(a, 'read', path: '/pb.txt', branch: 'feature')
    c = a.ws.of('content').last['payload']
    assert_equal ["pb1\n", 'feature', ProjectFs.head_revision_id(store.find('/pb.txt'))], c.values_at('content', 'branch', 'revision')

    # Write on the branch, anchored at that revision: main unchanged, disk unchanged.
    fs(a, 'write', path: '/pb.txt', branch: 'feature', base_revision_id: c['revision'], batch_id: 'pb-1',
                   changes: [{ change_type: 'insertDataSingleLine', change_data: { startLine: 0, startChar: 3, data: '-feature' }.to_json }])
    w = a.ws.of('written').last['payload']
    assert_equal ['feature', 'append'], w.values_at('branch', 'mode'), w.inspect
    assert_equal "pb1-feature\n", store.read('/pb.txt', branch: 'feature')
    assert_equal "pb1\n", store.read('/pb.txt')
    sleep 1.0
    assert_equal "pb1\n", File.read(disk('/pb.txt')), 'disk mirrors main only'

    # Existence on the branch: create, rename, delete; main's tree does not move.
    fs(a, 'create_file', path: '/only-feature.txt', content: "of\n", branch: 'feature')
    assert_equal ['/only-feature.txt', 'feature'], a.ws.of('created').last['payload'].values_at('path', 'branch')
    assert_equal 'feature', b.ws.of('created').last['payload']['branch']
    fs(a, 'rename', path: '/pb.txt', new_name: 'pb-renamed.txt', branch: 'feature')
    assert_equal ['/pb.txt', '/pb-renamed.txt', 'feature'], a.ws.of('renamed').last['payload'].values_at('old_path', 'new_path', 'branch')
    fs(a, 'delete', path: '/run.sh', branch: 'feature')
    assert_equal ['/run.sh', 'feature'], a.ws.of('deleted').last['payload'].values_at('path', 'branch')
    fs(a, 'tree', branch: 'feature')
    fp = flat_paths(a.ws.of('tree').last['payload']['tree'])
    assert_includes fp, '/only-feature.txt'
    assert_includes fp, '/pb-renamed.txt'
    refute_includes fp, '/pb.txt'
    refute_includes fp, '/run.sh'
    fs(a, 'tree')
    mp = flat_paths(a.ws.of('tree').last['payload']['tree'])
    assert_includes mp, '/pb.txt'
    assert_includes mp, '/run.sh'
    refute_includes mp, '/only-feature.txt'
    assert File.exist?(disk('/run.sh')), 'a branch delete does not touch disk'
    assert_equal "pb1-feature\n", store.read('/pb-renamed.txt', branch: 'feature'), 'history followed the rename'

    fs(a, 'project_branch_delete', name: 'feature')
    assert_equal 'feature', a.ws.of('project_branch_deleted').last['payload']['name']
    fs(a, 'project_branches')
    assert_equal %w[main], a.ws.of('project_branches').last['payload']['branches'].map { |x| x['name'] }
    fs(a, 'tree', branch: 'feature')
    assert_equal 'main', a.ws.of('tree').last['payload']['branch'], 'a dead branch name falls back to main'
  end

  # A project merge on the wire: preview reports the identity conflict and
  # applies nothing; the merge with a resolution commits, the project hears,
  # viewers of a changed file get its text, and disk follows main.
  def test_17_project_merge_on_the_wire
    a, b = w[:a], w[:b]
    a.ws.frames.clear
    b.ws.frames.clear
    store.create_folder('/pm')
    store.create_file('/pm/keep.txt', content: "k\n")
    store.create_file('/pm/ren.txt', content: "r\n")
    fs(a, 'project_branch_create', name: 'pm')
    store.write('/pm/keep.txt', DbfsV2::Delta.new('insertDataSingleLine', { startLine: 0, startChar: 1, data: '-pm' }), branch: 'pm')
    store.move('/pm/ren.txt', '/pm/theirs.txt', branch: 'pm')
    store.move('/pm/ren.txt', '/pm/ours.txt')
    store.create_file('/pm/added.txt', content: "added\n", branch: 'pm')
    fs(b, 'open', path: '/pm/keep.txt')

    fs(a, 'project_merge_preview', source: 'pm')
    pre = a.ws.of('project_merge_preview').last['payload']
    refute pre['merged']
    assert_equal ['rename/rename'], pre['conflicts'].map { |c| c['kind'] }
    assert_nil store.find('/pm/added.txt'), 'preview applied nothing'

    fs(a, 'project_merge', source: 'pm', resolutions: { pre['conflicts'][0]['id'] => { action: 'theirs' } })
    m = a.ws.of('project_merged').last['payload']
    assert m['merged'], m.inspect
    assert_equal 'pm', b.ws.of('project_merged').last['payload']['source'], 'the project heard'
    assert_equal "k-pm\n", store.read('/pm/keep.txt')
    assert_equal "added\n", store.read('/pm/added.txt')
    assert store.find('/pm/theirs.txt')
    assert_nil store.find('/pm/ours.txt')
    sc = b.ws.of('set_contents').last&.dig('payload')
    assert sc, "no set_contents; actions=#{m['actions'].inspect} frames=#{b.ws.frames.map { |f| f['cmd'] }.inspect}"
    assert_equal ['/pm/keep.txt', 'main', "k-pm\n"], sc.values_at('path', 'branch', 'content'), 'the viewer got the merged text'
    assert em_with_flusher { File.exist?(disk('/pm/theirs.txt')) && !File.exist?(disk('/pm/ours.txt')) }
    assert em_with_flusher { File.exist?(disk('/pm/added.txt')) && File.read(disk('/pm/added.txt')) == "added\n" }
    assert em_with_flusher { File.read(disk('/pm/keep.txt')) == "k-pm\n" }

    fs(a, 'project_dag', gap_ms: 0)
    g = a.ws.of('project_dag').last['payload']
    assert_includes g['branches'].map { |x| x['name'] }, 'pm'
    assert g['edges'].any? { |e| e['kind'] == 'second_parent' }, 'the merge is drawn'
    assert g.key?('users')
  end

  def flat_paths(node)
    [node['path']] + (node['children'] || []).flat_map { |c| flat_paths(c) }
  end

  def test_14_restart_does_not_rewrite_the_tree
    sleep 1.1
    mtimes = %w[/a.txt /run.sh].to_h { |p| [p, File.mtime(disk(p))] }
    fresh = VfsFlusher.new(project_id: w[:project].id, root_path: w[:root], suppress_set: VFS_FLUSH_SUPPRESS)
    on_reactor { fresh.flush! }
    mtimes.each { |p, m| assert_equal m, File.mtime(disk(p)), "#{p} rewritten on restart" }
  end

  def test_99_stop
    w[:watcher]&.stop!
  end
end
