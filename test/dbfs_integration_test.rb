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
require 'active_record'
require 'active_support/all'
require 'tmpdir'
require 'uri'

WORKER_DIR = File.expand_path('..', __dir__)
SERVER_DIR = ENV['CARBIDE_SERVER_DIR'] || [File.expand_path('../carbide2-server', WORKER_DIR), File.expand_path('..', WORKER_DIR)]
  .find { |d| File.exist?(File.join(d, 'lib/dbfs_v2.rb')) } or abort 'set CARBIDE_SERVER_DIR'

url = URI(ENV.fetch('DBFS_IT_DATABASE_URL') { abort 'set DBFS_IT_DATABASE_URL (the database is dropped and recreated)' })
db  = url.path.delete_prefix('/')
abort "refusing to drop #{db.inspect}: the database name must end in _test" unless db.end_with?('_test')
admin = url.dup.tap { |u| u.path = '/postgres' }
ActiveRecord::Base.establish_connection(admin.to_s)
ActiveRecord::Base.connection.drop_database(db)
ActiveRecord::Base.connection.create_database(db)
ActiveRecord::Base.establish_connection(url.to_s)
ActiveRecord::Schema.verbose = false
load File.join(SERVER_DIR, 'db/schema.rb')

ENV['PROJECTS_ROOT'] = Dir.mktmpdir('carbide-dbfs-it-projects')
ActiveRecord::Base.belongs_to_required_by_default = true # as under Rails load_defaults
class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end
require File.join(SERVER_DIR, 'lib/dbfs_v2')
Dir[File.join(SERVER_DIR, 'app/models/*.rb')].sort.each { |f| require f }
require File.join(SERVER_DIR, 'app/services/project_fs')
require File.join(SERVER_DIR, 'app/services/fs_loader')

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

    # A stale multi-change batch that would need transforming is refused whole.
    fs(a, 'read', path: '/a.txt')
    stale = a.ws.of('content').last['payload']['revision']
    fs(b, 'write', path: '/a.txt', changes: [{ change_type: 'insertDataSingleLine',
                                               change_data: { startLine: 0, startChar: 0, data: 'b' }.to_json }])
    snapshot = store.read('/a.txt')
    fs(a, 'write', path: '/a.txt', base_revision_id: stale, changes: [
      { change_type: 'insertDataSingleLine', change_data: { startLine: 2, startChar: 0, data: '1' }.to_json },
      { change_type: 'insertDataSingleLine', change_data: { startLine: 2, startChar: 1, data: '2' }.to_json }
    ])
    err = a.ws.of('error').last['payload']
    assert err['conflict'], err.inspect
    assert_equal snapshot, store.read('/a.txt'), 'nothing from the refused batch was committed'
  end

  def test_07_external_binary_ingest
    b = w[:b]
    bytes = ("\x00" + SecureRandom.random_bytes(4096)).b
    File.binwrite(disk('/blob.bin'), bytes)
    assert em_with_flusher { store.head_blob_digest('/blob.bin') == Digest::SHA256.hexdigest(bytes) }
    assert em_with_flusher { b.ws.of('created').any? { |f| f['payload']['path'] == '/blob.bin' && f['payload']['binary'] } }
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
