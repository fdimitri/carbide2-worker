# DBFS v2 client/worker sync convergence test.
#
# The real worker FsStore (Ruby, real Postgres) against real carbide2-client
# fileSync state machines (JavaScript, hosted in Node by
# test/support/sync_driver.mjs). Several clients edit one file concurrently
# while a seeded scheduler delivers their frames in random interleavings, drops
# connections (losing whatever was in flight both ways) and reconnects them.
# Then everything is drained and checked:
#
#   * every client is idle and its text equals the server's head (convergence);
#   * insert-only runs: every typed token that wasn't refused is in the file
#     exactly once — no lost edits, and no batch applied twice by a retry.
#
# Needs: DBFS_IT_DATABASE_URL (see test/dbfs_integration_test.rb), node >= 20,
# CARBIDE_CLIENT_DIR (default ../carbide2-client beside this repo).
# SYNC_SEEDS=1,2,3 picks seeds; SYNC_STEPS the scheduler steps per run;
# SYNC_CLIENTS the number of concurrent clients.
require_relative 'support/dbfs_boot'
require 'open3'
require 'set'
require 'minitest/autorun'

CLIENT_DIR = ENV['CARBIDE_CLIENT_DIR'] || File.expand_path('../carbide2-client', WORKER_DIR)
abort "set CARBIDE_CLIENT_DIR (no services/fileSync.js under #{CLIENT_DIR})" unless
  File.exist?(File.join(CLIENT_DIR, 'src/services/fileSync.js'))

OPEN_DOCUMENTS      = {}
SESSIONS_BY_PROJECT = {}
VFS_FLUSHERS        = {}
require File.join(WORKER_DIR, 'open_document')
require File.join(WORKER_DIR, 'fs_store')

class SyncDriver
  def initialize
    @in, @out, @wait = Open3.popen2('node', File.join(__dir__, 'support/sync_driver.mjs'), CLIENT_DIR)
  end

  # Returns [sends, dump]
  def call(msg)
    @in.puts(msg.to_json)
    @in.flush
    sends = []
    dump = nil
    loop do
      line = @out.gets or raise 'sync driver exited'
      o = JSON.parse(line)
      case o['op']
      when 'ok' then return [sends, dump]
      when 'send' then sends << o
      when 'dump' then dump = o['clients']
      end
    end
  end

  def close
    @in.close
    @wait.value
  end
end

class ClientNet
  attr_reader :id, :session, :to_server, :to_client
  attr_accessor :connected

  class WS
    def initialize(net) = @net = net
    def send(msg)
      return unless @net.connected
      f = JSON.parse(msg)
      @net.to_client << [f['cmd'], f['payload']] if f['cs'] == 'fs'
    end
  end

  def initialize(id, project_id)
    @id = id
    @connected = true
    @to_server = []
    @to_client = []
    ws = WS.new(self)
    @session = Struct.new(:ws, :project_id, :user_id, :name) do
      def open_file(_) = nil
      def close_file(_) = nil
    end.new(ws, project_id, 1000 + id, "client#{id}")
  end

  def drop!
    @to_server.clear
    @to_client.clear
  end
end

class DbfsClientSyncTest < Minitest::Test
  PATH = '/doc.txt'

  def seeds = (ENV['SYNC_SEEDS'] || '11,12,13,14').split(',').map(&:to_i)
  def steps = Integer(ENV.fetch('SYNC_STEPS', '400'))
  def client_count = Integer(ENV.fetch('SYNC_CLIENTS', '3'))

  def fs(net, cmd, payload)
    FsStore.handle(net.session, cmd, JSON.parse(payload.to_json), SESSIONS_BY_PROJECT,
                   method(:send_msg), method(:broadcast))
  end

  def send_msg(ws, cs, cmd, payload = {}) = ws.send({ cs: cs, cmd: cmd, payload: payload }.to_json)

  def broadcast(clients, cs, cmd, payload = {})
    clients.each { |ws| send_msg(ws, cs, cmd, payload) }
    []
  end

  def route(nets, sends)
    sends.each { |s| nets[s['id']].to_server << [s['cmd'], s['payload']] if nets[s['id']].connected }
  end

  def run_scenario(seed:, clients:, mode:, drops:)
    rnd = Random.new(seed)
    project = Project.create!(name: "sync-#{seed}-#{mode}", uuid: SecureRandom.uuid)
    store = ProjectFs.store(project.id)
    node = store.create_file(PATH, content: "start\nline two\n")
    driver = SyncDriver.new
    nets = {}
    SESSIONS_BY_PROJECT[project.id] = []

    clients.times do |i|
      net = ClientNet.new(i, project.id)
      nets[i] = net
      SESSIONS_BY_PROJECT[project.id] << net.session
      fs(net, 'open', id: node.id)
      route(nets, driver.call(op: 'create', id: i, file_id: node.id, seed: seed * 100 + i).first)
    end

    deliver_to_server = lambda do |net|
      cmd, payload = net.to_server.shift
      fs(net, cmd, payload) if cmd
    end
    deliver_to_client = lambda do |net|
      cmd, payload = net.to_client.shift
      route(nets, driver.call(op: 'frame', id: net.id, cmd: cmd, payload: payload).first) if cmd
    end

    steps.times do
      net = nets[rnd.rand(clients)]
      r = rnd.rand
      if r < 0.40
        route(nets, driver.call(op: 'type', id: net.id, mode: mode).first)
      elsif r < 0.65
        deliver_to_server.call(net) if net.connected
      elsif r < 0.95
        deliver_to_client.call(net) if net.connected
      elsif drops
        if net.connected
          net.connected = false
          net.drop!
          driver.call(op: 'disconnect', id: net.id)
        else
          net.connected = true
          fs(net, 'open', id: node.id)
          route(nets, driver.call(op: 'connect', id: net.id).first)
        end
      end
    end

    nets.each_value do |net|
      next if net.connected
      net.connected = true
      fs(net, 'open', id: node.id)
      route(nets, driver.call(op: 'connect', id: net.id).first)
    end
    200.times do
      break if nets.values.all? { |n| n.to_server.empty? && n.to_client.empty? }
      nets.each_value { |n| deliver_to_server.call(n) until n.to_server.empty? }
      nets.each_value { |n| deliver_to_client.call(n) until n.to_client.empty? }
    end

    _, dump = driver.call(op: 'dump')
    head = store.read(PATH)
    [dump, head, nets]
  ensure
    driver&.close
  end

  def assert_converged(dump, head, label)
    dump.each do |id, c|
      assert c['loaded'], "#{label} client #{id} never loaded"
      refute c['outstanding'], "#{label} client #{id} still holds unacknowledged edits"
      assert_equal head, c['text'], "#{label} client #{id} diverged from the server head"
    end
  end

  def test_insert_only_concurrent_editing_converges_without_loss_or_duplication
    seeds.each do |seed|
      dump, head, = run_scenario(seed: seed, clients: client_count, mode: 'insert', drops: false)
      assert_converged(dump, head, "seed #{seed}")
      assert_tokens(dump, head, "seed #{seed}")
    end
  end

  def test_insert_only_with_dropped_connections
    seeds.each do |seed|
      dump, head, = run_scenario(seed: seed + 1000, clients: client_count, mode: 'insert', drops: true)
      assert_converged(dump, head, "drops seed #{seed}")
      assert_tokens(dump, head, "drops seed #{seed}")
    end
  end

  def test_mixed_inserts_and_deletes_converge
    seeds.each do |seed|
      dump, head, = run_scenario(seed: seed + 2000, clients: client_count, mode: 'mixed', drops: true)
      assert_converged(dump, head, "mixed seed #{seed}")
    end
  end

  def assert_tokens(dump, head, label)
    present = head.scan(/<\d+\.\d+>/)
    dupes = present.tally.select { |_, n| n > 1 }.keys
    assert_empty dupes, "#{label}: tokens applied more than once: #{dupes.first(10)}"
    lost = dump.values.flat_map { |c| c['lost'] }.to_set
    typed = DbfsClientSyncTest.typed_tokens(dump)
    missing = typed.reject { |t| present.include?(t) || lost.include?(t) }
    assert_empty missing, "#{label}: acknowledged edits missing from the file: #{missing.first(10)}"
    puts "#{label}: #{present.size} tokens in file, #{lost.size} refused with their batch"
  end

  # Tokens are "<client.n>", n counting 1..typed per client.
  def self.typed_tokens(dump)
    dump.flat_map { |id, c| (1..c['typed'].to_i).map { |k| "<#{id}.#{k}>" } }
  end
end
