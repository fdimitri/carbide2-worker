# WorkerDbPool — who is holding the worker's ActiveRecord connections.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# The worker is one process, ~20 EM.defer threads, and a leased-connection
# pool. When the pool is empty the failure is ConnectionTimeoutError and
# the useful question is "which threads still have a lease, and what were
# they doing?" This answers it.
#
# Attach:
#   kill -USR1 <worker-pid>          # dump to the worker log
#   { cs: 'debug', cmd: 'db_pool' }  # reply + Debug Channel event
# A timeout also dumps itself (Command.with_error_handling and AgentSession).
require 'thread'

module WorkerDbPool
  @sql_mu   = Mutex.new
  @last_sql = {} # thread object_id => { sql, name, ms, at }

  ROLE_HINTS = [
    [/agent_session|agent_tools|agent_handlers/, 'agent'],
    [/vfs_flusher/,                              'flusher'],
    [/vfs_watcher/,                              'watcher'],
    [/fs_loader/,                                'loader'],
    [/branch_mirrors/,                           'mirrors'],
    [/fs_store|project_fs|dbfs_v2/,              'fs'],
    [/shell_client|term_handlers/,               'term'],
    [/worker\.rb/,                               'worker'],
  ].freeze

  class << self
    def install!
      install_sql_tap!
      trap_usr1!
    end

    def snapshot
      pool = ActiveRecord::Base.connection_pool
      {
        at:     Time.now.utc.iso8601(3),
        pid:    Process.pid,
        stat:   pool.stat,
        holders: pool.connections.each_with_index.map { |c, i| holder_h(c, i) },
        threads: Thread.list.map { |t| thread_h(t) },
      }
    rescue => e
      { at: Time.now.utc.iso8601(3), pid: Process.pid, error: "#{e.class}: #{e.message}" }
    end

    def dump_stderr(reason: nil)
      snap = snapshot
      $stderr.puts format_dump(snap, reason: reason)
      snap
    end

    def emit!(reason: nil)
      snap = dump_stderr(reason: reason)
      if defined?(DebugStream)
        DebugStream.emit(:db_pool, level: reason ? :error : :info,
          message: reason ? "pool dump (#{reason})" : 'pool dump',
          meta: snap)
      end
      snap
    end

    private

    def install_sql_tap!
      return if @tapped
      @tapped = true
      ActiveSupport::Notifications.subscribe('sql.active_record') do |*args|
        ev   = ActiveSupport::Notifications::Event.new(*args)
        sql  = ev.payload[:sql].to_s
        next if sql.start_with?('SHOW', 'SET ')
        @sql_mu.synchronize do
          @last_sql[Thread.current.object_id] = {
            sql:  sql[0, 240],
            name: ev.payload[:name].to_s,
            ms:   ev.duration.round(1),
            at:   Time.now.utc.iso8601(3),
          }
        end
      end
    end

    def trap_usr1!
      # Dump on a side thread: the signal handler itself must stay tiny.
      Signal.trap('USR1') { Thread.new { dump_stderr(reason: 'SIGUSR1') } }
    rescue ArgumentError
      nil # platform without USR1
    end

    def holder_h(conn, i)
      t = conn.owner
      {
        slot: i,
        in_use: !t.nil?,
        last_activity_s: (conn.seconds_since_last_activity rescue nil)&.round(2),
        idle_s: (conn.seconds_idle rescue nil)&.round(2),
        thread: thread_h(t),
        last_sql: t && @sql_mu.synchronize { @last_sql[t.object_id] },
      }
    end

    def thread_h(t)
      return { id: nil, status: 'none' } unless t
      bt = Array(t.backtrace)
      {
        id:      t.object_id,
        name:    (t.name rescue nil),
        status:  t.status.to_s,
        reactor: reactor?(t),
        role:    role_of(bt),
        backtrace: bt.first(12),
      }
    end

    def reactor?(t)
      return false unless defined?(EM) && EM.reactor_running?
      defined?(REACTOR_THREAD) && t.equal?(REACTOR_THREAD)
    end

    def role_of(bt)
      joined = bt.join("\n")
      ROLE_HINTS.each { |re, name| return name if re.match?(joined) }
      'other'
    end

    def format_dump(snap, reason: nil)
      s = snap[:stat] || {}
      lines = []
      lines << "[db_pool] pid=#{snap[:pid]} #{reason || 'dump'} " \
               "size=#{s[:size]} busy=#{s[:busy]} idle=#{s[:idle]} " \
               "dead=#{s[:dead]} waiting=#{s[:waiting]} checkout=#{s[:checkout_timeout]}"
      Array(snap[:holders]).each do |h|
        th = h[:thread] || {}
        sql = h[:last_sql]
        lines << "  conn[#{h[:slot]}] #{h[:in_use] ? 'BUSY' : 'idle'} " \
                 "role=#{th[:role]} reactor=#{th[:reactor]} " \
                 "thr=#{th[:id]} status=#{th[:status]} " \
                 "last_sql=#{sql ? "#{sql[:name]} #{sql[:sql].inspect} (#{sql[:ms]}ms)" : '-'}"
        Array(th[:backtrace]).first(6).each { |f| lines << "    #{f}" } if h[:in_use]
      end
      lines.join("\n")
    end
  end
end
