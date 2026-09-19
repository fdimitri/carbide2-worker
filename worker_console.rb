# WorkerConsole — attach to the live worker from inside the pod.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Not a new process (rails console would be). Same VM, same AR pool, same
# EM threads, same VFS_FLUSHERS. Unix socket only — kubectl exec, then:
#
#   ruby /app/worker/console
#   # or
#   socat - UNIX-CONNECT:/tmp/carbide-worker.console.sock
#
# Off with WORKER_CONSOLE=0. Path: WORKER_CONSOLE_SOCK (default
# /tmp/carbide-worker.console.sock).
require 'socket'
require 'fileutils'

module WorkerConsole
  class << self
    def sock_path
      ENV.fetch('WORKER_CONSOLE_SOCK', '/tmp/carbide-worker.console.sock')
    end

    def start!
      return if ENV['WORKER_CONSOLE'] == '0'
      path = sock_path
      File.unlink(path) if File.exist?(path)
      serv = UNIXServer.new(path)
      File.chmod(0o600, path)
      Thread.new { accept_loop(serv) }.tap { |t| t.abort_on_exception = false; t.name = 'worker-console' }
      puts "[console] attach: ruby #{File.expand_path('console', __dir__)}  (#{path})"
      maybe_rdbg!
    rescue => e
      puts "[console] not listening: #{e.class}: #{e.message}"
    end

    # Same idea as `rdbg -A` if the debug gem is in this image (dev/test
    # Gemfile). Production often does not have it; the eval socket still works.
    def maybe_rdbg!
      return if ENV['WORKER_RDBG'] == '0'
      require 'debug'
      path = ENV.fetch('WORKER_RDBG_SOCK', '/tmp/carbide-worker.rdbg.sock')
      File.unlink(path) if File.exist?(path)
      DEBUGGER__.open(sock_path: path, nonstop: true)
      puts "[console] rdbg -A #{path}"
    rescue LoadError
      nil
    rescue => e
      puts "[console] rdbg not open: #{e.class}: #{e.message}"
    end

    def accept_loop(serv)
      loop do
        sock = serv.accept
        Thread.new(sock) { |s| session(s) }.tap { |t| t.name = 'worker-console-client' }
      end
    rescue => e
      puts "[console] accept died: #{e.class}: #{e.message}"
    end

    def session(sock)
      bind = TOPLEVEL_BINDING
      sock.puts "carbide worker console  pid=#{Process.pid}  pool=#{pool_line}"
      sock.puts "same process as the worker. exit / Ctrl-D to detach."
      sock.puts "WorkerDbPool.snapshot   ActiveRecord::Base.connection_pool.stat"
      sock.puts "VFS_FLUSHERS   VFS_WATCHERS   Thread.list"
      buf = []
      loop do
        sock.write(buf.empty? ? '>> ' : '.. ')
        line = sock.gets
        break if line.nil?
        line = line.chomp
        break if buf.empty? && %w[exit quit].include?(line)
        buf << line
        src = buf.join("\n")
        next unless complete?(src)
        buf.clear
        begin
          val = bind.eval(src, '(worker-console)')
          sock.puts "=> #{safe_inspect(val)}"
        rescue SystemExit, SignalException
          break
        rescue Exception => e
          sock.puts "#{e.class}: #{e.message}"
          e.backtrace.to_a.first(12).each { |f| sock.puts "    #{f}" }
        end
      end
    rescue Errno::EPIPE, IOError
      nil
    ensure
      sock.close rescue nil
    end

    def complete?(src)
      return false if src.strip.empty?
      RubyVM::InstructionSequence.compile(src)
      true
    rescue SyntaxError => e
      e.message !~ /unexpected end-of-input|expecting (end|')'|unterminated/
    end

    def pool_line
      return '?' unless defined?(ActiveRecord::Base)
      s = ActiveRecord::Base.connection_pool.stat
      "size=#{s[:size]} busy=#{s[:busy]} idle=#{s[:idle]} waiting=#{s[:waiting]}"
    rescue
      '?'
    end

    def safe_inspect(obj)
      s = obj.inspect
      s.length > 8000 ? s[0, 8000] + '…' : s
    rescue
      "#<#{obj.class} (inspect failed)>"
    end
  end
end
