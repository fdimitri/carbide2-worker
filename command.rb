# worker/command.rb
#
# Shared command-dispatch helpers for the worker.
#
# Each commandSet (term/chat/fs/agent) lives in its own module under
# worker/handlers/. Those modules `extend Command::Dispatcher` and register
# their commands with `register 'cmd_name', :method_name`. The top-level
# router in worker.rb looks up the module in ROUTES and calls .dispatch.
#
# Goals over the previous nested case/when:
#   - Unknown commands always produce a structured system/error (no silent drop).
#   - Every handler invocation is wrapped in error handling so a raise
#     doesn't kill the EM reactor.
#   - One obvious place for cross-cutting concerns (auth, validation, logging).
#
# This module deliberately does not introduce a class per command. If a
# handler grows complex enough to want its own class, promote just that
# one — don't pre-commit to the heavier shape.

module Command
  module_function

  # Reply to a single client (the originating session).
  def reply(session, cs, cmd, payload = {})
    send_msg(session.ws, cs, cmd, payload)
  end

  # Structured error envelope to a single client. Use this instead of
  # raising when the failure is expected (bad input, missing resource,
  # permission denied) — raises are reserved for genuine bugs and get
  # wrapped by with_error_handling.
  def error(session, message)
    send_msg(session.ws, 'system', 'error', { message: message })
  end

  # Broadcast to every connected ws in a project. Returns the list of
  # dead sockets (same contract as the top-level broadcast helper).
  def broadcast_project(project_id, cs, cmd, payload = {})
    clients = (SESSIONS_BY_PROJECT[project_id] || []).map(&:ws)
    broadcast(clients, cs, cmd, payload)
  end

  # Wrap a handler invocation. Logs to stdout and emits a system/error
  # envelope so a buggy handler doesn't take the worker down.
  def with_error_handling(session, cs, cmd)
    yield
  rescue => e
    warn "[#{cs}/#{cmd}] ERROR: #{e.class} #{e.message}\n  " \
         "#{e.backtrace.first(5).join("\n  ")}"
    error(session, "#{cs}/#{cmd} failed: #{e.message}") if session&.ws
  end

  # Mix-in for handler modules. Each module:
  #
  #   module TermHandlers
  #     extend Command::Dispatcher
  #     namespace 'term'
  #
  #     def self.create(session, payload); ...; end
  #     register 'create', :create
  #   end
  #
  # `route` in worker.rb then calls TermHandlers.dispatch(cmd, session, payload).
  module Dispatcher
    def namespace(name = nil)
      @namespace = name if name
      @namespace
    end

    def handlers
      @handlers ||= {}
    end

    def register(cmd, method_name)
      handlers[cmd.to_s] = method_name
    end

    def dispatch(cmd, session, payload)
      m = handlers[cmd.to_s]
      if m
        Command.with_error_handling(session, namespace, cmd) do
          send(m, session, payload)
        end
      else
        Command.error(session, "unknown #{namespace} cmd: #{cmd}")
      end
    end
  end
end
