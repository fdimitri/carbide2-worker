# worker/handlers/fs_handlers.rb
#
# Filesystem commandSet handlers. FsStore.handle already has its own
# internal cmd dispatch and error handling, so this module is a thin
# pass-through. We deliberately don't register individual commands here —
# any 'fs' cmd is forwarded to FsStore.handle, which owns the routing
# table. Reasoning: FsStore predates the dispatcher refactor and its
# internal commands all share the same (session, payload, deps...)
# signature; extracting them into separate registered methods would just
# be wrapping a wrapper.

module FsHandlers
  extend Command::Dispatcher
  namespace 'fs'

  # Override dispatch — FsStore.handle does its own cmd routing.
  def self.dispatch(cmd, session, payload)
    Command.with_error_handling(session, 'fs', cmd) do
      FsStore.handle(
        session, cmd, payload,
        SESSIONS_BY_PROJECT,
        method(:send_msg),
        method(:broadcast)
      )
    end
  end
end
