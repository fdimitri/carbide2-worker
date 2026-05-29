# worker/handlers/term_handlers.rb
#
# Terminal commandSet handlers. Extracted from the monolithic handle_term
# in worker.rb. Each command is its own class method on this module,
# registered via Command::Dispatcher.
#
# All handlers receive (session, payload) and call the top-level helpers
# send_msg / broadcast / broadcast_terminals_to_project / on_terminal_exit
# defined in worker.rb (those still own the global state maps).

module TermHandlers
  extend Command::Dispatcher
  namespace 'term'

  def self.create(session, payload)
    # Create new terminal in current project — attach to (or start) the
    # project's persistent Docker container.
    terminal_id    = (TERMINALS.keys.map(&:to_i).max || 0) + 1
    requested_name = payload['name']
    agent_acc      = !!payload['agent_accessible']
    puts "[term/create] terminal=#{terminal_id} project=#{session.project_id}"

    proj = Project.find_by(id: session.project_id)

    case ENV.fetch('CARBIDE_BACKEND', 'local')
    when 'kube'
      pod = PROJECT_PODS[session.project_id] ||= ProjectPod.new(session.project_id)
      pod.ensure_running!
      POD_REFCOUNTS[session.project_id] += 1
      term = TerminalInstance.new(
        terminal_id,
        project_id: session.project_id,
        cols: 80, rows: 24,
        name: requested_name,
        cmd:  pod.exec_cmd,
        cwd:  nil,
        agent_accessible: agent_acc
      )
    when 'docker'
      container = PROJECT_CONTAINERS[session.project_id] ||=
        ProjectContainer.new(session.project_id, root_path: proj&.project_setting&.root_path.presence)
      container.ensure_running!
      term = TerminalInstance.new(
        terminal_id,
        project_id: session.project_id,
        cols: 80, rows: 24,
        name: requested_name,
        cmd:  container.exec_cmd,
        cwd:  nil,
        agent_accessible: agent_acc
      )
    else
      cwd  = proj&.project_setting&.root_path.presence || PROJECT_ROOT
      term = TerminalInstance.new(
        terminal_id,
        project_id: session.project_id,
        cols: 80, rows: 24,
        name: requested_name,
        cwd:  cwd,
        agent_accessible: agent_acc
      )
    end

    TERMINALS[terminal_id] = term
    term.on_exit { |tid| on_terminal_exit(tid, session.project_id) }
    term.on_state_change { |_t| broadcast_terminals_to_project(session.project_id) }
    Command.reply(session, 'term', 'created',
                  { terminal_id: terminal_id, name: term.name })
    broadcast_terminals_to_project(session.project_id)
  end
  register 'create', :create

  def self.join(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id
      term.add_client(session.ws)
      session.terminals << tid unless session.terminals.include?(tid)
      Command.reply(session, 'term', 'joined',
                    { terminal_id: tid, rows: term.rows, cols: term.cols })
    else
      # Terminal is gone — reply with 'exit' so the client treats it as a
      # closed session rather than a hard error in the UI.
      Command.reply(session, 'term', 'exit',
                    { terminal_id: tid, code: nil, reason: 'gone' })
    end
  end
  register 'join', :join

  def self.input(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    term.write_input(payload['data'].to_s) if term
  end
  register 'input', :input

  def self.resize(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    return unless term

    term.apply_winsize(payload['rows'], payload['cols'])
    broadcast(term.clients.values, 'term', 'resized', {
      terminal_id: tid,
      rows: term.rows,
      cols: term.cols,
      user_id: session.user_id,
    })
  end
  register 'resize', :resize

  def self.rename(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id && term.rename(payload['name'])
      Command.reply(session, 'term', 'renamed',
                    { terminal_id: tid, name: term.name })
      broadcast_terminals_to_project(session.project_id)
    else
      Command.error(session, "terminal #{tid} rename failed")
    end
  end
  register 'rename', :rename

  # Toggle whether this terminal can be claimed by an agent. The flag is
  # in-memory on TerminalInstance. Allowing toggle AFTER creation is
  # intentional — a long-running shell can be handed to an agent mid-session.
  def self.set_agent_accessible(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id
      term.set_agent_accessible(!!payload['enabled'])
      # state_change_cb already triggers a broadcast; no need to re-call.
    else
      Command.error(session, "terminal #{tid} not found or access denied")
    end
  end
  register 'set_agent_accessible', :set_agent_accessible

  def self.leave(session, payload)
    tid = payload['terminal_id'].to_i
    TERMINALS[tid]&.remove_client(session.ws)
    session.terminals.delete(tid)
  end
  register 'leave', :leave

  def self.destroy(session, payload)
    tid  = payload['terminal_id'].to_i
    term = TERMINALS[tid]
    if term && term.project_id == session.project_id
      puts "[term/destroy] tid=#{tid}"
      term.destroy!
      # destroy! triggers the PTY reader's EOF path, which fires on_exit
      # and broadcasts 'term' 'exit'. Belt-and-braces fallback in case
      # the reader thread is wedged.
      EM.add_timer(0.5) { on_terminal_exit(tid, session.project_id) if TERMINALS.key?(tid) }
    else
      Command.error(session, "terminal #{tid} not found or access denied")
    end
  end
  register 'destroy', :destroy
end
