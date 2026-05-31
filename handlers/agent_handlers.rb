# worker/handlers/agent_handlers.rb
#
# Agent commandSet handlers — LLM tool-call loop entry points.
# Heavy lifting (HTTP to model server) runs in EM.defer; these handlers
# return promptly.

module AgentHandlers
  extend Command::Dispatcher
  namespace 'agent'

  def self.list(session, _payload)
    agents = Agent.enabled.order(:role, :name).map do |a|
      {
        slug:        a.slug,
        name:        a.name,
        description: a.description,
        role:        a.role,
        model:       a.model,
        tools:       a.allowed_tool_slugs,
      }
    end
    Command.reply(session, 'agent', 'list', { agents: agents })
  end
  register 'list', :list

  # Conversations visible to the requesting user in this project:
  # all 'project'-visibility threads + this user's own 'private' threads.
  def self.recent(session, payload)
    limit = (payload['limit'] || 25).to_i.clamp(1, 100)
    rows  = AgentConversation
              .visible_to(session.user_id, session.project_id)
              .limit(limit)
              .includes(:user, :agent)
              .to_a
    items = rows.map do |c|
      email = c.user&.email.to_s
      {
        conversation_id:  c.uuid,
        agent_slug:       c.agent.slug,
        agent_name:       c.agent.name,
        title:            c.title.presence || '(untitled)',
        visibility:       c.visibility,
        owner_user_id:    c.user_id,
        owner_name:       email.split('@').first.presence || "user #{c.user_id}",
        owner_is_self:    (c.user_id == session.user_id),
        last_activity_at: c.last_activity_at&.iso8601,
        message_count:    c.agent_messages.count,
      }
    end
    Command.reply(session, 'agent', 'recent', { conversations: items })
  end
  register 'recent', :recent

  def self.load(session, payload)
    conv  = payload['conversation_id'].to_s
    convo = AgentConversation.find_by(uuid: conv)
    unless convo && convo.project_id == session.project_id
      Command.error(session, 'agent/load: conversation not found in this project')
      return
    end
    unless convo.visible_to?(session.user_id)
      Command.error(session, 'agent/load: conversation is private')
      return
    end

    # Replay messages in the wire-shape AgentPane already understands.
    msgs = convo.agent_messages.order(:turn).to_a
    items = msgs.flat_map do |m|
      case m.role
      when 'user'
        [{ kind: 'user', text: m.content.to_s }]
      when 'assistant'
        out = []
        out << { kind: 'assistant', text: m.content.to_s } if m.content.to_s.strip != ''
        # tool_calls surface as their own UI rows; tool_call_id pairs with
        # the role=tool row that follows.
        (m.tool_calls || []).each do |tc|
          out << {
            kind: 'tool_call',
            id:   tc['id'],
            name: tc.dig('function', 'name'),
            args: (JSON.parse(tc.dig('function', 'arguments').to_s) rescue {}),
          }
        end
        out
      when 'tool'
        result = (JSON.parse(m.content.to_s) rescue m.content)
        [{ kind: 'tool_result', id: m.tool_call_id, name: m.name, result: result }]
      else
        [] # 'system' is hidden from UI
      end
    end

    Command.reply(session, 'agent', 'loaded', {
      conversation_id: conv,
      agent:           convo.agent.slug,
      title:           convo.title,
      visibility:      convo.visibility,
      owner_user_id:   convo.user_id,
      owner_is_self:   (convo.user_id == session.user_id),
      messages:        items,
    })
  end
  register 'load', :load

  def self.set_visibility(session, payload)
    conv = payload['conversation_id'].to_s
    vis  = payload['visibility'].to_s
    unless AgentConversation::VISIBILITIES.include?(vis)
      Command.error(session,
        "agent/set_visibility: visibility must be one of #{AgentConversation::VISIBILITIES.inspect}")
      return
    end
    convo = AgentConversation.find_by(uuid: conv)
    unless convo && convo.project_id == session.project_id
      Command.error(session, 'agent/set_visibility: conversation not found in this project')
      return
    end
    unless convo.user_id == session.user_id
      Command.error(session, 'agent/set_visibility: only the owner can change visibility')
      return
    end
    convo.update!(visibility: vis)
    # Tell every project client to refresh their dropdown (visibility
    # changes can grant or revoke access).
    Command.broadcast_project(session.project_id, 'agent', 'visibility_changed', {
      conversation_id: conv,
      visibility:      vis,
      owner_user_id:   convo.user_id,
    })
  end
  register 'set_visibility', :set_visibility

  def self.ask(session, payload)
    slug   = payload['agent_slug'].to_s
    msg    = payload['message'].to_s
    conv   = payload['conversation_id'].to_s
    # images: optional array of {mime, base64}. We assume the model supports
    # vision; if it doesn't, the provider returns an error that surfaces via
    # the existing agent/error path. Persistence-on-reload is intentionally
    # out of scope for v1 (base64 payloads are big and the AgentMessage
    # schema is text-only; a future migration can add an attachments table).
    images = payload['images'].is_a?(Array) ? payload['images'] : nil
    conv = SecureRandom.uuid if conv.empty?

    if msg.empty? && (images.nil? || images.empty?)
      Command.error(session, 'agent/ask: message or images required')
      return
    end

    agent = Agent.enabled.find_by(slug: slug)
    unless agent
      Command.error(session, "agent/ask: no enabled agent with slug=#{slug}")
      return
    end

    # If resuming an existing conversation: any project member may post
    # into a 'project'-visibility thread (multi-user agent collaboration
    # is a stated goal); 'private' threads are owner-only.
    if (existing = AgentConversation.find_by(uuid: conv))
      unless existing.project_id == session.project_id && existing.visible_to?(session.user_id)
        Command.error(session, 'agent/ask: not allowed to post into this conversation')
        return
      end
    end

    sess = AgentSession.find(conv) ||
           AgentSession.start(session: session, agent: agent,
                              project_id: session.project_id,
                              conversation_id: conv)
    # Ack immediately so the UI can show the conversation id.
    Command.reply(session, 'agent', 'started',
                  { conversation_id: conv, agent: agent.slug })

    EM.defer { sess.ask(msg, images: images) }
  end
  register 'ask', :ask
end
