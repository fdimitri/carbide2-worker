# worker/handlers/agent_handlers.rb
#
# Agent commandSet handlers — LLM tool-call loop entry points.
# Heavy lifting (HTTP to model server) runs in EM.defer; these handlers
# return promptly.

require 'time'

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

  # Advertise the tools this worker can make available, so the client builds
  # its per-agent allowlist UI from the live registry rather than a hardcoded
  # list. See fdimitri/carbide2#73.
  def self.tools(session, _payload)
    Command.reply(session, 'agent', 'tools', { tools: AgentTools.catalog })
  end
  register 'tools', :tools

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
        # ADR-032 fork lineage (nil for root conversations).
        forked_from_conversation_id: c.forked_from&.uuid,
        forked_at_turn:             c.forked_at_turn,
      }
    end
    Command.reply(session, 'agent', 'recent', { conversations: items })
  end
  register 'recent', :recent

  # Create a conversation WITHOUT a message (#85): the client gets the
  # worker-minted UUID up front, writes its tab as agent:<uuid>, then sends the
  # first ask against that id. This avoids any temp-key → real-id promotion of an
  # optimistic first message.
  def self.create(session, payload)
    slug = payload['agent_slug'].to_s
    agent = Agent.enabled.find_by(slug: slug)
    unless agent
      Command.error(session, "agent/create: no enabled agent with slug=#{slug}")
      return
    end
    conv = SecureRandom.uuid
    AgentSession.start(session: session, agent: agent,
                       project_id: session.project_id,
                       conversation_id: conv)
    AgentSession.subscribe(session, conv)
    session.agent_subs << conv unless session.agent_subs.include?(conv)
    Command.reply(session, 'agent', 'created', { conversation_id: conv, agent: agent.slug })
  end
  register 'create', :create

  # ADR-032: fork a conversation at a turn boundary into a new, independent
  # conversation (deep copy of the prefix). fork_at_turn defaults to the latest
  # message turn. Authorization mirrors ask (any member for project threads,
  # owner-only for private).
  def self.fork_conversation(session, payload)
    conv  = payload['conversation_id'].to_s
    convo = AgentConversation.find_by(uuid: conv)
    unless convo && convo.project_id == session.project_id
      Command.error(session, 'agent/fork: conversation not found in this project')
      return
    end
    unless convo.visible_to?(session.user_id)
      Command.error(session, 'agent/fork: conversation is private')
      return
    end

    latest = convo.agent_messages.order(:turn).last
    unless latest
      Command.error(session, 'agent/fork: conversation has no messages to fork')
      return
    end
    fork_at_turn = payload['fork_at_turn'].to_i
    fork_at_turn = latest.turn if fork_at_turn <= 0 || fork_at_turn > latest.turn

    forker = User.find_by(id: session.user_id) || convo.user
    fork = convo.fork_from!(forker: forker, fork_at_turn: fork_at_turn)

    AgentSession.start(session: session, agent: fork.agent,
                       project_id: fork.project_id,
                       conversation_id: fork.uuid)
    AgentSession.subscribe(session, fork.uuid)
    session.agent_subs << fork.uuid unless session.agent_subs.include?(fork.uuid)

    Command.reply(session, 'agent', 'forked', {
      conversation_id:            fork.uuid,
      agent:                      fork.agent.slug,
      forked_from_conversation_id: conv,
      forked_at_turn:             fork.forked_at_turn,
    })
  end
  register 'fork', :fork_conversation

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
    # includes(:user) so per-message display-name resolution doesn't N+1.
    # Each item also carries turn + agent_turn_id (ADR-032) so the client can
    # group messages into turns and offer per-turn fork points.
    msgs = convo.agent_messages.includes(:user).order(:turn).to_a
    items = msgs.flat_map do |m|
      base = { turn: m.turn, agent_turn_id: m.agent_turn_id }
      case m.role
      when 'user'
        [{ kind: 'user', text: m.content.to_s, user_id: m.user_id, name: m.user&.display_name, **base }]
      when 'assistant'
        out = []
        out << { kind: 'assistant', text: m.content.to_s, **base } if m.content.to_s.strip != ''
        # tool_calls surface as their own UI rows; tool_call_id pairs with
        # the role=tool row that follows.
        (m.tool_calls || []).each do |tc|
          out << {
            kind: 'tool_call',
            id:   tc['id'],
            name: tc.dig('function', 'name'),
            args: (JSON.parse(tc.dig('function', 'arguments').to_s) rescue {}),
            **base,
          }
        end
        out
      when 'tool'
        result = (JSON.parse(m.content.to_s) rescue m.content)
        [{ kind: 'tool_result', id: m.tool_call_id, name: m.name, result: result, **base }]
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
      # ADR-032 fork lineage.
      forked_from_conversation_id: convo.forked_from&.uuid,
      forked_at_turn:             convo.forked_at_turn,
      messages:        items,
    })
  end
  register 'load', :load

  # --- subscribe (delivery membership, #85) ---------------------------------
  # Authorization lives here: a client may subscribe only to a conversation it
  # is allowed to see. Once subscribed, AgentSession#emit fans out to
  # subscribers only — no project-wide broadcast, no visibility branch at emit.
  def self.subscribe(session, payload)
    conv = payload['conversation_id'].to_s
    convo = AgentConversation.find_by(uuid: conv)
    unless convo && convo.project_id == session.project_id
      Command.error(session, 'agent/subscribe: conversation not found in this project')
      return
    end
    unless convo.visible_to?(session.user_id)
      Command.error(session, 'agent/subscribe: conversation is private')
      return
    end
    AgentSession.subscribe(session, conv)
    session.agent_subs << conv unless session.agent_subs.include?(conv)
    Command.reply(session, 'agent', 'subscribed', { conversation_id: conv })
  end
  register 'subscribe', :subscribe

  def self.unsubscribe(session, payload)
    conv = payload['conversation_id'].to_s
    return if conv.empty?
    AgentSession.unsubscribe(session, conv)
    session.agent_subs.delete(conv)
    Command.reply(session, 'agent', 'unsubscribed', { conversation_id: conv })
  end
  register 'unsubscribe', :unsubscribe

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
    # Prune subscribers who lost access (project → private), then notify only
    # the authorized audience. Never project-wide for a private thread;
    # private → project is discovered manually via `recent` (no push).
    AgentSession.prune_unauthorized(conv)
    AgentSession.broadcast(conv, 'visibility_changed', {
      conversation_id: conv,
      visibility:      vis,
      owner_user_id:   convo.user_id,
    }, origin_ws: session.ws)
    Command.reply(session, 'agent', 'visibility_changed', {
      conversation_id: conv,
      visibility:      vis,
      owner_user_id:   convo.user_id,
    })
  end
  register 'set_visibility', :set_visibility

  # Interrupt an in-flight turn. Any project member who can see the
  # conversation may stop it (project-visible => all members; private => owner
  # only) — matches the existing "any member may post" rule for shared threads.
  def self.stop(session, payload)
    conv = payload['conversation_id'].to_s
    sess = AgentSession.find(conv)
    unless sess && sess.convo&.project_id == session.project_id
      Command.error(session, 'agent/stop: conversation not found in this project')
      return
    end
    unless sess.convo.visible_to?(session.user_id)
      Command.error(session, 'agent/stop: conversation is private')
      return
    end
    sess.request_cancel!
    Command.reply(session, 'agent', 'stopping', { conversation_id: conv })
  end
  register 'stop', :stop

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

    sess = AgentSession.find(conv)
    if sess
      # Existing in-memory conversation: adopt the freshly-loaded agent so
      # runtime config edits (model, provider_url, tools, sampling) propagate.
      sess.refresh_agent!(agent)
    else
      sess = AgentSession.start(session: session, agent: agent,
                                project_id: session.project_id,
                                conversation_id: conv)
    end

    # Serialize turns per conversation. agent/ask runs on EM.defer, so without
    # this a second ask (or Stop then a quick resend) could clear the cancel
    # flag for an in-flight loop and interleave @history/@turn.
    unless sess.try_begin_turn!
      Command.error(session, 'agent/ask: a turn is already in progress for this conversation')
      return
    end

    begin
      # The asker is an implicit subscriber (delivery membership). Explicit
      # subscribe is also available for opening/loading a conversation without
      # asking.
      AgentSession.subscribe(session, conv)
      session.agent_subs << conv unless session.agent_subs.include?(conv)
      # Ack immediately so the UI can show the conversation id.
      Command.reply(session, 'agent', 'started',
                    { conversation_id: conv, agent: agent.slug })

      # Resolve the display name from the authoritative user record rather than
      # the JWT claim (session.name is user_email on new control-plane tokens).
      author = User.find_by(id: session.user_id)
      name   = author&.display_name || session.name || "user #{session.user_id}"

      # Fan the user turn out to everyone else who is allowed to see this
      # conversation. The sender pushed it locally already, so exclude only the
      # originating socket (same user's other sessions still receive it).
      sess.broadcast_user_turn(
        user_id:   session.user_id,
        name:      name,
        text:      msg,
        images:    images,
        origin_ws: session.ws,
      )

      EM.defer do
        begin
          sess.ask(msg, images: images, author_user_id: session.user_id)
        ensure
          sess.finish_turn!
        end
      end
    rescue StandardError
      # A synchronous failure between try_begin_turn! and EM.defer must still
      # release the turn lock, or the conversation is stuck in
      # "a turn is already in progress" until the worker restarts (#93).
      # Re-raise so Command.with_error_handling surfaces it.
      sess.finish_turn!
      raise
    end
  end
  register 'ask', :ask

  # ADR-033 phase 1: soft-evict (tombstone) tool results and/or tool call text
  # from a conversation's history, oldest-first. dry_run returns a preview
  # without writing. Selected messages keep their rows and structural fields
  # (turn/tool_call_id/name); only the payload leaves the prompt.
  def self.clean(session, payload)
    conv  = payload['conversation_id'].to_s
    convo = AgentConversation.find_by(uuid: conv)
    unless convo && convo.project_id == session.project_id
      Command.error(session, 'agent/clean: conversation not found in this project')
      return
    end
    unless convo.visible_to?(session.user_id)
      Command.error(session, 'agent/clean: conversation is private')
      return
    end

    scope = payload['scope'].to_s
    scope = 'both' if scope.empty?
    unless %w[results calls both].include?(scope)
      Command.error(session, 'agent/clean: scope must be results, calls, or both')
      return
    end
    mode = payload['mode'].to_s
    unless %w[before_datetime first_n n_size].include?(mode)
      Command.error(session, 'agent/clean: mode must be before_datetime, first_n, or n_size')
      return
    end

    candidates = clean_candidates(convo, scope)
    selected   = select_candidates(candidates, mode, payload)

    preview = {
      conversation_id: conv,
      total_results:   candidates.count { |m| m.role == 'tool' },
      total_calls:     candidates.count { |m| m.role == 'assistant' },
      total_bytes:     candidates.sum { |m| clean_size(m) },
      removed_results: selected.count { |m| m.role == 'tool' },
      removed_calls:   selected.count { |m| m.role == 'assistant' },
      bytes_reclaimed: selected.sum { |m| clean_size(m) },
    }

    if payload['dry_run']
      Command.reply(session, 'agent', 'clean_preview', preview)
      return
    end

    # Reflect the eviction in the live in-memory @history too (ADR-033 §4).
    # Reject while a turn is in flight rather than mutate @history mid-loop.
    sess = AgentSession.find(conv)
    if sess && !sess.try_begin_turn!
      Command.error(session, 'agent/clean: a turn is in progress; retry after it finishes')
      return
    end

    begin
      sess.evict!(selected) if sess
      Command.reply(session, 'agent', 'cleaned', {
        conversation_id: conv,
        removed_results: preview[:removed_results],
        removed_calls:   preview[:removed_calls],
        bytes_reclaimed: preview[:bytes_reclaimed],
      })
    ensure
      sess.finish_turn! if sess
    end
  end
  register 'clean', :clean

  # Candidate evictable messages in turn order: tool results and/or assistant
  # rows carrying tool_calls, not already evicted.
  def self.clean_candidates(convo, scope)
    convo.agent_messages.not_evicted.order(:turn).to_a.select do |m|
      case scope
      when 'results' then m.role == 'tool'
      when 'calls'   then m.role == 'assistant' && m.tool_calls.present?
      else                m.role == 'tool' || (m.role == 'assistant' && m.tool_calls.present?)
      end
    end
  end

  # Evictable payload size in bytes: result content, or the sum of a call's
  # arguments. (Phase 1 measures bytes; token-aware sizing is phase 2.)
  def self.clean_size(m)
    case m.role
    when 'tool'
      m.content.to_s.bytesize
    when 'assistant'
      (m.tool_calls || []).sum { |tc| tc.dig('function', 'arguments').to_s.bytesize }
    else
      0
    end
  end

  # Front-trim the candidate list per the requested mode.
  def self.select_candidates(candidates, mode, payload)
    case mode
    when 'before_datetime'
      t = parse_clean_time(payload['cutoff'])
      return [] unless t
      candidates.select { |m| m.created_at < t }
    when 'first_n'
      n = payload['n'].to_i
      n.positive? ? candidates.first(n) : []
    when 'n_size'
      bytes = payload['bytes'].to_i
      return [] unless bytes.positive?
      out   = []
      total = 0
      candidates.each do |m|
        break if total >= bytes
        out << m
        total += clean_size(m)
      end
      out
    end
  end

  def self.parse_clean_time(cutoff)
    return nil if cutoff.to_s.empty?
    Time.iso8601(cutoff)
  rescue ArgumentError
    Time.parse(cutoff)
  rescue ArgumentError
    nil
  end
end
