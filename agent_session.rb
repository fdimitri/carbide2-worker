require 'net/http'
require 'uri'
require 'json'
require 'securerandom'

require_relative 'agent_tools'

# AgentSession — one running agent conversation, scoped to (session, agent,
# project). Lives in memory in the worker. Per the architecture decision in
# repo/future-work.md, the loop runs WORKER-SIDE so tool calls automatically
# inherit the WebSocket session's project scope.
#
# Lifecycle:
#
#   sess = AgentSession.start(session: ws_session, agent: agent_record,
#                              project_id: 1, conversation_id: 'abc')
#   sess.ask("read README.md and summarise it")
#     -> sends 'agent/stream' messages with partial assistant text and
#        'agent/tool_call' messages when the model invokes a tool
#        and 'agent/done' when the loop finishes
#
# The loop:
#   1. POST messages + tool list to <provider_url>/chat/completions
#   2. If response contains tool_calls, dispatch each via AgentTools.invoke,
#      append the assistant message and tool messages, GOTO 1.
#   3. If response is plain content, emit 'agent/done' and exit.
#
# Streaming: the first cut uses non-streaming (single POST per turn). Adding
# stream=true SSE comes later; the WS frame protocol (agent/stream chunks
# with a delta payload) is already designed for it.
class AgentSession
  MAX_TURNS  = 8     # hard cap on tool-call loop iterations per ask()
  TIMEOUT_S  = 120   # HTTP timeout to the model server

  attr_reader :agent, :project_id, :conversation_id

  # Class-level registry so a client can reconnect to an in-flight
  # conversation. Keyed by conversation_id (client-provided UUID).
  @@sessions = {}

  def self.find(conversation_id)
    @@sessions[conversation_id]
  end

  def self.start(session:, agent:, project_id:, conversation_id:)
    inst = new(session: session, agent: agent, project_id: project_id,
               conversation_id: conversation_id)
    @@sessions[conversation_id] = inst
    inst
  end

  def initialize(session:, agent:, project_id:, conversation_id:)
    @session         = session
    @agent           = agent
    @project_id      = project_id
    @conversation_id = conversation_id
    @owner_user_id   = session.user_id
    @history         = []
    @turn            = 0  # monotonic AgentMessage row counter

    # Resume from DB if a conversation with this uuid exists, otherwise
    # create one and seed with the agent's system prompt. We persist
    # eagerly so a crash mid-turn still leaves a coherent transcript.
    @convo = AgentConversation.find_by(uuid: @conversation_id)
    if @convo
      @owner_user_id = @convo.user_id
      msgs = @convo.agent_messages.order(:turn).to_a
      @history = msgs.map(&:to_history_entry)
      @turn    = (msgs.last&.turn || -1) + 1
    else
      @convo = AgentConversation.create!(
        uuid:             @conversation_id,
        project_id:       @project_id,
        user_id:          session.user_id,
        agent_id:         @agent.id,
        visibility:       'project',
        last_activity_at: Time.current,
      )
      if @agent.system_prompt.present?
        push_history!(role: 'system', content: @agent.system_prompt)
      end
    end
  end

  # Public — used by worker.rb to authorize set_visibility, ask, etc.
  def owner_user_id ; @owner_user_id ; end
  def convo         ; @convo         ; end

  # Send a user message into the loop. Runs until the model returns a plain
  # assistant reply (no tool calls) or MAX_TURNS is exceeded.
  def ask(user_text)
    push_history!(role: 'user', content: user_text.to_s)
    # Auto-title from first user message so the recent-conversations
    # dropdown shows something useful. Owner can rename later.
    if @convo.title.blank?
      t = user_text.to_s.strip.gsub(/\s+/, ' ')[0, 80]
      @convo.update_column(:title, t) unless t.empty?
    end
    MAX_TURNS.times do |turn|
      response = post_chat_completion
      msg      = response.dig('choices', 0, 'message') || {}
      content  = msg['content']
      calls    = msg['tool_calls'] || []
      finish   = response.dig('choices', 0, 'finish_reason')

      # One-line per-turn trace so we can see exactly what the model did.
      content_preview = content.to_s.gsub(/\s+/, ' ').strip[0, 80]
      puts "[AgentSession #{@conversation_id[0,8]} turn=#{turn}] " \
           "finish=#{finish.inspect} calls=#{calls.size} " \
           "content=#{content_preview.inspect}"

      # Always append whatever the model said, even if empty (tool-only turn).
      push_history!(role: 'assistant', content: content, tool_calls: calls)

      if calls.empty?
        emit('done', { content: content.to_s, turn: turn })
        return content.to_s
      end

      # Execute every tool the model asked for this turn, append the results,
      # and loop back so the model can read them and continue.
      calls.each { |call| run_tool_call(call) }
    end
    emit('error', { message: "agent exceeded MAX_TURNS=#{MAX_TURNS}" })
    nil
  rescue => e
    emit('error', { message: "#{e.class}: #{e.message}" })
    nil
  end

  # ─────────────────────────────────────────────────────────────────────
  private

  def post_chat_completion
    uri = URI.parse(File.join(@agent.provider_url, 'chat/completions'))
    req = Net::HTTP::Post.new(uri)
    req['Content-Type']  = 'application/json'
    req['Authorization'] = @agent.auth_header if @agent.auth_header

    body = {
      model:    @agent.model,
      messages: @history,
    }
    body.merge!(@agent.sampling_params)
    tools = AgentTools.openai_tools_for(@agent.allowed_tool_slugs)
    if tools.any?
      body[:tools]       = tools
      body[:tool_choice] = 'auto'
    end
    req.body = body.to_json

    http = Net::HTTP.new(uri.hostname, uri.port)
    http.use_ssl = uri.scheme == 'https'
    http.open_timeout = 10
    http.read_timeout = TIMEOUT_S

    resp = http.request(req)
    unless resp.is_a?(Net::HTTPSuccess)
      raise "model server #{resp.code}: #{resp.body.to_s[0, 300]}"
    end
    JSON.parse(resp.body)
  end

  def run_tool_call(call)
    fn_name   = call.dig('function', 'name')
    raw_args  = call.dig('function', 'arguments').to_s
    call_id   = call['id'] || SecureRandom.hex(6)
    args      = parse_args(raw_args)

    emit('tool_call', { tool: fn_name, args: args, call_id: call_id })

    result = AgentTools.invoke(
      fn_name,
      allowed_slugs: @agent.allowed_tool_slugs,
      session:       @session,
      project_id:    @project_id,
      args:          args,
      agent:         @agent,
    )

    emit('tool_result', { tool: fn_name, call_id: call_id, result: result })

    # The OpenAI tool-call protocol requires a 'tool' message keyed by the
    # original call id with the JSON-encoded result.
    push_history!(role:         'tool',
                  tool_call_id: call_id,
                  name:         fn_name,
                  content:      result.to_json)
  end

  def parse_args(raw)
    return {} if raw.empty?
    JSON.parse(raw)
  rescue JSON::ParserError
    { '_raw' => raw }
  end

  # Fan agent events out to every client in the project who is allowed to
  # see this conversation. 'project'-visibility conversations broadcast to
  # all project sessions; 'private' conversations only go to the owner's
  # sessions. Falls back to the originating ws if SESSIONS_BY_PROJECT is
  # missing (e.g. in tests).
  def emit(cmd, payload)
    full = payload.merge(conversation_id: @conversation_id, agent: @agent.slug)
    msg  = { cs: 'agent', cmd: cmd, payload: full }.to_json

    sessions =
      if defined?(SESSIONS_BY_PROJECT)
        (SESSIONS_BY_PROJECT[@project_id] || []).select do |s|
          @convo.visibility == 'project' || s.user_id == @owner_user_id
        end
      else
        []
      end
    sessions = [@session] if sessions.empty? && @session

    sessions.each do |s|
      next unless s.ws
      begin
        s.ws.send(msg)
      rescue => e
        puts "[AgentSession.emit] ws send failed: #{e.class} #{e.message}"
      end
    end
  end

  # Append a message to both the in-memory @history and the persistent
  # AgentConversation. tool_calls is the assistant turn's tool_calls array
  # (or nil/[]); we store [] as nil to keep the column clean.
  def push_history!(role:, content: nil, tool_calls: nil, tool_call_id: nil, name: nil)
    entry =
      case role
      when 'tool'
        { role: 'tool', tool_call_id: tool_call_id, name: name, content: content.to_s }
      when 'assistant'
        h = { role: 'assistant', content: content }
        h[:tool_calls] = tool_calls if tool_calls && !tool_calls.empty?
        h.compact
      else
        { role: role, content: content.to_s }
      end
    @history << entry
    persist_calls = tool_calls && !tool_calls.empty? ? tool_calls : nil
    @convo.append!(turn: @turn, role: role, content: content,
                   tool_calls: persist_calls,
                   tool_call_id: tool_call_id, name: name)
    @turn += 1
  rescue => e
    # Persistence failure is logged but does not kill the conversation —
    # the in-memory copy still lets the user finish their turn. The next
    # successful save will pick up from the new @turn counter.
    puts "[AgentSession] persist failed: #{e.class} #{e.message}"
  end
end
