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
    @history         = []
    if @agent.system_prompt.present?
      @history << { role: 'system', content: @agent.system_prompt }
    end
  end

  # Send a user message into the loop. Runs until the model returns a plain
  # assistant reply (no tool calls) or MAX_TURNS is exceeded.
  def ask(user_text)
    @history << { role: 'user', content: user_text.to_s }
    MAX_TURNS.times do |turn|
      response = post_chat_completion
      msg      = response.dig('choices', 0, 'message') || {}
      content  = msg['content']
      calls    = msg['tool_calls'] || []

      # Always append whatever the model said, even if empty (tool-only turn).
      @history << { role: 'assistant', content: content, tool_calls: calls }.compact

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
    )

    emit('tool_result', { tool: fn_name, call_id: call_id, result: result })

    # The OpenAI tool-call protocol requires a 'tool' message keyed by the
    # original call id with the JSON-encoded result.
    @history << {
      role:         'tool',
      tool_call_id: call_id,
      name:         fn_name,
      content:      result.to_json,
    }
  end

  def parse_args(raw)
    return {} if raw.empty?
    JSON.parse(raw)
  rescue JSON::ParserError
    { '_raw' => raw }
  end

  def emit(cmd, payload)
    return unless @session&.ws
    full = payload.merge(conversation_id: @conversation_id, agent: @agent.slug)
    @session.ws.send({ cs: 'agent', cmd: cmd, payload: full }.to_json)
  end
end
