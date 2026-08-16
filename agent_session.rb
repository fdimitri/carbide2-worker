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

  # Re-point an in-memory session at the latest Agent record so config edits
  # made mid-conversation (model, provider_url, api_key, tools, sampling) take
  # effect on the next turn. The system prompt already seeded into @history is
  # intentionally left as-is — rewriting an in-flight transcript's system
  # message would be surprising and isn't reversible.
  def refresh_agent!(agent)
    @agent = agent if agent
  end

  # Send a user message into the loop. Runs until the model returns a plain
  # assistant reply (no tool calls) or MAX_TURNS is exceeded.
  #
  # images: optional array of { 'mime' => 'image/png', 'base64' => '...' }.
  # When present, the user message is sent to the model as the OpenAI
  # multimodal content-array shape (text part + one image_url part per image
  # encoded as a data: URL). Per #4 in May30-Questions, we assume the model
  # supports vision; non-vision models will return an HTTP error that the
  # caller surfaces via agent/error.
  def ask(user_text, images: nil)
    push_history!(role: 'user', content: user_text.to_s, images: images)
    DebugStream.emit(:agent, level: :info,
      message: "ask: #{user_text.to_s[0, 80]}", project_id: @project_id,
      meta: { conversation: @conversation_id[0, 8], chars: user_text.to_s.length,
              images: images&.size.to_i, agent: @agent.name }) if defined?(DebugStream)
    # Auto-title from first user message so the recent-conversations
    # dropdown shows something useful. Owner can rename later.
    if @convo.title.blank?
      t = user_text.to_s.strip.gsub(/\s+/, ' ')[0, 80]
      t = '(image)' if t.empty? && images && !images.empty?
      @convo.update_column(:title, t) unless t.empty?
    end
    MAX_TURNS.times do |turn|
      response = post_chat_completion
      msg      = response.dig('choices', 0, 'message') || {}
      content  = msg['content']
      reasoning = msg['reasoning_content']
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
        # Pass finish_reason and reasoning_content through so the client can
        # distinguish "model genuinely had nothing to say" (stop, empty
        # content) from "model was cut off mid-output by the context window"
        # (length). Reasoning content is what some models (Qwen3, DeepSeek-R1)
        # emit in `reasoning_content` — strip nil so older endpoints that
        # don't return the field don't get noise.
        emit('done', {
          content:       content.to_s,
          turn:          turn,
          finish_reason: finish,
          reasoning:     reasoning,
        }.compact)
        DebugStream.emit(:agent, level: finish == 'length' ? :warn : :info,
          message: "done turn=#{turn} finish=#{finish} chars=#{content.to_s.length}",
          project_id: @project_id,
          meta: { conversation: @conversation_id[0, 8], turn: turn,
                  finish_reason: finish, chars: content.to_s.length,
                  reasoning_chars: reasoning.to_s.length }) if defined?(DebugStream)
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
    DebugStream.emit(:agent, level: :error,
      message: "error: #{e.class}: #{e.message}", project_id: @project_id,
      meta: { conversation: @conversation_id[0, 8] }) if defined?(DebugStream)
    nil
  end

  # ─────────────────────────────────────────────────────────────────────
  private

  # Maximum AGENTS.md size we inject, in characters. A runaway AGENTS.md
  # shouldn't be able to crowd out the actual conversation; oversized files
  # are truncated with a marker.
  AGENTS_MD_MAX_CHARS = 32_000

  # The messages array sent for inference: @history with the project's
  # AGENTS.md injected as a system message directly behind the agent's own
  # system prompt, exactly once. Read fresh each turn so edits made via
  # agents_md_write (or by the user) take effect immediately. AGENTS.md is
  # NEVER persisted into @history — it's re-derived on every send.
  def outgoing_messages
    md = agents_md_content
    return @history if md.nil?

    banner = { role: 'system',
               content: "# AGENTS.md — project instructions (always in effect)\n\n#{md}" }
    if @history[0] && @history[0][:role] == 'system'
      [@history[0], banner, *@history[1..]]
    else
      [banner, *@history]
    end
  end

  # Current project AGENTS.md text, or nil when absent/empty. Truncated to
  # AGENTS_MD_MAX_CHARS. Never raises into the inference path.
  def agents_md_content
    entry = DirectoryEntry.find_by_project_and_path(@project_id, AgentTools::AGENTS_MD_PATH)
    return nil unless entry && entry.ftype == 'file' && !entry.binary?
    txt = entry.get_content.to_s
    return nil if txt.strip.empty?
    if txt.length > AGENTS_MD_MAX_CHARS
      txt = txt[0, AGENTS_MD_MAX_CHARS] + "\n\n[AGENTS.md truncated at #{AGENTS_MD_MAX_CHARS} chars]"
    end
    txt
  rescue => e
    puts "[AgentSession] AGENTS.md load failed: #{e.class} #{e.message}"
    nil
  end

  # POST one chat-completion turn. Uses SSE streaming (stream=true) so we can
  # forward partial assistant text and reasoning to the client as it arrives
  # via emit('stream', ...). Accumulates the deltas and returns a hash shaped
  # exactly like a non-streaming response so the ask() loop is unchanged:
  #   { 'choices' => [ { 'message' => {...}, 'finish_reason' => ... } ] }
  #
  # Tool-call fragments stream as partial pieces keyed by index; we reassemble
  # id/name/arguments before returning. If the server ignores stream=true and
  # returns a normal JSON body, we fall back to parsing it whole.
  def post_chat_completion
    uri = URI.parse(File.join(@agent.provider_url, 'chat/completions'))
    req = Net::HTTP::Post.new(uri)
    req['Content-Type']  = 'application/json'
    req['Accept']        = 'text/event-stream'
    req['Authorization'] = @agent.auth_header if @agent.auth_header

    body = {
      model:    @agent.model,
      messages: outgoing_messages,
      stream:   true,
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

    content   = +''
    reasoning = +''
    tool_acc  = {}   # index => { 'id', 'type', 'function' => { 'name', 'arguments' } }
    finish    = nil
    buffer    = +''
    saw_sse   = false

    http.request(req) do |resp|
      unless resp.is_a?(Net::HTTPSuccess)
        errbody = (resp.read_body rescue '')
        raise "model server #{resp.code}: #{errbody.to_s[0, 300]}"
      end

      resp.read_body do |chunk|
        buffer << chunk
        while (nl = buffer.index("\n"))
          line = buffer.slice!(0..nl).chomp
          next if line.empty? || line.start_with?(':')   # blank or SSE comment
          next unless line.start_with?('data:')
          saw_sse = true
          data = line.sub(/\Adata:\s*/, '')
          next if data == '[DONE]'
          begin
            json = JSON.parse(data)
          rescue JSON::ParserError
            next
          end
          choice = (json['choices'] || [])[0] || {}
          delta  = choice['delta'] || {}
          finish = choice['finish_reason'] if choice['finish_reason']

          if (c = delta['content']) && !c.empty?
            content << c
            emit('stream', { delta: c })
          end
          if (r = delta['reasoning_content']) && !r.empty?
            reasoning << r
            emit('stream', { reasoning_delta: r })
          end
          Array(delta['tool_calls']).each do |tc|
            idx = tc['index'] || 0
            acc = (tool_acc[idx] ||= {
              'id' => nil, 'type' => 'function',
              'function' => { 'name' => +'', 'arguments' => +'' },
            })
            acc['id'] = tc['id'] if tc['id']
            fn = tc['function'] || {}
            acc['function']['name']      << fn['name']      if fn['name']
            acc['function']['arguments'] << fn['arguments'] if fn['arguments']
          end
        end
      end
    end

    # Fallback: server ignored stream=true and returned a whole JSON body.
    if !saw_sse && !buffer.strip.empty?
      whole = JSON.parse(buffer) rescue nil
      return whole if whole.is_a?(Hash) && whole['choices']
    end

    tool_calls = tool_acc.keys.sort.map { |k| tool_acc[k] }
    {
      'choices' => [{
        'message' => {
          'content'           => content,
          'reasoning_content' => (reasoning.empty? ? nil : reasoning),
          'tool_calls'        => (tool_calls.empty? ? nil : tool_calls),
        }.compact,
        'finish_reason' => finish,
      }],
    }
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
  #
  # images (user turns only): when provided, the @history entry uses the
  # OpenAI multimodal content-array shape. The persisted AgentMessage row
  # still stores plain text only — base64 payloads are too big for the
  # current text column, and conversation replay therefore loses image
  # context. Acceptable for v1.
  def push_history!(role:, content: nil, tool_calls: nil, tool_call_id: nil, name: nil, images: nil)
    entry =
      case role
      when 'tool'
        { role: 'tool', tool_call_id: tool_call_id, name: name, content: content.to_s }
      when 'assistant'
        h = { role: 'assistant', content: content }
        h[:tool_calls] = tool_calls if tool_calls && !tool_calls.empty?
        h.compact
      when 'user'
        if images && !images.empty?
          parts = []
          text = content.to_s
          parts << { type: 'text', text: text } unless text.empty?
          images.each do |img|
            mime = (img['mime']   || img[:mime]   || 'image/png').to_s
            b64  = (img['base64'] || img[:base64]).to_s
            next if b64.empty?
            parts << { type: 'image_url',
                       image_url: { url: "data:#{mime};base64,#{b64}" } }
          end
          # Some providers (e.g. older llama.cpp builds) choke on an empty
          # text part; ensure at least an empty-string text element exists.
          parts.unshift({ type: 'text', text: '' }) unless parts.any? { |p| p[:type] == 'text' }
          { role: 'user', content: parts }
        else
          { role: 'user', content: content.to_s }
        end
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
