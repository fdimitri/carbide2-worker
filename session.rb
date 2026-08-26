# Session tracks per-connection identity and subscriptions.
class Session
  attr_reader :ws, :user_id, :name, :project_id, :terminals, :rooms, :open_files, :session_subs, :agent_subs
  # Unix timestamp (seconds) at which the presenting JWT expires. The socket is
  # forcibly closed once this lapses (plus a small grace) unless the client
  # presents a fresh token via system/reauth first. nil means "no exp claim"
  # (legacy tokens) — those are not expiry-enforced.
  attr_reader :token_exp

  # A socket that has connected but not yet authenticated (ADR-018). It holds
  # no principal and may only send system/auth until it is promoted.
  def self.unauthenticated(ws)
    new(ws, nil)
  end

  def initialize(ws, payload)
    @ws         = ws
    @terminals  = []  # terminal_ids joined
    @rooms      = []  # room_ids joined
    @open_files = []  # normalized paths currently open
    @session_subs = []  # browser-session uuids this ws is subscribed to
    @agent_subs   = []  # agent conversation ids this ws is subscribed to (#85)
    pin_principal(payload) if payload
  end

  # True once a validated token has been pinned (via system/auth or the legacy
  # ?token= URI path). An unauthenticated session has no project yet.
  def authenticated?
    !@project_id.nil?
  end

  # Pin identity from a validated JWT payload. Accepts both the new
  # control-plane JWT format (user_id/project_id) and the legacy
  # server-minted format (user/project). See JWT_CLAIMS.md.
  def pin_principal(payload)
    @user_id    = payload['user_id']    || payload['user']
    @name       = payload['user_email'] || payload['name'] || "user_#{@user_id}"
    @project_id = payload['project_id'] || payload['project']
    @token_exp  = payload['exp']
  end

  # Adopt a freshly-minted token (already validated) without dropping the
  # socket. We only refresh the expiry — identity/project are pinned at connect
  # and a token that changed them would have failed validation upstream.
  def reauth(payload)
    @token_exp = payload['exp']
  end

  # True once the token has lapsed beyond the given grace window. Sessions with
  # no exp claim (legacy tokens) never expire here.
  def token_expired?(grace_seconds = 0)
    return false unless @token_exp
    Time.now.to_i > (@token_exp + grace_seconds)
  end

  def open_file(path)
    @open_files << path unless @open_files.include?(path)
  end

  def close_file(path)
    @open_files.delete(path)
  end

  def cleanup
    @terminals.each do |tid|
      TERMINALS[tid]&.remove_client(@ws)
    end
    @rooms.each do |rid|
      CHAT_ROOMS[rid]&.remove_client(@ws)
    end
    @open_files.dup.each do |path|
      key = "#{@project_id}:#{path}"
      doc = OPEN_DOCUMENTS[key]
      next unless doc
      doc.remove_client(@ws)
      OPEN_DOCUMENTS.delete(key) if doc.empty?
    end
    @open_files.clear
    @session_subs.dup.each do |uuid|
      subs = SESSION_SUBSCRIBERS[uuid]
      next unless subs
      subs.delete(@ws)
      SESSION_SUBSCRIBERS.delete(uuid) if subs.empty?
    end
    @session_subs.clear
    @agent_subs.dup.each do |conversation_id|
      AgentSession.unsubscribe(self, conversation_id)
    end
    @agent_subs.clear
  end
end
