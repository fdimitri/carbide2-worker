# ChatRoom is an IRC-style room that broadcasts messages to members.
class ChatRoom
  attr_reader :room_id, :channel_id, :clients

  def initialize(room_id, channel_id: nil)
    @room_id = room_id
    @channel_id = channel_id
    @clients = {}       # ws => { user_id:, name: }  (text chat members)
    @call_clients = {}  # ws => { user_id:, name: }  (live WebRTC call members)
  end

  def add_client(ws, user_id:, name:)
    already_joined = @clients.key?(ws)
    @clients[ws] = { user_id: user_id, name: name }
    unless already_joined
      broadcast_to_others(ws, 'user_join', { room_id: @room_id, channel_id: @channel_id, user_id: user_id, name: name })
    end
    send_msg(ws, 'chat', 'user_list', { room_id: @room_id, channel_id: @channel_id, users: user_list })
    # Tell the (re)joining member whether a call is already in progress here so
    # they can offer to join it instead of starting a fresh one.
    send_call_state(ws)
  end

  def remove_client(ws)
    # Leaving the text room also drops the peer from any in-progress call so
    # the two never desync (a disconnect cleans up both at once).
    remove_call_client(ws)
    info = @clients.delete(ws)
    return unless info
    broadcast_all('user_leave', { room_id: @room_id, channel_id: @channel_id, user_id: info[:user_id], name: info[:name] })
  end

  def handle_message(ws, text)
    info = @clients[ws]
    return unless info
    broadcast_all('message', {
      room_id:   @room_id,
      channel_id: @channel_id,
      user_id:   info[:user_id],
      name:      info[:name],
      text:      text,
      timestamp: Time.now.utc.iso8601
    })
  end

  def handle_typing(ws)
    info = @clients[ws]
    return unless info
    broadcast_to_others(ws, 'typing', {
      channel_id: @channel_id,
      user_id:    info[:user_id],
      name:       info[:name]
    })
  end

  def user_list
    @clients.values.map { |c| { user_id: c[:user_id], name: c[:name] } }
  end

  def member?(ws)
    @clients.key?(ws)
  end

  # ── WebRTC call signalling ──────────────────────────────────────────────────
  # A call lives inside a channel: only text-room members may join it, so video
  # and text always share the same context. We use a full-mesh topology where
  # the newcomer is always the offerer (glare-free), so this room only relays
  # SDP/ICE between the two endpoints; it never inspects the payloads.

  def call_member?(ws)
    @call_clients.key?(ws)
  end

  # Add ws to the call and return the peers it should send offers to (everyone
  # already in the call). Returns nil if ws is not a text-room member.
  def add_call_client(ws)
    info = @clients[ws]
    return nil unless info
    existing = call_peers_excluding(ws)
    peer_id  = SecureRandom.uuid
    @call_clients[ws] = { peer_id: peer_id, user_id: info[:user_id], name: info[:name] }
    broadcast_call_to_others(ws, 'peer_join',
      { channel_id: @channel_id, peer_id: peer_id, user_id: info[:user_id], name: info[:name] })
    # Presence: let every channel member (not just call members) know a call is
    # live so non-participants can see it and join.
    broadcast_call_state
    existing
  end

  def remove_call_client(ws)
    info = @call_clients.delete(ws)
    return unless info
    broadcast_call_all('peer_leave',
      { channel_id: @channel_id, peer_id: info[:peer_id], user_id: info[:user_id], name: info[:name] })
    broadcast_call_state
  end

  # Relay a signalling blob (offer/answer/ICE candidate) from one call member to
  # a specific peer, addressed by peer_id.
  def relay_signal(from_ws, to_peer_id, data)
    from = @call_clients[from_ws]
    return unless from
    target_ws, = @call_clients.find { |_, c| c[:peer_id] == to_peer_id }
    return unless target_ws
    send_msg(target_ws, 'rtc', 'signal',
      { channel_id: @channel_id, from: from[:peer_id], data: data })
  end

  def call_peers_excluding(ws)
    @call_clients.reject { |s, _| s == ws }
                 .values.map { |c| { peer_id: c[:peer_id], user_id: c[:user_id], name: c[:name] } }
  end

  # Full roster of the live call (everyone, including the asker). Used for
  # channel-wide call presence so non-participants can choose to join.
  def call_roster
    @call_clients.values.map { |c| { peer_id: c[:peer_id], user_id: c[:user_id], name: c[:name] } }
  end

  private

  def send_call_state(ws)
    send_msg(ws, 'rtc', 'call_state',
      { channel_id: @channel_id, participants: call_roster })
  end

  def broadcast_call_state
    payload = { channel_id: @channel_id, participants: call_roster }
    dead = broadcast(@clients.keys, 'rtc', 'call_state', payload)
    dead.each { |ws| @clients.delete(ws) }
  end

  def broadcast_call_all(cmd, payload)
    dead = broadcast(@call_clients.keys, 'rtc', cmd, payload)
    dead.each { |ws| @call_clients.delete(ws) }
  end

  def broadcast_call_to_others(ws, cmd, payload)
    dead = broadcast(@call_clients.keys.reject { |s| s == ws }, 'rtc', cmd, payload)
    dead.each { |dead_ws| @call_clients.delete(dead_ws) }
  end

  def broadcast_all(cmd, payload)
    dead = broadcast(@clients.keys, 'chat', cmd, payload)
    dead.each { |ws| @clients.delete(ws) }
  end

  def broadcast_to_others(ws, cmd, payload)
    dead = broadcast(@clients.keys.reject { |s| s == ws }, 'chat', cmd, payload)
    dead.each { |dead_ws| @clients.delete(dead_ws) }
  end
end
