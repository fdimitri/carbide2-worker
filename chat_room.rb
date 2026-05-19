# ChatRoom is an IRC-style room that broadcasts messages to members.
class ChatRoom
  attr_reader :room_id, :channel_id, :clients

  def initialize(room_id, channel_id: nil)
    @room_id = room_id
    @channel_id = channel_id
    @clients = {}  # ws => { user_id:, name: }
  end

  def add_client(ws, user_id:, name:)
    already_joined = @clients.key?(ws)
    @clients[ws] = { user_id: user_id, name: name }
    unless already_joined
      broadcast_to_others(ws, 'user_join', { room_id: @room_id, channel_id: @channel_id, user_id: user_id, name: name })
    end
    send_msg(ws, 'chat', 'user_list', { room_id: @room_id, channel_id: @channel_id, users: user_list })
  end

  def remove_client(ws)
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

  def user_list
    @clients.values.map { |c| { user_id: c[:user_id], name: c[:name] } }
  end

  def member?(ws)
    @clients.key?(ws)
  end

  private

  def broadcast_all(cmd, payload)
    dead = broadcast(@clients.keys, 'chat', cmd, payload)
    dead.each { |ws| @clients.delete(ws) }
  end

  def broadcast_to_others(ws, cmd, payload)
    dead = broadcast(@clients.keys.reject { |s| s == ws }, 'chat', cmd, payload)
    dead.each { |dead_ws| @clients.delete(dead_ws) }
  end
end
