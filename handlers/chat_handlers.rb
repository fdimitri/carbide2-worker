# worker/handlers/chat_handlers.rb
#
# Chat commandSet handlers. Rooms are keyed by "project_#{pid}_channel_#{cid}".

module ChatHandlers
  extend Command::Dispatcher
  namespace 'chat'

  def self.channel_id_or_error(session, payload, what)
    cid = Integer(payload['channel_id']) rescue nil
    unless cid
      Command.error(session, "chat #{what} requires channel_id")
      return nil
    end
    cid
  end

  def self.room_id_for(session, cid)
    "project_#{session.project_id}_channel_#{cid}"
  end

  def self.join(session, payload)
    cid  = channel_id_or_error(session, payload, 'join') or return
    rid  = room_id_for(session, cid)
    room = CHAT_ROOMS[rid] ||= ChatRoom.new(rid, channel_id: cid)
    already_joined = room.member?(session.ws)
    room.add_client(session.ws, user_id: session.user_id, name: session.name)
    session.rooms << rid unless session.rooms.include?(rid)
    Command.reply(session, 'chat', 'joined',
                  { channel_id: cid, room_id: rid, already_joined: already_joined })
  end
  register 'join', :join

  def self.message(session, payload)
    cid  = channel_id_or_error(session, payload, 'message') or return
    rid  = room_id_for(session, cid)
    room = CHAT_ROOMS[rid]
    unless room && room.member?(session.ws)
      Command.error(session, 'not joined to channel')
      return
    end
    room.handle_message(session.ws, payload['text'].to_s)
  end
  register 'message', :message

  def self.typing(session, payload)
    cid = Integer(payload['channel_id']) rescue nil
    return unless cid

    rid = room_id_for(session, cid)
    CHAT_ROOMS[rid]&.handle_typing(session.ws)
  end
  register 'typing', :typing

  def self.leave(session, payload)
    cid  = channel_id_or_error(session, payload, 'leave') or return
    rid  = room_id_for(session, cid)
    room = CHAT_ROOMS[rid]
    unless room && room.member?(session.ws)
      Command.error(session, 'not joined to channel')
      return
    end
    room.remove_client(session.ws)
    session.rooms.delete(rid)
    Command.reply(session, 'chat', 'left',
                  { channel_id: cid, room_id: rid })
  end
  register 'leave', :leave
end
