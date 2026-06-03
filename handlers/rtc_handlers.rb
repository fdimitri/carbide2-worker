# worker/handlers/rtc_handlers.rb
#
# WebRTC signalling commandSet. A call is scoped to a chat channel (the same
# room used for text), so video and text share one context. The worker is a
# pure signalling relay — it forwards SDP offers/answers and ICE candidates
# between call members and never touches the media itself.
#
# Topology: full mesh. The newcomer (whoever just sent rtc/join) is always the
# offerer toward every existing peer, which avoids offer/answer glare.

module RtcHandlers
  extend Command::Dispatcher
  namespace 'rtc'

  def self.room_for(session, payload, what)
    cid = Integer(payload['channel_id']) rescue nil
    unless cid
      Command.error(session, "rtc #{what} requires channel_id")
      return [nil, nil]
    end
    rid = "project_#{session.project_id}_channel_#{cid}"
    [cid, CHAT_ROOMS[rid]]
  end

  def self.join(session, payload)
    cid, room = room_for(session, payload, 'join')
    return unless cid
    unless room && room.member?(session.ws)
      Command.error(session, 'join the channel before starting a call')
      return
    end
    peers = room.add_call_client(session.ws)
    Command.reply(session, 'rtc', 'peers', { channel_id: cid, peers: peers })
  end
  register 'join', :join

  def self.leave(session, payload)
    cid, room = room_for(session, payload, 'leave')
    return unless cid
    room&.remove_call_client(session.ws)
    Command.reply(session, 'rtc', 'left', { channel_id: cid })
  end
  register 'leave', :leave

  def self.signal(session, payload)
    cid, room = room_for(session, payload, 'signal')
    return unless cid
    to = payload['to']
    unless room && room.call_member?(session.ws) && to
      Command.error(session, 'not in call')
      return
    end
    room.relay_signal(session.ws, to, payload['data'])
  end
  register 'signal', :signal
end
