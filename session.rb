# Session tracks per-connection identity and subscriptions.
class Session
  attr_reader :ws, :user_id, :name, :project_id, :terminals, :rooms

  def initialize(ws, payload)
    @ws         = ws
    @user_id    = payload['user']
    @name       = payload['name'] || "user_#{@user_id}"
    @project_id = payload['project']
    @terminals  = []  # terminal_ids joined
    @rooms      = []  # room_ids joined
  end

  def cleanup
    @terminals.each do |tid|
      TERMINALS[tid]&.remove_client(@ws)
    end
    @rooms.each do |rid|
      CHAT_ROOMS[rid]&.remove_client(@ws)
    end
  end
end
