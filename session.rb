# Session tracks per-connection identity and subscriptions.
class Session
  attr_reader :ws, :user_id, :name, :project_id, :terminals, :rooms, :open_files

  def initialize(ws, payload)
    @ws         = ws
    # Accept both the new control-plane JWT format (user_id/project_id) and
    # the legacy server-minted format (user/project). See JWT_CLAIMS.md.
    @user_id    = payload['user_id']    || payload['user']
    @name       = payload['user_email'] || payload['name'] || "user_#{@user_id}"
    @project_id = payload['project_id'] || payload['project']
    @terminals  = []  # terminal_ids joined
    @rooms      = []  # room_ids joined
    @open_files = []  # normalized paths currently open
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
  end
end
