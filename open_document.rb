# OpenDocument — tracks which clients have a specific file open.
# Only those clients receive fs:change broadcasts for that file.
# Mirrors the ChatRoom subscriber pattern.
class OpenDocument
  attr_reader :path, :project_id, :clients

  def initialize(project_id, path)
    @project_id = project_id
    @path       = path   # normalized with leading /
    @clients    = {}     # ws => { user_id:, name: }
  end

  def add_client(ws, user_id:, name:)
    @clients[ws] = { user_id: user_id, name: name, cursor: nil }
  end

  def remove_client(ws)
    @clients.delete(ws)
  end

  def update_cursor(ws, line:, char:)
    return unless @clients[ws]
    @clients[ws][:cursor] = { line: line, char: char }
  end

  def member?(ws)
    @clients.key?(ws)
  end

  def empty?
    @clients.empty?
  end

  # List of viewers for sending to newly-joining clients.
  def viewers
    @clients.values.map { |c| { user_id: c[:user_id], name: c[:name], cursor: c[:cursor] } }
  end

  # ws sockets for all subscribers except the given one.
  def others(ws)
    @clients.keys.reject { |s| s == ws }
  end
end
