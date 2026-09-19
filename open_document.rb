# OpenDocument — tracks which clients have a specific file open on a specific
# branch. Only those clients receive fs:change broadcasts for that (FileNode
# UUID, branch). Path is the node's current location, not identity. Mirrors
# the ChatRoom subscriber pattern.
class OpenDocument
  attr_reader :node_id, :branch, :project_id, :clients
  attr_accessor :path

  def initialize(project_id, node_id, branch = 'main', path: nil)
    @project_id = project_id
    @node_id    = node_id.to_s
    @branch     = branch
    @path       = path   # location; rename updates this, not the OPEN_DOCUMENTS key
    @clients    = {}     # ws => { user_id:, name: }
  end

  # Display location follows a move. Identity (node_id, branch) does not.
  # A folder move rewrites this path when it sits at `from` or under it.
  def relocate_under(from, to)
    p = @path.to_s
    from = from.to_s
    to   = to.to_s
    return self if p.empty? || from.empty? || to.empty? || from == to
    prefix = from.end_with?('/') ? from : "#{from}/"
    if p == from
      @path = to
    elsif p.start_with?(prefix)
      @path = to + p[from.length..]
    end
    self
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
