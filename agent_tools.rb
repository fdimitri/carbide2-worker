# AgentTools — worker-side registry of tools an Agent may invoke.
#
# Each tool is:
#   - a JSON schema definition (sent to the LLM in tool_choice/tools)
#   - a Ruby block that takes (session:, project_id:, args:) and returns a
#     hash suitable for JSON serialization.
#
# Tools intentionally route through existing worker code paths (FsStore,
# DirectoryEntry, etc.) so they inherit the same authorization the user has
# over the project. Never add a tool that bypasses the session's project_id
# scope.
#
# Safety posture: tools added here are *capabilities*. Each Agent row picks
# which subset it's allowed to call via the allowed_tools column. That means
# a "safety-guard" agent can be wired with chat-only and zero tools, while a
# "coder" agent gets read_file + list_dir + (later) propose_patch.
module AgentTools
  # ---------------------------------------------------------------------
  # Registry. Each entry:
  #   slug => {
  #     schema:   { ...OpenAI tools[i] payload... },
  #     callable: ->(session:, project_id:, args:) { ...returns Hash... }
  #   }
  # ---------------------------------------------------------------------
  REGISTRY = {}

  def self.register(slug, schema:, &callable)
    raise ArgumentError, "tool #{slug} already registered" if REGISTRY.key?(slug)
    REGISTRY[slug] = { schema: schema, callable: callable }
  end

  def self.openai_tools_for(allowed_slugs)
    allowed_slugs.filter_map { |s| REGISTRY.dig(s, :schema) }
  end

  # Invoke a tool by name. Returns the tool's result (Hash). Raises
  # ArgumentError if the tool isn't registered or isn't in allowed_slugs.
  # Any exception inside the tool is caught and returned as { error: ... }
  # so the model can read it and retry rather than killing the loop.
  def self.invoke(slug, allowed_slugs:, session:, project_id:, args:)
    unless allowed_slugs.include?(slug)
      raise ArgumentError, "tool #{slug.inspect} not allowed for this agent"
    end
    entry = REGISTRY[slug] or raise ArgumentError, "unknown tool #{slug.inspect}"
    begin
      entry[:callable].call(session: session, project_id: project_id, args: args)
    rescue => e
      { error: "#{e.class}: #{e.message}" }
    end
  end

  # ---------------------------------------------------------------------
  # read_file(path) — return current text content of a VFS file.
  # ---------------------------------------------------------------------
  register('read_file',
    schema: {
      type: 'function',
      function: {
        name: 'read_file',
        description: 'Read the current contents of a single file in the ' \
                     "user's project filesystem. Path is the VFS path " \
                     "(absolute, starting with '/').",
        parameters: {
          type: 'object',
          required: ['path'],
          properties: {
            path: { type: 'string', description: "VFS path, e.g. '/README.md'" }
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:|
    path  = args['path'].to_s
    entry = DirectoryEntry.find_by_project_and_path(project_id, path)
    if entry.nil?
      { error: "no such path: #{path}" }
    elsif entry.ftype != 'file'
      { error: "not a file: #{path} (ftype=#{entry.ftype})" }
    else
      content = entry.calc_current
      # Cap returned content so a 5 MB log doesn't blow up the prompt.
      truncated = content.length > 64_000
      {
        path: path,
        bytes: content.bytesize,
        truncated: truncated,
        content: truncated ? content.byteslice(0, 64_000) : content,
      }
    end
  end

  # ---------------------------------------------------------------------
  # list_dir(path) — list immediate children of a VFS directory.
  # ---------------------------------------------------------------------
  register('list_dir',
    schema: {
      type: 'function',
      function: {
        name: 'list_dir',
        description: 'List the immediate children (files and folders) of a ' \
                     'directory in the project VFS.',
        parameters: {
          type: 'object',
          required: ['path'],
          properties: {
            path: { type: 'string', description: "VFS path, e.g. '/' or '/src'" }
          },
          additionalProperties: false,
        },
      },
    }
  ) do |session:, project_id:, args:|
    path  = args['path'].to_s
    entry = DirectoryEntry.find_by_project_and_path(project_id, path)
    if entry.nil?
      { error: "no such path: #{path}" }
    elsif entry.ftype != 'folder' && path != '/'
      { error: "not a directory: #{path}" }
    else
      children = DirectoryEntry.where(project_id: project_id, owner_id: entry.id).order(:cur_name)
      {
        path: path,
        entries: children.map { |c| { name: c.cur_name, type: c.ftype } },
      }
    end
  end
end
