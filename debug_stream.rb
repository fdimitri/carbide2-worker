# DebugStream — opt-in server-side observability channel.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# Lightweight pub/sub for "what is the server doing right now?" messages —
# inotify events, flusher activity, agent requests/responses, terminal
# lifecycle, fs deletes/renames, etc. Sessions subscribe by sending
# {cs:'debug', cmd:'subscribe'} over the WS; events are broadcast to all
# subscribed sessions for the relevant project (or all projects when
# project_id is nil — used by global-scope events like worker startup).
#
# Emit-side API (call from anywhere in the worker):
#   DebugStream.emit(:flusher, level: :info, message: 'flushed', project_id: 1,
#                    meta: {path: '/foo.rb', bytes: 123, dirty_for_ms: 4200})
#
# Levels: :trace :debug :info :warn :error
# Categories are free-form strings; suggested taxonomy:
#   :flusher :watcher :fs :agent :terminal :pod :worker :http
#
# The wire payload is:
#   {cs:'debug', cmd:'event', payload:{
#     ts: <epoch ms>,
#     category: 'flusher',
#     level: 'info',
#     message: 'flushed /foo.rb',
#     meta: { ... }
#   }}
#
# Keeping this in-memory and ephemeral on purpose — for "what's happening
# right now" use only. No persistence, no replay on reconnect.
module DebugStream
  # Map of project_id (Integer or :all) => Set of Session
  @subscriptions = Hash.new { |h, k| h[k] = Set.new }
  @mutex = Mutex.new

  class << self
    # Subscribe a session to debug events. `scope` may be a project_id or
    # :all (which gets every event regardless of project).
    def subscribe(session, scope: nil)
      key = scope || session.project_id || :all
      @mutex.synchronize { @subscriptions[key].add(session) }
    end

    def unsubscribe(session)
      @mutex.synchronize do
        @subscriptions.each_value { |set| set.delete(session) }
      end
    end

    # Emit a debug event. project_id may be nil for global events; in that
    # case only :all-scope subscribers see it. Returns nothing.
    def emit(category, level: :info, message: '', project_id: nil, meta: {})
      payload = {
        ts:       (Time.now.to_f * 1000).to_i,
        category: category.to_s,
        level:    level.to_s,
        message:  message.to_s,
        meta:     meta || {}
      }

      targets = @mutex.synchronize do
        set = Set.new
        set.merge(@subscriptions[:all])
        set.merge(@subscriptions[project_id]) if project_id
        set
      end

      return if targets.empty?

      msg = { cs: 'debug', cmd: 'event', payload: payload }.to_json
      targets.each do |session|
        session.ws.send(msg) rescue nil
      end
    rescue => e
      # Never let debug emission break the caller. Log to stderr only.
      warn "[DebugStream] emit failed: #{e.class}: #{e.message}"
    end

    # For diagnostics — how many sessions are currently listening.
    def subscriber_count
      @mutex.synchronize { @subscriptions.values.sum(&:size) }
    end
  end
end
