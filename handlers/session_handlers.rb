# worker/handlers/session_handlers.rb
#
# 'session' commandSet — server-side browser-session tracking (resume + watch).
#
# GENERIC BOILERPLATE: the wire shape of the session document is NOT finalized.
# The server treats `doc` as an OPAQUE JSON tree and only applies generic path
# patches ([key, key, ...] -> value | delete), persists on EVERY patch (plain AR
# save — fine at carbide scale), and rebroadcasts to watchers. All session
# semantics (what a "pane"/"focus" is) live client-side.
#
# Commands:
#   create      {from_uuid?, name?, doc?, client_version?, client_sha?, doc_version?} -> create (or fork) a session; you are its producer
#   resume      {session_uuid}              -> load a session you own; become producer + subscribe
#   patch       {session_uuid, ops:[...], client_sha?, doc_version?} -> apply path ops to doc, re-stamp last-writer signature, persist, rebroadcast
#   resync      {session_uuid, doc, client_sha?, doc_version?} -> REPLACE the whole doc (producer's normalized toDoc), re-stamp, persist, snapshot watchers
#   subscribe   {session_uuid}              -> WATCH read-only; get snapshot + future patches
#   unsubscribe {session_uuid}
#   snapshot    {session_uuid}              -> current doc (resume/join)
#   delete      {session_uuid}              -> destroy a session you own (garbage collection)
#   list        {}                          -> this user's sessions (resume picker)
#
# ops shape (opaque values):
#   [ { "path": ["a","b"], "value": <any> },   # set
#     { "path": ["a","c"], "op": "delete" } ]   # delete
module SessionHandlers
  extend Command::Dispatcher
  namespace 'session'

  # --- lookup helper ---------------------------------------------------------
  def self.find_session(session, payload, what)
    uuid = payload['session_uuid'].to_s
    if uuid.empty?
      Command.error(session, "session #{what} requires session_uuid")
      return nil
    end
    bs = BrowserSession.find_by(session_uuid: uuid)
    unless bs
      Command.error(session, "session not found: #{uuid}")
      return nil
    end
    bs
  end

  def self.owns?(session, bs)
    bs.user_id.to_s == session.user_id.to_s
  end

  # --- create / fork ---------------------------------------------------------
  def self.create(session, payload)
    user = User.find_by(id: session.user_id)
    unless user
      Command.error(session, 'session create requires a known user')
      return
    end

    from_uuid = payload['from_uuid'].to_s
    bs =
      if from_uuid.empty?
        dv = payload['doc_version']
        BrowserSession.create!(user: user, name: payload['name'],
                               client_version: payload['client_version'],
                               client_sha: payload['client_sha'],
                               doc_version: dv,
                               # Fresh doc was just written by this build.
                               version_history: (dv ? [dv.to_i] : []),
                               doc: payload['doc'].is_a?(Hash) ? payload['doc'] : {})
      else
        src = BrowserSession.find_by(session_uuid: from_uuid)
        return Command.error(session, "cannot fork unknown session: #{from_uuid}") unless src
        forked = src.fork_for(user)
        # A fork adopts the FORKING client's build fingerprints (its doc will
        # now be driven by this build), not the source's. doc_version and
        # version_history stay FAITHFUL to the parent (copied by fork_for) so the
        # clone still describes the content it inherited.
        forked.update!(client_version: payload['client_version'],
                       client_sha: payload['client_sha'])
        forked
      end

    subscribe_ws(session, bs, role: 'producer')
    Command.reply(session, 'session', 'created',
                  { session_uuid: bs.session_uuid, name: bs.name, doc: bs.doc,
                    client_sha: bs.client_sha, doc_version: bs.doc_version,
                    version_history: bs.version_history,
                    forked_from: bs.forked_from&.session_uuid })
  end
  register 'create', :create

  # --- resume (own it again) -------------------------------------------------
  def self.resume(session, payload)
    bs = find_session(session, payload, 'resume') or return
    unless owns?(session, bs)
      Command.error(session, 'cannot resume a session you do not own')
      return
    end
    subscribe_ws(session, bs, role: 'producer')
    Command.reply(session, 'session', 'resumed',
                  { session_uuid: bs.session_uuid, name: bs.name, doc: bs.doc,
                    client_sha: bs.client_sha, doc_version: bs.doc_version,
                    version_history: bs.version_history,
                    forked_from: bs.forked_from&.session_uuid })
  end
  register 'resume', :resume

  # --- patch (producer only) -------------------------------------------------
  def self.patch(session, payload)
    bs = find_session(session, payload, 'patch') or return
    unless owns?(session, bs)
      Command.error(session, 'cannot patch a session you do not own')
      return
    end
    ops = payload['ops']
    ops = [] unless ops.is_a?(Array)

    doc = bs.doc || {}
    ops.each { |op| apply_op(doc, op) if op.is_a?(Hash) }
    bs.doc = doc
    bs.doc_will_change!   # in-place jsonb mutation → force the dirty flag
    # Re-stamp the LAST-WRITER signature: the fingerprint describes the bytes on
    # disk, and the producer that just wrote them is the honest author. Only
    # overwrite when the producer supplied a value (nil payload leaves it as-is).
    # doc_version is last-writer-wins AND appends to version_history via
    # record_version! (sequential-duplicate collapsed).
    bs.client_sha  = payload['client_sha']  if payload.key?('client_sha')
    bs.record_version!(payload['doc_version']) if payload.key?('doc_version')
    bs.save!              # persist on every patch (settled: save every update)

    # Live relay to WATCHERS (not the producer that sent it). The in-memory
    # rebroadcast is independent of the DB write above.
    broadcast_patch(bs, ops, except: session.ws)
    Command.reply(session, 'session', 'patched',
                  { session_uuid: bs.session_uuid, rev: bs.updated_at.to_f })
  end
  register 'patch', :patch

  # --- resync (producer replaces the WHOLE doc) ------------------------------
  # A diff-patch stream can never delete a key the current build doesn't know
  # about, so a session authored by an older/foreign build accretes defunct keys
  # forever. resync ships the producer's fully normalized doc (its loadDoc->toDoc
  # round-trip has already dropped unknown keys and adopted the current shape),
  # replacing the stored tree wholesale and re-stamping the signature. The client
  # fires this after resuming a session whose signature differs from its own.
  def self.resync(session, payload)
    bs = find_session(session, payload, 'resync') or return
    unless owns?(session, bs)
      Command.error(session, 'cannot resync a session you do not own')
      return
    end
    doc = payload['doc']
    return Command.error(session, 'resync requires a doc object') unless doc.is_a?(Hash)

    bs.doc         = doc
    bs.client_sha  = payload['client_sha']  if payload.key?('client_sha')
    bs.record_version!(payload['doc_version']) if payload.key?('doc_version')
    bs.save!

    # Watchers get a full snapshot (not path ops) since the whole tree changed.
    if (subs = SESSION_SUBSCRIBERS[bs.session_uuid])
      targets = subs.keys.reject { |ws| ws == session.ws }
      broadcast(targets, 'session', 'snapshot',
                { session_uuid: bs.session_uuid, name: bs.name, doc: bs.doc })
    end
    Command.reply(session, 'session', 'patched',
                  { session_uuid: bs.session_uuid, rev: bs.updated_at.to_f })
  end
  register 'resync', :resync

  # --- subscribe (watch, read-only) -----------------------------------------
  def self.subscribe(session, payload)
    bs = find_session(session, payload, 'subscribe') or return
    subscribe_ws(session, bs, role: 'watcher')
    Command.reply(session, 'session', 'snapshot',
                  { session_uuid: bs.session_uuid, name: bs.name, doc: bs.doc,
                    doc_version: bs.doc_version, version_history: bs.version_history,
                    forked_from: bs.forked_from&.session_uuid })
  end
  register 'subscribe', :subscribe

  def self.unsubscribe(session, payload)
    uuid = payload['session_uuid'].to_s
    return if uuid.empty?
    unsubscribe_ws(session, uuid)
    Command.reply(session, 'session', 'unsubscribed', { session_uuid: uuid })
  end
  register 'unsubscribe', :unsubscribe

  # --- delete (garbage-collect a session you own) ---------------------------
  # Tears down the in-memory subscriber registry for the session (so any live
  # watchers are dropped) and destroys the row. Only the owner may delete.
  def self.delete(session, payload)
    bs = find_session(session, payload, 'delete') or return
    unless owns?(session, bs)
      Command.error(session, 'cannot delete a session you do not own')
      return
    end
    uuid = bs.session_uuid
    # Notify + drop every subscriber (producer + watchers), then forget the
    # registry entry so nothing lingers pointing at a destroyed row.
    if (subs = SESSION_SUBSCRIBERS[uuid])
      targets = subs.keys
      broadcast(targets, 'session', 'deleted', { session_uuid: uuid })
      SESSION_SUBSCRIBERS.delete(uuid)
    end
    bs.destroy!
    # Reply to the requester too (they may not be in the subscriber set).
    Command.reply(session, 'session', 'deleted', { session_uuid: uuid })
  end
  register 'delete', :delete

  # --- snapshot (fetch current doc) -----------------------------------------
  def self.snapshot(session, payload)
    bs = find_session(session, payload, 'snapshot') or return
    Command.reply(session, 'session', 'snapshot',
                  { session_uuid: bs.session_uuid, name: bs.name, doc: bs.doc,
                    doc_version: bs.doc_version, version_history: bs.version_history,
                    forked_from: bs.forked_from&.session_uuid })
  end
  register 'snapshot', :snapshot

  # --- list (resume picker) --------------------------------------------------
  # `in_use` = the session currently has a live PRODUCER ws in the in-memory
  # subscriber registry (another tab/window is driving it right now). It is the
  # client's "give me the most-recent session NOT in use" signal — no DB column
  # needed, and it self-cleans on disconnect via Session#cleanup. NOTE: the
  # registry is per worker process; if this ever runs multi-replica, `in_use`
  # only reflects THIS process and would need a shared store.
  def self.in_use?(uuid)
    subs = SESSION_SUBSCRIBERS[uuid]
    return false unless subs
    subs.any? { |_ws, meta| meta[:role] == 'producer' }
  end

  def self.list(session, _payload)
    sessions = BrowserSession.where(user_id: session.user_id).order(updated_at: :desc)
    Command.reply(session, 'session', 'list',
                  { sessions: sessions.map { |bs|
                    { session_uuid: bs.session_uuid, name: bs.name,
                      client_version: bs.client_version,
                      client_sha: bs.client_sha,
                      doc_version: bs.doc_version,
                      version_history: bs.version_history,
                      forked_from: bs.forked_from&.session_uuid,
                      updated_at: bs.updated_at, created_at: bs.created_at,
                      in_use: in_use?(bs.session_uuid) } } })
  end
  register 'list', :list

  # --- generic path-patch application (opaque doc) ---------------------------
  # Mutates `doc` in place. Missing intermediate keys are created as hashes.
  def self.apply_op(doc, op)
    return doc unless doc.is_a?(Hash)
    path = Array(op['path']).map(&:to_s)
    return doc if path.empty?

    parent = doc
    path[0...-1].each do |k|
      parent[k] = {} unless parent[k].is_a?(Hash)
      parent = parent[k]
    end
    last = path.last
    if op['op'] == 'delete'
      parent.delete(last)
    else
      parent[last] = op['value']
    end
    doc
  end

  # --- in-memory subscriber registry ----------------------------------------
  # SESSION_SUBSCRIBERS[uuid] = { ws => { user_id:, name:, role: } }
  def self.subscribe_ws(session, bs, role:)
    # A socket drives AT MOST ONE session. Becoming producer of a new session
    # relinquishes the producer role on any OTHER session this socket was
    # driving, so `in_use` releases on switch (create/resume) rather than only
    # on disconnect. Watcher subscriptions are independent and left intact — a
    # socket may watch several sessions at once.
    release_other_producer_sessions(session, keep: bs.session_uuid) if role == 'producer'

    subs = (SESSION_SUBSCRIBERS[bs.session_uuid] ||= {})
    subs[session.ws] = { user_id: session.user_id, name: session.name, role: role }
    session.session_subs << bs.session_uuid unless session.session_subs.include?(bs.session_uuid)
  end

  # Drop this socket's PRODUCER entry from every session except `keep`. Leaves
  # watcher entries (and the socket's session_subs for those) untouched.
  def self.release_other_producer_sessions(session, keep:)
    session.session_subs.dup.each do |uuid|
      next if uuid == keep
      subs = SESSION_SUBSCRIBERS[uuid]
      next unless subs
      meta = subs[session.ws]
      next unless meta && meta[:role] == 'producer'
      unsubscribe_ws(session, uuid)
    end
  end

  def self.unsubscribe_ws(session, uuid)
    if (subs = SESSION_SUBSCRIBERS[uuid])
      subs.delete(session.ws)
      SESSION_SUBSCRIBERS.delete(uuid) if subs.empty?
    end
    session.session_subs.delete(uuid)
  end

  def self.broadcast_patch(bs, ops, except:)
    subs = SESSION_SUBSCRIBERS[bs.session_uuid] or return
    targets = subs.keys.reject { |ws| ws == except }
    broadcast(targets, 'session', 'patch',
              { session_uuid: bs.session_uuid, ops: ops })
  end
end
