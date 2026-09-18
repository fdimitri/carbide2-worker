// Hosts N carbide2-client fileSync instances for test/dbfs_client_sync_test.rb.
// Line protocol on stdin/stdout, one JSON object per line.
//
// in:  { op: 'create', id, path, seed }
//      { op: 'frame', id, cmd, payload }      a worker frame delivered to client id
//      { op: 'type', id, mode }               client id makes one random local edit
//      { op: 'disconnect' | 'connect', id }
//      { op: 'dump' }
// out: { op: 'send', id, cmd, payload }       client id sends a frame to the worker
//      { op: 'dump', clients: { id: { text, baseRev, outstanding, loaded, lost } } }
//      { op: 'ok' }                           every command is answered after its sends
import readline from 'node:readline'
import { pathToFileURL } from 'node:url'

const clientDir = process.argv[2]
const { createFileSync } = await import(pathToFileURL(`${clientDir}/src/services/fileSync.js`).href)
const { applyChanges } = await import(pathToFileURL(`${clientDir}/src/utils/textChanges.js`).href)

const out = (o) => process.stdout.write(JSON.stringify(o) + '\n')
const clients = {}

function rng(seed) {
  let x = seed >>> 0 || 1
  return () => { x ^= x << 13; x >>>= 0; x ^= x >>> 17; x ^= x << 5; x >>>= 0; return x / 4294967296 }
}

function makeClient(id, path, seed) {
  const view = { text: '' }
  const editor = {
    applyChanges: (cs) => { view.text = applyChanges(view.text, cs) },
    replaceContent: (t) => { view.text = t },
  }
  const c = { id, view, random: rng(seed), counter: 0, lost: [], batches: new Map() }
  c.sync = createFileSync({
    path,
    editor,
    // The scheduler delivers, delays and loses every frame itself; a wall-clock
    // resend/abandon firing between steps would make a run non-reproducible.
    ackTimeoutMs: 0,
    send: (cmd, payload) => {
      if (cmd === 'write') c.batches.set(payload.batch_id, tokensOf(payload.changes))
      out({ op: 'send', id, cmd, payload })
    },
  })
  return c
}

function tokensOf(changes) {
  return changes.flatMap(ch => {
    const d = JSON.parse(ch.change_data)
    return typeof d.data === 'string' ? (d.data.match(/<[^<>]+>/g) || []) : []
  })
}

// Positions on a line that are not inside a "<id.n>" token, so an insert never
// splits another token (the insert-only checks look tokens up by pattern).
function boundaries(text) {
  const out = []
  let depth = 0
  for (let i = 0; i <= text.length; i++) {
    if (depth === 0) out.push(i)
    if (text[i] === '<') depth++
    else if (text[i] === '>') depth = Math.max(0, depth - 1)
  }
  return out
}

function randomEdit(c, mode) {
  const lines = c.view.text.split('\n')
  const line = Math.floor(c.random() * lines.length)
  const spots = boundaries(lines[line])
  const char = spots[Math.floor(c.random() * spots.length)]
  if (mode === 'mixed' && c.random() < 0.3 && c.view.text.length > 0) {
    const len = lines[line].length - char
    if (len > 0) {
      const n = 1 + Math.floor(c.random() * Math.min(len, 5))
      return { change_type: 'deleteDataSingleLine', change_data: JSON.stringify({ startLine: line, startChar: char, endChar: char + n }) }
    }
  }
  c.counter += 1
  const token = `<${c.id}.${c.counter}>` + (c.random() < 0.2 ? '\n' : '')
  const type = token.includes('\n') ? 'insertDataMultiLine' : 'insertDataSingleLine'
  return { change_type: type, change_data: JSON.stringify({ startLine: line, startChar: char, data: token }) }
}

const rl = readline.createInterface({ input: process.stdin })
for await (const line of rl) {
  if (!line.trim()) continue
  const msg = JSON.parse(line)
  const c = clients[msg.id]
  switch (msg.op) {
    case 'create': {
      const nc = makeClient(msg.id, msg.path, msg.seed)
      clients[msg.id] = nc
      nc.sync.load()
      break
    }
    case 'frame': {
      const p = msg.payload
      if (msg.cmd === 'content') {
        const r = c.sync.onContent(p)
        if (r.initial) c.view.text = r.content
      } else if (msg.cmd === 'written') {
        if (c.sync.state.inflight && p.batch_id === c.sync.state.inflight.batchId) c.batches.delete(p.batch_id)
        c.sync.onWritten(p)
      } else if (msg.cmd === 'error') {
        // A refused batch and everything typed after it are dropped by design.
        const inflight = c.sync.state.inflight
        const pending = tokensOf(c.sync.state.pending)
        if (c.sync.onError(p) && inflight) {
          c.lost.push(...(c.batches.get(inflight.batchId) || []), ...pending)
          c.batches.delete(inflight.batchId)
        }
      } else if (['change', 'set_contents'].includes(msg.cmd)) {
        c.sync.onRemote(msg.cmd, p)
      }
      break
    }
    case 'type': {
      if (!c.sync.state.loaded) break
      const edit = randomEdit(c, msg.mode)
      c.view.text = applyChanges(c.view.text, [edit])
      if (!c.sync.localChanges([edit])) c.lost.push(...tokensOf([edit]))
      break
    }
    case 'disconnect': c.sync.onDisconnected(); break
    case 'connect': c.sync.onConnected(); break
    case 'dump': {
      const clientsOut = {}
      for (const [id, cl] of Object.entries(clients)) {
        clientsOut[id] = { text: cl.view.text, baseRev: cl.sync.state.baseRev, outstanding: cl.sync.outstanding,
                           loaded: cl.sync.state.loaded, lost: cl.lost, connected: cl.sync.state.connected, typed: cl.counter }
      }
      out({ op: 'dump', clients: clientsOut })
      break
    }
  }
  out({ op: 'ok' })
}
