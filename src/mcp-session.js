// mcp-session.js — a SINGLE persistent Axona peer shared across MCP tool calls.
//
// ops.js connects a throwaway peer per call (publish/pull/subscribe → connect →
// do one thing → cleanupWebRTC). That's correct for the CLI and for fire-and-
// forget, but it means the agent holds NO mesh membership between calls, so it
// can't be a standing subscriber. This module is the opposite: it lazily opens
// ONE long-lived peer (Claude's peer), keeps it connected, and lets the agent:
//
//   • publish() / pull()         — point ops over the SAME live peer (no reconnect)
//   • watch(topic)               — open a STANDING subscription; arrivals buffer
//   • poll(topic?)               — drain the buffer (this is how the agent "reads")
//   • unwatch(topic) / status()  — manage + introspect
//
// The peer signs with a DURABLE author (persisted), so Claude has a stable
// Author ID on the network across restarts — a real publisher/subscriber, not a
// fresh persona each call. `cleanupWebRTC()` is process-global, so this module
// owns the only peer in the process and tears it down ONCE, on exit.

import './polyfill.js';                          // RTCPeerConnection/WebSocket globals — first
import { cleanupWebRTC } from './polyfill.js';
import { connectPeer, regionToDescriptor, DEFAULT_BRIDGE } from './ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const REGION       = process.env.MCP_REGION || 'useast';
const AUTHOR_PATH  = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-author.json');
const BUFFER_CAP   = Number(process.env.MCP_BUFFER_CAP) || 1000;   // per-watch ring cap
const AUTHOR_KEY   = 'claude';

// ── durable author store (Node file-backed { get, set }) ────────────────
// createAuthorIdentity({persistAs}) defaults to browser localStorage; in Node we
// hand it a tiny JSON-file store so Claude's keypair survives MCP restarts.
function fileStore(path) {
  const read = () => { try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return {}; } };
  return {
    get: (k) => read()[k] ?? null,
    set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, JSON.stringify(o, null, 2)); },
  };
}

// ── module state ────────────────────────────────────────────────────────
let _session = null;          // { peer, regionName, center, nodeId, author, close }
let _connecting = null;       // in-flight connect promise (dedupes concurrent ensure())
const WATCHES = new Map();    // key "region|topic" → { topic, region, descriptor, buffer[], total, dropped, since, startedAt }

const keyOf = (region, topic) => `${region}|${topic}`;
const now = () => Date.now();

function descriptorFor(topic, region) {
  const { name: regionName } = regionToDescriptor(region || REGION);
  return { region: regionName, name: topic };
}

/** Lazily open (once) the persistent peer; concurrent callers await the same connect. */
export async function ensureSession() {
  if (_session) return _session;
  if (_connecting) return _connecting;
  _connecting = (async () => {
    const author = await createAuthorIdentity({ persistAs: AUTHOR_KEY, store: fileStore(AUTHOR_PATH) });
    const h = await connectPeer({ region: REGION, author });   // DURABLE author, peer kept alive
    _session = h;
    _connecting = null;
    return h;
  })();
  try { return await _connecting; }
  catch (e) { _connecting = null; throw e; }
}

// ── point operations over the live peer (no reconnect) ──────────────────
export async function publish({ topic, message, region }) {
  const s = await ensureSession();
  const msgId = await s.peer.pub(descriptorFor(topic, region), message, { signWith: s.author });
  return { ok: true, topic, region: region || REGION, msgId, signer: s.author.authorId, nodeId: s.nodeId, persistent: true };
}

export async function pull({ topic, region }) {
  const s = await ensureSession();
  const env = await s.peer.pull(null, { topic: descriptorFor(topic, region) });
  return { ok: true, topic, region: region || REGION, found: !!env, message: env ? env.message : null, msgId: env?.msgId ?? null };
}

// ── standing subscription (the persistent-subscriber half) ──────────────
export async function watch({ topic, region, since = 'all' }) {
  const s = await ensureSession();
  const r = region || REGION;
  const key = keyOf(r, topic);
  if (WATCHES.has(key)) {
    const w = WATCHES.get(key);
    return { ok: true, watching: true, alreadyWatching: true, topic, region: r, buffered: w.buffer.length, total: w.total };
  }
  const descriptor = descriptorFor(topic, region);
  const w = { topic, region: r, descriptor, buffer: [], total: 0, dropped: 0, since, startedAt: now() };
  WATCHES.set(key, w);
  await s.peer.sub(descriptor, (env) => {
    if (!env || env.deleted) return;
    w.total += 1;
    w.buffer.push({
      message: env.message, signer: env.signerPubkey ?? null,
      msgId: env.msgId ?? null, seq: env.seq ?? null, ts: env.ts ?? null, receivedAt: now(),
    });
    if (w.buffer.length > BUFFER_CAP) { w.buffer.shift(); w.dropped += 1; }   // bounded ring
  }, { since });
  return { ok: true, watching: true, alreadyWatching: false, topic, region: r, since };
}

/** Drain (or peek) buffered messages — for one watch (topic given) or all. */
export async function poll({ topic, region, peek = false, max } = {}) {
  const collect = (w) => {
    const out = max ? w.buffer.slice(0, Number(max)) : w.buffer.slice();
    if (!peek) { if (max) w.buffer.splice(0, out.length); else w.buffer.length = 0; }
    return { topic: w.topic, region: w.region, drained: peek ? 0 : out.length, remaining: w.buffer.length, total: w.total, dropped: w.dropped, messages: out };
  };
  if (topic) {
    const w = WATCHES.get(keyOf(region || REGION, topic));
    if (!w) return { ok: false, error: `not watching ${region || REGION}|${topic} (call axona_watch first)` };
    return { ok: true, peek, ...collect(w) };
  }
  return { ok: true, peek, watches: [...WATCHES.values()].map(collect) };
}

export async function unwatch({ topic, region }) {
  const r = region || REGION;
  const key = keyOf(r, topic);
  const w = WATCHES.get(key);
  if (!w) return { ok: false, error: `not watching ${r}|${topic}` };
  const s = await ensureSession();
  try { await s.peer.unsub?.(w.descriptor); } catch { /* best effort */ }
  WATCHES.delete(key);
  return { ok: true, unwatched: true, topic, region: r, hadBuffered: w.buffer.length, total: w.total };
}

export async function status() {
  if (!_session) return { ok: true, connected: false, watches: [] };
  let health = null;
  try { health = _session.peer.health(); } catch { /* */ }
  return {
    ok: true, connected: true, persistent: true,
    region: REGION, bridge: DEFAULT_BRIDGE,
    nodeId: _session.nodeId, authorId: _session.author.authorId,
    mesh: health ? { synaptomeSize: health.synaptomeSize ?? null, peers: health.peers?.length ?? null, state: health.state ?? null } : null,
    watches: [...WATCHES.values()].map((w) => ({
      topic: w.topic, region: w.region, buffered: w.buffer.length, total: w.total, dropped: w.dropped, since: w.since, ageSec: Math.round((now() - w.startedAt) / 1000),
    })),
  };
}

/** Back-compat one-shot window: watch (if needed) → wait → return arrivals; clean up a watch we created. */
export async function subscribeWindow({ topic, region, seconds = 20, since = 'all' }) {
  const secs = Math.max(1, Math.min(120, Number(seconds) || 20));
  const r = region || REGION;
  const key = keyOf(r, topic);
  const preexisting = WATCHES.has(key);
  await watch({ topic, region, since });
  const w = WATCHES.get(key);
  const startLen = preexisting ? w.buffer.length : 0;   // new arrivals only if it was already watched
  await new Promise((res) => setTimeout(res, secs * 1000));
  const messages = w.buffer.slice(startLen);
  if (!preexisting) await unwatch({ topic, region });   // leave no standing watch behind
  return { ok: true, topic, region: r, listenedSec: secs, since, received: messages.length, messages };
}

let _shuttingDown = false;
export async function shutdown() {
  if (_shuttingDown) return; _shuttingDown = true;
  try {
    if (_session) {
      for (const w of WATCHES.values()) { try { await _session.peer.unsub?.(w.descriptor); } catch { /* */ } }
      try { await _session.close(); } catch { /* */ }
    }
  } finally { try { cleanupWebRTC(); } catch { /* */ } _session = null; WATCHES.clear(); }
}

for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { shutdown().finally(() => process.exit(0)); });
process.on('exit', () => { try { cleanupWebRTC(); } catch { /* */ } });
