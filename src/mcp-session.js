// mcp-session.js — a SINGLE persistent Axona peer shared across MCP tool calls.
//
// ops.js connects a throwaway peer per call. This module is the opposite: ONE
// long-lived peer (Claude's peer) that stays connected so the agent is a real,
// standing participant — full publisher AND subscriber, and an infrastructure
// host for its own topics.
//
//   • publish() / pull()         — point ops over the SAME live peer
//   • watch(topic)               — STANDING subscription; arrivals buffer + notify
//   • poll(topic?, {wait})       — drain the buffer; long-poll blocks until arrival
//   • host(topic) / unhost()     — root the topic on Claude's peer (store + serve)
//   • onArrival(fn)              — register a push sink (mcp.js → MCP notifications)
//   • status()                   — peer + mesh + watches + hosted topics
//
// Identity is DURABLE: both the NODE identity (stable nodeId) and the AUTHOR
// identity (stable Author ID) persist to ~/.axona/claude-mcp-identity.json, so
// Claude keeps the same on-network identity across restarts. `cleanupWebRTC()`
// is process-global, so this module owns the only peer and tears it down ONCE.

import './polyfill.js';                          // RTCPeerConnection/WebSocket globals — first
import { cleanupWebRTC } from './polyfill.js';
import { connectPeer, regionToDescriptor, DEFAULT_BRIDGE } from './ops.js';
import { createNodeIdentity, createAuthorIdentity, dumpIdentity, loadIdentity }
  from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const REGION       = process.env.MCP_REGION || 'useast';
const STORE_PATH   = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const BUFFER_CAP   = Number(process.env.MCP_BUFFER_CAP) || 1000;
const AUTHOR_KEY   = 'claude';     // author keypair key in the store
const NODE_KEY     = 'node';       // node-identity envelope key in the store

// ── durable store (Node file-backed { get, set }) ───────────────────────
function fileStore(path) {
  const read = () => { try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return {}; } };
  return {
    get: (k) => read()[k] ?? null,
    set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, JSON.stringify(o, null, 2)); },
  };
}
const STORE = fileStore(STORE_PATH);

/** Load the durable node identity (stable nodeId) or mint + persist one. */
async function loadOrCreateNodeIdentity(center) {
  const saved = STORE.get(NODE_KEY);
  if (saved) { try { return await loadIdentity(saved); } catch { /* corrupt → re-mint */ } }
  const id = await createNodeIdentity({ lat: center.lat, lng: center.lng, extractable: true });   // extractable so we can dump
  STORE.set(NODE_KEY, await dumpIdentity(id));
  return id;
}

// ── module state ────────────────────────────────────────────────────────
let _session = null;          // { peer, regionName, center, nodeId, author, close }
let _connecting = null;
const WATCHES = new Map();    // "region|topic" → { topic, region, descriptor, buffer[], total, dropped, since, startedAt, waiters[] }
const HOSTED  = new Map();    // "region|topic" → { topic, region, descriptor, since }
const ARRIVAL_LISTENERS = new Set();   // fn({topic,region,message,signer,msgId})

const keyOf = (region, topic) => `${region}|${topic}`;
const now = () => Date.now();
function descriptorFor(topic, region) {
  const { name: regionName } = regionToDescriptor(region || REGION);
  return { region: regionName, name: topic };
}

/** Register a push sink — called for every arrival on any watch (best-effort, never throws into the peer). */
export function onArrival(fn) { ARRIVAL_LISTENERS.add(fn); return () => ARRIVAL_LISTENERS.delete(fn); }
function emitArrival(evt) {
  for (const fn of ARRIVAL_LISTENERS) { try { fn(evt); } catch { /* a bad sink must not break delivery */ } }
}

export async function ensureSession() {
  if (_session) return _session;
  if (_connecting) return _connecting;
  _connecting = (async () => {
    const { center } = regionToDescriptor(REGION);
    const identity = await loadOrCreateNodeIdentity(center);                                  // stable nodeId
    const author   = await createAuthorIdentity({ persistAs: AUTHOR_KEY, store: STORE });     // stable Author ID
    const h = await connectPeer({ region: REGION, identity, author });
    _session = h;
    _connecting = null;
    return h;
  })();
  try { return await _connecting; } catch (e) { _connecting = null; throw e; }
}

// ── point operations over the live peer ─────────────────────────────────
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

// ── standing subscription ───────────────────────────────────────────────
export async function watch({ topic, region, since = 'all' }) {
  const s = await ensureSession();
  const r = region || REGION;
  const key = keyOf(r, topic);
  if (WATCHES.has(key)) { const w = WATCHES.get(key); return { ok: true, watching: true, alreadyWatching: true, topic, region: r, buffered: w.buffer.length, total: w.total }; }
  const descriptor = descriptorFor(topic, region);
  const w = { topic, region: r, descriptor, buffer: [], total: 0, dropped: 0, since, startedAt: now(), waiters: [] };
  // kernel `since`: 'all' (replay backlog) | 'latest' (most recent only) | a
  // timestamp | undefined (live tail). Expose 'live' as the friendly name for
  // undefined. Subscribe FIRST; only register the watch if sub() succeeds, so a
  // rejected sub can't leave a phantom watch that a retry then no-ops over.
  const sinceArg = (since === 'live' || since == null) ? undefined : since;
  await s.peer.sub(descriptor, (env) => {
    if (!env || env.deleted) return;
    w.total += 1;
    const m = { message: env.message, signer: env.signerPubkey ?? null, msgId: env.msgId ?? null, seq: env.seq ?? null, ts: env.ts ?? null, receivedAt: now() };
    w.buffer.push(m);
    if (w.buffer.length > BUFFER_CAP) { w.buffer.shift(); w.dropped += 1; }
    const waiters = w.waiters.splice(0); for (const res of waiters) res();   // wake long-pollers
    emitArrival({ topic: w.topic, region: w.region, ...m });                 // push sink (notifications)
  }, { since: sinceArg });
  WATCHES.set(key, w);                                                       // only after sub() resolves
  return { ok: true, watching: true, alreadyWatching: false, topic, region: r, since };
}

/** Drain (or peek) buffered messages. With `wait`, long-poll: block until an arrival or `timeoutSec`. */
export async function poll({ topic, region, peek = false, max, wait = false, timeoutSec = 25 } = {}) {
  const collect = (w) => {
    const out = max ? w.buffer.slice(0, Number(max)) : w.buffer.slice();
    if (!peek) { if (max) w.buffer.splice(0, out.length); else w.buffer.length = 0; }
    return { topic: w.topic, region: w.region, drained: peek ? 0 : out.length, remaining: w.buffer.length, total: w.total, dropped: w.dropped, messages: out };
  };
  const anyBuffered = () => [...WATCHES.values()].some((w) => w.buffer.length);

  if (wait && !peek) {
    const targetEmpty = topic ? !(WATCHES.get(keyOf(region || REGION, topic))?.buffer.length) : !anyBuffered();
    if (targetEmpty) {
      const secs = Math.max(1, Math.min(60, Number(timeoutSec) || 25));
      await new Promise((resolve) => {
        let done = false; const fire = () => { if (!done) { done = true; resolve(); } };
        const t = setTimeout(fire, secs * 1000);
        const wrap = () => { clearTimeout(t); fire(); };
        if (topic) { const w = WATCHES.get(keyOf(region || REGION, topic)); if (w) w.waiters.push(wrap); else fire(); }
        else { for (const w of WATCHES.values()) w.waiters.push(wrap); }   // any watch wakes us
      });
    }
  }

  if (topic) {
    const w = WATCHES.get(keyOf(region || REGION, topic));
    if (!w) return { ok: false, error: `not watching ${region || REGION}|${topic} (call axona_watch first)` };
    return { ok: true, peek, waited: !!wait, ...collect(w) };
  }
  return { ok: true, peek, waited: !!wait, watches: [...WATCHES.values()].map(collect) };
}

export async function unwatch({ topic, region }) {
  const r = region || REGION; const key = keyOf(r, topic); const w = WATCHES.get(key);
  if (!w) return { ok: false, error: `not watching ${r}|${topic}` };
  const s = await ensureSession();
  try { await s.peer.unsub?.(w.descriptor); } catch { /* */ }
  for (const res of w.waiters.splice(0)) res();    // release any blocked long-pollers
  WATCHES.delete(key);
  return { ok: true, unwatched: true, topic, region: r, hadBuffered: w.buffer.length, total: w.total };
}

// ── hosting: root Claude's own topics (store + serve, no subscribe) ──────
export async function host({ topic, region }) {
  const s = await ensureSession();
  const r = region || REGION; const key = keyOf(r, topic);
  const descriptor = descriptorFor(topic, region);
  if (!HOSTED.has(key)) { await s.peer.host(descriptor); HOSTED.set(key, { topic, region: r, descriptor, since: now() }); }
  return { ok: true, hosting: true, topic, region: r };
}

export async function unhost({ topic, region }) {
  const r = region || REGION; const key = keyOf(r, topic);
  if (!HOSTED.has(key)) return { ok: false, error: `not hosting ${r}|${topic}` };
  const s = await ensureSession();
  try { await s.peer.unhost?.(HOSTED.get(key).descriptor); } catch { /* */ }
  HOSTED.delete(key);
  return { ok: true, unhosted: true, topic, region: r };
}

export async function status() {
  if (!_session) return { ok: true, connected: false, watches: [], hosted: [] };
  let health = null; try { health = _session.peer.health(); } catch { /* */ }
  return {
    ok: true, connected: true, persistent: true, region: REGION, bridge: DEFAULT_BRIDGE,
    nodeId: _session.nodeId, authorId: _session.author.authorId, identityPath: STORE_PATH,
    mesh: health ? { synaptomeSize: health.synaptomeSize ?? null, peers: health.peers?.length ?? null, state: health.state ?? null } : null,
    watches: [...WATCHES.values()].map((w) => ({ topic: w.topic, region: w.region, buffered: w.buffer.length, total: w.total, dropped: w.dropped, since: w.since, ageSec: Math.round((now() - w.startedAt) / 1000) })),
    hosted: [...HOSTED.values()].map((h) => ({ topic: h.topic, region: h.region, ageSec: Math.round((now() - h.since) / 1000) })),
  };
}

export async function subscribeWindow({ topic, region, seconds = 20, since = 'all' }) {
  const secs = Math.max(1, Math.min(120, Number(seconds) || 20));
  const r = region || REGION; const key = keyOf(r, topic);
  const preexisting = WATCHES.has(key);
  await watch({ topic, region, since });
  const w = WATCHES.get(key);
  const startLen = preexisting ? w.buffer.length : 0;
  await new Promise((res) => setTimeout(res, secs * 1000));
  const messages = w.buffer.slice(startLen);
  if (!preexisting) await unwatch({ topic, region });
  return { ok: true, topic, region: r, listenedSec: secs, since, received: messages.length, messages };
}

let _shuttingDown = false;
export async function shutdown() {
  if (_shuttingDown) return; _shuttingDown = true;
  try {
    if (_session) {
      for (const w of WATCHES.values()) { try { await _session.peer.unsub?.(w.descriptor); } catch { /* */ } }
      for (const h of HOSTED.values())  { try { await _session.peer.unhost?.(h.descriptor); } catch { /* */ } }
      try { await _session.close(); } catch { /* */ }
    }
  } finally { try { cleanupWebRTC(); } catch { /* */ } _session = null; WATCHES.clear(); HOSTED.clear(); }
}

for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { shutdown().finally(() => process.exit(0)); });
process.on('exit', () => { try { cleanupWebRTC(); } catch { /* */ } });
