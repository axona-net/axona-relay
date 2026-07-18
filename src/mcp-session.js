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
import { authorClassTopic } from '../vendor/axona-protocol/src/index.js';   // kernel author-class helper
export { authorClassTopic };                                               // re-export for callers/smoke
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const REGION       = process.env.MCP_REGION || 'useast';
const STORE_PATH   = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const BUFFER_CAP   = Number(process.env.MCP_BUFFER_CAP) || 1000;
const AUTHOR_KEY   = 'claude';     // author keypair key in the store
const NODE_KEY     = 'node';       // node-identity envelope key in the store

// ── author-class attestation (human/agent provenance) ───────────────────
// Voluntary, signed "this author is an agent" claim. The object, the owner-only
// pinned-region profile topic (authorClassTopic), and verification now live in
// the KERNEL (src/pubsub/authorClass.js) so every consumer derives + verifies
// identically; these env knobs only drive THIS peer's behaviour.
const AUTHOR_CLASS  = process.env.MCP_AUTHOR_CLASS || 'agent';    // this peer IS an agent
const OPERATOR      = process.env.MCP_OPERATOR || null;          // optional: who runs it
const DECLARE_CLASS = process.env.MCP_DECLARE_CLASS !== '0';     // auto-declare on connect

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

// owner + write FOLD INTO THE TOPIC ID — an owned topic is a different topic
// from the open one of the same name, so both the descriptor and the watch
// key must carry them (else a watch on "axona.bot" listens to the wrong topic).
const keyOf = (region, topic, owner, write) => `${region}|${topic}|${owner || ''}|${write || 'open'}`;
const now = () => Date.now();
function descriptorFor(topic, region, owner, write) {
  const { name: regionName } = regionToDescriptor(region || REGION);
  const d = { region: regionName, name: topic };
  if (owner) d.owner = owner;
  if (write) d.write = write;
  return d;
}
/** 'self' resolves to this peer's durable Author ID (for owner-only topics we own). */
function resolveOwner(s, owner) {
  return owner === 'self' ? s.author.authorId : (owner || undefined);
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
    // Voluntary, signed self-declaration: this peer is an AI agent. Best-effort —
    // a failed declare must never fail the connection.
    if (DECLARE_CLASS) { try { await setAuthorClass({ cls: AUTHOR_CLASS, operator: OPERATOR, label: 'axona-relay MCP peer' }); } catch { /* */ } }
    return h;
  })();
  try { return await _connecting; } catch (e) { _connecting = null; throw e; }
}

// ── author-class: declare (owner-only) + resolve (from the Author ID alone) ──
/** Publish this author's class attestation to its owner-only profile topic. */
export async function setAuthorClass({ cls = AUTHOR_CLASS, operator = OPERATOR, label = null } = {}) {
  const s = await ensureSession();
  const { attestation } = await s.peer.setAuthorClass(cls, { signWith: s.author, operator, label });   // kernel peer method
  _session.declaredClass = attestation;
  // Durability: HOST our own owner-only profile topic so the attestation is served
  // from our persistent peer even after the K-closest roots evict it from their
  // bounded queues. A declare-once-on-connect peer otherwise becomes unresolvable
  // after a few hours (observed: a 12h-old prod attestation was gone from roots).
  // Mirrors the bridge-directory "host so the launch publish survives" pattern.
  try {
    const descriptor = authorClassTopic(s.author.pubkeyHex);
    const key = `${descriptor.region}|class:${s.author.pubkeyHex}`;
    if (!HOSTED.has(key)) { await s.peer.host(descriptor); HOSTED.set(key, { topic: 'axona:author-class', region: descriptor.region, descriptor, since: now() }); }
  } catch { /* hosting is best-effort; the declare already published */ }
  return { ok: true, declared: { class: attestation.class, operator: attestation.operator ?? null, label: attestation.label ?? null }, msgId: undefined };
}

/** Resolve any author's class from its Author ID alone (kernel peer method). */
export async function getAuthorClass({ authorId } = {}) {
  if (!authorId || authorId.length !== 64) return { ok: false, error: 'authorId (64-hex Author ID) required' };
  const s = await ensureSession();
  const r = await s.peer.getAuthorClass(authorId);
  return { ok: true, authorId, class: r.class, operator: r.operator ?? null, operatorVerified: r.operatorVerified ?? false, label: r.label ?? null, ts: r.ts ?? null };
}

// ── point operations over the live peer ─────────────────────────────────
export async function publish({ topic, message, region, handle, authorClass, raw = false, owner, write }) {
  const s = await ensureSession();
  // Default to the cross-app std/message shape WITH an in-payload declaration.
  // Chat clients enforce §6.5 at render: a message whose payload lacks
  // handle/authorClass is withheld as undeclared — a bare-string publish is
  // invisible to exactly the humans it addresses. authorClass defaults to
  // this peer's declared class; raw:true opts out for machine topics.
  const body = raw
    ? message
    : { v: 1, text: message, handle: handle || 'Claude', authorClass: authorClass || AUTHOR_CLASS };
  const msgId = await s.peer.pub(descriptorFor(topic, region, resolveOwner(s, owner), write), body, { signWith: s.author });
  return { ok: true, topic, region: region || REGION, owner: resolveOwner(s, owner) ?? null, write: write ?? null, msgId, signer: s.author.authorId, nodeId: s.nodeId, persistent: true, shape: raw ? 'raw' : 'std-message' };
}

export async function pull({ topic, region, owner, write }) {
  const s = await ensureSession();
  const env = await s.peer.pull(null, { topic: descriptorFor(topic, region, resolveOwner(s, owner), write) });
  return { ok: true, topic, region: region || REGION, found: !!env, message: env ? env.message : null, msgId: env?.msgId ?? null };
}

// ── standing subscription ───────────────────────────────────────────────
export async function watch({ topic, region, since = 'all', owner, write }) {
  const s = await ensureSession();
  const r = region || REGION;
  const ro = resolveOwner(s, owner);
  const key = keyOf(r, topic, ro, write);
  if (WATCHES.has(key)) { const w = WATCHES.get(key); return { ok: true, watching: true, alreadyWatching: true, topic, region: r, buffered: w.buffer.length, total: w.total }; }
  const descriptor = descriptorFor(topic, region, ro, write);
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
export async function poll({ topic, region, peek = false, max, wait = false, timeoutSec = 25, owner, write } = {}) {
  const s0 = await ensureSession();
  const pollKey = topic ? keyOf(region || REGION, topic, resolveOwner(s0, owner), write) : null;
  const collect = (w) => {
    const out = max ? w.buffer.slice(0, Number(max)) : w.buffer.slice();
    if (!peek) { if (max) w.buffer.splice(0, out.length); else w.buffer.length = 0; }
    return { topic: w.topic, region: w.region, drained: peek ? 0 : out.length, remaining: w.buffer.length, total: w.total, dropped: w.dropped, messages: out };
  };
  const anyBuffered = () => [...WATCHES.values()].some((w) => w.buffer.length);

  if (wait && !peek) {
    const targetEmpty = topic ? !(WATCHES.get(pollKey)?.buffer.length) : !anyBuffered();
    if (targetEmpty) {
      const secs = Math.max(1, Math.min(60, Number(timeoutSec) || 25));
      await new Promise((resolve) => {
        let done = false; const fire = () => { if (!done) { done = true; resolve(); } };
        const t = setTimeout(fire, secs * 1000);
        const wrap = () => { clearTimeout(t); fire(); };
        if (topic) { const w = WATCHES.get(pollKey); if (w) w.waiters.push(wrap); else fire(); }
        else { for (const w of WATCHES.values()) w.waiters.push(wrap); }   // any watch wakes us
      });
    }
  }

  if (topic) {
    const w = WATCHES.get(pollKey);
    if (!w) return { ok: false, error: `not watching ${region || REGION}|${topic} (call axona_watch first, with matching owner/write)` };
    return { ok: true, peek, waited: !!wait, ...collect(w) };
  }
  return { ok: true, peek, waited: !!wait, watches: [...WATCHES.values()].map(collect) };
}

export async function unwatch({ topic, region, owner, write }) {
  const s0 = await ensureSession();
  const r = region || REGION; const key = keyOf(r, topic, resolveOwner(s0, owner), write); const w = WATCHES.get(key);
  if (!w) return { ok: false, error: `not watching ${r}|${topic}` };
  const s = await ensureSession();
  try { await s.peer.unsub?.(w.descriptor); } catch { /* */ }
  for (const res of w.waiters.splice(0)) res();    // release any blocked long-pollers
  WATCHES.delete(key);
  return { ok: true, unwatched: true, topic, region: r, hadBuffered: w.buffer.length, total: w.total };
}

// ── hosting: root Claude's own topics (store + serve, no subscribe) ──────
export async function host({ topic, region, owner, write }) {
  const s = await ensureSession();
  const ro = resolveOwner(s, owner);
  const r = region || REGION; const key = keyOf(r, topic, ro, write);
  const descriptor = descriptorFor(topic, region, ro, write);
  if (!HOSTED.has(key)) { await s.peer.host(descriptor); HOSTED.set(key, { topic, region: r, descriptor, since: now() }); }
  return { ok: true, hosting: true, topic, region: r };
}

export async function unhost({ topic, region, owner, write }) {
  const s0 = await ensureSession();
  const r = region || REGION; const key = keyOf(r, topic, resolveOwner(s0, owner), write);
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
    declaredClass: _session.declaredClass?.class ?? 'unstated', operator: _session.declaredClass?.operator ?? null,
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
