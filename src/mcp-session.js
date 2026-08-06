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
// Identity is SPLIT — durable WHO, ephemeral WHERE (INVARIANT I-ID):
//
//   • the AUTHOR identity persists to ~/.axona/claude-mcp-identity.json. It is
//     the durable WHO, and for owned topics it IS the authority: owner + write
//     fold into the topic id, so this key is what makes #axona.bot addressable.
//   • the TRANSPORT identity is minted FRESH on every start and is never
//     written anywhere. A long-lived nodeId is a durable correlator — it links a
//     node's sessions, which exposes its IP, which locates it physically. A
//     returning node gains nothing from its old id either: the mesh has already
//     restructured and healed around its absence. All cost, no benefit.
//
// `cleanupWebRTC()` is process-global, so this module owns the only peer and
// tears it down ONCE.

import './polyfill.js';                          // RTCPeerConnection/WebSocket globals — first
import { cleanupWebRTC } from './polyfill.js';
import { connectPeer, regionToDescriptor, DEFAULT_BRIDGE } from './ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { authorClassTopic } from '../vendor/axona-protocol/src/index.js';   // kernel author-class helper
export { authorClassTopic };                                               // re-export for callers/smoke
import { readFileSync, writeFileSync, mkdirSync, chmodSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const REGION       = process.env.MCP_REGION || 'eagle';
const STORE_PATH   = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const BUFFER_CAP   = Number(process.env.MCP_BUFFER_CAP) || 1000;
const AUTHOR_KEY   = 'claude';     // author keypair key in the store
// There is deliberately NO node key. The store holds authors only — see I-ID
// in the header. A 'node' entry left behind by an older build is not read.

// ── author-class attestation (human/agent provenance) ───────────────────
// Voluntary, signed "this author is an agent" claim. The object, the owner-only
// pinned-region profile topic (authorClassTopic), and verification now live in
// the KERNEL (src/pubsub/authorClass.js) so every consumer derives + verifies
// identically; these env knobs only drive THIS peer's behaviour.
const AUTHOR_CLASS  = process.env.MCP_AUTHOR_CLASS || 'agent';    // this peer IS an agent
const OPERATOR      = process.env.MCP_OPERATOR || null;          // optional: who runs it
const DECLARE_CLASS = process.env.MCP_DECLARE_CLASS !== '0';     // auto-declare on connect

// ── per-INSTALL display handle (2026-07-30) ─────────────────────────────
// Was hardcoded 'axona.bot' at the publish site. That is correct while exactly
// one agent runs this server, and wrong the moment a second one does: every
// install defaults to the same handle, so a multi-agent conversation renders
// as one participant talking to itself. A caller CAN pass handle per call, but
// an agent forgets, and forgetting is silent — the message still lands, just
// misattributed. So the default belongs to the INSTALL, not the call site.
//
// This is the cosmetic half of a pair. The other half is not cosmetic: two
// installs sharing MCP_AUTHOR_PATH share an AUTHOR KEYPAIR, so their messages
// are cryptographically indistinguishable, not merely identically labelled
// (#356, same failure at the identity layer). assertDistinctIdentity() below
// makes that visible at startup instead of at forensics time.
const HANDLE        = process.env.MCP_HANDLE || 'axona.bot';

// ── durable store (Node file-backed { get, set }) ───────────────────────
// This file holds the author PRIVATE key. writeFileSync without an explicit
// mode takes 0666 & ~umask, and the default umask here is 022 — so the store
// was created 0644, world-readable. Two identity files minted 2026-07-30 landed
// that way and had to be chmod'd by hand; the older ones were 0600 only because
// something else tightened them. Relying on the operator's umask to protect a
// signing key is not a safeguard, so the mode is stated here. chmodSync as well
// as the create mode, because mode: on writeFileSync applies only when the file
// is CREATED — an existing 0644 file keeps its permissions on rewrite, which is
// exactly how these two would have stayed exposed.
function fileStore(path) {
  const read = () => { try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return {}; } };
  return {
    get: (k) => read()[k] ?? null,
    set: (k, v) => {
      const o = read(); o[k] = v;
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      writeFileSync(path, JSON.stringify(o, null, 2), { mode: 0o600 });
      try { chmodSync(path, 0o600); } catch { /* best-effort on exotic filesystems */ }
    },
  };
}
const STORE = fileStore(STORE_PATH);

// REMOVED 2026-07-25 — loadOrCreateNodeIdentity(). This function loaded, and on
// first run minted-and-persisted, a durable transport keypair, giving this peer a
// stable nodeId across every restart. That is the anti-pattern I-ID forbids: the
// id became a correlator tying all of Claude's sessions together, and it also put
// two peers on prod under one nodeId whenever two sessions ran at once (#356).
// connectPeer() now mints an ephemeral transport identity per connection; there
// is no way to hand it a durable one.

// ── module state ────────────────────────────────────────────────────────
let _session = null;          // { peer, regionName, center, nodeId, author, close }
let _connecting = null;
const WATCHES = new Map();    // "region|topic" → { topic, region, descriptor, buffer[], total, dropped, since, startedAt, waiters[] }
const HOSTED  = new Map();    // "region|topic" → { topic, region, descriptor, since }
const ARRIVAL_LISTENERS = new Set();   // fn({topic,region,message,signer,msgId})

// owner + write FOLD INTO THE TOPIC ID — an owned topic is a different topic
// from the open one of the same name, so both the descriptor and the watch
// key must carry them (else a watch on "axona.bot" listens to the wrong topic).
import { sendFileBytes, listPointers, fetchFileToDisk, readLocalFile, namespaced, FILES_DIR }
  from './file-transfer.js';

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
    const author = await createAuthorIdentity({ persistAs: AUTHOR_KEY, store: STORE });   // durable WHO
    const h = await connectPeer({ region: REGION, author });   // transport identity: fresh, ephemeral, per connection
    _session = h;
    // Identity provenance, on stderr, once per session. stderr because stdout is
    // the MCP JSON-RPC channel — anything written there corrupts the protocol.
    //
    // WHY THIS IS PRINTED AT ALL. With several agents each running their own copy
    // of this server, the failure that costs the most to diagnose is two installs
    // sharing MCP_AUTHOR_PATH: they load the SAME author keypair, so their posts
    // carry the same signer and are not merely mislabelled but cryptographically
    // indistinguishable. Nothing downstream can separate them after the fact —
    // not the chat UI, not the envelope, not a later audit. So the one moment the
    // distinction is still cheap to observe is startup, and the fix is to make
    // each install state who it is out loud. An operator comparing two logs sees
    // a collision immediately; #356 was the same class at the transport layer and
    // took a live prod investigation to find.
    process.stderr.write(
      `[axona-mcp] handle=${HANDLE} authorId=${author.authorId.slice(0, 16)}… `
      + `region=${REGION} authorPath=${STORE_PATH}\n`,
    );
    _connecting = null;
    // Voluntary, signed self-declaration: this peer is an AI agent. Best-effort —
    // a failed declare must never fail the connection.
    if (DECLARE_CLASS) { try { await setAuthorClass({ cls: AUTHOR_CLASS, operator: OPERATOR, label: 'axona-relay MCP peer' }); } catch { /* */ } }
    await armStandingWatches();
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
  // REMOVED 2026-07-25 — we used to host() our own owner-only profile topic here,
  // reasoning that our persistent peer should keep the attestation alive after the
  // K-closest roots evict it. That reasoning is architecturally wrong and the fix
  // was a fix to the wrong layer.
  //
  // Hosting is decided by ADDRESS, never by ownership or interest: a node hosts a
  // topic only if its own nodeId lands in that topic's keyspace neighbourhood
  // (David's rule; now enforced in kernel peer.host() as HOST_NOT_IN_NEIGHBOURHOOD).
  // Owning the profile, publishing it, and caring whether it survives are all
  // irrelevant to whether THIS node is one of its holders — and hosting it anyway
  // gave us a role for a topic we are not near, which makes this node eligible to
  // handle and stamp its traffic: an interloper root.
  //
  // The real problem (attestations aging out of bounded root queues) is a
  // DURABILITY problem and belongs where durability lives — replication width,
  // retention, or periodic re-publish by the owner. Re-declaring on a timer is a
  // legitimate owner action; hosting is not.
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
    : { v: 1, text: message, handle: handle || HANDLE, authorClass: authorClass || AUTHOR_CLASS };
  const msgId = await s.peer.pub(descriptorFor(topic, region, resolveOwner(s, owner), write), body, { signWith: s.author });
  return { ok: true, topic, region: region || REGION, owner: resolveOwner(s, owner) ?? null, write: write ?? null, msgId, signer: s.author.authorId, nodeId: s.nodeId, persistent: true, shape: raw ? 'raw' : 'std-message' };
}

// A READ THAT DID NOT COMPLETE IS NOT AN EMPTY TOPIC.
//
// This returned `{ ok:true, found:false }` for any pull the kernel could not
// answer inside its 1000ms default — indistinguishable from a genuinely empty
// topic, and reported with ok:true so nothing downstream could tell. On
// 2026-07-31 that false negative cost most of a day: axona.bot, a fresh owned
// probe topic, and axona:bridge-directory all read `found:false` here while a
// direct peer.pull returned their newest message in 368-663ms. Four hypotheses
// were built and discarded on top of that reading — including "the channel is
// dead", which I stated publicly.
//
// Two changes, and the second matters more than the first:
//   1. A usable budget (PULL_TIMEOUT_MS, was the kernel's 1000ms default).
//   2. Absence is only ever reported when it was actually OBSERVED. If the read
//      threw, or ran out the clock, `found` is null and ok is false — never
//      false. Callers may not read "no answer" as "no message".
const PULL_TIMEOUT_MS = 8000;

export async function pull({ topic, region, owner, write, timeoutMs = PULL_TIMEOUT_MS }) {
  const s = await ensureSession();
  const descriptor = descriptorFor(topic, region, resolveOwner(s, owner), write);
  const startedAt = Date.now();
  let env = null, failure = null;
  try {
    env = await s.peer.pull(null, { topic: descriptor, timeoutMs });
  } catch (e) {
    failure = String((e && e.message) || e);
  }
  const elapsedMs = Date.now() - startedAt;
  // Ran the clock out with nothing to show → the network did not answer. The
  // kernel resolves a timed-out pull to null, same as a real miss, so elapsed
  // time is the only signal available here to tell them apart.
  const timedOut = !env && !failure && elapsedMs >= timeoutMs * 0.9;
  const inconclusive = !!failure || timedOut;
  return {
    ok: !inconclusive,
    topic,
    region: region || REGION,
    found: inconclusive ? null : !!env,      // null = UNKNOWN, never false
    message: env ? env.message : null,
    msgId: env?.msgId ?? null,
    elapsedMs,
    timeoutMs,
    ...(inconclusive
      ? { reason: failure
            ? `pull failed: ${failure} — absence NOT established`
            : `no answer within ${timeoutMs}ms — absence NOT established, retry or use watch+poll` }
      : {}),
  };
}

// ── standing watches that survive a restart ─────────────────────────────
//
// A watch is a session object: it lives on this peer, and when the MCP server
// restarts — which it does every time the Claude host restarts — every watch is
// gone. Re-arming them was a thing the agent had to REMEMBER to do, and the
// failure mode is silence: no error, no empty result, just a channel nobody is
// listening to. It went unnoticed for a whole session at least once.
//
// So the list lives in the environment and the server arms it at connect time.
// Nothing has to remember anything.
//
//   MCP_STANDING_WATCHES="axona.dev,general,jokes,axona.chat,axona.bot!owned"
//
// Entry syntax:  name  ·  name@region  ·  name!owned  (owner-only: this peer's
// own durable author, which is how the axona.bot channel is addressed — the
// bare name resolves to a DIFFERENT topic id, which is exactly the bug that
// once made every post to it invisible).
const STANDING = String(process.env.MCP_STANDING_WATCHES || '').split(',').map(s => s.trim()).filter(Boolean);

export function parseWatchSpec(spec) {
  let s = String(spec);
  const owned = s.endsWith('!owned');
  if (owned) s = s.slice(0, -'!owned'.length);
  const [topic, region] = s.split('@');
  return { topic, region: region || undefined, ...(owned ? { owner: 'self', write: 'owner' } : {}) };
}

async function armStandingWatches() {
  for (const spec of STANDING) {
    // Best-effort and individually guarded: one unreachable topic must not stop
    // the others, and must never fail the connection itself.
    try {
      const w = parseWatchSpec(spec);
      await watch({ ...w, since: 'all' });
      console.error(`[mcp] standing watch armed: ${spec}`);
    } catch (e) {
      console.error(`[mcp] standing watch FAILED: ${spec} — ${e.message}`);
    }
  }
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
    // `self` is carried on the ARRIVAL EVENT only, never into the buffer, so
    // poll()'s shape is unchanged. A push sink that wakes an agent must be able
    // to ignore that agent's own posts — otherwise every message it publishes
    // wakes it up to read its own words, and the alert channel becomes noise
    // that gets ignored, which is how an alert channel dies.
    const self = !!(env.signerPubkey && env.signerPubkey === s.author?.authorId);
    emitArrival({ topic: w.topic, region: w.region, ...m, self });           // push sink (notifications)
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
    // handle is reported so an agent can check WHICH participant it is before
    // posting into a shared conversation, rather than discovering it from how
    // its own message rendered to everyone else.
    handle: HANDLE,
    declaredClass: _session.declaredClass?.class ?? 'unstated', operator: _session.declaredClass?.operator ?? null,
    mesh: health ? { synaptomeSize: health.synaptomeSize ?? null, peers: health.peers?.length ?? null, state: health.state ?? null } : null,
    watches: [...WATCHES.values()].map((w) => ({ topic: w.topic, region: w.region, buffered: w.buffer.length, total: w.total, dropped: w.dropped, since: w.since, ageSec: Math.round((now() - w.startedAt) / 1000) })),
    hosted: [...HOSTED.values()].map((h) => ({ topic: h.topic, region: h.region, ageSec: Math.round((now() - h.since) / 1000) })),
  };
}

/**
 * Rebuild the session: drop the transport, mint a fresh one, and re-seat every
 * watch and hosted topic that was live before.
 *
 * WHY THIS EXISTS. `ensureSession()` caches `_session` and returns it forever,
 * so a peer that goes deaf — slept host, dead socket, a mesh that healed around
 * it — stays deaf for the life of the process. There was no way for an agent to
 * say "I think I am wedged, start over" short of restarting its whole MCP
 * server, which for a council reviewer means losing the session it is reviewing
 * from. Orion hit exactly this on 2026-08-05 and could not reach #council.
 *
 * This is the same failure the browser capture showed the same day, one layer
 * over: every recovery layer works, nobody owns the session. Session-Supervisor
 * v0.2 proposes the kernel own it automatically; this is the manual lever, and
 * it stays useful afterwards — an agent that suspects itself should be able to
 * act on the suspicion.
 *
 * The rebuild is deliberately FULL: a fresh transport means a fresh nodeId
 * (I-15 — transport identity is ephemeral by construction), while the durable
 * author identity is untouched, so the peer comes back as the same participant
 * on a new seat. Watches re-subscribe from WATCHES, not just the standing list,
 * so runtime-added watches survive; each keeps its buffer so nothing already
 * drained-but-unread is lost, and re-subscribes with `since:'all'` by default
 * so the deaf window replays.
 *
 * @param {{ since?: 'all'|'latest'|'live' }} [opts]
 */
export async function reconnect({ since = 'all' } = {}) {
  const before = {
    nodeId: _session?.nodeId ?? null,
    watches: [...WATCHES.entries()].map(([key, w]) => ({ key, topic: w.topic, region: w.region, descriptor: w.descriptor, buffer: w.buffer, total: w.total, dropped: w.dropped })),
    hosted: [...HOSTED.values()].map((h) => ({ topic: h.topic, region: h.region, descriptor: h.descriptor })),
  };

  // Tear down. A half-dead session may throw on close — that is precisely the
  // case this function exists for, so it must not stop the rebuild (I-3).
  try { await _session?.close?.(); } catch (e) { console.error(`[mcp] reconnect: close failed (continuing) — ${e.message}`); }
  _session = null;
  _connecting = null;
  WATCHES.clear();
  HOSTED.clear();

  const s = await ensureSession();   // fresh transport + nodeId; re-arms STANDING watches

  // Re-seat everything that was live, including watches added at runtime and
  // any the standing list does not cover. Buffers carry over: a reconnect must
  // not silently discard messages the agent had not yet polled.
  const restored = [];
  const failed = [];
  for (const w of before.watches) {
    try {
      await watch({ topic: w.topic, region: w.region, since });
      const nw = WATCHES.get(w.key);
      if (nw && w.buffer.length) { nw.buffer.unshift(...w.buffer); nw.total += w.total; nw.dropped += w.dropped; }
      restored.push(w.topic);
    } catch (e) { failed.push({ topic: w.topic, error: e.message }); }
  }
  for (const h of before.hosted) {
    try { await host({ topic: h.topic, region: h.region }); } catch (e) { failed.push({ topic: h.topic, error: `host: ${e.message}` }); }
  }

  const health = (() => { try { return s.peer.health(); } catch { return null; } })();
  return {
    ok: true,
    reconnected: true,
    // The OLD nodeId is deliberately absent: a log that chains transport
    // identities across a restart is exactly the durable correlator I-15
    // exists to prevent. What matters is that it changed, not what it was.
    nodeIdChanged: before.nodeId !== s.nodeId,
    nodeId: s.nodeId,
    authorId: s.author.authorId,      // unchanged by design — same participant
    watchesRestored: restored,
    watchesFailed: failed,
    hostedRestored: before.hosted.length,
    peers: health?.peers?.length ?? null,
    note: 'Peers may read 0 for a few seconds while the mesh forms; poll axona_status to confirm.',
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

// ── files ───────────────────────────────────────────────────────────────
// Content-addressed file transfer, PULL-ONLY. See src/file-transfer.js for
// why: a shared topic is public, so auto-saving arrivals would let any
// stranger put bytes on this host. Listing is free; disk is touched only by an
// explicit axona_get_file naming a hash.

export async function sendFile({ path, topic, region, filename }) {
  const s = await ensureSession();
  const { bytes, filename: base } = await readLocalFile(path);
  const shareName = namespaced(topic);
  const r = await sendFileBytes(s.peer, {
    bytes, filename: filename || base, mime: 'application/octet-stream',
    shareTopic: descriptorFor(shareName, region, undefined, undefined),
    region: regionToDescriptor(region || REGION).name,
    signWith: s.author,
  });
  return {
    ok: true, topic: shareName, region: region || REGION,
    sha256: r.sha256, filename: r.pointer.filename, bytes: r.pointer.bytes,
    chunks: r.chunks, repaired: r.repaired, pointerMsgId: r.pointerMsgId,
    note: 'Recipients fetch with axona_get_file using this sha256.',
  };
}

export async function listFiles({ topic, region, seconds = 15 }) {
  const s = await ensureSession();
  const shareName = namespaced(topic);
  const files = await listPointers(s.peer, descriptorFor(shareName, region, undefined, undefined), { seconds });
  return {
    ok: true, topic: shareName, region: region || REGION, count: files.length,
    files: files.map((f) => ({
      sha256: f.sha256, filename: f.filename, bytes: f.bytes, mime: f.mime,
      signer: f.signer, ts: f.ts,
    })),
    note: 'Nothing has been written to disk. Call axona_get_file with a sha256 to fetch one.',
  };
}

export async function getFile({ sha256, region, filename, timeoutSec = 90 }) {
  const s = await ensureSession();
  const r = await fetchFileToDisk(s.peer, {
    sha256, region: regionToDescriptor(region || REGION).name, filename,
    timeoutMs: Math.max(5, Math.min(300, timeoutSec)) * 1000,
  });
  return {
    ok: true, path: r.path, filename: r.filename, bytes: r.bytes, sha256: r.sha256, mime: r.mime,
    verified: true,
    warning: r.executable
      ? 'This file has an executable extension. It was written WITHOUT the execute bit and was not opened. Do not run it unless you know its provenance.'
      : undefined,
  };
}
