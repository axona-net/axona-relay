// =============================================================================
// harness/sidecar.mjs — one harness peer, run alongside a host's relays.
//
// Executes ITS slice of the seeded plan against the live testnet mesh and
// writes the three-truths ledger (spec v0.3 §2/§3). The relays under test
// are never touched — this peer uses the network the way an application
// does, through the same bridge and mesh.
//
//   HOST=m4 OS=darwin PEER_IDX=0 NODES=8 SEED=42 DURATION_MS=43200000 \
//     node harness/sidecar.mjs
//
// Owned-topic coordination: author identities are minted at runtime (real
// Ed25519 — never seeded), so owned-topic subscribers cannot know the
// publisher's authorId from the plan alone. Each sidecar announces
// {peerIdx, authorId} on the open coordination topic harness/coord-<seed>;
// owned subscribers resolve the publisher's author from those
// announcements before subscribing. The bootstrap phase is itself
// ledgered — a coord announcement that fails to propagate is a stranded
// write on record, not a silent setup failure.
// =============================================================================
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { generatePlan, planCanonical } from './lib/workload.mjs';
import { Ledger, sha256 } from './lib/ledger.mjs';
import { appendFileSync } from 'node:fs';

const env = (k, d) => process.env[k] ?? d;
// --peer N in argv mirrors PEER_IDX and — the real point — puts the peer
// identity ON THE COMMAND LINE, so a churn kill can target one sidecar by
// exact pattern. Env vars are invisible to pkill -f; argv is not. The
// under-scoped fallback kill that took out a whole host's sidecars is why.
const argPeer = (() => { const i = process.argv.indexOf('--peer'); return i >= 0 ? Number(process.argv[i + 1]) : undefined; })();
const HOST = env('HOST'); const OS = env('OS', process.platform);
const PEER_IDX = argPeer ?? Number(env('PEER_IDX')); const NODES = Number(env('NODES'));
const SEED = Number(env('SEED')); const DURATION_MS = Number(env('DURATION_MS'));
const REGION = env('REGION', 'eagle'); const BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const LEDGER_DIR = env('LEDGER_DIR', 'harness/results');
for (const [k, v] of Object.entries({ HOST, PEER_IDX, NODES, SEED, DURATION_MS })) {
  if (v === undefined || Number.isNaN(v)) { console.error(`sidecar: ${k} required`); process.exit(1); }
}

// Sparse stderr progress markers — stderr is unbuffered, so a hung sidecar
// tells the operator its last completed phase even under file redirection.
const phase = (m) => console.error(`[sidecar ${PEER_IDX}] ${m}`);
phase('plan');
const plan = generatePlan({ seed: SEED, nodes: NODES, durationMs: DURATION_MS,
  openN: env('OPEN_N') ? Number(env('OPEN_N')) : undefined,
  ownedN: env('OWNED_N') ? Number(env('OWNED_N')) : undefined });
const planHash = sha256(planCanonical(plan)).slice(0, 16);
phase(`connecting ${BRIDGE}`);
const { peer, author } = await connectPeer({ region: REGION, bridge: BRIDGE });
phase(`connected; author ${author.authorId.slice(0, 12)}`);
const led = new Ledger(`${LEDGER_DIR}/sidecar-${SEED}-${PEER_IDX}.jsonl`,
  { host: HOST, os: OS, peerIdx: PEER_IDX, author: author.authorId });

// TRACE=1: capture the smoking-gun kernel events for the owned-topic install
// trace — undeliverable, replay drops, root instability — filtered so the file
// stays small. Kernel log API is peer.onLog(level, handler), registered here.
if (env('TRACE') === '1') {
  const KLOG = `${LEDGER_DIR}/klog-${SEED}-${PEER_IDX}.jsonl`;
  const K = /undeliver|replayup|root-formed|root-transition|root-evicted|root-claimed|singleton-root|empty-root|upstream-unpinned/i;
  const kt0 = Date.now();
  for (const lvl of ['info', 'warn', 'error']) {
    try {
      peer.onLog(lvl, (...a) => {
        let s; try { s = JSON.stringify(a); } catch { return; }
        if (K.test(s)) { try { appendFileSync(KLOG, JSON.stringify({ ms: Date.now() - kt0, lvl, a }) + '\n'); } catch { /* */ } }
      });
    } catch { /* older kernel */ }
  }
  phase('TRACE on');
}

// LAT_TRACE=1: capture the kernel's per-stage delivery-latency stamps (David
// 2026-08-30). The kernel emits `pubsub:lat-stage` via onLog with { msgId, stage,
// t, mono }; one row per stage per peer into latstage-<seed>-<peer>.jsonl. The
// analyzer joins these by msgId across peers to locate where the 1.7s median is.
if (env('LAT_TRACE') === '1') {
  const LSF = `${LEDGER_DIR}/latstage-${SEED}-${PEER_IDX}.jsonl`;
  const onStage = (msg, ctx) => {
    if (msg !== 'pubsub:lat-stage' || !ctx) return;
    try { appendFileSync(LSF, JSON.stringify({ msgId: ctx.msgId, stage: ctx.stage, t: ctx.t, mono: ctx.mono, peerIdx: PEER_IDX, host: HOST }) + '\n'); } catch { /* */ }
  };
  try { peer.onLog('info', onStage); } catch { /* older kernel */ }
  // disc events (became-root / sub-root / pub-root / root-members / term-verify /
  // branch-reattach) → disc-<seed>-<peer>.jsonl. Each carries `t` (topicId hex
  // prefix) so the analyzer segments delivery/latency by what the mesh was doing
  // under each topic: root migration, reattach fire, dead-skip, term-verify verdict.
  const DSF = `${LEDGER_DIR}/disc-${SEED}-${PEER_IDX}.jsonl`;
  const onDisc = (msg, ctx) => {
    if (msg !== 'pubsub:disc' || !ctx) return;
    try { appendFileSync(DSF, JSON.stringify({ wall: Date.now(), peerIdx: PEER_IDX, host: HOST, ...ctx }) + '\n'); } catch { /* */ }
  };
  try { peer.onLog('info', onDisc); } catch { /* older kernel */ }
  phase('LAT_TRACE on (+disc)');
}
led.event({ kind: 'start', detail: { planHash, seed: SEED, nodes: NODES, durationMs: DURATION_MS, bridge: BRIDGE } });

const D = (name) => ({ region: REGION, name });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nonceFor = (ti, seq) => `${SEED}-${PEER_IDX}-${ti}-${seq}`;

// ── bootstrap: announce my author; collect everyone else's ───────────
phase('coord bootstrap');
const coord = D(`harness/coord-${SEED}`);
const authors = new Map();                    // peerIdx -> authorId
authors.set(PEER_IDX, author.authorId);
await peer.sub(coord, (envp) => {
  const m = envp?.message;
  if (m?.k === 'coord' && Number.isInteger(m.peerIdx)) authors.set(m.peerIdx, m.authorId);
}, { since: 'all' });
{
  const body = { v: 1, k: 'coord', peerIdx: PEER_IDX, authorId: author.authorId };
  led.intent({ topic: coord.name, topicSeq: -1, nonce: `coord-${PEER_IDX}`, payloadHash: sha256(JSON.stringify(body)) });
  try {
    const msgId = await peer.pub(coord, body, { signWith: author });
    led.api({ topic: coord.name, topicSeq: -1, nonce: `coord-${PEER_IDX}`, confirmed: true, msgId });
  } catch (e) {
    led.api({ topic: coord.name, topicSeq: -1, nonce: `coord-${PEER_IDX}`, error: String(e?.message).slice(0, 120) });
  }
}
// Owned publishers must be resolvable before their subscribers attach; give
// the coord topic a bounded settle, re-announcing once (byte-identical —
// content-addressing dedups; a strand that heals on retry is itself signal).
const COORD_WAIT_MS = Number(env('COORD_WAIT_MS', 120_000));
{
  const needed = new Set(plan.topics.filter((t) => t.kind === 'owned').map((t) => t.publishers));
  const deadline = Date.now() + COORD_WAIT_MS;
  let reannounced = false;
  while (Date.now() < deadline && ![...needed].every((p) => authors.has(p))) {
    await sleep(2_000);
    if (!reannounced && Date.now() > deadline - COORD_WAIT_MS / 2) {
      reannounced = true;
      try { await peer.pub(coord, { v: 1, k: 'coord', peerIdx: PEER_IDX, authorId: author.authorId }, { signWith: author }); } catch { /* ledgered above */ }
    }
  }
  const missing = [...needed].filter((p) => !authors.has(p));
  led.event({ kind: 'coord-settled', detail: { known: authors.size, missing } });
}

// ── subscriptions: every topic where I am a required reader ──────────
const myTopics = [];                          // {ti, t, desc}
plan.topics.forEach((t, ti) => {
  const reader = t.requiredReaders.includes(PEER_IDX);
  const isOwnedPublisher = t.kind === 'owned' && t.publishers === PEER_IDX;
  if (!reader && !isOwnedPublisher && t.kind !== 'open') return;
  const desc = t.kind === 'owned'
    ? { ...D(t.name), owner: authors.get(t.publishers) ?? author.authorId, write: 'owner' }
    : D(t.name);
  myTopics.push({ ti, t, desc, lastSeqSeen: -1, lastMsgId: null, watchLastMono: 0 });
});
for (const m of myTopics) {
  const sh = await peer.sub(m.desc, (envp) => {
    const msg = envp?.message;
    if (msg?.k !== 'load') return;
    m.watchLastMono = Date.now();
    if (Number.isInteger(msg.seq) && msg.seq > m.lastSeqSeen) { m.lastSeqSeen = msg.seq; m.lastMsgId = envp.msgId; }
    led.observe({ topic: m.t.name, topicSeq: msg.seq, nonce: msg.nonce, msgId: envp.msgId,
      via: 'watch', payloadHash: sha256(JSON.stringify(msg)) });
  }, { since: 'all' });
  // topicId <-> (name, kind) map — lets the analyzer group relay-log root events
  // (which key on the 12-hex topicId) by open vs owned, to test whether owned
  // topics form more empty sub-terminal roots (the cold-hint hypothesis).
  led.event({ kind: 'topicmap', detail: { name: m.t.name, kind: m.t.kind, topicId: sh?.topicId ?? null } });
}
led.event({ kind: 'subscribed', detail: { topics: myTopics.length } });
phase(`subscribed to ${myTopics.length} topics; entering publish loop`);

// ── readiness barrier (Aster) ────────────────────────────────────────
// Settle so subscriptions install before the measured window opens, then mark
// the boundary. Publishes and observations before measure-start are STARTUP and
// the analyzer keeps them separate from steady-state losses, so a startup
// control-plane race is not masked into the headline number. A full
// authoritative-root install confirmation needs kernel introspection we do not
// have yet; this time-boxed settle is the honest proxy.
const READINESS_MS = Number(env('READINESS_MS', 15_000));
await sleep(READINESS_MS);
led.event({ kind: 'measure-start', detail: { readinessMs: READINESS_MS } });

// ── publish loop: my slice of the schedule ───────────────────────────
const mine = plan.schedule.filter((e) => e.publisher === PEER_IDX);
led.event({ kind: 'plan-slice', detail: { events: mine.length } });
const t0 = Date.now();
(async () => {
  for (const e of mine) {
    const wait = e.atMs - (Date.now() - t0);
    if (wait > 0) await sleep(wait);
    const t = plan.topics[e.topic];
    const desc = t.kind === 'owned' ? { ...D(t.name), owner: author.authorId, write: 'owner' } : D(t.name);
    const nonce = nonceFor(e.topic, e.seq);
    const body = { v: 1, k: 'load', seed: SEED, topic: t.name, seq: e.seq, nonce, from: PEER_IDX };
    led.intent({ topic: t.name, topicSeq: e.seq, nonce, payloadHash: sha256(JSON.stringify(body)), descriptor: desc });
    try {
      const msgId = await peer.pub(desc, body, { signWith: author });
      led.api({ topic: t.name, topicSeq: e.seq, nonce, confirmed: true, msgId });
    } catch (err) {
      led.api({ topic: t.name, topicSeq: e.seq, nonce, confirmed: false, error: String(err?.message).slice(0, 120) });
    }
  }
})();

// ── samplers ─────────────────────────────────────────────────────────
const samplers = [];
// Head sweep — LOCAL read (2026-08-31, David's catch). The subscriber is ON the
// topic's axon tree and already holds every message it received via the watch
// callback, so its head is m.lastSeqSeen / m.lastMsgId — no routed peer.pull to
// the root. The old routed pull was the OBSERVER EFFECT: each pull walked to the
// root and contended with the live push there, dropping delivery 97.9%→85.4% at
// 10s cadence (seed-33). Reading local is free, so the cadence no longer perturbs
// the run. Feeds split-root detection (two readers' local heads compared). The
// observe(via:'pull') channel is gone: everything a subscriber receives — live push
// AND renewal replay — already arrives through the watch callback and is recorded
// there, so the pull row was redundant as well as expensive.
samplers.push(setInterval(() => {
  for (const m of myTopics) {
    led.head({ topic: m.t.name, descriptor: m.desc,
      headSeq: m.lastSeqSeen >= 0 ? m.lastSeqSeen : null, headMsgId: m.lastMsgId });
  }
}, Number(env('HEAD_SWEEP_MS', 60_000))));
// State sweep: honest watch liveness + the participant connection set + roles.
samplers.push(setInterval(() => {
  const nowMs = Date.now();
  for (const m of myTopics) {
    led.watchState({ topic: m.t.name, buffered: null, total: m.lastSeqSeen + 1,
      lastArrivalMono: m.watchLastMono ? m.watchLastMono - t0 : null,
      silentMs: m.watchLastMono ? nowMs - m.watchLastMono : null });
  }
  let mesh = null, roles = null;
  try {
    const h = peer.health?.();
    if (h) {
      mesh = { synaptomeSize: h.synaptomeSize ?? null,
        peers: Array.isArray(h.peers) ? h.peers.length : (h.peers ?? null), state: h.state ?? null };
      roles = h.axonRoles ?? null;
    }
  } catch { /* health unavailable this tick */ }
  led.connSnapshot({ mesh, roles });
  led.resources({ rssMb: +(process.memoryUsage().rss / 1048576).toFixed(1) });
}, 30_000));

// ── end of window ────────────────────────────────────────────────────
await sleep(Math.max(0, DURATION_MS - (Date.now() - t0)));
for (const s of samplers) clearInterval(s);
// Final head sample from LOCAL state (2026-08-31) — no routed pull. The watch
// record is the authoritative view of what THIS subscriber received; a closing
// local head lets the analyzer see the reader's final position without injecting
// a last burst of root-routed reads at window end.
for (const m of myTopics) {
  led.head({ topic: m.t.name, descriptor: m.desc,
    headSeq: m.lastSeqSeen >= 0 ? m.lastSeqSeen : null, headMsgId: m.lastMsgId });
}
led.event({ kind: 'end', detail: { planHash } });
try { await peer.leave({ timeoutMs: 8_000 }); } catch { /* dying */ }
process.exit(0);
