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

const env = (k, d) => process.env[k] ?? d;
const HOST = env('HOST'); const OS = env('OS', process.platform);
const PEER_IDX = Number(env('PEER_IDX')); const NODES = Number(env('NODES'));
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
  myTopics.push({ ti, t, desc, lastSeqSeen: -1, watchLastMono: 0 });
});
for (const m of myTopics) {
  await peer.sub(m.desc, (envp) => {
    const msg = envp?.message;
    if (msg?.k !== 'load') return;
    m.watchLastMono = Date.now();
    if (Number.isInteger(msg.seq) && msg.seq > m.lastSeqSeen) m.lastSeqSeen = msg.seq;
    led.observe({ topic: m.t.name, topicSeq: msg.seq, nonce: msg.nonce, msgId: envp.msgId,
      via: 'watch', payloadHash: sha256(JSON.stringify(msg)) });
  }, { since: 'all' });
}
led.event({ kind: 'subscribed', detail: { topics: myTopics.length } });
phase(`subscribed to ${myTopics.length} topics; entering publish loop`);

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
    led.intent({ topic: t.name, topicSeq: e.seq, nonce, payloadHash: sha256(JSON.stringify(body)) });
    try {
      const msgId = await peer.pub(desc, body, { signWith: author });
      led.api({ topic: t.name, topicSeq: e.seq, nonce, confirmed: true, msgId });
    } catch (err) {
      led.api({ topic: t.name, topicSeq: e.seq, nonce, confirmed: false, error: String(err?.message).slice(0, 120) });
    }
  }
})();

// ── samplers: pull heads, watch state, resources ─────────────────────
const samplers = [];
samplers.push(setInterval(async () => {
  const m = myTopics[Math.floor(Math.random() * myTopics.length)];
  if (!m) return;
  try {
    const head = await peer.pull(null, { topic: m.desc, timeoutMs: 15_000 });
    const msg = head?.message;
    led.pullHead({ topic: m.t.name, headSeq: Number.isInteger(msg?.seq) ? msg.seq : null, headMsgId: head?.msgId ?? null });
    if (Number.isInteger(msg?.seq)) {
      led.observe({ topic: m.t.name, topicSeq: msg.seq, nonce: msg.nonce, msgId: head.msgId,
        via: 'pull', payloadHash: sha256(JSON.stringify(msg)) });
    }
  } catch { led.pullHead({ topic: m.t.name, headSeq: null, headMsgId: null }); }
}, 20_000));
samplers.push(setInterval(() => {
  for (const m of myTopics) {
    led.watchState({ topic: m.t.name, buffered: 0, total: m.lastSeqSeen + 1,
      lastArrivalMono: m.watchLastMono ? m.watchLastMono - t0 : null });
  }
  led.resources({ rssMb: +(process.memoryUsage().rss / 1048576).toFixed(1) });
}, 30_000));

// ── end of window ────────────────────────────────────────────────────
await sleep(Math.max(0, DURATION_MS - (Date.now() - t0)));
for (const s of samplers) clearInterval(s);
// Final replay sweep: a LATE pull per topic so eventual-replay integrity has
// its closing sample (the reconciliation window's read).
for (const m of myTopics) {
  try {
    const head = await peer.pull(null, { topic: m.desc, timeoutMs: 20_000 });
    const msg = head?.message;
    if (Number.isInteger(msg?.seq)) {
      led.observe({ topic: m.t.name, topicSeq: msg.seq, nonce: msg.nonce, msgId: head.msgId,
        via: 'replay', payloadHash: sha256(JSON.stringify(msg)) });
    }
  } catch { /* ledgered by absence */ }
}
led.event({ kind: 'end', detail: { planHash } });
try { await peer.leave({ timeoutMs: 8_000 }); } catch { /* dying */ }
process.exit(0);
