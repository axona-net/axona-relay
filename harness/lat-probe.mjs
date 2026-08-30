// harness/lat-probe.mjs — per-stage delivery-latency probe (David 2026-08-30).
//
// Runs a publisher and a subscriber IN ONE PROCESS so their monotonic clocks are
// directly comparable — no cross-host offset ambiguity. LAT_TRACE=1 makes the
// kernel stamp each delivery stage (pub:built → pub:send → root:recv/verified/
// fanout → sub:recv → deliver:app → deliver:cb) via onLog; we join them by msgId
// and report where the time goes. root:* stamps only appear when the emergent
// root is one of OUR peers; when it is a fleet relay they are absent and the
// pub:send → sub:recv gap is the whole wire+forward-tree black box.
//
// Two phases per the council's "converged topology" invariant:
//   converged — subscribe, wait SETTLE, then publish (steady state)
//   cold      — publish immediately after subscribe (un-settled install)
//
//   BRIDGE=wss://testnet.axona.net N=30 GAP_MS=1500 SETTLE_MS=30000 \
//     node harness/lat-probe.mjs
import '../src/polyfill.js';
process.env.LAT_TRACE = '1';                      // MUST be set before the kernel constructs
import { connectPeer } from '../src/ops.js';

const BRIDGE   = process.env.BRIDGE || 'wss://testnet.axona.net';
const REGION   = process.env.REGION || 'eagle';
const N        = Number(process.env.N || 30);
const GAP_MS   = Number(process.env.GAP_MS || 1500);
const SETTLE_MS= Number(process.env.SETTLE_MS || 30000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// msgId -> { stage -> {t, mono, peer} }
const stages = new Map();
const record = (peerTag) => (msg, ctx) => {
  if (msg !== 'pubsub:lat-stage' || !ctx?.msgId) return;
  let s = stages.get(ctx.msgId); if (!s) { s = {}; stages.set(ctx.msgId, s); }
  if (!s[ctx.stage]) s[ctx.stage] = { t: ctx.t, mono: ctx.mono, peer: peerTag };
};

console.error(`connecting pub+sub to ${BRIDGE} (LAT_TRACE on)`);
const sub = await connectPeer({ region: REGION, bridge: BRIDGE });
const pub = await connectPeer({ region: REGION, bridge: BRIDGE });
for (const [tag, h] of [['sub', sub], ['pub', pub]]) {
  try { h.peer.onLog('info', record(tag)); } catch (e) { console.error('onLog reg failed', e?.message); }
}
console.error('connected', sub.author.authorId.slice(0, 6), pub.author.authorId.slice(0, 6));

const runPhase = async (label, settleMs) => {
  const topic = `harness/lat-${label}-${sub.author.authorId.slice(0, 6)}`;
  const desc = { region: REGION, name: topic };
  const got = new Map();                              // msgId -> app-callback wall ms
  await sub.peer.sub(desc, (env) => { const id = env?.msgId; if (id) got.set(id, Date.now()); }, { since: 'all' });
  if (settleMs) { console.error(`[${label}] settling ${settleMs}ms`); await sleep(settleMs); }
  const ids = [];
  for (let i = 0; i < N; i++) {
    const id = await pub.peer.pub(desc, { v: 1, k: 'lat', i, nonce: `${label}-${i}` }, { signWith: pub.author });
    ids.push(id);
    await sleep(GAP_MS);
  }
  console.error(`[${label}] published ${N}, draining 40s for stragglers`);
  await sleep(40000);
  return { label, ids, got };
};

const results = [];
results.push(await runPhase('converged', SETTLE_MS));
results.push(await runPhase('cold', 0));

// ── report ────────────────────────────────────────────────────────────
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(a.length * p))] : null;
const ORDER = ['pub:built', 'pub:send', 'root:recv', 'root:verified', 'root:fanout', 'sub:recv', 'deliver:app', 'deliver:cb'];
const HOPS = [
  ['pub:built', 'pub:send', 'publisher-local (build+warmRootHint)'],
  ['pub:send', 'sub:recv', 'WIRE + fleet forward tree (black box)'],
  ['sub:recv', 'deliver:app', 'subscriber-local dispatch'],
  ['deliver:app', 'deliver:cb', 'app callback'],
  ['pub:built', 'deliver:cb', 'END-TO-END'],
];

for (const { label, ids } of results) {
  console.log(`\n===== phase: ${label} (n=${ids.length}) =====`);
  // stage coverage
  const cov = {}; for (const st of ORDER) cov[st] = 0;
  for (const id of ids) { const s = stages.get(id) || {}; for (const st of ORDER) if (s[st]) cov[st]++; }
  console.log('stage coverage: ' + ORDER.map((st) => `${st}=${cov[st]}`).join('  '));
  // per-hop deltas using mono (same-process peers → comparable)
  for (const [a, b, name] of HOPS) {
    const d = [];
    for (const id of ids) {
      const s = stages.get(id); if (!s || !s[a] || !s[b]) continue;
      // same-peer → mono delta (exact); cross-peer → mono still comparable in-process
      d.push(s[b].mono - s[a].mono);
    }
    if (d.length) console.log(`  ${a} → ${b}  [${name}]  n=${d.length}  p50=${Math.round(pct(d, 0.5))}ms  p95=${Math.round(pct(d, 0.95))}ms  max=${Math.round(Math.max(...d))}ms`);
    else console.log(`  ${a} → ${b}  [${name}]  no paired samples (stage absent — root likely a fleet relay)`);
  }
}
for (const p of [sub, pub]) { try { await p.close(); } catch { /* */ } }
process.exit(0);
