// live-delivery.mjs — measure LIVE pub/sub delivery against the running testnet
// fleet (the same watch-only per-(message,reader) contract the ~60% soak used).
//
// N ephemeral subscriber peers + 1 publisher connect to the live bridge, all
// subscribe one OPEN topic, the publisher publishes on a cadence, and every
// (message, subscriber) pair is scored watch-only against a sweep of latency
// cutoffs. The fleet's own churn (conn-decay, root flaps) is the live condition;
// this is the number the 4.73.0 subscriber-list-replication fix should lift.
//
//   SUBS=24 PUBS=24 PUB_EVERY_MS=5000 SETTLE_MS=30000 DRAIN_MS=130000 \
//     BRIDGE=wss://testnet.axona.net node harness/live-delivery.mjs
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const SUBS = Number(env('SUBS', '24'));
const PUBS = Number(env('PUBS', '24'));
const PUB_EVERY_MS = Number(env('PUB_EVERY_MS', '5000'));
const SETTLE_MS = Number(env('SETTLE_MS', '30000'));   // mesh warm-up before publishing
const DRAIN_MS  = Number(env('DRAIN_MS', '130000'));   // > the live 120s deadline so 'eventual' is real
const TOPIC = env('TOPIC', `live-delivery-${(Date.now?.() ?? 0)}`);
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.error(`[live] ${m}`);
const desc = { region: REGION, name: TOPIC };

log(`connecting ${SUBS} subscribers + 1 publisher to ${BRIDGE}  topic=${REGION}/${TOPIC}`);
const subs = [];
for (let i = 0; i < SUBS; i++) subs.push(await connectPeer({ region: REGION, bridge: BRIDGE }));
const pub = await connectPeer({ region: REGION, bridge: BRIDGE });
log(`connected; settling ${SETTLE_MS / 1000}s for mesh warm-up`);
await wait(SETTLE_MS);

// each subscriber records first-arrival wall time per msgId
const recvAt = subs.map(() => new Map());
for (let i = 0; i < subs.length; i++) {
  const m = recvAt[i];
  await subs[i].peer.sub(desc, (envp) => { const id = envp?.msgId; if (id && !m.has(String(id))) m.set(String(id), Date.now()); });
  await wait(20);
}
await wait(3000);   // let the last subscribes seat

// publish a series on a steady cadence
const published = [];
for (let s = 0; s < PUBS; s++) {
  try { const id = String(await pub.peer.pub(desc, { v: 1, seq: s, t: Date.now() }, { signWith: pub.author })); published.push({ id, tPub: Date.now() }); }
  catch (e) { log(`pub ${s} err: ${String(e?.message || e).slice(0, 60)}`); }
  await wait(PUB_EVERY_MS);
}
log(`published ${published.length}/${PUBS}; draining ${DRAIN_MS / 1000}s for late arrivals`);
await wait(DRAIN_MS);

// ── score per (message, subscriber) at a sweep of cutoffs ──
const CUTOFFS = [
  { name: '2s', ms: 2000 }, { name: '5s', ms: 5000 }, { name: '15s', ms: 15000 },
  { name: '30s', ms: 30000 }, { name: '60s (1x renew)', ms: 60000 },
  { name: '120s (2x renew)', ms: 120000 }, { name: 'eventual', ms: Infinity },
];
let trials = 0; const got = new Map(CUTOFFS.map((c) => [c.name, 0])); const lat = [];
for (const msg of published) {
  for (let i = 0; i < subs.length; i++) {
    trials++;
    const at = recvAt[i].get(msg.id);
    const arrival = at === undefined ? Infinity : (at - msg.tPub);
    for (const c of CUTOFFS) if (arrival <= c.ms) got.set(c.name, got.get(c.name) + 1);
    if (arrival !== Infinity) lat.push(arrival);
  }
}
lat.sort((a, b) => a - b);
const pctl = (p) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(p / 100 * lat.length))] : 0;
const pct = (x) => trials ? (100 * x / trials).toFixed(1) : '0.0';

console.log('\n============ LIVE DELIVERY (testnet fleet) ============');
console.log(`subscribers=${subs.length}  publishes=${published.length}  trials=${trials}  bridge=${BRIDGE}`);
for (const c of CUTOFFS) console.log(`  <= ${c.name.padEnd(16)} ${pct(got.get(c.name))}%`);
console.log(`delivered-latency p50/p90/p99 (ms): ${pctl(50)} / ${pctl(90)} / ${pctl(99)}`);
console.log('=======================================================');
console.log('RESULT_JSON ' + JSON.stringify({
  subs: subs.length, publishes: published.length, trials,
  pct2s: +pct(got.get('2s')), pct5s: +pct(got.get('5s')), pct15s: +pct(got.get('15s')),
  pct60s: +pct(got.get('60s (1x renew)')), pct120s: +pct(got.get('120s (2x renew)')),
  eventual: +pct(got.get('eventual')), p50: pctl(50), p90: pctl(90), p99: pctl(99),
}));
for (const s of subs) { try { await s.peer.leave?.({ timeoutMs: 5000 }); } catch {} }
try { await pub.peer.leave?.({ timeoutMs: 5000 }); } catch {}
process.exit(0);
