// harness/route-diag.mjs — WHY is the routing lookup slow / why do local minima
// form? (David 2026-08-30, investigation step 1). ROUTE_TRACE makes findKClosest
// emit a per-call summary; we run it from N fresh origins for one topicId and
// read where the time goes and how dense each origin's seed pool is.
//
// Separates the two local-minimum modes:
//   sparse table    seedPool small, closerInSeed=0, rounds<=1, elapsed~0, terminus=self
//   slow convergence rounds high and/or rejected>0 (dead-peer waits), elapsed large
//
//   BRIDGE=wss://testnet.axona.net PEERS=6 TOPIC=harness/localmin-fixed node harness/route-diag.mjs
import '../src/polyfill.js';
process.env.ROUTE_TRACE = '1';
import { connectPeer } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const REGION = process.env.REGION || 'eagle';
const PEERS  = Number(process.env.PEERS || 6);
const TOPIC  = process.env.TOPIC || 'harness/localmin-fixed';
const SETTLE_MS = Number(process.env.SETTLE_MS || 25000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = (x) => (x == null ? null : String(x).slice(0, 12));

const events = [];                                  // { peer, ...route-lookup ctx }
const rec = (tag) => (msg, ctx) => { if (msg === 'route-lookup' && ctx) events.push({ peer: tag, ...ctx }); };

const topicBig = await deriveTopicIdBig({ region: REGION, name: TOPIC });
console.error(`topic ${TOPIC} topicId ${S(topicBig.toString(16))} connecting ${PEERS} peers (ROUTE_TRACE on)`);
const peers = [];
for (let i = 0; i < PEERS; i++) { const h = await connectPeer({ region: REGION, bridge: BRIDGE }); h.peer.onLog('info', rec(S(h.nodeId))); peers.push(h); }
console.error(`settling ${SETTLE_MS}ms`);
await sleep(SETTLE_MS);

// Snapshot each peer's live synaptome density, then run the lookup (fires telemetry).
const density = [];
for (const h of peers) {
  let syn = null, incoming = null, healthPeers = null;
  try { syn = h.peer._node?.synaptome?.size ?? null; } catch { /* */ }
  try { incoming = h.peer._node?.incomingSynapses?.size ?? null; } catch { /* */ }
  try { healthPeers = h.peer.health?.().peers?.length ?? null; } catch { /* */ }
  density.push({ peer: S(h.nodeId), syn, incoming, healthPeers });
  try { await h.peer.findKClosest(topicBig, 1); } catch (e) { console.error('findKClosest err', e?.message); }
}

console.log('\n===== ROUTE-LOOKUP DIAGNOSTIC =====');
console.log('topicId', S(topicBig.toString(16)), ' peers', peers.length);
console.log('\n-- synaptome density per origin --');
for (const d of density) console.log(`  ${d.peer}  synaptome=${d.syn}  incoming=${d.incoming}  healthPeers=${d.healthPeers}`);
console.log('\n-- findKClosest telemetry per origin --');
for (const e of events) console.log(
  `  ${e.peer}  seedPool=${e.seedPool} closerInSeed=${e.closerInSeed} rounds=${e.rounds} ` +
  `probes=${e.probes} ok=${e.fulfilled} dead=${e.rejected} elapsed=${e.elapsedMs}ms ` +
  `terminus=${e.terminus}${e.terminusIsSelf ? '(SELF)' : ''}`);

const selfTerm = events.filter((e) => e.terminusIsSelf);
const sparse   = events.filter((e) => e.terminusIsSelf && e.closerInSeed === 0 && e.rounds <= 1);
const slow     = events.filter((e) => e.elapsedMs >= 100);
console.log('\n-- summary --');
console.log(`self-terminus: ${selfTerm.length}/${events.length}  | sparse-table minima (0 closer, <=1 round, ~0ms): ${sparse.length}  | slow (>=100ms): ${slow.length}`);
console.log(`elapsed p50/max: ${(() => { const a = events.map((e) => e.elapsedMs).sort((x, y) => x - y); return a.length ? `${a[Math.floor(a.length / 2)]}/${a[a.length - 1]}ms` : 'n/a'; })()}`);
console.log(`total dead-peer waits (rejected sends): ${events.reduce((s, e) => s + (e.rejected || 0), 0)}`);

for (const h of peers) { try { await h.close(); } catch { /* */ } }
process.exit(0);
