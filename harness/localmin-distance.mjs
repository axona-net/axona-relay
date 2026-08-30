// harness/localmin-distance.mjs — WHERE (in distance-to-target) do greedy local
// minima occur? (David 2026-08-30). If they cluster NEAR the target, the fix is a
// denser last-mile: guarantee each node's k XOR-nearest successors. If FAR, it is a
// mid-route sparse-bucket problem and density near self does not help.
//
// Metric: distance(node, target) = bitLength(node ^ target). SMALL = near target
// (few bits differ), LARGE = far. A greedy LOCAL MINIMUM is a peer that is a greedy
// terminal (_greedyNextHopToward(target) == null → it would self-root) yet the
// iterative lookup finds a STRICTLY closer node. We record the stall distance
// (bitLength(peer ^ target)) for every confirmed local minimum, across many random
// targets and many origin peers, and histogram it.
//
// Efficiency: greedy-terminal is a LOCAL check (no network); only a terminal needs
// the findKClosest lookup to confirm a closer node exists. Per target typically one
// peer is the terminal, so ~TARGETS lookups, not PEERS*TARGETS.
//
// CAVEAT: origin peers are ephemeral clients; their synaptome is built from the
// fleet during SETTLE but is not a fleet relay's maintained (kNear) table. This
// measures the SHAPE of the stall-distance distribution (near vs far), which is a
// property of the keyspace density gradient and is the question asked; absolute
// rates would need fleet-relay-side capture.
//
//   BRIDGE=wss://testnet.axona.net PEERS=14 TARGETS=48 node harness/localmin-distance.mjs
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';
import { idBig } from '../vendor/axona-protocol/src/pubsub/ids.js';

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const REGION = process.env.REGION || 'eagle';
const PEERS  = Number(process.env.PEERS || 14);
const TARGETS = Number(process.env.TARGETS || 48);
const SETTLE_MS = Number(process.env.SETTLE_MS || 30000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = (x) => (x == null ? null : String(typeof x === 'bigint' ? x.toString(16) : x).slice(0, 12));
const toBig = (x) => { try { return typeof x === 'bigint' ? x : idBig(x); } catch { return null; } };
const bitLen = (x) => (x <= 0n ? 0 : x.toString(2).length);       // distance in bits; smaller = closer
const distBits = (a, b) => { const A = toBig(a), B = toBig(b); return (A == null || B == null) ? null : bitLen(A ^ B); };

console.error(`connecting ${PEERS} peers to ${BRIDGE}`);
const peers = [];
for (let i = 0; i < PEERS; i++) peers.push(await connectPeer({ region: REGION, bridge: BRIDGE }));
console.error(`settling ${SETTLE_MS}ms for synaptomes to warm`);
await sleep(SETTLE_MS);

// synaptome density per peer (context for the caveat)
const dens = peers.map((h) => { try { return h.peer._node?.synaptome?.size ?? null; } catch { return null; } }).filter((n) => n != null);

const localMins = [];        // { stallBits, trueBits, gap }
const allTerminals = [];     // stallBits for every greedy terminal (min or not)
let targetsRun = 0, lookups = 0;
for (let ti = 0; ti < TARGETS; ti++) {
  const name = `harness/lmd-${Date.now?.() ? ti : ti}-${ti}`;               // deterministic-ish names
  const target = await deriveTopicIdBig({ region: REGION, name: `harness/localmin-dist/t${ti}` });
  if (target == null) continue;
  targetsRun++;
  for (const h of peers) {
    let terminal = false;
    try { terminal = (h.peer._greedyNextHopToward(target) == null); } catch { continue; }
    if (!terminal) continue;
    const selfBits = distBits(h.nodeId, target);
    allTerminals.push(selfBits);
    let closer = null;
    try { const arr = await h.peer.findKClosest(target, 1); closer = (Array.isArray(arr) && arr.length) ? arr[0] : null; } catch { closer = null; }
    lookups++;
    const trueBits = closer != null ? distBits(closer, target) : null;
    if (trueBits != null && trueBits < selfBits) localMins.push({ stallBits: selfBits, trueBits, gap: selfBits - trueBits });
  }
}

// ── histogram of stall distance (bitLength of xor; smaller = nearer target) ──
const pct = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(a.length * p))] : null;
const hist = (vals, edges) => {
  const b = new Array(edges.length + 1).fill(0);
  for (const v of vals) { let put = false; for (let i = 0; i < edges.length; i++) if (v <= edges[i]) { b[i]++; put = true; break; } if (!put) b[b.length - 1]++; }
  return b;
};
// The routing-relevant metric is the GAP to the true closest (how many bits/rungs
// short greedy stalled), NOT the absolute distance — absolute distance is dominated
// by keyspace sparsity (~few dozen nodes in 2^264, so the closest node to any target
// shares only ~13 leading bits). gap = extra leading bits the true closest has over
// the stall node = rungs remaining on the last-mile descent. Small gap = last-mile.
const gaps = localMins.map((m) => m.gap);
const G_EDGES = [1, 2, 3, 4, 6, 8, 12, 16];
const G_LABELS = ['1 bit', '2', '3', '4', '5-6', '7-8', '9-12', '13-16', '>16'];

console.log('\n===== LOCAL-MINIMUM: HOW FAR SHORT OF THE TRUE CLOSEST DOES GREEDY STALL? =====');
console.log(`peers=${peers.length}  synaptome sizes (min/median/max)=${dens.length ? `${Math.min(...dens)}/${pct(dens, 0.5)}/${Math.max(...dens)}` : 'n/a'}`);
console.log(`targets=${targetsRun}  greedy terminals sampled=${allTerminals.length}  network lookups=${lookups}`);
console.log(`confirmed local minima (greedy terminal but a strictly-closer node exists)=${localMins.length}/${allTerminals.length} terminals (${allTerminals.length ? Math.round(100 * localMins.length / allTerminals.length) : 0}%)`);
console.log(`context: closest-node proximity to a random target ~= ${allTerminals.length ? (264 - pct(allTerminals, 0.5)) : '?'} shared leading bits (keyspace sparsity)`);
if (gaps.length) {
  console.log(`\ngap = leading bits the TRUE closest has over the greedy stall (rungs short on the last mile):`);
  console.log(`  p50=${pct(gaps, 0.5)}  p90=${pct(gaps, 0.9)}  min=${Math.min(...gaps)}  max=${Math.max(...gaps)}`);
  const h = hist(gaps, G_EDGES);
  console.log('\n  histogram of the last-mile gap:');
  G_LABELS.forEach((lab, i) => console.log(`    ${lab.padEnd(6)} ${'#'.repeat(h[i])} ${h[i]}`));
  console.log('\n-- verdict --');
  const lastMile = gaps.filter((g) => g <= 8).length / gaps.length;
  if (lastMile >= 0.7) console.log(`LAST-MILE: ${Math.round(lastMile * 100)}% of local minima are within 8 bits of the true closest — greedy reaches the target's neighborhood and stalls on the final rungs. A denser k XOR-nearest successor set at each node (your fix; Synaptome-Maintenance kNear) is exactly the lever.`);
  else console.log(`NOT purely last-mile: ${Math.round(lastMile * 100)}% within 8 bits — some stalls are many rungs short, so mid-route escalation matters too.`);
} else {
  console.log('\nno confirmed local minima in this sample.');
}
for (const h of peers) { try { await h.close(); } catch { /* */ } }
process.exit(0);
