// harness/localmin-probe.mjs — is the SUB self-root a routing LOCAL MINIMUM,
// and is the failure in greedy or in the iterative lookup itself? (David, 2026-08-30)
//
// Design principle under test (David): routing a message to a destination must be
// independent of where it started. A "closest node to topicId" query is a global
// property — every origin should converge on ONE terminus. If it does not, the
// neuromorphic routing is origin-dependent (local minima), and THAT is the defect
// to fix at the routing layer — not with a subscribe hint.
//
// For the SAME topicId, from N independent origin peers, we read the routing's own
// primitives — no kernel change, no hint:
//   greedy   = _greedyNextHopToward(topicId)  → null means this peer is a greedy
//              TERMINAL (it would self-root); a greedy terminal that is NOT the
//              true closest is a local minimum.
//   iterative= findKClosest(topicId, 1)       → the α-parallel lookup that claims
//              to "escape greedy local minima".
// Then:
//   • do all peers' ITERATIVE termini agree?  (origin-independence of the lookup)
//   • for each greedy-terminal peer, does iterative find a STRICTLY CLOSER node?
//     (greedy local minimum, and whether the iterative escape actually fires)
//
//   BRIDGE=wss://testnet.axona.net PEERS=6 TOPIC=harness/localmin node harness/localmin-probe.mjs
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';
import { idBig } from '../vendor/axona-protocol/src/pubsub/ids.js';

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const REGION = process.env.REGION || 'eagle';
const PEERS  = Number(process.env.PEERS || 6);
const TOPIC  = process.env.TOPIC || 'harness/localmin-fixed';
const SETTLE_MS = Number(process.env.SETTLE_MS || 20000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = (x) => (x == null ? null : String(typeof x === 'bigint' ? x.toString(16) : x).slice(0, 12));
const toBig = (x) => (typeof x === 'bigint' ? x : idBig(x));

const topicBig = await deriveTopicIdBig({ region: REGION, name: TOPIC });
console.error(`topic ${TOPIC}  topicId ${S(topicBig)}  connecting ${PEERS} peers`);
const peers = [];
for (let i = 0; i < PEERS; i++) peers.push(await connectPeer({ region: REGION, bridge: BRIDGE }));
console.error('connected', peers.map((p) => S(p.nodeId)).join(','));
console.error(`settling ${SETTLE_MS}ms for tables to warm`);
await sleep(SETTLE_MS);

const dist = (idAny) => { try { return toBig(idAny) ^ topicBig; } catch { return null; } };
const rows = [];
for (const p of peers) {
  const selfBig = toBig(p.nodeId);
  const selfDist = selfBig ^ topicBig;
  let greedyNext = null, greedyTerminal = false;
  try { greedyNext = p.peer._greedyNextHopToward(topicBig); greedyTerminal = (greedyNext == null); } catch (e) { greedyNext = `ERR:${e?.message?.slice(0, 30)}`; }
  let iter = null;
  try { const arr = await p.peer.findKClosest(topicBig, 1); iter = (Array.isArray(arr) && arr.length) ? arr[0] : null; } catch (e) { iter = `ERR:${e?.message?.slice(0, 30)}`; }
  const iterDist = (iter && !String(iter).startsWith('ERR')) ? dist(iter) : null;
  rows.push({
    self: S(selfBig), selfDist,
    greedyTerminal,
    iterTerminus: S(iter), iterDist,
    iterStrictlyCloser: (iterDist != null) ? (iterDist < selfDist) : null,
  });
}

// ── report ──────────────────────────────────────────────────────────────
console.log('\n===== LOCAL-MINIMUM DETERMINATION =====');
console.log('topicId', S(topicBig), ' peers', peers.length);
for (const r of rows) {
  console.log(`  self=${r.self} greedyTerminal=${r.greedyTerminal ? 'YES(would self-root)' : 'no '} ` +
    `iterTerminus=${r.iterTerminus} iterCloser=${r.iterStrictlyCloser === null ? '?' : (r.iterStrictlyCloser ? 'YES' : 'no')}`);
}
const termini = [...new Set(rows.map((r) => r.iterTerminus).filter((x) => x && !String(x).startsWith('ERR')))];
const winner = rows.filter((r) => r.iterDist != null).sort((a, b) => (a.iterDist < b.iterDist ? -1 : 1))[0];
console.log('\n-- verdict --');
console.log(`distinct iterative termini across origins: ${termini.length}  ${JSON.stringify(termini)}`);
if (termini.length <= 1) {
  console.log('ITERATIVE LOOKUP IS ORIGIN-INDEPENDENT (all origins agree on one terminus).');
  console.log('→ the escape works; a self-root would be corrected by _verifyRoots. Local minimum, if any, is in the GREEDY path or verify timing.');
} else {
  console.log('ITERATIVE LOOKUP IS ORIGIN-DEPENDENT (origins disagree on the closest node).');
  console.log('→ findKClosest ITSELF hits local minima on this mesh. The routing result depends on where the query started — the defect is in the neuromorphic lookup, not the subscribe path.');
  console.log(`   best (strictly-closest seen): ${winner ? winner.iterTerminus : '?'} — origins that returned a worse terminus are stuck in a local minimum.`);
}
const greedyTerminals = rows.filter((r) => r.greedyTerminal);
const greedyLocalMins = greedyTerminals.filter((r) => r.iterStrictlyCloser === true);
console.log(`greedy terminals (would self-root): ${greedyTerminals.length}/${rows.length}; of those, iterative finds a strictly-closer node (greedy LOCAL MINIMUM): ${greedyLocalMins.length}`);

for (const p of peers) { try { await p.close(); } catch { /* */ } }
process.exit(0);
