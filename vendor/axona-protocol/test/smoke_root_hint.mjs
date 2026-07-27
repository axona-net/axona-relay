// =====================================================================
// smoke_root_hint.mjs — lookup-assisted root resolution (v4.3.1).
//
// Regression for the 0%-cross-peer-delivery bug: `_rootHint_` resolved the
// topic's true root via `dht.lookup(topicBig)` — but lookup() returns
// `{ path, hops, found }` (NOT an id), so `idBig(result)` threw, the catch
// swallowed it, the hint NEVER seeded, and every SUB/PUB fell back to the
// single-pass greedy walk → stranded → ~0% delivery on a real mesh.
//
// The resolver MUST be `findKClosest(topicBig, 1)[0]` (the node XOR-closest to
// the virtual topic id = the emergent root). This pins:
//   1. _rootHint_ kicks findKClosest (NOT lookup) and seeds the hint with the id
//   2. warmRootHint() resolves + seeds synchronously-awaitable (bounded)
//   3. self-closest → hint stays null (route greedily, become root as terminus)
//   4. a lookup()-style {path,...} return is NEVER treated as an id
//
// Run: node test/smoke_root_hint.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const idHex = (b) => b.toString(16).padStart(66, '0');

function mkManager({ closest, findKClosestCalls, lookupCalls }) {
  const selfId = 0x89n << 248n | 0x11n;
  const dht = {
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: () => {},
    neighbors: () => [],
    async findKClosest(target, K) { findKClosestCalls.push({ target, K }); return closest != null ? [closest] : []; },
    async lookup(target) { lookupCalls.push(target); return { path: [selfId, 0xdeadn], hops: 1, found: false }; },
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), renewMs: 60_000, dropMs: 180_000 });
  am.nodeId = selfId;
  return am;
}

// ── 1. warmRootHint resolves via findKClosest and seeds the hint ──
{
  const findKClosestCalls = [], lookupCalls = [];
  const root = 0x89n << 248n | 0xabcdn;
  const am = mkManager({ closest: root, findKClosestCalls, lookupCalls });
  const topic = 0x89n << 248n | 0xbeefn;
  await am.warmRootHint(topic, 1000);
  ok('warmRootHint called findKClosest', findKClosestCalls.length === 1 && findKClosestCalls[0].K === 1);
  ok('warmRootHint did NOT use lookup()', lookupCalls.length === 0);
  const hint = am._rootHint.get(topic);
  ok('hint seeded with the closest node id', hint && hint.via === idHex(root));
  ok('_rootHint_ now returns the warmed hint', am._rootHint_(topic) === idHex(root));
}

// ── 2. _rootHint_ backgrounds findKClosest (not lookup) + seeds on resolve ──
{
  const findKClosestCalls = [], lookupCalls = [];
  const root = 0x89n << 248n | 0x1234n;
  const am = mkManager({ closest: root, findKClosestCalls, lookupCalls });
  const topic = 0x89n << 248n | 0x5678n;
  const first = am._rootHint_(topic);          // cold → null, kicks bg resolve
  ok('_rootHint_ returns null while cold', first === null);
  await sleep(50);                              // let the bg promise settle
  ok('_rootHint_ background used findKClosest, not lookup', findKClosestCalls.length === 1 && lookupCalls.length === 0);
  ok('_rootHint_ seeded the hint after resolve', am._rootHint.get(topic)?.via === idHex(root));
}

// ── 3. self-closest → hint left null (greedy → become root as terminus) ──
{
  const findKClosestCalls = [], lookupCalls = [];
  const am = mkManager({ closest: null, findKClosestCalls, lookupCalls });   // we'll override below
  const topic = 0x89n << 248n | 0x9999n;
  // make findKClosest return SELF
  am.dht.findKClosest = async () => [am.nodeId];
  await am.warmRootHint(topic, 1000);
  ok('self-closest → hint via is null (route greedily)', am._rootHint.get(topic)?.via === null);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_root_hint: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
