// smoke_root_reconcile_reach.mjs — HOW FAR CAN TWO ROOTS SEE EACH OTHER?
//
// Pins the reconciliation reach, because it is a load-bearing constant that no
// other test states. Two roots for one topic MERGE only while they are within
// each other's kept-replica set; beyond it they never reconcile, no matter how
// long the repair plane runs.
//
// The reach is `rootReplicas` — TWO — not the candidate window:
//   repairPlane: findKClosest(t, (rootReplicas + 1) * 2)  → 6 candidates FETCHED
//                .slice(0, rootReplicas)                   → 2 replicas KEPT
//   syncEngine:  UNION_AT_ROOT fires only on "a REPLICATE arriving at a node
//                that itself holds the ROOT claim" — there is NO background
//                root-to-root gossip.
// Reconciliation therefore requires one root to pick the other as one of its 2
// replicas, which can only happen while N ≤ rootReplicas + 1.
//
// WHY THIS IS FENCED (David's model, demonstrated 2026-07-25): a node that hosts
// a topic it is not near creates a second root. At tiny N that is invisible —
// both roots sit in each other's replica set and UNION merges them, so the
// system looks correct. GROWTH ALONE converts it into a permanent split: no
// churn, no code change, no version change, and nothing announces it. That is
// what happened to #axona.bot, whose MCP peer hand-hosted its own channel for
// weeks while the mesh grew past the window.
//
// The failure shape is worse than divergence: the distant root ends up holding
// NOTHING (cache 2/0, not 1/1). It still has a role, so routing hands it traffic
// and it answers as an authority over an empty topic — a read that resolves
// there sees no history at all.
//
// If ROOT_REPLICAS changes, or a real root-to-root anti-entropy is added, this
// test SHOULD fail. That is the point: update the expectation deliberately.
//
// FULL=1 runs the wide sweep (3..40) used to locate the threshold; the default
// fences either side of it so the suite stays fast.
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { ROOT_REPLICAS } from '../src/pubsub/constants.js';

const __LOC = regionCenter('useast');
const idHex = (b) => b.toString(16).padStart(66, '0');

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      // Answer K-closest truthfully from the fabric so replica selection behaves
      // exactly as it does on a real mesh.
      findKClosest: (target, K) => [...self.nodes.keys()]
        .filter((id) => self.nodes.get(id).alive)
        .sort((a, b) => ((a ^ target) < (b ^ target) ? -1 : 1))
        .slice(0, K).map(idHex),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true };
    this.nodes.set(idBig, rec);
    return rec;
  }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) {
      if (!n.alive) continue;
      const d = id ^ target;
      if (bestD === null || d < bestD) { bestD = d; best = id; }
    }
    return best;
  }
  async settle(maxJobs = 200000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > maxJobs) throw new Error('settle: did not converge');
      const job = this.queue.shift();
      const n = this.nodes.get(job.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(job.type);
      if (h) await h(job.payload, job.meta);
    }
  }
  async tickAll(rounds) {   // give reconciliation every chance it has
    for (let r = 0; r < rounds; r++) {
      this.clock += 30_000;
      for (const n of this.nodes.values()) {
        if (!n.alive) continue;
        try { await n.am.refreshTick(); } catch { /* best-effort, as in production */ }
      }
      await this.settle();
    }
  }
}

let SEQ = 1;
async function signed(desc, message, author) {
  const env = await buildEnvelope({ topic: desc, message, seq: SEQ++, identity: author, sign: !!author });
  return { json: JSON.stringify(env), msgId: env.msgId };
}

/** Build N nodes, mint two roots (closest + farthest), give each its own message, reconcile. */
async function trial(N, run, author) {
  const fab = new Fabric();
  const nodes = [];
  for (let i = 0; i < N; i++) {
    const id = await createNodeIdentity(__LOC);
    nodes.push(fab.addNode(BigInt('0x' + id.id)));
  }
  const desc = { region: 'useast', owner: null, name: `reach-${N}-${run}`, write: 'open' };
  const T = await deriveTopicIdBig(desc);

  const byDist = [...nodes].sort((a, b) => ((a.id ^ T) < (b.id ^ T) ? -1 : 1));
  const trueRoot = byDist[0];                      // the keyspace's rightful root
  const selfHoster = byDist[byDist.length - 1];    // the self-hosting node, farthest away

  trueRoot.am.pubsubHost(T);
  // Manager layer ON PURPOSE: peer.host() refuses this since 4.39.1 (the address
  // rule). We are reproducing the pre-guard condition that ran on production.
  selfHoster.am.pubsubHost(T);
  await fab.settle();

  const a = await signed(desc, { from: 'true-root' }, author);
  const b = await signed(desc, { from: 'self-hoster' }, author);
  trueRoot.am.pubsubPublish(T, a.json);
  selfHoster.am.pubsubPublish(T, b.json);
  await fab.settle();
  await fab.tickAll(5);

  const held = (n) => { const r = n.am.axonRoles?.get(T); return r ? r.cacheIds : new Set(); };
  const A = held(trueRoot), B = held(selfHoster);
  return {
    merged: A.has(a.msgId) && A.has(b.msgId) && B.has(a.msgId) && B.has(b.msgId),
    sizeA: A.size, sizeB: B.size,
  };
}

const FULL = process.env.FULL === '1';
const SIZES = FULL ? [3, 4, 6, 8, 12, 20, 40] : [3, 4, 8];
const REPS = FULL ? 5 : 3;
const author = await createAuthorIdentity();

console.log(`\n── root reconciliation reach (ROOT_REPLICAS = ${ROOT_REPLICAS}) ──`);
console.log(`   two roots merge only while each is inside the other's ${ROOT_REPLICAS} kept replicas\n`);

const results = new Map();
for (const N of SIZES) {
  let merged = 0; const shapes = [];
  for (let run = 0; run < REPS; run++) {
    const r = await trial(N, run, author);
    if (r.merged) merged++;
    shapes.push(`${r.sizeA}/${r.sizeB}`);
  }
  results.set(N, merged);
  console.log(`   N=${String(N).padStart(2)}  merged ${merged}/${REPS}   trueRoot/selfHoster cache: ${shapes.join(' ')}`);
}

console.log('');
// Reconciliation reaches only rootReplicas peers, so the largest network in which
// two roots can still see each other is rootReplicas + 1.
const MAX_MERGEABLE_N = ROOT_REPLICAS + 1;
for (const [N, merged] of results) {
  if (N <= MAX_MERGEABLE_N) check(`N=${N} (≤ rootReplicas+1): two roots MERGE — union is in reach`, merged === REPS);
  else                      check(`N=${N} (> rootReplicas+1): two roots NEVER merge — union is out of reach`, merged === 0);
}
const beyond = [...results.keys()].filter((n) => n > MAX_MERGEABLE_N);
check('the split is permanent — repeated repair ticks do not heal it', beyond.every((n) => results.get(n) === 0));
check('reach is rootReplicas, NOT the (rootReplicas+1)*2 candidate window',
  MAX_MERGEABLE_N === 3 && results.get(4) === 0);

console.log(`\n  ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
