// smoke_replica_fast_promote.mjs — single-root election on root churn (v4.18.2).
//
// A topic's root replicates its full cache to its ROOT_REPLICAS nearest neighbours
// (warm backups). When the root departs, the backups do NOT each run a bespoke
// local-only "am I closest?" promotion — that SPLIT the root when two backups
// couldn't see each other (both flipped isRoot=true → disjoint roots → dropped
// pubs/kills). Instead every backup is a SUBSCRIBING child relay: it renews a SUB
// toward the topic each tick, so root churn is resolved by the SAME probe-protected
// subscribe machinery every subscriber uses — a single globally-closest terminus.
//
// This proves, in a real routing fabric: after the root is killed, EXACTLY ONE node
// holds the topic as root (no split), it is the closest surviving node, it took over
// gap-free from its prefetched cache, and a since:'all' late joiner recovers all
// history.
//
// Run: node test/smoke_replica_fast_promote.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

// Minimal routing fabric: routeMessage delivers to the closest ALIVE node as a
// terminal; findKClosest is globally-correct (as a well-connected in-mesh relay's
// iterative lookup would be). No dht.lookup → _rootHint_ uses findKClosest.
class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const self = this; const handlers = new Map();
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        // A dead node has left the routing table: a message toward its id is carried to
        // the closest LIVE node (which adopts a via-pinned subscriber / re-homes it) —
        // BUT it must never bounce back to its own sender. That self-loop is the stale-
        // beacon case: a node forwards a PUB toward the departed root, the closest-alive
        // is itself, and it re-emits forever. The real router drops that (viaHopBudget);
        // here we drop the degenerate sender===dest hop toward a known-dead target.
        const named = self.nodes.get(target);
        if (named && !named.alive && dest === idBig) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      neighbors: () => [...self.nodes.entries()].filter(([id, n]) => n.alive && id !== idBig).map(([id]) => idHex(id)),
      bridgeId: () => null,
      findKClosest: async (target, _k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive)
        .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
        .slice(0, _k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(cap = 500_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle: did not converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type);
      if (!h) continue;
      await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const roleOf = (rec, t) => rec.am.axonRoles.get(t);
const isRoot = (rec, t) => !!roleOf(rec, t)?.isRoot;
const cacheSize = (rec, t) => roleOf(rec, t)?.cache.length ?? 0;

async function main() {
  console.log('Axona pub/sub — single-root election on root churn (v4.18.2)');
  const author = await createAuthorIdentity();
  const M = 4; const SEQ = { n: 1 };

  // A relay fleet: every node host()s the topic (as the testnet backbone does), so the
  // topic-closest node is always a participant that can win the election.
  const fab = new Fabric();
  const nodes = [];
  for (let i = 0; i < 9; i++) { const id = await createNodeIdentity(__LOC); nodes.push(fab.addNode(BigInt('0x' + id.id))); }
  const desc = { region: 'useast', owner: null, name: 'churn-election', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  for (const nd of nodes) nd.am.pubsubHost(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();
  const ids = [];
  for (let k = 0; k < M; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ.n++, identity: author, ts: fab.clock });
    ids.push(e.msgId); nodes[nodes.length - 1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  // Let the tree + cohort replication settle.
  for (let r = 0; r < 3; r++) { fab.clock += 5_000; await fab.tickAll(); }

  const rootsBefore = nodes.filter(n => isRoot(n, topicId));
  const root1 = rootsBefore[0];
  check('EXACTLY ONE root formed and holds the full cache', rootsBefore.length === 1 && cacheSize(root1, topicId) === M,
    `(roots=${rootsBefore.length} cache=${cacheSize(root1, topicId)}/${M})`);
  const backups = nodes.filter(n => n !== root1 && n.am._backupTopics.has(topicId));
  check('the root replicated to warm backups (subscribing child relays)', backups.length >= 1, `(backups=${backups.length})`);
  const backupsHoldCache = backups.every(b => cacheSize(b, topicId) === M);
  check('every backup prefetched the full cache (gap-free takeover ready)', backups.length >= 1 && backupsHoldCache);

  // Kill the root. The departed root's "root=me" beacon lingers as soft state up to
  // BEACON_TTL_MS (50s) — during that window pubs still chase the dead id (a separate,
  // known post-churn convergence delay). Advance past it so the stale beacon expires,
  // then let the mesh re-elect through the subscribe path.
  fab.kill(root1.id);
  fab.clock += 55_000; await fab.tickAll();
  for (let r = 0; r < 4; r++) { fab.clock += 5_000; await fab.tickAll(); }

  // THE invariant: exactly one surviving node roots the topic (no split brain).
  const roots = nodes.filter(n => n.alive && isRoot(n, topicId));
  check('EXACTLY ONE surviving root after the old root departs (no split)', roots.length === 1,
    `(roots=${roots.length}: ${roots.map(r => idHex(r.id).slice(0, 8)).join(',')})`);
  const heir = fab.nodes.get(fab._closestAlive(topicId));
  check('the new root is the closest surviving node', roots.length === 1 && roots[0] === heir,
    `(heir=${idHex(heir.id).slice(0, 8)})`);
  check('the new root took over gap-free (full cache from the prefetched replica)', cacheSize(heir, topicId) === M,
    `(${cacheSize(heir, topicId)}/${M})`);

  // A since:'all' late joiner recovers the full history from the promoted root.
  const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
  late.am._lastSeenTsByTopic.set(topicId, 0);
  late.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 5_000; await fab.tickAll(); await fab.settle();
  const got = ids.filter(id => late.got.includes(id)).length;
  check('post-churn late subscriber recovers ALL history', got === M, `(${got}/${M})`);

  console.log(`\n${failed ? '✗' : '✓'} smoke_replica_fast_promote: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}
main().catch(e => { console.error(e); process.exit(1); });
