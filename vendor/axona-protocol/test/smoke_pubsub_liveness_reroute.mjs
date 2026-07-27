// =====================================================================
// smoke_pubsub_liveness_reroute.mjs — root re-route after churn (v4.8.3).
//
// A subscriber re-homes only when it renews. If the root churns, the renewal
// SUB routed via the dead pin reroutes toward the bare topic and lands on the
// new (closest-alive) root, which re-pins the subscriber. The v4.8.3 change makes
// this fast for the UNATTACHED window: while a subscriber has no pin (a fresh
// subscriber, or one mid-re-home), the root-hint is re-resolved every renewal
// (not 60s-cached) and renewals stay at the fast floor — so a stranded subscriber
// chases the current closest reachable root in a few seconds instead of sitting
// on a stale hint for up to the 60s TTL.
//
// This test proves an attached subscriber re-homes to a fresh root within a few
// ticks of the old root dying — not after the 60s hint TTL.
//
// Run: node test/smoke_pubsub_liveness_reroute.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');  // region-lock: co-region test nodes with the 'useast' topics

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');
const lc = (h) => String(h).toLowerCase();

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, _k = 1) => { const c = self._closestAlive(target); return c === null ? [] : [c]; },
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

const upstreamOf = (rec, t) => { const u = rec.am._upstream.get(t); return (u && u.length) ? lc(u[0]) : null; };

async function main() {
  console.log('Axona pub/sub — liveness-based root re-route (v4.8.3)');
  const fab = new Fabric();
  const author = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'reroute', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // 30 nodes; 6 subscribe. A tree forms; the closest node is root.
  const nodes = [];
  for (let i = 0; i < 30; i++) {
    const id = await createNodeIdentity(__LOC);
    nodes.push(fab.addNode(BigInt('0x' + id.id)));
  }
  const subs = nodes.slice(0, 6);
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  fab.clock += 6_000; await fab.tickAll();   // let renewals + re-pin settle

  const root1 = fab._closestAlive(topicId);
  const root1hex = lc(idHex(root1));
  // every subscriber that isn't the root itself is pinned to a live upstream
  const leaves = subs.filter(s => s.id !== root1);
  check('subscribers attached (pinned upstream) before churn',
    leaves.every(s => upstreamOf(s, topicId) !== null), `(${leaves.map(s=>upstreamOf(s,topicId)?.slice(0,6)).join(',')})`);

  // ── KILL THE ROOT ────────────────────────────────────────────────────
  fab.kill(root1);
  const root2expected = lc(idHex(fab._closestAlive(topicId)));
  check('a different node is now closest (new root)', root2expected !== root1hex);

  // A few renewal ticks: the dead-waypoint reroute (+ miss-detection / unattached
  // fast re-resolve as backstops) re-home leaves onto the new root, bounded — NOT
  // the 60s hint TTL. The node that ITSELF becomes the new root is exempt: a root
  // has no upstream, so its leftover pin is irrelevant.
  const isNewRoot = (s) => lc(idHex(s.id)) === lc(idHex(fab._closestAlive(topicId)));
  let reHomedTick = null;
  for (let k = 1; k <= 5; k++) {
    fab.clock += 5_000;            // one fast-floor renewal interval
    await fab.tickAll();
    const movers = leaves.filter(s => s.alive && !isNewRoot(s));
    const rehomed = movers.every(s => { const u = upstreamOf(s, topicId); return u && u !== root1hex; });
    if (rehomed && reHomedTick === null) { reHomedTick = k; break; }
  }
  check('subscribers re-homed off the dead root within a few ticks',
    reHomedTick !== null, `(reHomedTick=${reHomedTick})`);
  check('no (non-root) subscriber is still pinned to the dead root',
    leaves.filter(s => s.alive && !isNewRoot(s)).every(s => upstreamOf(s, topicId) !== root1hex));

  // ── delivery proves the re-routed tree works ─────────────────────────
  const pub = nodes[29].alive ? nodes[29] : nodes.find(n => n.alive && !subs.includes(n));
  const e = await buildEnvelope({ topic: desc, message: { hi: 1 }, seq: 1, identity: author, ts: fab.clock });
  pub.am.pubsubPublish(topicId, JSON.stringify(e));
  await fab.settle();
  fab.clock += 5_000; await fab.tickAll();
  await fab.settle();
  const delivered = leaves.filter(s => s.alive && s.got.includes(e.msgId)).length;
  const liveLeaves = leaves.filter(s => s.alive).length;
  check('a publish after re-route reaches the re-homed subscribers',
    delivered === liveLeaves, `(${delivered}/${liveLeaves})`);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
