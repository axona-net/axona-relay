// =====================================================================
// smoke_pubsub_leave_handoff.mjs — graceful-leave cache handoff (v4.8.4).
//
// Single-root durability: with few subscribers (< MAX_DIRECT, so no child relay
// forms), the ROOT holds the ONLY copy of a topic's history. If it just vanishes,
// the cache dies and a later since:'all' subscriber gets nothing (the restart-
// phase failure in Howard's suite). The fix: on a GRACEFUL leave the departing
// root pushes its cache to its heir (next-closest live node) FIRST, so the heir
// becomes the cache-bearing root and late joiners still replay the full history.
//
// This test proves: (1) the root is the sole cache-holder (no replica), (2) WITH
// handoff a post-departure late subscriber recovers everything, (3) WITHOUT
// handoff (abrupt loss) it recovers nothing — so the handoff is what saved it.
//
// Run: node test/smoke_pubsub_leave_handoff.mjs
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
      findKClosest: async (target, _k = 3) => {
        return [...self.nodes.entries()].filter(([, n]) => n.alive)
          .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
          .slice(0, _k);
      },
    };
    // rootReplicas: 0 — this suite isolates the graceful-leave HANDOFF mechanism; with
    // cohort replication on (default) a backup would also hold the cache, defeating the
    // "handoff is the ONLY thing that saved it" control. Replication is covered separately
    // in smoke_root_replication / smoke_kill_migration.
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, rootReplicas: 0 });
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
const cacheSize = (rec, t) => rec.am.axonRoles.get(t)?.cache.length ?? 0;

async function buildAndPublish(fab, nodes, desc, topicId, author, M, SEQ) {
  const subs = nodes.slice(0, 3);   // few subs → no child relay → root is the only cache-holder
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  fab.clock += 6_000; await fab.tickAll();
  const ids = [];
  const pub = nodes[nodes.length - 1];
  for (let k = 0; k < M; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ.n++, identity: author, ts: fab.clock });
    ids.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  return ids;
}

async function main() {
  console.log('Axona pub/sub — graceful-leave cache handoff (v4.8.4)');
  const author = await createAuthorIdentity();
  const M = 4; const SEQ = { n: 1 };

  // ── Scenario 1: graceful leave WITH handoff → history survives ───────
  {
    const fab = new Fabric();
    const nodes = [];
    for (let i = 0; i < 20; i++) { const id = await createNodeIdentity(__LOC); nodes.push(fab.addNode(BigInt('0x'+id.id))); }
    const desc = { region: 'useast', owner: null, name: 'handoff-on', write: 'open' };
    const topicId = await deriveTopicIdBig(desc);
    const ids = await buildAndPublish(fab, nodes, desc, topicId, author, M, SEQ);

    const root1 = fab.nodes.get(fab._closestAlive(topicId));
    check('root holds the full cache', cacheSize(root1, topicId) === M, `(${cacheSize(root1, topicId)}/${M})`);
    const otherHolders = nodes.filter(n => n !== root1 && cacheSize(n, topicId) > 0).length;
    check('root is the SOLE cache-holder (no replica — abrupt death would lose it)', otherHolders === 0, `(others=${otherHolders})`);

    // graceful leave: hand off, THEN depart
    await root1.am.pubsubLeaveHandoff();
    await fab.settle();
    fab.kill(root1.id);
    for (let r = 0; r < 3; r++) { fab.clock += 5_000; await fab.tickAll(); }

    const heir = fab.nodes.get(fab._closestAlive(topicId));
    check('the heir inherited the full cache via handoff', cacheSize(heir, topicId) === M, `(${cacheSize(heir, topicId)}/${M})`);

    const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
    late.am._lastSeenTsByTopic.set(topicId, 0);
    late.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 5_000; await fab.tickAll(); await fab.settle();
    const got = ids.filter(id => late.got.includes(id)).length;
    check('post-handoff late subscriber recovers ALL history', got === M, `(${got}/${M})`);
  }

  // ── Scenario 2: abrupt loss (NO handoff) → history is gone (control) ──
  {
    const fab = new Fabric();
    const nodes = [];
    for (let i = 0; i < 20; i++) { const id = await createNodeIdentity(__LOC); nodes.push(fab.addNode(BigInt('0x'+id.id))); }
    const desc = { region: 'useast', owner: null, name: 'handoff-off', write: 'open' };
    const topicId = await deriveTopicIdBig(desc);
    const ids = await buildAndPublish(fab, nodes, desc, topicId, author, M, SEQ);

    const root1 = fab.nodes.get(fab._closestAlive(topicId));
    // Exhaust the publisher's persistent publish-retry window FIRST (v4.8.6:
    // a recent publish is re-sent to the current root for up to maxTries/TTL,
    // which is a SEPARATE durability path that would otherwise re-land these
    // messages on the new root). Past that window, only the cache (handoff)
    // can save the history — which is exactly what this control isolates.
    for (let r = 0; r < 9; r++) { fab.clock += 5_000; await fab.tickAll(); }
    fab.kill(root1.id);   // abrupt — NO handoff (cache dies with it)
    for (let r = 0; r < 3; r++) { fab.clock += 5_000; await fab.tickAll(); }

    const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
    late.am._lastSeenTsByTopic.set(topicId, 0);
    late.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 5_000; await fab.tickAll(); await fab.settle();
    const got = ids.filter(id => late.got.includes(id)).length;
    check('control: WITHOUT handoff the abrupt-loss late subscriber recovers nothing', got === 0, `(${got}/${M} — proves the handoff is what saved scenario 1)`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
