// =====================================================================
// smoke_pubsub_durability.mjs — Phase 3 GATE: stamped-replay-up.
//
// Abrupt root death must not erase the topic's recent history. A child relay
// holds the feed (cache replicated down the tree); when it reattaches to the
// fresh (empty) root, the root pulls its stamped cache UP and adopts it —
// keeping the stamps, advancing lastTs, propagating it down. A brand-new late
// subscriber that joins AFTER the death must still get the full pre-death
// history, and a post-recovery publish must stamp monotonically above it.
//
// Without the fix the new root is empty → the late subscriber would get 0.
//
// Run: node test/smoke_pubsub_durability.mjs
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
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId, ts) => rec.got.push({ msgId, ts }));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(cap = 2_000_000) {
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

async function makeNodes(fab, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity(__LOC);
    out.push(fab.addNode(BigInt('0x' + id.id)));
  }
  return out;
}
const cacheSize = (rec, topicBig) => (rec.am.axonRoles.get(topicBig)?.cache.length ?? 0);

async function main() {
  console.log('Axona pub/sub — Phase 3 durability (stamped-replay-up; kill the root)');
  const author = await createAuthorIdentity();
  let SEQ = 1;

  // ── backlog survives GC: messages published with NO subscribers must persist
  //    through refreshTick (the root holds cache for its TTL, not torn down the
  //    instant subscribers hit 0) so a LATE joiner recovers them. Regression for
  //    the live-soak finding (backlog/gap recovered 0%). ──
  {
    const fab = new Fabric();
    const nodes = await makeNodes(fab, 30);
    const desc = { region: 'useast', owner: null, name: 'backlog-gc', write: 'open' };
    const topicId = await deriveTopicIdBig(desc);
    const pub = nodes[29];
    const ids = [];
    for (let k = 0; k < 4; k++) {
      const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ++, identity: author, ts: fab.clock });
      ids.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    }
    const root = fab.nodes.get(fab._closestAlive(topicId));
    check('root cached the backlog with zero subscribers', cacheSize(root, topicId) === 4);
    // run the refresh sweep (the teardown) several times — must NOT drop the cache
    for (let r = 0; r < 3; r++) { fab.clock += 11_000; await fab.tickAll(); }
    check('backlog survives refreshTick GC (root role + cache kept)', cacheSize(root, topicId) === 4, `(${cacheSize(root, topicId)}/4)`);
    const late = nodes[10];
    late.am._lastSeenTsByTopic.set(topicId, 0);
    late.am.pubsubSubscribe(topicId);
    await fab.settle();
    const got = ids.filter(id => late.got.some(g => g.msgId === id)).length;
    check('late joiner recovers the full backlog after GC sweeps', got === 4, `(${got}/4)`);

    // Regression for the LIVE since:'all' backlog-0% bug: exercise the REAL
    // reset→subscribe path AxonaPeer._applySince('all') drives, instead of
    // manually seeding ts=0. pubsubResetTopicConsumption must leave the
    // since-floor at 0 (not delete it) — a deleted entry makes pubsubSubscribe
    // fall back to since=now(), so the root replays nothing.
    const late2 = nodes[11];
    late2.am.pubsubResetTopicConsumption(topicId);          // what _applySince('all') calls
    check('reset leaves since-floor at 0 (not now) so since:all replays all',
      late2.am._lastSeenTsByTopic.get(topicId) === 0);
    late2.am.pubsubSubscribe(topicId);
    check('subscribe after reset carries since=0 on the wire',
      late2.am.mySubscriptions.get(topicId)?.since === 0,
      `(since=${late2.am.mySubscriptions.get(topicId)?.since})`);
    await fab.settle();
    const got2 = ids.filter(id => late2.got.some(g => g.msgId === id)).length;
    check('since:all late joiner (real reset path) recovers full backlog', got2 === 4, `(${got2}/4)`);
  }

  const N = 60, S = 50, M = 8;
  const fab = new Fabric();
  const nodes = await makeNodes(fab, N);
  const desc = { region: 'useast', owner: null, name: 'durable', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // The topic's root is fixed by XOR distance the moment the ids exist — and it
  // is the node this test KILLS. The publisher and the two late joiners must be
  // drawn from nodes that survive that kill: with random ids, nodes[51] IS the
  // root ~1/60 of the time, and a dead late joiner can send (the fabric doesn't
  // gate outbound) but never receive — the post-recovery checks fail for reasons
  // that have nothing to do with durability.
  const root1 = fab._closestAlive(topicId);
  const fresh = nodes.slice(S).filter(n => n.id !== root1);  // non-subscribers that outlive the root
  const pub = fresh[fresh.length - 1];

  // S subscribers → a real tree with child relays that replicate the cache.
  for (const s of nodes.slice(0, S)) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  fab.clock += 61_000; await fab.tickAll();

  // publish M messages
  const ids = [];
  for (let k = 0; k < M; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ++, identity: author, ts: fab.clock });
    ids.push(e.msgId);
    pub.am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
  }

  // a child relay (non-root holder of the cache) exists and holds all M
  let childRelay = null;
  for (const n of nodes.slice(0, S)) {
    const role = n.am.axonRoles.get(topicId);
    if (role && !role.isRoot && role.subscribers.size > 0) { childRelay = n; break; }
  }
  check('a child relay formed (tree replicated the cache)', childRelay !== null);
  check('the child relay holds the full feed', childRelay && cacheSize(childRelay, topicId) === M, `(${childRelay && cacheSize(childRelay, topicId)}/${M})`);

  // pre-death sanity: a late subscriber on the live root gets all M
  const lateA = fresh[0];
  lateA.am._lastSeenTsByTopic.set(topicId, 0);
  lateA.am.pubsubSubscribe(topicId);
  await fab.settle();
  check('pre-death late subscriber gets all M from the live root', lateA.got.length === M, `(${lateA.got.length}/${M})`);

  // ── KILL THE ROOT abruptly ───────────────────────────────────────────
  fab.kill(root1);
  for (let r = 0; r < 5; r++) { fab.clock += 61_000; await fab.tickAll(); }
  const root2 = fab._closestAlive(topicId);
  check('a new (different) root formed after the old one died', root2 !== null && root2 !== root1);

  const root2rec = fab.nodes.get(root2);
  check('the new root recovered the full history via stamped-replay-up',
    cacheSize(root2rec, topicId) === M, `(new-root cache ${cacheSize(root2rec, topicId)}/${M})`);

  // ── the real test: a BRAND-NEW late subscriber AFTER the death ────────
  const lateB = fresh[1];
  lateB.am._lastSeenTsByTopic.set(topicId, 0);
  lateB.am.pubsubSubscribe(topicId);
  await fab.settle();
  const gotIds = new Set(lateB.got.map(g => g.msgId));
  const recovered = ids.filter(id => gotIds.has(id)).length;
  check('post-death late subscriber recovers ALL pre-death history', recovered === M, `(${recovered}/${M}) — 0 would mean history was lost`);
  check('recovered history is delivered in monotonic timestamp order',
    lateB.got.every((g, i) => i === 0 || g.ts >= lateB.got[i - 1].ts));

  // ── monotonic continuation: a post-recovery publish stamps above it ───
  const maxRecoveredTs = Math.max(...lateB.got.map(g => g.ts));
  const e2 = await buildEnvelope({ topic: desc, message: { k: 'post' }, seq: SEQ++, identity: author, ts: fab.clock });
  pub.am.pubsubPublish(topicId, JSON.stringify(e2));
  await fab.settle();
  const postEntry = lateB.got.find(g => g.msgId === e2.msgId);
  check('post-recovery publish reaches the late subscriber', !!postEntry);
  check('post-recovery publish stamps strictly above the recovered history',
    postEntry && postEntry.ts > maxRecoveredTs, postEntry ? `(${postEntry.ts} > ${maxRecoveredTs})` : '');

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
