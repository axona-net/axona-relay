// =====================================================================
// smoke_since_latest.mjs — since:'latest' delivers the current value even
// when it was published by ANOTHER client BEFORE this one subscribed.
//
// Regression for the v4.2.3 bug: 'latest' seeded the replay floor to
// `now - 1000`, a 1-second cache window. A message last published more than
// ~1s before subscribe sat below the floor and was never replayed — so a
// late `since:'latest'` subscriber got no callback. (since:'all' and a publish
// AFTER subscribe both worked, masking it.)
//
// Fix (v4.3.0): a `latest` flag on the SUB makes the root replay its single
// newest cache entry regardless of age, then live-tail.
//
//   1. publish (old) → THEN subscribe latest → delivered (the bug case)
//   2. control: same, but since=now floor (no latest) → NOT delivered
//   3. control: subscribe latest, THEN publish → delivered (live), once
//   4. exactly-once: latest replay + a racing live publish don't double-deliver
//
// Harness: the same in-memory routing Fabric as smoke_pubsub_fundamental —
// drives the SHIPPED AxonaManager over a deterministic closest-terminus fabric.
//
// Run: node test/smoke_since_latest.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');  // region-lock: co-region test nodes with the 'useast' topics

let passed = 0, failed = 0;
const check = (label, cond) => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

class Fabric {
  // clock starts at wall time: buildEnvelope stamps ts via Date.now(), and the
  // root's freshness check compares against this clock — they must share a domain.
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  now() { return this.clock; }
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
    const rec = { id: idBig, am, handlers, alive: true, received: [] };
    am.onPubsubDelivery((topicId, json, msgId, ts) => rec.received.push({ topicId, json, msgId, ts }));
    this.nodes.set(idBig, rec);
    return rec;
  }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(maxJobs = 100000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > maxJobs) throw new Error('settle: did not converge');
      const job = this.queue.shift();
      const n = this.nodes.get(job.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(job.type);
      if (!h) continue;
      await h(job.payload, job.meta);
    }
  }
}

let SEQ = 1;
async function signedJson(descriptor, message, author) {
  const env = await buildEnvelope({ topic: descriptor, message, seq: SEQ++, identity: author, sign: true });
  return { json: JSON.stringify(env), msgId: env.msgId };
}
async function makeNodes(fab, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity(__LOC);
    out.push(fab.addNode(BigInt('0x' + id.id)));
  }
  return out;
}

// Subscribe `node` to `topicId` emulating peer.sub's since-mode seeding:
//  'latest' → floor = now, replayLatest:true ; undefined → floor = now (tail only)
function subscribe(node, topicId, fab, since) {
  node.am._lastSeenTsByTopic.set(topicId, fab.clock);     // both modes anchor the floor at "now"
  node.am.pubsubSubscribe(topicId, { replayLatest: since === 'latest' });
}

async function testLatestAfterForeignPublish() {
  console.log("\n── publish (old) by client A, THEN B subscribes since:'latest' → delivered ──");
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 8);
  const desc = { region: 'useast', owner: null, name: 'status', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // pick a publisher and a (different) subscriber that is NOT the root, so the
  // value lives at a remote root and must be replayed over the wire.
  const root = fab._closestAlive(topicId);
  const pub  = nodes.find(n => n.id !== root);
  const subB = nodes.find(n => n.id !== root && n !== pub);

  // A publishes ONE message; the root caches it.
  const { json, msgId } = await signedJson(desc, { v: 'current' }, author);
  pub.am.pubsubPublish(topicId, json);
  await fab.settle();

  // time passes well beyond the old 1-second window
  fab.clock += 60_000;

  // B subscribes since:'latest' AFTER the publish
  subscribe(subB, topicId, fab, 'latest');
  await fab.settle();

  check('B received the current value published before it subscribed', subB.received.length === 1);
  check('B got the right message', subB.received[0]?.msgId === msgId);
}

async function testNoLatestMissesOldPublish() {
  console.log('\n── control: same, but plain live-tail (no latest) → NOT delivered ──');
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 8);
  const desc = { region: 'useast', owner: null, name: 'status2', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const root = fab._closestAlive(topicId);
  const pub  = nodes.find(n => n.id !== root);
  const subB = nodes.find(n => n.id !== root && n !== pub);

  const { json } = await signedJson(desc, { v: 'old' }, author);
  pub.am.pubsubPublish(topicId, json);
  await fab.settle();
  fab.clock += 60_000;

  subscribe(subB, topicId, fab, undefined);     // live tail only
  await fab.settle();
  check('live-tail subscriber does NOT get the pre-subscribe message', subB.received.length === 0);
}

async function testLatestThenLivePublishOnce() {
  console.log('\n── subscribe latest, THEN A publishes → delivered live, exactly once ──');
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 8);
  const desc = { region: 'useast', owner: null, name: 'status3', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const root = fab._closestAlive(topicId);
  const pub  = nodes.find(n => n.id !== root);
  const subB = nodes.find(n => n.id !== root && n !== pub);

  subscribe(subB, topicId, fab, 'latest');       // no cache yet → nothing to replay
  await fab.settle();
  check('latest on an empty topic delivers nothing yet', subB.received.length === 0);

  fab.clock += 5_000;
  const { json, msgId } = await signedJson(desc, { v: 'fresh' }, author);
  pub.am.pubsubPublish(topicId, json);
  await fab.settle();
  check('live publish after a latest-subscribe is delivered', subB.received.length === 1);
  check('delivered exactly once (no latest-replay duplicate)', subB.received.filter(r => r.msgId === msgId).length === 1);
}

async function main() {
  console.log("Axona since:'latest' — current-value delivery (v4.3.0 regression)");
  await testLatestAfterForeignPublish();
  await testNoLatestMissesOldPublish();
  await testLatestThenLivePublishOnce();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
