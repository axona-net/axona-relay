// =====================================================================
// smoke_pubsub_fundamental.mjs — Phase 1 GATE for the routing-only
// axonic-tree AxonaManager (design: Pubsub-Axon-Tree-v0.1).
//
// The fundamental case the old sendDirect path failed ([4,4,2]/5):
//   one topic, one emergent root, five subscribers — a publish must reach
//   ALL five, exactly once, every time.
//
// Plus the core invariants Phase 1 must hold:
//   · late-subscriber cache replay (since:'all')
//   · renewal does NOT re-flood (exactly-once across re-subscribe)
//   · root ingress enforces write policy (owner-only) + rejects unsigned
//   · via dead-waypoint fall-through self-heals onto a fresh root
//
// Harness: an in-memory routing fabric that delivers every routed message to
// the single live node CLOSEST (XOR) to its target — the kernel's terminus
// semantics — and drains the resulting routed-message graph deterministically.
// This is a faithful unit test of the manager core; multi-hop routing-table
// convergence is a separate concern validated in dht-sim.
//
// Run: node test/smoke_pubsub_fundamental.mjs
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

// ── routing fabric ───────────────────────────────────────────────────
class Fabric {
  constructor() {
    this.nodes = new Map();      // idBig -> { am, handlers:Map, alive, received:[] }
    this.queue = [];             // pending routed deliveries
    this.clock = Date.now();
  }
  now() { return this.clock; }

  addNode(idBig) {
    const handlers = new Map();
    const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload /*, opts */) => {
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

  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }

  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) {
      if (!n.alive) continue;
      const d = id ^ target;
      if (bestD === null || d < bestD) { bestD = d; best = id; }
    }
    return best;
  }

  async settle(maxJobs = 100000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > maxJobs) throw new Error('fabric.settle: routed-message graph did not converge');
      const job = this.queue.shift();
      const n = this.nodes.get(job.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(job.type);
      if (!h) continue;
      await h(job.payload, job.meta);    // handler may enqueue more (fanout/deliver/reroute)
    }
  }
}

// ── envelope helpers ──────────────────────────────────────────────────
let SEQ = 1;
async function signedJson(descriptor, message, author) {
  const env = await buildEnvelope({ topic: descriptor, message, seq: SEQ++, identity: author, sign: !!author });
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

// ── tests ─────────────────────────────────────────────────────────────

async function testFundamental() {
  console.log('\n── fundamental: 1 root, 5 subscribers, publish reaches all 5 (×25 configs) ──');
  const author = await createAuthorIdentity();
  let allFive = 0;
  const RUNS = 25;
  for (let run = 0; run < RUNS; run++) {
    const fab = new Fabric();
    const nodes = await makeNodes(fab, 8);                 // 8 nodes; closest-to-topic is the root
    const desc = { region: 'useast', owner: null, name: `room-${run}`, write: 'open' };
    const topicId = await deriveTopicIdBig(desc);

    // five distinct subscribers (nodes 0..4); node 7 is the publisher
    const subs = nodes.slice(0, 5);
    for (const s of subs) s.am.pubsubSubscribe(topicId);
    await fab.settle();

    const { json } = await signedJson(desc, { hello: run }, author);
    nodes[7].am.pubsubPublish(topicId, json);
    await fab.settle();

    const got = subs.filter(s => s.received.length === 1).length;
    if (got === 5) allFive++;
    else console.log(`    run ${run}: only ${got}/5 received`);
    // exactly-once: nobody got duplicates
    const dup = subs.some(s => s.received.length > 1);
    if (dup) console.log(`    run ${run}: a subscriber received a DUPLICATE`);
  }
  check(`all 5 subscribers received the publish in ${RUNS}/${RUNS} configs`, allFive === RUNS);
}

async function testLateReplay() {
  console.log('\n── late subscriber gets cached history (since:\'all\') ──');
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 6);
  const desc = { region: 'useast', owner: null, name: 'history', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // publish 3 messages with NO subscribers — the root caches them
  for (let k = 0; k < 3; k++) {
    const { json } = await signedJson(desc, { n: k }, author);
    nodes[0].am.pubsubPublish(topicId, json);
    await fab.settle();
  }
  // a late subscriber seeds since=0 (the kernel does this for since:'all')
  const late = nodes[3];
  late.am._lastSeenTsByTopic.set(topicId, 0);
  late.am.pubsubSubscribe(topicId);
  await fab.settle();
  check('late subscriber replayed all 3 cached messages', late.received.length === 3);
  check('replay is in timestamp order', late.received.every((r, i) => i === 0 || r.ts >= late.received[i - 1].ts));
}

async function testNoReflood() {
  console.log('\n── renewal does NOT re-flood (exactly-once across re-subscribe) ──');
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 6);
  const desc = { region: 'useast', owner: null, name: 'steady', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const sub = nodes[2];
  sub.am.pubsubSubscribe(topicId);
  await fab.settle();
  for (let k = 0; k < 4; k++) { const { json } = await signedJson(desc, { n: k }, author); nodes[0].am.pubsubPublish(topicId, json); await fab.settle(); }
  check('subscriber received 4 live messages', sub.received.length === 4);

  // advance past the renewal interval and tick everyone — re-subscribe fires
  fab.clock += 61_000;
  for (const n of fab.nodes.values()) await n.am.refreshTick();
  await fab.settle();
  check('no duplicates after renewal (still 4)', sub.received.length === 4);
}

async function testWritePolicy() {
  console.log('\n── root ingress: owner-only write policy + reject unsigned ──');
  const owner   = await createAuthorIdentity();
  const stranger = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 6);
  const desc = { region: 'useast', owner: owner.pubkeyHex.toLowerCase(), name: 'wall', write: 'owner' };
  const topicId = await deriveTopicIdBig(desc);
  const sub = nodes[1];
  sub.am.pubsubSubscribe(topicId);
  await fab.settle();

  // stranger publish to an owner-only topic → dropped at root
  const bad = await signedJson(desc, { evil: 1 }, stranger);
  nodes[0].am.pubsubPublish(topicId, bad.json);
  await fab.settle();
  check('non-owner publish to owner-only topic is dropped', sub.received.length === 0);

  // unsigned publish to an owner-only topic → dropped at root
  const unsigned = await signedJson(desc, { anon: 1 }, null);
  nodes[0].am.pubsubPublish(topicId, unsigned.json);
  await fab.settle();
  check('unsigned publish to owner-only topic is dropped', sub.received.length === 0);

  // owner publish → delivered
  const good = await signedJson(desc, { ok: 1 }, owner);
  nodes[0].am.pubsubPublish(topicId, good.json);
  await fab.settle();
  check('owner publish to owner-only topic is delivered', sub.received.length === 1);
}

async function testViaSelfHeal() {
  console.log('\n── via dead-waypoint fall-through self-heals onto a fresh root ──');
  const author = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 8);
  const desc = { region: 'useast', owner: null, name: 'resilient', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // The subscriber must NOT be the closest-to-topic node (else IT is the root,
  // which delivers to itself locally — no via pin, and killing "the root" would
  // kill the subscriber) — and it must not become the NEW root after root1 dies
  // either: a root never seats itself as a subscriber (self-delivery is local),
  // so the re-seat assertion below would be vacuous in that topology. Pick the
  // FARTHEST-from-topic node: it can never be the routing terminus while any
  // other node is alive. The publisher is any other non-root node.
  const root1 = fab._closestAlive(topicId);
  const byDist = nodes.slice().sort((a, b) => ((a.id ^ topicId) < (b.id ^ topicId) ? -1 : 1));
  const sub = byDist[byDist.length - 1];
  const pub = nodes.find(n => n.id !== root1 && n !== sub);
  sub.am.pubsubSubscribe(topicId);
  await fab.settle();
  // deliver one message so the subscriber records via=[root]
  const m1 = await signedJson(desc, { n: 1 }, author);
  pub.am.pubsubPublish(topicId, m1.json);
  await fab.settle();
  check('subscriber has a via pin to its root', (sub.am._upstream.get(topicId) || []).length === 1);

  // KILL the root; the subscriber renews via=[deadRoot] → falls through to the
  // topic id → a NEW closest node becomes root.
  fab.kill(root1);
  fab.clock += 61_000;
  await sub.am.refreshTick();           // re-subscribe with via=[root1]
  await fab.settle();
  const root2 = fab._closestAlive(topicId);
  check('a new (different) root formed after the old one died', root2 !== null && root2 !== root1);
  check('new root holds the re-seated subscriber', fab.nodes.get(root2)?.am.axonRoles.get(topicId)?.subscribers.size >= 1);

  // a publish after the heal reaches the subscriber
  const before = sub.received.length;
  const m2 = await signedJson(desc, { n: 2 }, author);
  pub.am.pubsubPublish(topicId, m2.json);
  await fab.settle();
  check('publish after root death reaches the re-seated subscriber', sub.received.length === before + 1);
}

async function main() {
  console.log('Axona pub/sub — Phase 1 routing-only core (fundamental gate)');
  await testFundamental();
  await testLateReplay();
  await testNoReflood();
  await testWritePolicy();
  await testViaSelfHeal();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
