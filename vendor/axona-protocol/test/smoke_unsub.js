// =====================================================================
// smoke_unsub.js — peer.unsub(topic) (Phase A #1), v0.3 structured topics.
//
// Topic-keyed counterpart to peer.sub: stops all local subscriptions for
// a topic and sends the network unsubscribe once the last one goes.
// Idempotent; topic-id derivation must match sub() (structured descriptor).
//
// Run: node test/smoke_unsub.js
// =====================================================================

import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { createNodeIdentity }   from '../src/identity/index.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

// Structured open-topic descriptors (region required for an open topic).
const CATS  = { region: 'useast', name: 'cats' };
const NEWS  = { region: 'useast', name: 'news' };
const ALPHA = { region: 'useast', name: 'alpha' };
const BETA  = { region: 'useast', name: 'beta' };

class MockAxonaManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.subscribed   = [];     // topicId BigInts
    this.unsubscribed = [];     // topicId BigInts
    this._deliveryCallback = null;
    this._lastSeenTsByTopic = new Map();
  }
  pubsubSubscribe(topicId)   { this.subscribed.push(topicId); }
  pubsubUnsubscribe(topicId) { this.unsubscribed.push(topicId); }
  pubsubPublish() { return 'm-1'; }
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
}

async function mkPeer() {
  const id = await createNodeIdentity(LONDON);
  const am = new MockAxonaManager(id.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + id.id), alive: true }, axonaManager: am, nodeIdentity: id,
  });
  return { peer, am, id };
}

async function testBasicUnsub() {
  console.log('\n── unsub stops all local subs for a topic + network leave ──');
  const { peer, am } = await mkPeer();
  const a = await peer.sub(CATS, () => {});
  const b = await peer.sub(CATS, () => {});   // 2nd handler, same topic
  check('two subscriptions registered', am.subscribed.length === 2);

  const r = await peer.unsub(CATS);
  check('unsub reports removed: 2',  r.removed === 2 && r.ok === true);
  check('both subscriptions stopped', a.stopped === true && b.stopped === true);
  check('network unsubscribe sent ONCE (set emptied)', am.unsubscribed.length === 1);
}

async function testIdempotent() {
  console.log('\n── unsub is idempotent ──');
  const { peer, am } = await mkPeer();
  await peer.sub(NEWS, () => {});
  await peer.unsub(NEWS);
  const again = await peer.unsub(NEWS);
  check('second unsub → removed: 0', again.removed === 0 && again.ok === true);
  check('no extra network unsubscribe', am.unsubscribed.length === 1);
  const never = await peer.unsub({ region: 'useast', name: 'never-subscribed' });
  check('unsub of unknown topic → removed: 0', never.removed === 0);
}

async function testTopicIsolation() {
  console.log('\n── unsub only affects the named topic ──');
  const { peer, am } = await mkPeer();
  const a = await peer.sub(ALPHA, () => {});
  const b = await peer.sub(BETA,  () => {});
  await peer.unsub(ALPHA);
  check('alpha stopped', a.stopped === true);
  check('beta still active', b.stopped === false);
  check('exactly one topic left the network', am.unsubscribed.length === 1);
}

async function testTopicIdParity() {
  console.log('\n── unsub topic-id derivation matches sub (structured descriptor) ──');
  const { peer } = await mkPeer();
  // Subscribe in one region; unsub from a DIFFERENT region derives a
  // different topicId → no match (region is part of the topic id).
  const sub = await peer.sub({ region: 'useast', name: 'room' }, () => {});
  const wrong = await peer.unsub({ region: 'iberia', name: 'room' });
  check('different-region unsub does NOT match the sub', wrong.removed === 0);
  check('original sub still active', sub.stopped === false);
  // unsub with the SAME descriptor matches.
  const right = await peer.unsub({ region: 'useast', name: 'room' });
  check('same-descriptor unsub matches', right.removed === 1 && sub.stopped === true);
}

async function testValidation() {
  console.log('\n── unsub input validation ──');
  const { peer } = await mkPeer();
  let err = null;
  try { await peer.unsub({ region: 'useast', name: '' }); } catch (e) { err = e; }
  check('empty topic name → PublishError(PUBLISH_INVALID_TOPIC)',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_INVALID_TOPIC);
}

async function main() {
  console.log('Axona peer.unsub (Phase A #1) smoke — v0.3 structured topics');
  await testBasicUnsub();
  await testIdempotent();
  await testTopicIsolation();
  await testTopicIdParity();
  await testValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
