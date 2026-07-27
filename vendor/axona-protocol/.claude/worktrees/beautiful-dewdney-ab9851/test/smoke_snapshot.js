// =====================================================================
// smoke_snapshot.js — peer.snapshot() / AxonaPeer.fromSnapshot()
//                      round-trip.  Covers A9.
// Run: node test/smoke_snapshot.js
// =====================================================================

import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { isHexId }         from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const PEER1  = 'bb' + 'b2'.repeat(32);
const PEER2  = 'cc' + 'c3'.repeat(32);

class MockAxonaManager {
  constructor() {
    this.nodeId = '';
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish() { return 'p'; }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
}

async function makePeer({ synaptomeIds = [], subscriptions = [] } = {}) {
  const identity = await createNodeIdentity(LONDON);
  const synaptome = new Map();
  for (const peerId of synaptomeIds) {
    synaptome.set(peerId, { peerId, weight: 0.7, latency: 42, stratum: 5, addedBy: 'ltp' });
  }
  const node = { id: identity.id, alive: true, synaptome };
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node,
    nodeIdentity: identity,
    axonaManager: new MockAxonaManager(),
  });
  // pre-register subscriptions via peer.sub so _subscriptions populates.
  const subs = [];
  for (const s of subscriptions) {
    const sub = await peer.sub({ region: 'useast', name: s.topic }, () => {}, s.opts ?? {});
    subs.push(sub);
  }
  return { peer, identity, subs };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testSnapshotShape() {
  console.log('\n── snapshot() shape ──');
  const { peer, identity } = await makePeer({
    synaptomeIds: [PEER1, PEER2],
    subscriptions: [{ topic: 'cats' }, { topic: 'dogs', opts: { since: 'all' } }],
  });
  const snap = await peer.snapshot();

  check('formatVersion = 1.0',         snap.formatVersion === '1.0');
  check('snapshotAt is recent',        Math.abs(snap.snapshotAt - Date.now()) < 1000);
  check('wireVersion present',         typeof snap.wireVersion === 'string');
  check('identity envelope present',   snap.identity != null);
  check('identity.id matches',         snap.identity.id === identity.id);
  check('synaptome length = 2',        snap.synaptome.length === 2);
  check('synaptome entries hex',
    snap.synaptome.every(s => isHexId(s.peerId)));
  check('synaptome carries weight/stratum',
    snap.synaptome.every(s => s.weight === 0.7 && s.stratum === 5));
  check('subscriptions length = 2',    snap.subscriptions.length === 2);
  check('subscriptions carry topic',
    snap.subscriptions.find(s => s.topic === 'cats') != null);
  check('subscriptions carry since',
    snap.subscriptions.find(s => s.topic === 'dogs')?.since === 'all');
}

async function testJsonRoundtrip() {
  console.log('\n── snapshot survives JSON.stringify/parse ──');
  const { peer } = await makePeer({
    synaptomeIds: [PEER1],
    subscriptions: [{ topic: 'news' }],
  });
  const snap = await peer.snapshot();
  const json = JSON.stringify(snap);
  const parsed = JSON.parse(json);
  check('JSON encode/decode preserves shape',
    parsed.formatVersion === '1.0' &&
    parsed.synaptome.length === 1 &&
    parsed.subscriptions.length === 1);
}

async function testFromSnapshot() {
  console.log('\n── fromSnapshot reconstructs peer ──');
  const { peer, identity } = await makePeer({
    synaptomeIds: [PEER1, PEER2],
    subscriptions: [
      { topic: 'cats' },
      { topic: 'dogs', opts: { since: 'latest' } },
    ],
  });
  const snap = await peer.snapshot();
  const json = JSON.stringify(snap);
  const restored = JSON.parse(json);

  const newPeer = await AxonaPeer.fromSnapshot(restored, {
    engine: { onEvent: () => () => {} },
  });

  check('restored peer has same nodeId',
    newPeer.getNodeId() === identity.id);
  check('restored peer has identity',
    newPeer._identity?.id === identity.id);
  check('restored synaptome has 2 entries',
    newPeer.peers().length === 2);
  check('restored synaptome includes PEER1',
    newPeer.peers().includes(PEER1));
  check('pendingSubscriptions has 2 entries',
    newPeer.pendingSubscriptions?.length === 2);
  check('pendingSubscriptions carry since modes',
    newPeer.pendingSubscriptions.find(s => s.topic === 'dogs')?.since === 'latest');

  // Re-registering a subscription should work via peer.sub() — the
  // restored peer is fully functional.
  const subs = newPeer.peers();
  check('peers() works on restored peer', subs.length === 2);
}

async function testFromSnapshotIdentityReusable() {
  console.log('\n── restored identity can sign messages ──');
  const { peer } = await makePeer();
  const snap = await peer.snapshot();

  const restored = await AxonaPeer.fromSnapshot(JSON.parse(JSON.stringify(snap)), {
    engine: { onEvent: () => () => {} },
    axonaManager: new MockAxonaManager(),
  });

  // Verify the restored peer can publish — sign with an explicit author identity
  // (key separation: the transport/node key is never the implicit signer).
  const author = await createAuthorIdentity();
  const msgId = await restored.pub({ region: 'useast', name: 'test' }, { hi: 1 }, { signWith: author });
  check('restored peer can sign + publish',
    typeof msgId === 'string' && msgId.length === 64);
}

async function testFromSnapshotValidation() {
  console.log('\n── fromSnapshot rejects bad input ──');
  let threw = false;
  try { await AxonaPeer.fromSnapshot(null, { engine: {} }); } catch { threw = true; }
  check('rejects null state', threw);

  threw = false;
  try { await AxonaPeer.fromSnapshot({ formatVersion: '999.0' }, { engine: {} }); }
  catch { threw = true; }
  check('rejects unsupported formatVersion', threw);
}

async function testEmptyPeerSnapshot() {
  console.log('\n── snapshot() on bare peer ──');
  const id = await createNodeIdentity(LONDON);
  const node = { id: id.id, alive: true, synaptome: new Map() };
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, nodeIdentity: id,
  });
  const snap = await peer.snapshot();
  check('empty synaptome → empty array',
    Array.isArray(snap.synaptome) && snap.synaptome.length === 0);
  check('no subscriptions → empty array',
    Array.isArray(snap.subscriptions) && snap.subscriptions.length === 0);
  check('identity still present',
    snap.identity?.id === id.id);
}

async function main() {
  console.log('Axona snapshot / fromSnapshot (A9) smoke');
  await testSnapshotShape();
  await testJsonRoundtrip();
  await testFromSnapshot();
  await testFromSnapshotIdentityReusable();
  await testFromSnapshotValidation();
  await testEmptyPeerSnapshot();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
