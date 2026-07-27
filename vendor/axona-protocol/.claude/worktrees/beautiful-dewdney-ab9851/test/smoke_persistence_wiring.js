// =====================================================================
// smoke_persistence_wiring.js — verify PersistenceAdapter is wired into
//                                the AxonaPeer lifecycle (P4):
//                                  - identity load on start
//                                  - subscriptions persisted on
//                                    sub/stop and flushed on leave
//                                  - synaptome (best-effort) persisted
//                                  - wireVersion stamp on leave
//                                  - round-trip: kill + rebuild peer
//                                    against same adapter
// Run: node test/smoke_persistence_wiring.js
// =====================================================================

import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { InMemoryPersistence }  from '../src/persistence/interface.js';
import { createNodeIdentity,
         createAuthorIdentity,
         dumpIdentity }         from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

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

// ── Tests ────────────────────────────────────────────────────────────

async function testIdentityLoadOnStart() {
  console.log('\n── start() loads identity from persist ──');
  const persist = new InMemoryPersistence();
  const id = await createNodeIdentity(LONDON);
  await persist.save('identity', await dumpIdentity(id));

  // Construct WITHOUT identity — should be loaded from persist.
  const node = { id: id.id, alive: true, synaptome: new Map() };
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, persist,
    axonaManager: new MockAxonaManager(),
  });
  check('no identity before start',   peer._identity === null);

  await peer.start();
  check('identity loaded from persist after start',
    peer._identity?.id === id.id);
  check('loaded identity can sign',
    peer._identity?.privateKey != null);

  await peer.leave({ drain: false, notify: false });
  await persist.close();
}

async function testIdentityFromConstructorWins() {
  console.log('\n── constructor identity takes precedence over persist ──');
  const persist = new InMemoryPersistence();
  const stored = await createNodeIdentity(LONDON);
  await persist.save('identity', await dumpIdentity(stored));

  const ctor = await createNodeIdentity({ lat: 35.6762, lng: 139.6503 });
  const node = { id: ctor.id, alive: true, synaptome: new Map() };
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node, nodeIdentity: ctor, persist,
    axonaManager: new MockAxonaManager(),
  });
  await peer.start();
  check('constructor identity preserved',
    peer._identity?.id === ctor.id);

  await peer.leave({ drain: false, notify: false });
}

async function testSubscriptionsPersisted() {
  console.log('\n── subscriptions persisted on sub() + leave() flushes ──');
  const persist = new InMemoryPersistence();
  const id = await createNodeIdentity(LONDON);
  const am = new MockAxonaManager();

  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true, synaptome: new Map() },
    nodeIdentity: id, persist, axonaManager: am,
  });
  // Tighten the debounce so the test doesn't wait forever.
  peer._persistFlushMs = 50;

  await peer.start();
  await peer.sub({ region: 'useast', name: 'cats' }, () => {});
  await peer.sub({ region: 'useast', name: 'dogs' }, () => {}, { since: 'all' });

  // Force the debounce to fire.
  await new Promise(r => setTimeout(r, 100));

  const stored = await persist.load('subscriptions');
  check('subscriptions written to persist',
    Array.isArray(stored) && stored.length === 2);
  check('subscription includes cats',
    stored.find(s => s.topic === 'cats') != null);
  check('subscription preserves since',
    stored.find(s => s.topic === 'dogs')?.since === 'all');

  await peer.leave({ drain: false, notify: false });

  // After leave, identity + wireVersion are written.
  const idEnv = await persist.load('identity');
  check('identity flushed on leave', idEnv?.id === id.id);
  const wire = await persist.load('wireVersion');
  check('wireVersion flushed on leave', typeof wire === 'string');

  await persist.close();
}

async function testSubscriptionStopPersisted() {
  console.log('\n── sub.stop() flushes subscriptions ──');
  const persist = new InMemoryPersistence();
  const id = await createNodeIdentity(LONDON);
  const am = new MockAxonaManager();

  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true, synaptome: new Map() },
    nodeIdentity: id, persist, axonaManager: am,
  });
  peer._persistFlushMs = 50;

  await peer.start();
  const subA = await peer.sub({ region: 'useast', name: 'cats' }, () => {});
  const subB = await peer.sub({ region: 'useast', name: 'dogs' }, () => {});
  await new Promise(r => setTimeout(r, 100));

  await subA.stop();
  await new Promise(r => setTimeout(r, 100));

  const stored = await persist.load('subscriptions');
  check('after subA.stop: only dogs remains in persist',
    stored.length === 1 && stored[0].topic === 'dogs');

  await peer.leave({ drain: false, notify: false });
  await persist.close();
}

async function testRoundTrip() {
  console.log('\n── kill peer + rebuild against same persist ──');
  const persist = new InMemoryPersistence();
  const id1 = await createNodeIdentity(LONDON);
  const am1 = new MockAxonaManager();

  const peer1 = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id1.id, alive: true, synaptome: new Map() },
    nodeIdentity: id1, persist, axonaManager: am1,
  });
  peer1._persistFlushMs = 50;

  await peer1.start();
  await peer1.sub({ region: 'useast', name: 'cats' }, () => {});
  await peer1.sub({ region: 'useast', name: 'news' }, () => {}, { since: 'latest' });
  await new Promise(r => setTimeout(r, 100));
  await peer1.leave({ drain: false, notify: false });

  // Rebuild — no constructor identity, let persist supply it.
  const peer2 = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id1.id, alive: true, synaptome: new Map() },
    persist, axonaManager: new MockAxonaManager(),
  });
  await peer2.start();

  check('rebuilt peer has same identity',
    peer2._identity?.id === id1.id);
  check('rebuilt peer has pendingSubscriptions',
    Array.isArray(peer2.pendingSubscriptions) &&
    peer2.pendingSubscriptions.length === 2);
  check('pending sub cats present',
    peer2.pendingSubscriptions.find(s => s.topic === 'cats') != null);
  check('pending sub news preserves since',
    peer2.pendingSubscriptions.find(s => s.topic === 'news')?.since === 'latest');

  // Synaptome wasn't populated this round (no peer-joined events fired),
  // so it stays empty.  Verify the rebuilt peer can publish — sign with an explicit
  // author identity (key separation: peer.pub never signs with the transport key).
  const author = await createAuthorIdentity();
  const msgId = await peer2.pub({ region: 'useast', name: 'test' }, { hi: 1 }, { signWith: author });
  check('rebuilt peer can sign + publish',
    typeof msgId === 'string' && msgId.length === 64);

  await peer2.leave({ drain: false, notify: false });
  await persist.close();
}

async function testNoPersistNoLoad() {
  console.log('\n── peer without persist still works ──');
  const id = await createNodeIdentity(LONDON);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true, synaptome: new Map() },
    nodeIdentity: id, axonaManager: new MockAxonaManager(),
    /* no persist */
  });
  await peer.start();
  await peer.sub({ region: 'useast', name: 'cats' }, () => {});
  check('peer started without persist', peer._started === true);
  await peer.leave({ drain: false, notify: false });
}

async function testCorruptedIdentityIgnored() {
  console.log('\n── corrupted identity in persist does not crash start ──');
  const persist = new InMemoryPersistence();
  await persist.save('identity', { id: 'corrupted' });   // missing fields

  const id = await createNodeIdentity(LONDON);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true, synaptome: new Map() },
    /* no constructor identity → persist load attempted */
    persist, axonaManager: new MockAxonaManager(),
  });

  await peer.start();
  // Identity remained null since persist value was corrupt.
  check('start does not throw on corrupt identity',
    peer._started === true);
  check('identity remains null after failed load',
    peer._identity === null);

  await peer.leave({ drain: false, notify: false });
  await persist.close();
}

async function main() {
  console.log('Axona persistence wiring (P4) smoke');
  await testIdentityLoadOnStart();
  await testIdentityFromConstructorWins();
  await testSubscriptionsPersisted();
  await testSubscriptionStopPersisted();
  await testRoundTrip();
  await testNoPersistNoLoad();
  await testCorruptedIdentityIgnored();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
