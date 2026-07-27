// =====================================================================
// smoke_join_leave.js — AxonaPeer.join() / .leave() over a SimNetwork.
// Covers A7 (join sponsor path) + A8 (leave with drain/notify).
// Run: node test/smoke_join_leave.js
// =====================================================================

import { AxonaPeer }                  from '../src/dht/AxonaPeer.js';
import { SimNetwork, simTransport }   from '../src/transport/sim/index.js';
import { createNodeIdentity }         from '../src/identity/index.js';
import { TransportError }             from '../src/errors.js';
import { fromHex }                    from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const TOKYO  = { lat: 35.6762, lng: 139.6503 };

// ── Helpers ──────────────────────────────────────────────────────────

async function makePeer(network, region) {
  const id = await createNodeIdentity(region);
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  const node = { id: id.id, alive: true, synaptome: new Map() };
  // listeners + emit helper so peer.onPeerJoin/Leave actually fire
  const engineListeners = new Set();
  const engine = {
    onEvent: (cb) => { engineListeners.add(cb); return () => engineListeners.delete(cb); },
    emit: (ev) => { for (const cb of engineListeners) cb(ev); },
    addSynapse: (n, peerId, { addedBy }) => {
      n.synaptome.set(peerId, { peerId, addedBy });
      // Mirror the engine's event emission so peer.onPeerJoin fires.
      for (const cb of engineListeners) {
        cb({ type: 'peer-joined', nodeId: n.id, peerId, addedBy });
      }
    },
  };
  // Mirror production wiring (bridge/peer set node.transport = transport)
  // so the kernel installs its routing/notification handlers — including
  // the graceful-departure `peer-leaving` fast path — at construction.
  node.transport = transport;
  const peer = new AxonaPeer({ engine, node, nodeIdentity: id, transport });
  return { peer, id, transport, engine };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testStandaloneJoin() {
  console.log('\n── join() without sponsor (standalone) ──');
  const net = new SimNetwork();
  const { peer, transport } = await makePeer(net, LONDON);

  await peer.join();
  check('peer is started after join()', peer._started === true);
  check('transport started',            transport._started === true);
  check('peers() = [] after standalone join', peer.peers().length === 0);

  await peer.leave();
  check('leave() resolves cleanly',     peer._started === false);
}

async function testSponsorJoin() {
  console.log('\n── join(sponsor) seeds synaptome ──');
  const net = new SimNetwork();
  const alice = await makePeer(net, LONDON);
  const bob   = await makePeer(net, TOKYO);

  // Bob is already up so alice can sponsor off him.
  await bob.peer.join();

  const joinEvents = [];
  alice.peer.onPeerJoin((id, ev) => joinEvents.push({ id, addedBy: ev.addedBy }));

  await alice.peer.join(bob.id.id);

  check('alice.peers() now includes bob',
    alice.peer.peers().includes(bob.id.id));
  check('transport channel open to bob',
    alice.transport.isConnected(bob.id.id));
  check('onPeerJoin fired with sponsor',
    joinEvents.length === 1 && joinEvents[0].id === bob.id.id);
  check('addedBy = bootstrap',
    joinEvents[0].addedBy === 'bootstrap');

  await alice.peer.leave({ drain: false, notify: false });
  await bob.peer.leave({ drain: false, notify: false });
}

async function testJoinRejectsBadSponsor() {
  console.log('\n── join(sponsor) input validation ──');
  const net = new SimNetwork();
  const { peer } = await makePeer(net, LONDON);

  let err = null;
  try { await peer.join('not-hex'); }
  catch (e) { err = e; }
  check('rejects non-hex sponsor', err instanceof TransportError);

  // Unreachable sponsor (not registered in SimNetwork)
  err = null;
  try { await peer.join('ff' + 'f'.repeat(64)); }
  catch (e) { err = e; }
  check('rejects unreachable sponsor', err instanceof TransportError);

  await peer.leave({ drain: false, notify: false });
}

async function testLeaveNotify() {
  console.log('\n── leave({ notify: true }) sends peer-leaving ──');
  const net = new SimNetwork();
  const alice = await makePeer(net, LONDON);
  const bob   = await makePeer(net, TOKYO);
  await bob.peer.join();
  await alice.peer.join(bob.id.id);

  const peerLeavings = [];
  bob.transport.onNotification('peer-leaving', (fromId, body) => {
    peerLeavings.push({ fromId, body });
  });

  await alice.peer.leave({ drain: false, notify: true });
  // notify is async via microtask; let it drain.
  await new Promise(r => setTimeout(r, 20));

  check('bob received peer-leaving notification',
    peerLeavings.length === 1);
  check('peer-leaving carries from = alice',
    peerLeavings[0]?.body?.from === alice.id.id);

  await bob.peer.leave({ drain: false, notify: false });
}

async function testPeerLeavingEviction() {
  console.log('\n── receiving peer-leaving proactively evicts the (authenticated) sender ──');
  const net = new SimNetwork();
  const alice = await makePeer(net, LONDON);
  const bob   = await makePeer(net, TOKYO);
  await bob.peer.join();
  await alice.peer.join(bob.id.id);     // alice now holds bob → leave() notifies him

  // Production keys the synaptome by BigInt nodeId (the join path stores
  // BigInt; peers() renders hex).  Seed bob with alice + an unrelated
  // third party (carol) bob also holds.
  const aliceBig = fromHex(alice.id.id);
  const carolId  = await createNodeIdentity(LONDON);
  const carolBig = fromHex(carolId.id);
  bob.engine.addSynapse(bob.peer._node, aliceBig, { addedBy: 'test' });
  bob.engine.addSynapse(bob.peer._node, carolBig, { addedBy: 'test' });
  check('precondition: bob holds alice', bob.peer._node.synaptome.has(aliceBig));
  check('precondition: bob holds carol', bob.peer._node.synaptome.has(carolBig));

  // Alice announces graceful departure; bob's kernel handler should
  // evict her *now* rather than waiting for the 10 s refreshTick.
  await alice.peer.leave({ drain: false, notify: true });
  await new Promise(r => setTimeout(r, 30));

  check('bob proactively evicted alice on peer-leaving',
    !bob.peer._node.synaptome.has(aliceBig));
  // Safety: the eviction subject is the authenticated SENDER only — a
  // third party bob also holds is untouched (can't force-evict others).
  check('bob did NOT evict the unrelated third party (carol)',
    bob.peer._node.synaptome.has(carolBig));

  await bob.peer.leave({ drain: false, notify: false });
}

async function testLeaveSilent() {
  console.log('\n── leave({ notify: false }) is silent ──');
  const net = new SimNetwork();
  const alice = await makePeer(net, LONDON);
  const bob   = await makePeer(net, TOKYO);
  await bob.peer.join();
  await alice.peer.join(bob.id.id);

  const peerLeavings = [];
  bob.transport.onNotification('peer-leaving', () => peerLeavings.push(1));

  await alice.peer.leave({ drain: false, notify: false });
  await new Promise(r => setTimeout(r, 20));

  check('no peer-leaving notification when notify:false',
    peerLeavings.length === 0);

  await bob.peer.leave({ drain: false, notify: false });
}

async function testLeaveIdempotent() {
  console.log('\n── leave() is idempotent ──');
  const net = new SimNetwork();
  const { peer } = await makePeer(net, LONDON);
  await peer.join();
  await peer.leave({ drain: false, notify: false });
  await peer.leave({ drain: false, notify: false });
  check('two leaves do not throw', peer._started === false);
}

async function testJoinAfterLeave() {
  console.log('\n── join() after leave() resumes operation ──');
  const net = new SimNetwork();
  const { peer, transport } = await makePeer(net, LONDON);
  await peer.join();
  await peer.leave({ drain: false, notify: false });
  // re-join.
  await peer.join();
  check('peer started again after re-join', peer._started === true);
  check('transport started again',          transport._started === true);
  await peer.leave({ drain: false, notify: false });
}

async function main() {
  console.log('Axona join/leave (A7 + A8) smoke');
  await testStandaloneJoin();
  await testSponsorJoin();
  await testJoinRejectsBadSponsor();
  await testLeaveNotify();
  await testPeerLeavingEviction();
  await testLeaveSilent();
  await testLeaveIdempotent();
  await testJoinAfterLeave();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
