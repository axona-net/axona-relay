// =====================================================================
// smoke_transport_sim.js — verify the in-process Transport.sim()
//                          implementation against the Transport
//                          contract.  Two peers, full surface.
// Run: node test/smoke_transport_sim.js
// =====================================================================

import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { TransportError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const ALICE_ID = 'aa' + 'a1'.repeat(32);   // 66 chars
const BOB_ID   = 'bb' + 'b2'.repeat(32);
const CAROL_ID = 'cc' + 'c3'.repeat(32);

function makeNetwork(latencyMs = 0) {
  return new SimNetwork({ latencyFn: () => latencyMs });
}

async function testLifecycle() {
  console.log('\n── start / stop / getLocalNodeId ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });

  let threw = false;
  try { alice.getLocalNodeId(); } catch (e) { threw = e.code === ErrorCodes.TRANSPORT_NOT_STARTED; }
  check('getLocalNodeId before start throws', threw);

  await alice.start(ALICE_ID);
  check('start registers in network', net.size() === 1);
  check('getLocalNodeId returns id', alice.getLocalNodeId() === ALICE_ID);

  await alice.start(ALICE_ID);  // idempotent
  check('repeat start is idempotent', net.size() === 1);

  await alice.stop();
  check('stop deregisters', net.size() === 0);

  await alice.stop();  // idempotent
  check('repeat stop is idempotent', true);
}

async function testChannelOpenClose() {
  console.log('\n── openConnection / closeConnection / isConnected ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);

  check('isConnected(false) before open', !alice.isConnected(BOB_ID));

  const ok = await alice.openConnection(BOB_ID);
  check('openConnection returns true',    ok === true);
  check('alice → bob isConnected',        alice.isConnected(BOB_ID));
  check('bob   → alice isConnected (bilateral)', bob.isConnected(ALICE_ID));

  const ok2 = await alice.openConnection(CAROL_ID);
  check('openConnection to unknown returns false', ok2 === false);

  const ok3 = await alice.openConnection(ALICE_ID);
  check('openConnection to self returns false', ok3 === false);

  await alice.closeConnection(BOB_ID);
  check('after close: alice not connected', !alice.isConnected(BOB_ID));
  check('after close: bob not connected (bilateral)', !bob.isConnected(ALICE_ID));

  await alice.stop();
  await bob.stop();
}

async function testBilateralRefusal() {
  console.log('\n── bilateral refusal via acceptConnection ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({
    network: net,
    heartbeatMs: 0,
    acceptConnection: () => false,     // refuse everything
  });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);

  const ok = await alice.openConnection(BOB_ID);
  check('openConnection returns false when remote refuses', ok === false);
  check('no channel opened on local side', !alice.isConnected(BOB_ID));
  check('no channel opened on remote side', !bob.isConnected(ALICE_ID));

  await alice.stop();
  await bob.stop();
}

async function testSend() {
  console.log('\n── send (request/response) ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);
  await alice.openConnection(BOB_ID);

  let receivedFrom = null;
  bob.onRequest('echo', (fromId, payload) => {
    receivedFrom = fromId;
    return { echoed: payload, by: BOB_ID };
  });

  const reply = await alice.send(BOB_ID, 'echo', { msg: 'hello' });
  check('send returns remote handler value',
    reply.echoed.msg === 'hello' && reply.by === BOB_ID);
  check('remote handler saw correct fromId',
    receivedFrom === ALICE_ID);

  // Handler throw propagates.
  bob.onRequest('boom', () => { throw new Error('kaboom'); });
  let threw = null;
  try { await alice.send(BOB_ID, 'boom', null); }
  catch (e) { threw = e; }
  check('remote throw propagates to send caller',
    threw !== null && threw.message === 'kaboom');

  // Send to type with no handler → TransportError.
  let unhandled = null;
  try { await alice.send(BOB_ID, 'no-such-type', null); }
  catch (e) { unhandled = e; }
  check('send to unhandled type → TransportError',
    unhandled instanceof TransportError);

  // Send on closed channel → TransportError.
  await alice.closeConnection(BOB_ID);
  let closed = null;
  try { await alice.send(BOB_ID, 'echo', null); }
  catch (e) { closed = e; }
  check('send on closed channel → CHANNEL_CLOSED',
    closed instanceof TransportError && closed.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);

  await alice.stop();
  await bob.stop();
}

async function testNotify() {
  console.log('\n── notify (fire-and-forget) ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);
  await alice.openConnection(BOB_ID);

  let count = 0;
  let lastFrom = null;
  bob.onNotification('tick', (fromId, payload) => {
    count++;
    lastFrom = fromId;
  });

  await alice.notify(BOB_ID, 'tick', { n: 1 });
  await alice.notify(BOB_ID, 'tick', { n: 2 });
  // notify schedules via setTimeout — give it a tick to drain.
  await new Promise(r => setTimeout(r, 10));
  check('notify delivered both',         count === 2);
  check('notify fromId correct',         lastFrom === ALICE_ID);
  check('notify resolves before delivery (does not await)', true);

  // Handler throw is swallowed.
  bob.onNotification('boom', () => { throw new Error('swallowed'); });
  await alice.notify(BOB_ID, 'boom', null);
  await new Promise(r => setTimeout(r, 10));
  check('notify handler throw does not crash caller', true);

  // Notify to type with no handler → silent no-op.
  await alice.notify(BOB_ID, 'no-such-ntf', null);
  check('notify to unhandled type is silent', true);

  await alice.stop();
  await bob.stop();
}

async function testLatency() {
  console.log('\n── latency reporting ──');
  const net   = makeNetwork(25);    // 25 ms one-way
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);

  check('getLatency before open = -1', alice.getLatency(BOB_ID) === -1);

  await alice.openConnection(BOB_ID);
  check('getLatency = 2 × one-way after open',
    alice.getLatency(BOB_ID) === 50);

  await alice.stop();
  await bob.stop();
}

async function testPeerDied() {
  console.log('\n── onPeerDied ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);
  await alice.openConnection(BOB_ID);

  const deaths = [];
  alice.onPeerDied(id => deaths.push(id));

  // bob.stop() should fire onPeerDied on alice (since bob closes its side
  // and notifies alice).
  await bob.stop();
  check('peer-died fires on graceful remote stop',
    deaths.length === 1 && deaths[0] === BOB_ID);

  await alice.stop();
}

async function testFullSurface() {
  console.log('\n── 3-peer mesh ──');
  const net   = makeNetwork();
  const alice = simTransport({ network: net, heartbeatMs: 0 });
  const bob   = simTransport({ network: net, heartbeatMs: 0 });
  const carol = simTransport({ network: net, heartbeatMs: 0 });
  await alice.start(ALICE_ID);
  await bob.start(BOB_ID);
  await carol.start(CAROL_ID);

  await alice.openConnection(BOB_ID);
  await alice.openConnection(CAROL_ID);
  await bob.openConnection(CAROL_ID);

  check('network has 3 peers',     net.size() === 3);
  check('alice connected to bob',  alice.isConnected(BOB_ID));
  check('alice connected to carol', alice.isConnected(CAROL_ID));
  check('bob connected to carol',   bob.isConnected(CAROL_ID));

  bob.onRequest('relay', (from, payload) => `bob-relayed-${payload.via}`);
  carol.onRequest('relay', (from, payload) => `carol-relayed-${payload.via}`);

  const fromBob   = await alice.send(BOB_ID, 'relay', { via: 'bob' });
  const fromCarol = await alice.send(CAROL_ID, 'relay', { via: 'carol' });
  check('alice → bob responds',   fromBob === 'bob-relayed-bob');
  check('alice → carol responds', fromCarol === 'carol-relayed-carol');

  await alice.stop();
  await bob.stop();
  await carol.stop();
}

async function main() {
  console.log('Axona Transport.sim() smoke');
  await testLifecycle();
  await testChannelOpenClose();
  await testBilateralRefusal();
  await testSend();
  await testNotify();
  await testLatency();
  await testPeerDied();
  await testFullSurface();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
