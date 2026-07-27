// =====================================================================
// smoke_direct.js — peer.send / peer.notify / peer.onMessage end-to-end
//                    using two peers on a shared SimNetwork.
// Run: node test/smoke_direct.js
// =====================================================================

import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const TOKYO  = { lat: 35.6762, lng: 139.6503 };

// ── Setup helper: two peers wired through SimNetwork ─────────────────

async function makePair() {
  const aliceId = await createNodeIdentity(LONDON);
  const bobId   = await createNodeIdentity(TOKYO);

  const network = new SimNetwork();
  const aliceTransport = simTransport({
    network, identity: aliceId, heartbeatMs: 0,
  });
  const bobTransport = simTransport({
    network, identity: bobId, heartbeatMs: 0,
  });
  await aliceTransport.start(aliceId.id);
  await bobTransport.start(bobId.id);
  await aliceTransport.openConnection(bobId.id);

  const alice = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: aliceId.id, alive: true },
    nodeIdentity: aliceId,
    transport: aliceTransport,
  });
  const bob = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: bobId.id, alive: true },
    nodeIdentity: bobId,
    transport: bobTransport,
  });
  return { alice, bob, aliceId, bobId, aliceTransport, bobTransport };
}

async function testSendRoundTrip() {
  console.log('\n── send: RPC round-trip ──');
  const { alice, bob, aliceId, bobId } = await makePair();

  let bobReceivedFrom = null;
  bob.onMessage((senderId, msg) => {
    bobReceivedFrom = senderId;
    return { echoed: msg, by: 'bob' };
  });

  const reply = await alice.send(bobId.id, { hello: 'mesh' });
  check('reply.echoed matches sent message',
    reply.echoed.hello === 'mesh');
  check('reply.by = bob',         reply.by === 'bob');
  check('bob saw senderId = alice', bobReceivedFrom === aliceId.id);
}

async function testNotifyOneWay() {
  console.log('\n── notify: fire-and-forget ──');
  const { alice, bob, aliceId, bobId } = await makePair();

  const inbox = [];
  bob.onMessage((senderId, msg) => {
    inbox.push({ senderId, msg });
    return 'discarded';   // notify discards return values
  });

  await alice.notify(bobId.id, { event: 'ping' });
  await alice.notify(bobId.id, { event: 'ping' });
  // Notify queues via setTimeout; drain.
  await new Promise(r => setTimeout(r, 20));

  check('bob received both notifications', inbox.length === 2);
  check('all from alice',
    inbox.every(e => e.senderId === aliceId.id));
}

async function testValidation() {
  console.log('\n── validation ──');
  const { alice } = await makePair();

  let threw = false;
  try { await alice.send('not-hex', 'x'); } catch { threw = true; }
  check('send rejects non-hex targetId', threw);

  threw = false;
  try { await alice.notify('not-hex', 'x'); } catch { threw = true; }
  check('notify rejects non-hex targetId', threw);

  threw = false;
  try { alice.onMessage('not-a-fn'); } catch { threw = true; }
  check('onMessage rejects non-function handler', threw);
}

async function testHandlerErrorPropagates() {
  console.log('\n── send: handler error propagates ──');
  const { alice, bob, bobId } = await makePair();

  bob.onMessage(() => { throw new Error('kaboom'); });
  let err = null;
  try { await alice.send(bobId.id, null); }
  catch (e) { err = e; }
  check('send rejects with the remote error',
    err !== null && /kaboom/.test(err.message));
}

async function testNoTransport() {
  console.log('\n── send: no transport configured ──');
  const id = await createNodeIdentity(LONDON);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: id.id, alive: true },
    nodeIdentity: id,
    /* no transport */
  });
  let err = null;
  try { await peer.send(id.id, 'x'); }
  catch (e) { err = e; }
  check('send without transport throws',
    err !== null && /transport/.test(err.message));
}

async function testHandlerReplacement() {
  console.log('\n── onMessage: replacing the handler ──');
  const { alice, bob, bobId } = await makePair();

  let active = 'A';
  bob.onMessage(() => 'replyA');
  let r = await alice.send(bobId.id, null);
  check('first handler responds', r === 'replyA');

  bob.onMessage(() => 'replyB');
  r = await alice.send(bobId.id, null);
  check('replaced handler now responds', r === 'replyB');
}

async function main() {
  console.log('Axona direct-messaging (A4) smoke');
  await testSendRoundTrip();
  await testNotifyOneWay();
  await testValidation();
  await testHandlerErrorPropagates();
  await testNoTransport();
  await testHandlerReplacement();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
