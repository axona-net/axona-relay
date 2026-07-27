// =====================================================================
// smoke_transport_web.js — WebRTCTransport smoke test (no real WebRTC
//                          needed). Two FakeMesh instances share a bus;
//                          we exercise the Transport contract end-to-end:
//                          send/response, notify, peer death,
//                          timeouts, bind/unbind, getLatency.
// Run: node test/smoke_transport_web.js
// =====================================================================

import { WebRTCTransport } from '../src/transport/web/index.js';
import { TransportError, ErrorCodes } from '../src/errors.js';
import { fromHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// Kernel-canonical form: 264-bit BigInt.  Wire/display form is hex.
const ALICE_HEX = 'aa' + 'a1'.repeat(32);
const BOB_HEX   = 'bb' + 'b2'.repeat(32);
const ALICE = fromHex(ALICE_HEX);
const BOB   = fromHex(BOB_HEX);
const ALICE_MESH = 'mesh-alice';
const BOB_MESH   = 'mesh-bob';

// ── FakeMesh: shares a bus with its linked peer ────────────────────────

class FakeMesh {
  constructor(myMeshId) {
    this.myMeshId = myMeshId;
    this._peer    = null;
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._connected = false;
    this._latency = 42;
  }

  linkTo(other) {
    this._peer = other;
    other._peer = this;
    this._connected = true;
    other._connected = true;
  }

  killLink() {
    if (!this._connected) return;
    this._connected = false;
    if (this._peer) this._peer._connected = false;
    const myMeshId    = this.myMeshId;
    const otherMeshId = this._peer?.myMeshId;
    if (otherMeshId) {
      for (const cb of this._peerLostListeners) cb(otherMeshId);
    }
    if (this._peer) {
      for (const cb of this._peer._peerLostListeners) cb(myMeshId);
    }
  }

  onMessage(cb)   { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb)  { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange(_cb)   { return () => {}; }
  isConnected(meshId) {
    return this._connected && this._peer && this._peer.myMeshId === meshId;
  }
  getLatency(meshId) {
    return this.isConnected(meshId) ? this._latency : -1;
  }

  send(meshId, payload) {
    if (!this.isConnected(meshId)) {
      throw new Error(`FakeMesh.send: ${meshId} not connected`);
    }
    queueMicrotask(() => {
      for (const cb of this._peer._messageListeners) {
        cb(this.myMeshId, payload);
      }
    });
  }
}

// ── helper: build a connected pair ──────────────────────────────────

function makePair({ aliceTimeoutMs = 200, bobTimeoutMs = 200 } = {}) {
  const aliceMesh = new FakeMesh(ALICE_MESH);
  const bobMesh   = new FakeMesh(BOB_MESH);
  aliceMesh.linkTo(bobMesh);

  const alice = new WebRTCTransport({
    mesh: aliceMesh,
    localNodeId: ALICE,
    requestTimeoutMs: aliceTimeoutMs,
  });
  const bob = new WebRTCTransport({
    mesh: bobMesh,
    localNodeId: BOB,
    requestTimeoutMs: bobTimeoutMs,
  });
  return { alice, bob, aliceMesh, bobMesh };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testLifecycle() {
  console.log('\n── start / stop / localNodeId ──');
  const { alice } = makePair();
  await alice.start();
  check('start() sets _started',         alice._started === true);
  check('getLocalNodeId returns BigInt id', alice.getLocalNodeId() === ALICE);

  await alice.start();
  check('repeat start is idempotent',    alice._started === true);

  await alice.stop();
  check('stop() clears _started',        alice._started === false);

  await alice.stop();
  check('repeat stop is idempotent',     true);
}

async function testStartValidation() {
  console.log('\n── start validation ──');
  const mesh = new FakeMesh(ALICE_MESH);
  const t = new WebRTCTransport({ mesh, localNodeId: 'not-a-bigint-id' });
  let err = null;
  try { await t.start(); }
  catch (e) { err = e; }
  check('rejects non-bigint localNodeId',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_NOT_STARTED);

  const t2 = new WebRTCTransport({ /* no mesh */ });
  err = null;
  try { await t2.start(ALICE); }
  catch (e) { err = e; }
  check('rejects start without mesh',
    err instanceof TransportError);
}

async function testBindUnbind() {
  console.log('\n── bindPeer / unbindPeer / lookups ──');
  const { alice } = makePair();
  await alice.start();

  check('isConnected before bind = false', !alice.isConnected(BOB));
  check('meshIdFor unbound = null',       alice.meshIdFor(BOB) === null);
  check('nodeIdFor unbound = null',       alice.nodeIdFor(BOB_MESH) === null);

  alice.bindPeer(BOB, BOB_MESH);
  check('meshIdFor after bind',           alice.meshIdFor(BOB) === BOB_MESH);
  check('nodeIdFor after bind',           alice.nodeIdFor(BOB_MESH) === BOB);
  check('isConnected after bind',         alice.isConnected(BOB));
  check('getLatency returns mesh value',  alice.getLatency(BOB) === 42);

  alice.unbindPeer(BOB_MESH);
  check('isConnected after unbind = false', !alice.isConnected(BOB));
  check('meshIdFor after unbind = null',    alice.meshIdFor(BOB) === null);

  let threw = false;
  try { alice.bindPeer('not-a-bigint', BOB_MESH); } catch { threw = true; }
  check('bindPeer rejects non-bigint nodeId', threw);
}

async function testSendRequest() {
  console.log('\n── send (request/response) ──');
  const { alice, bob } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  let receivedFromNodeId = null;
  bob.onRequest('echo', (fromNodeId, payload) => {
    receivedFromNodeId = fromNodeId;
    return { echoed: payload, by: BOB };
  });

  const reply = await alice.send(BOB, 'echo', { msg: 'hello' });
  check('send returns remote handler value',
    reply.echoed.msg === 'hello' && reply.by === BOB);
  check('remote handler saw correct fromNodeId',
    receivedFromNodeId === ALICE);

  // Handler throw → caller's send rejects.
  bob.onRequest('boom', () => { throw new Error('kaboom'); });
  let threw = null;
  try { await alice.send(BOB, 'boom', null); }
  catch (e) { threw = e; }
  check('handler error propagates as TransportError',
    threw instanceof TransportError && threw.message.includes('kaboom'));
}

async function testSendNoHandler() {
  console.log('\n── send to unhandled type ──');
  const { alice, bob } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB, BOB_MESH);

  let err = null;
  try { await alice.send(BOB, 'no-such-type', null); }
  catch (e) { err = e; }
  check('unhandled type → TransportError',
    err instanceof TransportError);
  check('error message names the missing type',
    err?.message?.includes('no-such-type'));
}

async function testSendUnbound() {
  console.log('\n── send to unbound peer ──');
  const { alice } = makePair();
  await alice.start();

  let err = null;
  try { await alice.send(BOB, 'echo', null); }
  catch (e) { err = e; }
  check('send to unbound nodeId → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);
}

async function testSendTimeout() {
  console.log('\n── send timeout ──');
  const { alice, bob } = makePair({ aliceTimeoutMs: 100 });
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  // Bob registers a slow handler.
  bob.onRequest('slow', async () => {
    await new Promise(r => setTimeout(r, 500));
    return 'eventually';
  });

  let err = null;
  try { await alice.send(BOB, 'slow', null); }
  catch (e) { err = e; }
  check('slow handler → TIMEOUT TransportError',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_TIMEOUT);
}

async function testNotify() {
  console.log('\n── notify ──');
  const { alice, bob } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  let count = 0;
  let lastFrom = null;
  bob.onNotification('tick', (from, body) => {
    count++;
    lastFrom = from;
  });

  await alice.notify(BOB, 'tick', { n: 1 });
  await alice.notify(BOB, 'tick', { n: 2 });
  await new Promise(r => queueMicrotask(r));
  check('notify delivered both',           count === 2);
  check('notify fromNodeId correct',       lastFrom === ALICE);

  // Handler throw → swallowed.
  bob.onNotification('boom', () => { throw new Error('eaten'); });
  await alice.notify(BOB, 'boom', null);
  check('notify handler throw does not propagate', true);

  // No handler → silent drop.
  await alice.notify(BOB, 'no-such', null);
  check('notify with no handler is silent', true);

  // Unbound peer → silent drop.
  await alice.notify(fromHex('cc' + '0'.repeat(64)), 'tick', null);
  check('notify to unbound peer is silent', true);
}

async function testPeerDied() {
  console.log('\n── peer death ──');
  const { alice, bob, aliceMesh } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  const deaths = [];
  alice.onPeerDied(id => deaths.push(id));

  aliceMesh.killLink();
  // Microtasks settle synchronously, but onPeerLost listeners are sync.
  check('onPeerDied fired once',          deaths.length === 1);
  check('reports bound nodeId, not meshId', deaths[0] === BOB);
  check('isConnected false after death',  !alice.isConnected(BOB));
}

async function testPendingRejectionOnDeath() {
  console.log('\n── pending requests reject on peer death ──');
  const { alice, bob, aliceMesh } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  bob.onRequest('slow', () => new Promise(() => {}));   // never resolves

  const sendPromise = alice.send(BOB, 'slow', null);
  // Kill the link mid-send.
  aliceMesh.killLink();

  let err = null;
  try { await sendPromise; }
  catch (e) { err = e; }
  check('in-flight send rejects after death',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_PEER_UNREACHABLE);
}

async function testStopRejectsPending() {
  console.log('\n── stop rejects pending requests ──');
  const { alice, bob } = makePair();
  await alice.start();
  await bob.start();
  alice.bindPeer(BOB,   BOB_MESH);
  bob  .bindPeer(ALICE, ALICE_MESH);

  bob.onRequest('slow', () => new Promise(() => {}));

  const sendPromise = alice.send(BOB, 'slow', null);
  await alice.stop();

  let err = null;
  try { await sendPromise; }
  catch (e) { err = e; }
  check('stop() rejects pending → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);
}

async function testUnboundFromIsNull() {
  console.log('\n── pre-bind notifications: fromId is meshId ──');
  // Hello/hello-ack notifications arrive BEFORE bindPeer is called.
  // The transport must deliver them with fromMeshId in the fromId slot
  // so the orchestrator can bind on receipt.
  const { alice, bob } = makePair();
  await alice.start();
  await bob.start();
  // bob binds alice; alice does NOT bind bob yet
  bob.bindPeer(ALICE, ALICE_MESH);

  let receivedFrom = null;
  alice.onNotification('hello', (from, body) => { receivedFrom = from; });
  await bob.notify(ALICE, 'hello', { iAm: BOB });

  // notify is async-microtask; drain.
  await new Promise(r => queueMicrotask(r));
  await new Promise(r => queueMicrotask(r));
  check('pre-bind notification: from = meshId (orchestrator pattern)',
    receivedFrom === BOB_MESH);
}

async function main() {
  console.log('Axona WebRTCTransport smoke');
  await testLifecycle();
  await testStartValidation();
  await testBindUnbind();
  await testSendRequest();
  await testSendNoHandler();
  await testSendUnbound();
  await testSendTimeout();
  await testNotify();
  await testPeerDied();
  await testPendingRejectionOnDeath();
  await testStopRejectsPending();
  await testUnboundFromIsNull();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
