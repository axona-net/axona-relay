// =====================================================================
// smoke_transport_web_full.js — BridgeTransport + CompositeTransport
//                                + webTransport factory smoke.
//
// WebRTCTransport itself is covered in smoke_transport_web.js.
// MeshManager + the live WebRTC stack require browser context to
// exercise; this smoke covers everything reachable from Node:
//
//   - BridgeTransport against a fake sendToBridge hook
//   - CompositeTransport routing send/notify between sub-transports
//   - webTransport() factory construction + start() with a fake WS
//
// Run: node test/smoke_transport_web_full.js
// =====================================================================

import {
  BridgeTransport,
  CompositeTransport,
  WebRTCTransport,
  webTransport,
} from '../src/transport/web/index.js';
import { TransportError, ErrorCodes } from '../src/errors.js';
import { fromHex } from '../src/utils/hexid.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { buildAuthHello, cbvFromNonces } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// Wire/display form is hex; kernel-internal canonical form is BigInt.
const ALICE_HEX  = 'aa' + 'a1'.repeat(32);
const BRIDGE_HEX = 'cc' + 'fe'.repeat(32);
const ALICE  = fromHex(ALICE_HEX);
const BRIDGE = fromHex(BRIDGE_HEX);

// ── BridgeTransport tests ────────────────────────────────────────────

async function testBridgeBindAndSend() {
  console.log('\n── BridgeTransport: bind / send / notify ──');
  const sent = [];
  const bt = new BridgeTransport({
    localNodeId:  ALICE,
    sendToBridge: (msg) => { sent.push(msg); return true; },
    isBridgeOpen: () => true,
  });
  await bt.start();

  check('isConnected before bind = false', !bt.isConnected(BRIDGE));
  bt.bindPeer(BRIDGE, 'bridge');
  check('isConnected after bind = true', bt.isConnected(BRIDGE));
  check('ownsPeer = true', bt.ownsPeer(BRIDGE));

  let threw = false;
  try { bt.bindPeer('not-a-bigint', 'bridge'); } catch { threw = true; }
  check('bindPeer rejects non-bigint nodeId', threw);

  threw = false;
  try { bt.bindPeer(BRIDGE, 'not-bridge-connId'); } catch { threw = true; }
  check('bindPeer rejects connId !== bridge', threw);

  // Send: synchronously writes the req envelope.
  const sendPromise = bt.send(BRIDGE, 'lookup', { target: 'x' });
  check('send wrote envelope to bridge',
    sent.length === 1 && sent[0].type === 'axona' &&
    sent[0].payload.k === 'req' && sent[0].payload.type === 'lookup');

  // Simulate the bridge replying.
  bt.handleIncoming({ k: 'res', id: sent[0].payload.id, ok: true, body: { result: 'ok' } });
  const reply = await sendPromise;
  check('send resolves with response body', reply.result === 'ok');

  // Notify: fire-and-forget.
  sent.length = 0;
  await bt.notify(BRIDGE, 'ping', { n: 1 });
  check('notify wrote ntf envelope',
    sent.length === 1 && sent[0].payload.k === 'ntf' && sent[0].payload.type === 'ping');
}

async function testBridgeUnbound() {
  console.log('\n── BridgeTransport: errors on unbound peer ──');
  const bt = new BridgeTransport({
    localNodeId:  ALICE,
    sendToBridge: () => true,
    isBridgeOpen: () => true,
  });
  await bt.start();

  let err = null;
  try { await bt.send(BRIDGE, 'echo', null); }
  catch (e) { err = e; }
  check('send to unbound peer → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);
}

async function testBridgeSocketClosed() {
  console.log('\n── BridgeTransport: socket closed ──');
  let open = true;
  const bt = new BridgeTransport({
    localNodeId:  ALICE,
    sendToBridge: (msg) => {
      if (!open) throw new Error('socket closed');
      return true;
    },
    isBridgeOpen: () => open,
  });
  await bt.start();
  bt.bindPeer(BRIDGE, 'bridge');

  open = false;
  let err = null;
  try { await bt.send(BRIDGE, 'lookup', null); }
  catch (e) { err = e; }
  check('send after close → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);
}

async function testBridgeHandlerError() {
  console.log('\n── BridgeTransport: remote handler error propagation ──');
  const sent = [];
  const bt = new BridgeTransport({
    localNodeId:  ALICE,
    sendToBridge: (msg) => { sent.push(msg); return true; },
    isBridgeOpen: () => true,
  });
  await bt.start();
  bt.bindPeer(BRIDGE, 'bridge');

  const sendPromise = bt.send(BRIDGE, 'boom', null);
  bt.handleIncoming({ k: 'res', id: sent[0].payload.id, ok: false, body: { error: 'kaboom' } });
  let err = null;
  try { await sendPromise; }
  catch (e) { err = e; }
  check('error response → TransportError',
    err instanceof TransportError && err.message.includes('kaboom'));
}

async function testBridgePeerDied() {
  console.log('\n── BridgeTransport: handleConnClosed cascades ──');
  const bt = new BridgeTransport({
    localNodeId:  ALICE,
    sendToBridge: () => true,
    isBridgeOpen: () => true,
  });
  await bt.start();
  bt.bindPeer(BRIDGE, 'bridge');

  const deaths = [];
  bt.onPeerDied(id => deaths.push(id));

  bt.handleConnClosed();
  check('peer-died fires with bridge nodeId',
    deaths.length === 1 && deaths[0] === BRIDGE);
  check('unbound after close', !bt.isConnected(BRIDGE));
}

// ── CompositeTransport tests ─────────────────────────────────────────

class FakeSubTransport {
  constructor(name, ownedPeer) {
    this.name = name;
    this._owned = ownedPeer;
    this._reqHandlers = new Map();
    this._ntfHandlers = new Map();
    this._diedHandlers = [];
    this.sendCalls = [];
    this.notifyCalls = [];
    this.started = false;
  }
  async start() { this.started = true; }
  async stop()  { this.started = false; }
  getLocalNodeId() { return 'local'; }
  ownsPeer(id) { return id === this._owned; }
  isConnected(id) { return id === this._owned; }
  async openConnection(id) { return id === this._owned; }
  async closeConnection() {}
  async send(id, type, body) {
    this.sendCalls.push({ id, type, body });
    return { via: this.name };
  }
  async notify(id, type, body) {
    this.notifyCalls.push({ id, type, body });
  }
  onRequest(type, h) { this._reqHandlers.set(type, h); }
  onNotification(type, h) { this._ntfHandlers.set(type, h); }
  onPeerDied(h) { this._diedHandlers.push(h); return () => {}; }
  getLatency() { return 0; }
}

async function testCompositeRouting() {
  console.log('\n── CompositeTransport: routes to owning sub-transport ──');
  const BOB   = fromHex('bb' + 'b2'.repeat(32));
  const CAROL = fromHex('cc' + 'c3'.repeat(32));

  const subA = new FakeSubTransport('bridge', BRIDGE);
  const subB = new FakeSubTransport('webrtc', BOB);

  const composite = new CompositeTransport({ localNodeId: ALICE });
  composite.addSubtransport(subA);
  composite.addSubtransport(subB);
  await composite.start();

  // Routing decisions.
  const r1 = await composite.send(BRIDGE, 'x', { a: 1 });
  check('send to bridge-owned peer routes through bridge subtransport',
    r1.via === 'bridge' && subA.sendCalls.length === 1 && subB.sendCalls.length === 0);

  const r2 = await composite.send(BOB, 'y', { a: 2 });
  check('send to webrtc-owned peer routes through webrtc subtransport',
    r2.via === 'webrtc' && subB.sendCalls.length === 1);

  let err = null;
  try { await composite.send(CAROL, 'z', null); }
  catch (e) { err = e; }
  check('send to unowned peer → PEER_UNREACHABLE',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_PEER_UNREACHABLE);

  // Notify routes the same way.
  await composite.notify(BRIDGE, 'tick', null);
  check('notify routes to bridge', subA.notifyCalls.length === 1);

  // notify to unowned peer is silent.
  await composite.notify(CAROL, 'tick', null);
  check('notify to unowned peer is silent', true);
}

async function testCompositeHandlerFanout() {
  console.log('\n── CompositeTransport: handlers fan out to every sub ──');
  const BOB = fromHex('bb' + 'b2'.repeat(32));
  const subA = new FakeSubTransport('a', BRIDGE);
  const subB = new FakeSubTransport('b', BOB);

  const composite = new CompositeTransport({ localNodeId: ALICE });
  composite.addSubtransport(subA);
  composite.addSubtransport(subB);

  const handler = () => {};
  composite.onRequest('lookup', handler);
  check('onRequest registered on subA', subA._reqHandlers.get('lookup') === handler);
  check('onRequest registered on subB', subB._reqHandlers.get('lookup') === handler);

  composite.onNotification('tick', handler);
  check('onNotification registered on subA', subA._ntfHandlers.get('tick') === handler);
  check('onNotification registered on subB', subB._ntfHandlers.get('tick') === handler);

  composite.onPeerDied(handler);
  check('onPeerDied registered on subA', subA._diedHandlers.includes(handler));
  check('onPeerDied registered on subB', subB._diedHandlers.includes(handler));
}

async function testCompositeLateAdd() {
  console.log('\n── CompositeTransport: late addSubtransport inherits handlers ──');
  const composite = new CompositeTransport({ localNodeId: ALICE });

  const handler = () => {};
  composite.onRequest('lookup', handler);

  const sub = new FakeSubTransport('late', BRIDGE);
  composite.addSubtransport(sub);

  check('late-added sub inherits onRequest handler',
    sub._reqHandlers.get('lookup') === handler);
}

async function testCompositeLocalIdValidation() {
  console.log('\n── CompositeTransport: localNodeId validation ──');
  let threw = false;
  // Post-v1.5: localNodeId is BigInt-only.  Hex string is rejected.
  try { new CompositeTransport({ localNodeId: 'aa' + 'a1'.repeat(32) }); } catch { threw = true; }
  check('rejects hex-string localNodeId (must be BigInt)', threw);

  threw = false;
  try { new CompositeTransport({ localNodeId: 'short' }); } catch { threw = true; }
  check('rejects short string', threw);

  // BigInt is accepted.
  threw = false;
  try { new CompositeTransport({ localNodeId: 12345n }); } catch { threw = true; }
  check('accepts BigInt localNodeId', !threw);
}

// ── webTransport factory tests ───────────────────────────────────────

class FakeWebSocket {
  constructor(url) {
    this.url = url;
    this.sent = [];
    this._listeners = new Map();
    this.readyState = 0;
    // Synchronously open on next microtask.
    queueMicrotask(() => {
      this.readyState = 1;
      this._fire('open');
    });
  }
  addEventListener(type, handler) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(handler);
  }
  send(data) {
    if (this.readyState !== 1) throw new Error('socket not open');
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
    this._fire('close');
  }
  _fire(type, ev = {}) {
    const set = this._listeners.get(type);
    if (set) for (const h of set) try { h(ev); } catch {}
  }
  _deliver(data) {
    this._fire('message', { data });
  }
}

async function testWebTransportFactory() {
  console.log('\n── webTransport factory: construction + start ──');
  let threw = false;
  // identity.id is the user-facing hex form.
  try { webTransport({ bridgeUrl: 'not-a-url', identity: { id: ALICE_HEX } }); }
  catch { threw = true; }
  check('rejects non-ws URL', threw);

  threw = false;
  try { webTransport({ bridgeUrl: 'wss://bridge', identity: { id: 'short' } }); }
  catch { threw = true; }
  check('rejects non-hex identity.id', threw);

  // Build with a fake WebSocket impl.  autoHandshake:false preserves
  // the pre-v1.2 behaviour — start() returns as soon as the socket
  // opens, without waiting for a bridge hello (which the fake socket
  // never sends).
  const t = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  { id: ALICE_HEX },
    WebSocketImpl: FakeWebSocket,
    autoHandshake: false,
  });
  check('factory returns object with mesh/webrtc/bridge sub-refs',
    !!(t.mesh && t.webrtc && t.bridge));
  check('factory exposes bridgeReady promise', t.bridgeReady instanceof Promise);
  check('factory exposes bridgeNodeId getter (initially null)',
    t.bridgeNodeId === null);

  await t.start();
  check('after start: socket is open',
    t.socket && t.socket.readyState === 1);
  check('after start: composite is started', t._started === true);

  // Mesh re-warm bootstrap (task #332 facet 2): the composite can ask the
  // bridge to resend its peer-list; the frame must land on the socket.
  check('factory exposes requestPeerIntroductions', typeof t.requestPeerIntroductions === 'function');
  const sentBefore = t.socket.sent.length;
  check('requestPeerIntroductions sends while socket open', t.requestPeerIntroductions() === true);
  const reqFrame = t.socket.sent.slice(sentBefore).map(s => { try { return JSON.parse(s); } catch { return null; } })
    .find(f => f && f.type === 'peer-list-request');
  check('peer-list-request frame reached the bridge socket', !!reqFrame);

  // Inbound axona-typed frame → routes to BridgeTransport.
  // bindPeer takes BigInt (kernel form).
  t.bridge.bindPeer(BRIDGE, 'bridge');
  t.socket._deliver(JSON.stringify({ type: 'axona', payload: { k: 'ntf', type: 'pong', body: { ok: true } } }));
  check('axona-typed frame dispatch reaches bridge transport without throwing',
    true);

  await t.stop();
  check('after stop: socket closed', t.socket === null);
}

// ── autoHandshake path ────────────────────────────────────────────
//
// Verify that start() with autoHandshake:true (the default):
//   - sends a raw {type:'client-hello', version} frame on socket open
//   - awaits the bridge's `hello` notification
//   - calls bridge.bindPeer + sends hello-ack
//   - resolves with the bridge's nodeId on bridgeReady
//
// The FakeWebSocket replays whatever we feed it through _deliver,
// so we can simulate the bridge's responses inline.

async function testWebTransportAutoHandshake() {
  console.log('\n── webTransport factory: autoHandshake (default) ──');

  // axona/4 — the handshake is now authenticated: real identities, a
  // serverNonce-bound CBV, and Ed25519 proof-of-possession both ways.
  const alice    = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  const bridgeId = await createNodeIdentity({ lat: 51.5,  lng: -0.12 });

  const t = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  alice,
    WebSocketImpl: FakeWebSocket,
    // autoHandshake defaults to true
    handshakeTimeoutMs: 1000,
  });

  const startPromise = t.start();
  await new Promise(r => queueMicrotask(r));
  await new Promise(r => setTimeout(r, 0));

  // (a) client-hello goes out first.
  const firstFrame = t.socket.sent[0] ? JSON.parse(t.socket.sent[0]) : null;
  check('autoHandshake sends client-hello as first frame',
    firstFrame && firstFrame.type === 'client-hello' && typeof firstFrame.version === 'string');

  // (b) welcome seeds the per-connection serverNonce + connId (the CBV).
  const serverNonce = 'beefcafe'.repeat(4);
  const connId      = 'zz';
  t.socket._deliver(JSON.stringify({
    type: 'welcome', connId, serverNonce, version: '9.9.9', kernelVersion: '9.9.9',
  }));

  // (c) bridge replies with an AUTHENTICATED hello, signed over the CBV.
  const cbv = cbvFromNonces(serverNonce, connId, 'bridge');
  const bridgeHello = await buildAuthHello({ identity: bridgeId, cbv });
  t.socket._deliver(JSON.stringify({
    type: 'axona', payload: { k: 'ntf', type: 'hello', body: bridgeHello },
  }));

  await startPromise;
  check('autoHandshake: start() resolves after authenticated bridge hello', true);
  check('autoHandshake: bridgeNodeId is the proven bridge id', t.bridgeNodeId === bridgeId.id);
  check('autoHandshake: bridgeNodeIdBig is set (BigInt)', t.bridgeNodeIdBig === fromHex(bridgeId.id));
  check('autoHandshake: bridge is bound', t.bridge.ownsPeer(fromHex(bridgeId.id)) === true);
  check('autoHandshake: bridgeReady resolves with the proven bridge id',
    (await t.bridgeReady) === fromHex(bridgeId.id));

  // (d) our reply is an AUTHENTICATED hello-ack: our id, our pubkey, a sig.
  const ack = t.socket.sent.map(s => JSON.parse(s))
    .find(f => f.type === 'axona' && f.payload?.type === 'hello-ack');
  check('autoHandshake: authenticated hello-ack sent in reply',
    ack && ack.payload.body.nodeId === alice.id
        && ack.payload.body.pubkey === alice.pubkeyHex
        && typeof ack.payload.body.sig === 'string'
        && ack.payload.body.sig.startsWith('ed25519:'));

  // (e) a hello a forger can't sign for is refused: present the bridge's
  // id+pubkey but a signature from a DIFFERENT key → not bound.
  const t2 = webTransport({ bridgeUrl: 'wss://test.example', identity: alice, WebSocketImpl: FakeWebSocket, handshakeTimeoutMs: 300 });
  const sp2 = t2.start();
  await new Promise(r => setTimeout(r, 0));
  t2.socket._deliver(JSON.stringify({ type: 'welcome', connId: 'yy', serverNonce: 'aa'.repeat(16) }));
  const forgedCbv = cbvFromNonces('aa'.repeat(16), 'yy', 'bridge');
  const aliceSig  = await buildAuthHello({ identity: alice, cbv: forgedCbv });   // alice's sig
  const forged    = { proto: 'axona/5', nodeId: bridgeId.id, pubkey: bridgeId.pubkeyHex, sig: aliceSig.sig };
  t2.socket._deliver(JSON.stringify({ type: 'axona', payload: { k: 'ntf', type: 'hello', body: forged } }));
  let rejected2 = false;
  try { await sp2; } catch { rejected2 = true; }
  check('autoHandshake: forged bridge hello (wrong key) is NOT bound',
    rejected2 && t2.bridgeNodeIdBig === null);
  await t2.stop();

  await t.stop();
}

async function testWebTransportAutoHandshakeTimeout() {
  console.log('\n── webTransport factory: autoHandshake timeout ──');

  const alice = await createNodeIdentity({ lat: 1, lng: 1 });
  const t = webTransport({
    bridgeUrl: 'wss://test.example',
    identity:  alice,
    WebSocketImpl: FakeWebSocket,
    handshakeTimeoutMs: 50,   // tight timeout so we fail fast
  });

  let rejected = false;
  let errCode = null;
  try {
    await t.start();
  } catch (err) {
    rejected = true;
    errCode = err.code;
  }
  check('autoHandshake timeout: start() rejects when bridge hello never arrives',
    rejected);
  check('autoHandshake timeout: UpgradeRequiredError with handshake_timeout context',
    errCode === 'UPGRADE_REQUIRED');

  await t.stop();
}

// ── main ─────────────────────────────────────────────────────────────

async function main() {
  console.log('Axona web transport (bridge + composite + factory) smoke');
  await testBridgeBindAndSend();
  await testBridgeUnbound();
  await testBridgeSocketClosed();
  await testBridgeHandlerError();
  await testBridgePeerDied();
  await testCompositeRouting();
  await testCompositeHandlerFanout();
  await testCompositeLateAdd();
  await testCompositeLocalIdValidation();
  await testWebTransportFactory();
  await testWebTransportAutoHandshake();
  await testWebTransportAutoHandshakeTimeout();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
