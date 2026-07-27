// =====================================================================
// smoke_transport_node.js — Node WebSocketTransport (server + client
//                            modes) and the wstransport core.
// Run: node test/smoke_transport_node.js
// =====================================================================

import {
  WebSocketTransport,
  serverTransport,
  clientTransport,
}                                from '../src/transport/node/index.js';
import { TransportError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const SELF  = 'aa' + 'a1'.repeat(32);
const PEER1 = 'bb' + 'b2'.repeat(32);
const PEER2 = 'cc' + 'c3'.repeat(32);
const BRIDGE = 'dd' + 'd4'.repeat(32);

// ── Core WebSocketTransport (server-style multi-peer) ─────────────────

async function testCoreLifecycle() {
  console.log('\n── core: lifecycle + binding ──');
  const t = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: () => true,
    isConnOpen: () => true,
  });
  await t.start();
  check('start sets _started', t._started === true);
  check('getLocalNodeId', t.getLocalNodeId() === SELF);

  t.bindPeer(PEER1, 'conn-1');
  check('isConnected after bind', t.isConnected(PEER1));
  check('connIdFor returns connId', t.connIdFor(PEER1) === 'conn-1');
  check('nodeIdFor returns nodeId', t.nodeIdFor('conn-1') === PEER1);

  t.unbindPeer('conn-1');
  check('isConnected after unbind = false', !t.isConnected(PEER1));

  let threw = false;
  try { t.bindPeer('not-hex', 'conn'); } catch { threw = true; }
  check('bindPeer rejects non-hex nodeId', threw);

  threw = false;
  try { t.bindPeer(PEER1, 12345); } catch { threw = true; }
  check('bindPeer rejects non-string connId', threw);

  await t.stop();
}

async function testCoreSendNotify() {
  console.log('\n── core: send + notify across multiple peers ──');
  const sent = [];
  const t = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: (cid, msg) => { sent.push({ cid, msg }); return true; },
    isConnOpen: () => true,
  });
  await t.start();
  t.bindPeer(PEER1, 'conn-1');
  t.bindPeer(PEER2, 'conn-2');

  const send1 = t.send(PEER1, 'lookup', { target: 'x' });
  const send2 = t.send(PEER2, 'lookup', { target: 'y' });
  check('send to PEER1 wrote on conn-1',
    sent.find(s => s.cid === 'conn-1') !== undefined);
  check('send to PEER2 wrote on conn-2',
    sent.find(s => s.cid === 'conn-2') !== undefined);

  // Deliver responses.
  const req1 = sent.find(s => s.cid === 'conn-1').msg.payload;
  const req2 = sent.find(s => s.cid === 'conn-2').msg.payload;
  t.handleIncoming('conn-1', { k: 'res', id: req1.id, ok: true, body: { v: 1 } });
  t.handleIncoming('conn-2', { k: 'res', id: req2.id, ok: true, body: { v: 2 } });
  const r1 = await send1;
  const r2 = await send2;
  check('send 1 resolves with right value', r1.v === 1);
  check('send 2 resolves with right value', r2.v === 2);

  // Notify writes on the correct conn.
  sent.length = 0;
  await t.notify(PEER1, 'tick', null);
  check('notify wrote on conn-1 only',
    sent.length === 1 && sent[0].cid === 'conn-1' && sent[0].msg.payload.k === 'ntf');

  await t.stop();
}

async function testCoreErrors() {
  console.log('\n── core: error paths ──');
  const t = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: () => true,
    isConnOpen: () => true,
  });
  await t.start();

  // Unbound peer → CHANNEL_CLOSED.
  let err = null;
  try { await t.send(PEER1, 'echo', null); }
  catch (e) { err = e; }
  check('send to unbound peer → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);

  // Bound but conn closed → also CHANNEL_CLOSED.
  let open = true;
  const t2 = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: () => true,
    isConnOpen: () => open,
  });
  await t2.start();
  t2.bindPeer(PEER1, 'conn-1');
  open = false;
  err = null;
  try { await t2.send(PEER1, 'echo', null); }
  catch (e) { err = e; }
  check('send to closed conn → CHANNEL_CLOSED',
    err instanceof TransportError && err.code === ErrorCodes.TRANSPORT_CHANNEL_CLOSED);

  // Remote handler error → propagated as TransportError.
  const sent = [];
  const t3 = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: (cid, msg) => { sent.push({ cid, msg }); return true; },
    isConnOpen: () => true,
  });
  await t3.start();
  t3.bindPeer(PEER1, 'conn-1');
  const sendPromise = t3.send(PEER1, 'boom', null);
  t3.handleIncoming('conn-1', { k: 'res', id: sent[0].msg.payload.id, ok: false, body: { error: 'oops' } });
  err = null;
  try { await sendPromise; }
  catch (e) { err = e; }
  check('remote handler error → TransportError',
    err instanceof TransportError && err.message.includes('oops'));
}

async function testCorePeerDied() {
  console.log('\n── core: handleConnClosed cascades onPeerDied ──');
  const t = new WebSocketTransport({
    localNodeId: SELF,
    sendToConn: () => true,
    isConnOpen: () => true,
  });
  await t.start();
  t.bindPeer(PEER1, 'conn-1');
  t.bindPeer(PEER2, 'conn-2');

  const deaths = [];
  t.onPeerDied(id => deaths.push(id));

  t.handleConnClosed('conn-1');
  check('peer-died fires with bound nodeId',
    deaths.length === 1 && deaths[0] === PEER1);
  check('PEER1 unbound after close', !t.isConnected(PEER1));
  check('PEER2 still connected',     t.isConnected(PEER2));
}

// ── serverTransport factory (mode used by axona-bridge) ───────────────

async function testServerFactory() {
  console.log('\n── serverTransport factory ──');
  let threw = false;
  try { serverTransport({ identity: { id: 'short' }, sendToConn: () => true, isConnOpen: () => true }); }
  catch { threw = true; }
  check('rejects bad identity', threw);

  const { transport, attach } = serverTransport({
    identity:   { id: SELF },
    sendToConn: () => true,
    isConnOpen: () => true,
  });
  await transport.start();
  check('factory returns transport + attach',
    transport instanceof WebSocketTransport && typeof attach.added === 'function');

  // attach.message routes axona-typed frames to handleIncoming.
  let receivedFrom = null;
  let receivedType = null;
  transport.onNotification('tick', (from, body) => {
    receivedFrom = from;
    receivedType = body?.n;
  });
  attach.added('conn-1');
  attach.message('conn-1', { type: 'axona', payload: { k: 'ntf', type: 'tick', body: { n: 7 } } });
  check('notification routed via attach.message',
    receivedType === 7);
  check('pre-bind from is connId', receivedFrom === 'conn-1');

  // Non-axona frame is silently ignored.
  attach.message('conn-1', { type: 'welcome', connId: 'conn-1' });
  check('non-axona frame ignored', true);

  // attach.closed fires peer-died.
  const deaths = [];
  transport.onPeerDied(id => deaths.push(id));
  transport.bindPeer(PEER1, 'conn-1');
  attach.closed('conn-1');
  check('attach.closed fires peer-died',
    deaths.length === 1 && deaths[0] === PEER1);
}

// ── clientTransport factory (Node consumer to bridge) ─────────────────

class FakeWebSocket {
  constructor(url) {
    this.url = url;
    this.sent = [];
    this._listeners = new Map();
    this.readyState = 0;
    queueMicrotask(() => {
      this.readyState = 1;
      this._fire('open');
    });
  }
  addEventListener(type, h) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(h);
  }
  send(data) {
    if (this.readyState !== 1) throw new Error('socket closed');
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

async function testClientFactory() {
  console.log('\n── clientTransport factory ──');
  let threw = false;
  try { clientTransport({ identity: { id: SELF }, wsUrl: 'not-a-url', WebSocketImpl: FakeWebSocket }); }
  catch { threw = true; }
  check('rejects non-ws URL', threw);

  const t = clientTransport({
    identity:      { id: SELF },
    wsUrl:         'wss://test.example',
    WebSocketImpl: FakeWebSocket,
  });
  check('client factory has bridgeConnId helper',
    t.bridgeConnId === 'bridge');

  await t.start();
  check('after start: socket open',
    t.socket && t.socket.readyState === 1);

  // bindPeer the bridge, send a request, simulate the bridge replying.
  t.bindPeer(BRIDGE, t.bridgeConnId);
  const sendPromise = t.send(BRIDGE, 'ping', { hi: 1 });

  // The fake socket has the outbound frame in t.socket.sent.
  const sentJson = t.socket.sent.find(s => JSON.parse(s).type === 'axona');
  check('outbound frame is axona-wrapped',
    sentJson && JSON.parse(sentJson).type === 'axona');
  const id = JSON.parse(sentJson).payload.id;
  // Simulate the reply.
  t.socket._deliver(JSON.stringify({
    type: 'axona',
    payload: { k: 'res', id, ok: true, body: { pong: 1 } },
  }));
  const reply = await sendPromise;
  check('send resolves with bridge reply',
    reply.pong === 1);

  await t.stop();
  check('after stop: socket null', t.socket === null);
}

async function main() {
  console.log('Axona node transport (server + client) smoke');
  await testCoreLifecycle();
  await testCoreSendNotify();
  await testCoreErrors();
  await testCorePeerDied();
  await testServerFactory();
  await testClientFactory();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
