// =====================================================================
// smoke_transport_node_auth.js — axona/4 authenticated handshake on the
// Node WebSocketTransport (channel binding via mutual nonces).
//
// Wires two WebSocketTransports together over an in-process loopback
// (each side's sendToConn delivers to the other's handleIncoming) and
// exercises the mutual-auth handshake end-to-end: both sides prove the
// nodeId they claim and bind to the *proven* id.  Then verifies that a
// forged identity (pubkey that doesn't hash to the claimed nodeId) and a
// legacy/garbled hello are both rejected with a 4426 close.
//
// Run: node test/smoke_transport_node_auth.js
// =====================================================================

import { WebSocketTransport, serverTransport } from '../src/transport/node/index.js';
import { createNodeIdentity }                       from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

// Must match the reserved constants in wstransport.js.
const AUTH_HELLO_TYPE = '__axona_auth_hello';

const settle = () => new Promise(r => setTimeout(r, 10));
async function waitFor(cond, tries = 50) {
  for (let i = 0; i < tries; i++) { if (cond()) return true; await settle(); }
  return cond();
}

/**
 * Build a loopback link between two transports. `aConn`/`bConn` are each
 * side's own connId for the link; a frame written by A on aConn is
 * delivered to B on bConn and vice versa.  Honours a "closed" flag so a
 * 4426 close stops further delivery (like a real socket teardown).
 */
function wireLoopback(aConn, bConn) {
  const link = { a: null, b: null, closed: false };
  link.sendFromA = (cid, msg) => {
    if (cid !== aConn || link.closed) return false;
    queueMicrotask(() => { if (!link.closed) link.b.handleIncoming(bConn, msg.payload); });
    return true;
  };
  link.sendFromB = (cid, msg) => {
    if (cid !== bConn || link.closed) return false;
    queueMicrotask(() => { if (!link.closed) link.a.handleIncoming(aConn, msg.payload); });
    return true;
  };
  return link;
}

async function testMutualAuthHappyPath() {
  console.log('\n── mutual auth: two honest peers bind to proven ids ──');
  const alice = await createNodeIdentity({ lat: 38, lng: -77 });
  const bob   = await createNodeIdentity({ lat: 51.5, lng: -0.1 });

  const A_CONN = 'a->b', B_CONN = 'b->a';
  const link = wireLoopback(A_CONN, B_CONN);

  const ta = new WebSocketTransport({
    localNodeId: alice.id, identity: alice, authenticate: true,
    sendToConn: link.sendFromA, isConnOpen: () => !link.closed,
    closeConn: () => { link.closed = true; },
  });
  const tb = new WebSocketTransport({
    localNodeId: bob.id, identity: bob, authenticate: true,
    sendToConn: link.sendFromB, isConnOpen: () => !link.closed,
    closeConn: () => { link.closed = true; },
  });
  link.a = ta; link.b = tb;
  await ta.start(); await tb.start();

  // Both sides open the handshake (server-open + client-open in reality).
  ta.beginAuth(A_CONN);
  tb.beginAuth(B_CONN);

  const ok = await waitFor(() => ta.isAuthenticated(A_CONN) && tb.isAuthenticated(B_CONN));
  check('both connections authenticated', ok);
  check('alice bound bob to bob\'s proven nodeId', ta.nodeIdFor(A_CONN) === bob.id);
  check('bob bound alice to alice\'s proven nodeId', tb.nodeIdFor(B_CONN) === alice.id);
  check('alice connIdFor(bobId) resolves the link', ta.connIdFor(bob.id) === A_CONN);
  check('isConnected(bob) true on alice', ta.isConnected(bob.id));

  // Proven binding means app traffic now routes by the real nodeId.
  let got = null;
  tb.onRequest('echo', (from, body) => { got = { from, body }; return { pong: body?.v }; });
  const res = await ta.send(bob.id, 'echo', { v: 42 });
  check('app request routes over the authenticated link', res?.pong === 42);
  check('receiver sees the authenticated sender id', got?.from === alice.id);

  await ta.stop(); await tb.stop();
}

async function testForgedIdentityRejected() {
  console.log('\n── forged identity: pubkey ≠ nodeId is rejected (4426) ──');
  const real = await createNodeIdentity({ lat: 38, lng: -77 });
  const bob  = await createNodeIdentity({ lat: 51.5, lng: -0.1 });
  // Forge: claim a different nodeId while signing with real's key. The
  // BIND check (SHA-256(pubkey) == nodeId suffix) must fail at the verifier.
  const forged = { ...real, id: bob.id };

  const A_CONN = 'a->b', B_CONN = 'b->a';
  const link = wireLoopback(A_CONN, B_CONN);

  let bobReject = null;
  const ta = new WebSocketTransport({
    localNodeId: forged.id, identity: forged, authenticate: true,
    sendToConn: link.sendFromA, isConnOpen: () => !link.closed,
    closeConn: () => { link.closed = true; },
  });
  const tb = new WebSocketTransport({
    localNodeId: bob.id, identity: bob, authenticate: true,
    sendToConn: link.sendFromB, isConnOpen: () => !link.closed,
    closeConn: (cid, code) => { link.closed = true; bobReject = { ...(bobReject || {}), cid, code }; },
    onAuthReject: ({ reason }) => { bobReject = { ...(bobReject || {}), reason }; },
  });
  link.a = ta; link.b = tb;
  await ta.start(); await tb.start();
  ta.beginAuth(A_CONN);
  tb.beginAuth(B_CONN);

  await waitFor(() => bobReject != null);
  check('bob rejected the forged peer', bobReject != null);
  check('rejection used 4426 (Upgrade Required)', bobReject?.code === 4426);
  check('rejection reason cites the bind failure',
    typeof bobReject?.reason === 'string' && bobReject.reason.includes('pubkey_nodeid_mismatch'));
  check('bob never bound the forged peer', !tb.isAuthenticated(B_CONN));

  await ta.stop(); await tb.stop();
}

async function testLegacyHelloRejected() {
  console.log('\n── legacy/garbled hello is rejected (4426) ──');
  const bob = await createNodeIdentity({ lat: 51.5, lng: -0.1 });
  let rejected = null;
  const tb = new WebSocketTransport({
    localNodeId: bob.id, identity: bob, authenticate: true,
    sendToConn: () => true, isConnOpen: () => true,
    closeConn: (cid, code, reason) => { rejected = { cid, code, reason }; },
  });
  await tb.start();
  tb.beginAuth('c1');
  // Peer speaks an older protocol in its hello.
  tb.handleIncoming('c1', { k: 'ntf', type: AUTH_HELLO_TYPE, body: { proto: 'axona/3', nonce: 'deadbeef' } });
  await settle();
  check('legacy hello rejected', rejected != null);
  check('legacy rejection used 4426', rejected?.code === 4426);
  check('legacy rejection reason is bad_auth_hello',
    typeof rejected?.reason === 'string' && rejected.reason.includes('bad_auth_hello'));
  await tb.stop();
}

async function testServerFactoryBeginsAuth() {
  console.log('\n── serverTransport(authenticate) opens the handshake on connect ──');
  const id = await createNodeIdentity({ lat: 38, lng: -77 });
  const sent = [];
  const { transport, attach } = serverTransport({
    identity: id, authenticate: true,
    sendToConn: (cid, msg) => { sent.push({ cid, msg }); return true; },
    isConnOpen: () => true,
    closeConn: () => {},
  });
  await transport.start();
  attach.added('conn-1');
  await settle();
  const hello = sent.find(s => s.cid === 'conn-1' && s.msg?.payload?.type === AUTH_HELLO_TYPE);
  check('attach.added emitted an auth-hello', hello != null);
  check('auth-hello advertises axona/5', hello?.msg?.payload?.body?.proto === 'axona/5');
  check('auth-hello carries a nonce', typeof hello?.msg?.payload?.body?.nonce === 'string');

  // Factory rejects authenticate mode without a signer.
  let threw = false;
  try {
    serverTransport({ identity: { id: id.id }, authenticate: true,
      sendToConn: () => true, isConnOpen: () => true });
  } catch { threw = true; }
  check('authenticate mode requires a signing identity', threw);
}

async function main() {
  console.log('Axona node transport — axona/4 authenticated handshake smoke');
  await testMutualAuthHappyPath();
  await testForgedIdentityRejected();
  await testLegacyHelloRejected();
  await testServerFactoryBeginsAuth();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
