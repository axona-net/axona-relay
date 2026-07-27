// =====================================================================
// smoke_mesh_relay_glue.js — webTransport peer-relayed-signaling glue
//   (no real WebRTC). Exercises the index.js seam added for bridgeless
//   connection: the peer-first/bridge-fallback sendSignal sink, the
//   setSignalRelay hook, deliverMeshSignal ingress, connectViaRelay
//   gating, and the capability surface — driving the REAL MeshManager
//   sink closure directly with a fake (never-opened) WebSocket.
//
//   node test/smoke_mesh_relay_glue.js
// =====================================================================

import { webTransport } from '../src/transport/web/index.js';
import { createNodeIdentity } from '../src/index.js';
import { toHex, fromHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

// Fake WebSocket: satisfies the constructor (a function) but is never
// instantiated because we never call transport.start() — we drive the
// mesh sink directly.  socketOpen stays false, so the bridge-fallback
// branch logs 'signal-drop-no-bridge', which we assert on.
class FakeWS { constructor() { /* never opened in this test */ } }

function makeTransport(identity, { meshRelay }) {
  const logs = [];
  const t = webTransport({
    bridgeUrl: 'ws://localhost:0',
    identity,
    autoHandshake: false,           // no bridge handshake / no sign() needed
    reconnect: false,
    meshRelay,
    WebSocketImpl: FakeWS,
    log: (e, d) => logs.push([e, d]),
  });
  return { t, logs };
}

async function main() {
  console.log('webTransport peer-relayed-signaling glue (sink / hooks / gating)\n');
  const me    = await createNodeIdentity({ lat: 40.71, lng: -74.0 });
  const other = await createNodeIdentity({ lat: 35.68, lng: 139.69 });
  const otherHex = other.id;            // 66-char hex nodeId
  const CONN_ID  = 'abc';               // a bridge connId (not hex)

  // ── meshRelay ON: peer-first when destination is a hex nodeId ────────
  {
    const { t, logs } = makeTransport(me, { meshRelay: true });
    const relayCalls = [];
    t.setSignalRelay((toId, payload) => { relayCalls.push({ toId, payload }); return true; });

    // Drive the REAL sink closure MeshManager would call.
    t.mesh._sendSignal(otherHex, { kind: 'sdp-offer', sdp: 'x' });
    check('relay invoked for hex-nodeId destination', relayCalls.length === 1);
    check('relay got the destination + payload',
      relayCalls[0]?.toId === otherHex && relayCalls[0]?.payload?.kind === 'sdp-offer');
    check('no bridge-drop logged when relay took ownership',
      !logs.some(([e]) => e === 'signal-drop-no-bridge'));

    // connId (bridge form) is NOT relayed — stays on the bridge path.
    t.mesh._sendSignal(CONN_ID, { kind: 'ice', candidate: {} });
    check('relay NOT invoked for connId destination', relayCalls.length === 1);
    check('connId signal fell back to bridge (drop logged, socket closed)',
      logs.some(([e, d]) => e === 'signal-drop-no-bridge' && d?.to === CONN_ID));
  }

  // ── relay declines (returns false) → bridge fallback ─────────────────
  {
    const { t, logs } = makeTransport(me, { meshRelay: true });
    t.setSignalRelay(() => false);     // e.g. not meshed / cold bootstrap
    t.mesh._sendSignal(otherHex, { kind: 'sdp-offer', sdp: 'x' });
    check('relay-declined hex signal falls back to bridge',
      logs.some(([e, d]) => e === 'signal-drop-no-bridge' && d?.to === otherHex));
  }

  // ── meshRelay OFF: relay is never consulted (pure bridge behaviour) ──
  {
    const { t, logs } = makeTransport(me, { meshRelay: false });
    let relayCalled = false;
    t.setSignalRelay(() => { relayCalled = true; return true; });
    t.mesh._sendSignal(otherHex, { kind: 'sdp-offer', sdp: 'x' });
    check('relay NOT consulted when meshRelay is off', relayCalled === false);
    check('signal went to bridge path when meshRelay off',
      logs.some(([e, d]) => e === 'signal-drop-no-bridge' && d?.to === otherHex));
  }

  // ── deliverMeshSignal feeds the mesh signaling state machine ─────────
  {
    const { t } = makeTransport(me, { meshRelay: true });
    const seen = [];
    const orig = t.mesh.onSignal.bind(t.mesh);
    t.mesh.onSignal = (from, payload) => { seen.push({ from, payload }); return orig(from, payload); };
    let threw = false;
    try { t.deliverMeshSignal(otherHex, { kind: 'ice', candidate: {} }); }
    catch { threw = true; }
    check('deliverMeshSignal invokes mesh.onSignal', seen.length === 1 && seen[0].from === otherHex);
    check('deliverMeshSignal does not throw for an unknown peer', !threw);
  }

  // ── connectViaRelay gating ───────────────────────────────────────────
  {
    const { t: tOff } = makeTransport(me, { meshRelay: false });
    check('connectViaRelay returns false when meshRelay off', tOff.connectViaRelay(otherHex) === false);

    const { t: tOn } = makeTransport(me, { meshRelay: true });
    check('connectViaRelay returns false for self', tOn.connectViaRelay(me.id) === false);
    check('connectViaRelay returns false for a non-hex id', tOn.connectViaRelay('abc') === false);
  }

  // ── connectViaRelay backpressure: cap concurrent in-flight negotiations ──
  {
    const { t: tOn } = makeTransport(me, { meshRelay: true });
    check('pendingNegotiations() starts at 0', tOn.mesh.pendingNegotiations() === 0);
    // An OPEN peer (openedAt>0) does not count; only never-opened negotiations do.
    tOn.mesh._peers.set('open-peer', { openedAt: Date.now() });
    for (let i = 0; i < 64; i++) tOn.mesh._peers.set('neg-' + i, { openedAt: 0 });
    check('pendingNegotiations() counts only never-opened (64, not 65)',
      tOn.mesh.pendingNegotiations() === 64);
    // At the cap, a NEW relay connect to an unrelated peer is throttled (returns
    // false BEFORE _initiateTo — so no RTCPeerConnection is needed in this unit
    // env). This is the DoS bound: a flood of fabricated peerIds can't drive
    // unbounded concurrent negotiations.
    check('connectViaRelay throttled at the pending-negotiation cap',
      tOn.connectViaRelay(otherHex) === false);
    // The watchdog reaps stuck never-opened entries; emulate one freeing → the
    // count drops below the ceiling, so a future connect would proceed again.
    tOn.mesh._peers.delete('neg-0');
    check('cap self-frees as negotiations are reaped', tOn.mesh.pendingNegotiations() === 63);
  }

  // ── capability surface ───────────────────────────────────────────────
  {
    const { t: tOn }  = makeTransport(me, { meshRelay: true });
    const { t: tOff } = makeTransport(me, { meshRelay: false });
    check("capabilities() includes 'mesh-relay' when on", tOn.hasCapability('mesh-relay') === true);
    check("capabilities() empty when off", tOff.capabilities().length === 0);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => { console.error('smoke_mesh_relay_glue threw:', err); process.exit(2); });
