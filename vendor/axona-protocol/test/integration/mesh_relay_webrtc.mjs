// =====================================================================
// mesh_relay_webrtc.mjs — Tier-2 headless REAL-WebRTC harness for
//   PEER-RELAYED SIGNALING (bridgeless connection; design:
//   axona-docs/implementation/Peer-Relayed-Signaling-v0.1.md, §9.5:
//   "headless multi-peer (real WebRTC) — A connects to C through B with
//   the bridge socket closed").
//
// This is the one layer neither the topology proxy (sim-peer-relay.mjs)
// nor the real-kernel routing pass (sim-peer-relay-kernel.mjs) models:
// the actual WebRTC offer/answer/ICE/DTLS negotiation, carried over a
// PEER RELAY instead of the bridge.
//
// SCENARIO (three real RTCPeerConnection peers via node-datachannel):
//
//   1. BOOTSTRAP (models "they met through the bridge at join"):
//      form A↔B and B↔C as real, axona/4-authenticated WebRTC channels.
//      Signaling for THESE two edges is delivered directly (the bridge
//      was up when they joined).
//
//   2. CLOSE THE BRIDGE: the direct signaling fabric is disabled.  From
//      here, the only conduit for SDP/ICE between any non-adjacent pair
//      is a relay through an existing peer.  A guard FAILS the test if
//      any direct-signaling delivery is attempted after this point.
//
//   3. THE EDGE UNDER TEST: A forms a NEW direct channel to C, to which
//      it has no signaling path of its own.  Every offer / answer / ICE
//      candidate is relayed A→B→C and C→B→A over the REAL A↔B and B↔C
//      data channels (B forwards opaque payloads, exactly as the kernel
//      `route_msg`/`mesh:signal` step will).  The bridge is closed.
//
// ASSERTS:
//   · bootstrap: A↔B and B↔C open + axona/4-bound
//   · precondition: A and C are NOT directly connected
//   · the relay was actually exercised (B forwarded signaling frames)
//   · NO direct-signaling delivery happened after the bridge closed
//   · the A↔C REAL DataChannel opens in both directions
//   · DTLS fingerprints are present for A↔C on both ends
//   · axona/4 binds A↔C end-to-end (the design note's safety claim:
//     authenticated regardless of who relayed the SDP)
//   · the resulting A↔C channel is DIRECT: an app message A→C arrives
//     at C without B forwarding it (B is out of the data path)
//
// OPT-IN: not part of `npm test`.  Run with `npm run test:mesh-relay`.
// Requires the optional native dep node-datachannel; SKIPs cleanly
// (exit 0) if it is absent so CI without the native module stays green.
//
//   node test/integration/mesh_relay_webrtc.mjs
// =====================================================================

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function waitFor(predicate, { timeoutMs = 15000, everyMs = 100 } = {}) {
  const t0 = Date.now();
  while (!predicate()) {
    if (Date.now() - t0 > timeoutMs) return false;
    await sleep(everyMs);
  }
  return true;
}

// ── 0. Optional prerequisite — skip gracefully ──────────────────────
let polyfill;
try {
  polyfill = await import('node-datachannel/polyfill');
} catch {
  console.log('SKIP: node-datachannel not installed — `npm i -D node-datachannel` to run this harness.');
  process.exit(0);
}
globalThis.RTCPeerConnection = polyfill.RTCPeerConnection;

// ── 1. Kernel under test (local working source) ─────────────────────
const { createNodeIdentity } = await import('../../src/index.js');
const { MeshManager }    = await import('../../src/transport/web/mesh.js');
const { MeshAuth }       = await import('../../src/transport/web/mesh-auth.js');

const REGIONS = {
  A: { lat: 40.71, lng: -74.0 },   // NYC
  B: { lat: 51.50, lng: -0.12 },   // London
  C: { lat: 35.68, lng: 139.69 },  // Tokyo
};

// Topology: B is the relay. A and B are adjacent; B and C are adjacent;
// A and C are NOT — their only conduit is a relay through B.
const ADJACENT = { A: ['B'], B: ['A', 'C'], C: ['B'] };

let bridgeClosed   = false;   // flips after bootstrap; disables direct signaling
let directAfterClose = 0;     // guard: must stay 0

async function main() {
  console.log('Tier-2 real-WebRTC peer-relayed signaling: A→C through B, bridge closed\n');

  // ── Build three peers, each a real MeshManager + MeshAuth ──────────
  const names = ['A', 'B', 'C'];
  const peers = {};
  for (const name of names) {
    const identity = await createNodeIdentity(REGIONS[name]);
    peers[name] = {
      name,
      identity,
      nodeId: identity.id,        // stable mesh key (66-char hex)
      mesh:   null,
      auth:   null,
      relayCount: 0,              // signaling frames this peer FORWARDED
      appRx:  [],                 // app-test messages received
    };
  }
  const nameByNodeId = (id) => names.find(n => peers[n].nodeId === id) ?? id;

  // sendSignal router: direct for adjacent (bootstrap) edges; relayed
  // through B over the real data channels for the A↔C edge under test.
  function makeSendSignal(srcName) {
    return (targetKey, payload) => {
      const tgtName = nameByNodeId(targetKey);
      if (ADJACENT[srcName].includes(tgtName)) {
        // Direct signaling — only legal while the "bridge" is up.
        if (bridgeClosed) {
          directAfterClose++;
          console.log(`  ! direct signaling attempted after bridge close: ${srcName}→${tgtName}`);
          return;
        }
        setTimeout(() => {
          try { peers[tgtName].mesh.onSignal(peers[srcName].nodeId, payload); }
          catch (err) { console.log(`  ! direct-deliver threw ${srcName}→${tgtName}: ${err.message}`); }
        }, 0);
        return;
      }
      // Non-adjacent (A↔C): relay through B over the REAL src↔B channel.
      const env = { __sigRelay: true, to: targetKey, from: peers[srcName].nodeId, signal: payload };
      try { peers[srcName].mesh.send(peers.B.nodeId, env); }
      catch (err) { console.log(`  ! relay send failed ${srcName}→B: ${err.message}`); }
    };
  }

  for (const name of names) {
    const p = peers[name];
    p.mesh = new MeshManager({ sendSignal: makeSendSignal(name), log: () => {} });
    p.mesh.setMyId(p.nodeId);
    p.auth = new MeshAuth({
      identity:     p.identity,
      send:         (meshId, frame) => p.mesh.send(meshId, frame),
      bindPeer:     (_nodeIdHex, _meshId, _channelKey) => { /* recorded via auth.isBound */ },
      fingerprints: (meshId) => p.mesh.fingerprintsFor(meshId),
      log:          () => {},
    });
    // Run axona/4 when any channel opens.
    p.mesh.onChange((list) => {
      for (const row of Array.isArray(list) ? list : []) {
        if (row?.state === 'open' && typeof row.peerId === 'string') {
          p.auth.onChannelOpen(row.peerId);
        }
      }
    });
    // Single inbound frame router: relay envelopes, axona/4 hellos, app.
    p.mesh.onMessage((fromKey, msg) => {
      if (msg && msg.__sigRelay) {
        if (msg.to === p.nodeId) {
          // Terminal: feed the relayed SDP/ICE into our own mesh.
          p.mesh.onSignal(msg.from, msg.signal);
        } else {
          // Relay hop: forward over our channel toward the target.
          p.relayCount++;
          try { p.mesh.send(msg.to, msg); }
          catch (err) { console.log(`  ! relay forward failed at ${name}: ${err.message}`); }
        }
        return;
      }
      if (msg && msg.k === 'ntf' && msg.type === 'hello')     { p.auth.onHello(fromKey, msg.body);    return; }
      if (msg && msg.k === 'ntf' && msg.type === 'hello-sig') { p.auth.onHelloSig(fromKey, msg.body); return; }
      if (msg && msg.type === 'app-test') { p.appRx.push(msg.text); return; }
    });
  }

  let exitCode = 1;
  try {
    // ── Phase 1: bootstrap A↔B and B↔C (direct signaling = "bridge up")
    console.log('── phase 1: bootstrap A↔B and B↔C (bridge up) ──');
    peers.A.mesh._initiateTo(peers.B.nodeId);   // A offers to B
    peers.B.mesh._initiateTo(peers.C.nodeId);   // B offers to C

    const bootstrapOpen = await waitFor(() =>
      peers.A.mesh.isConnected(peers.B.nodeId) &&
      peers.B.mesh.isConnected(peers.A.nodeId) &&
      peers.B.mesh.isConnected(peers.C.nodeId) &&
      peers.C.mesh.isConnected(peers.B.nodeId), { timeoutMs: 20000 });
    check('bootstrap: A↔B data channel open',  peers.A.mesh.isConnected(peers.B.nodeId));
    check('bootstrap: B↔C data channel open',  peers.B.mesh.isConnected(peers.C.nodeId));

    const bootstrapBound = await waitFor(() =>
      peers.A.auth.isBound(peers.B.nodeId) && peers.B.auth.isBound(peers.A.nodeId) &&
      peers.B.auth.isBound(peers.C.nodeId) && peers.C.auth.isBound(peers.B.nodeId),
      { timeoutMs: 10000 });
    check('bootstrap: A↔B axona/4-bound', peers.A.auth.isBound(peers.B.nodeId));
    check('bootstrap: B↔C axona/4-bound', peers.B.auth.isBound(peers.C.nodeId));
    check('bootstrap converged', bootstrapOpen && bootstrapBound);

    // ── Precondition: A and C are NOT directly connected ─────────────
    check('precondition: A not connected to C', !peers.A.mesh.isConnected(peers.C.nodeId));
    check('precondition: C not connected to A', !peers.C.mesh.isConnected(peers.A.nodeId));

    // ── Phase 2: CLOSE THE BRIDGE ────────────────────────────────────
    console.log('── phase 2: close the bridge (direct signaling disabled) ──');
    bridgeClosed = true;
    const relayBefore = peers.B.relayCount;

    // ── Phase 3: form A↔C with signaling relayed through B ───────────
    console.log('── phase 3: A initiates to C — signaling relayed through B ──');
    peers.A.mesh._initiateTo(peers.C.nodeId);

    const acOpen = await waitFor(() =>
      peers.A.mesh.isConnected(peers.C.nodeId) &&
      peers.C.mesh.isConnected(peers.A.nodeId), { timeoutMs: 20000 });
    check('A↔C REAL DataChannel opened (bridgeless, via relay)', acOpen);

    check('relay was exercised (B forwarded signaling frames)', peers.B.relayCount > relayBefore);
    check('NO direct signaling used after bridge close', directAfterClose === 0);

    // DTLS fingerprints present for the relay-formed channel.
    const fpA = peers.A.mesh.fingerprintsFor(peers.C.nodeId);
    const fpC = peers.C.mesh.fingerprintsFor(peers.A.nodeId);
    check('A↔C DTLS fingerprints present (A side)', !!(fpA && fpA.local && fpA.remote));
    check('A↔C DTLS fingerprints present (C side)', !!(fpC && fpC.local && fpC.remote));
    // Each side's local fp is the other's remote fp (consistent channel).
    if (fpA && fpC) {
      check('A↔C fingerprints cross-match (A.local == C.remote)', fpA.local === fpC.remote);
      check('A↔C fingerprints cross-match (C.local == A.remote)', fpC.local === fpA.remote);
    }

    // ── axona/4 binds A↔C end-to-end (authenticated regardless of relay)
    const acBound = await waitFor(() =>
      peers.A.auth.isBound(peers.C.nodeId) && peers.C.auth.isBound(peers.A.nodeId),
      { timeoutMs: 10000 });
    check('A↔C axona/4-bound end-to-end (auth survives relay)', acBound);

    // ── Phase 4: prove the new channel is DIRECT (B out of data path) ─
    console.log('── phase 4: A→C app message rides the direct channel (B uninvolved) ──');
    const relayAtSend = peers.B.relayCount;
    const token = `direct-${process.pid}`;
    peers.A.mesh.send(peers.C.nodeId, { type: 'app-test', text: token });
    const got = await waitFor(() => peers.C.appRx.includes(token), { timeoutMs: 5000 });
    check('C received A→C app message over the new channel', got);
    check('B did NOT forward the app message (channel is direct)', peers.B.relayCount === relayAtSend);

    exitCode = failed === 0 ? 0 : 1;
  } catch (err) {
    console.error('\nharness threw:', err);
    exitCode = 2;
  } finally {
    for (const name of names) { try { peers[name].mesh.dispose(); } catch {} }
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch {}
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  await sleep(100);
  process.exit(exitCode);
}

main().catch((err) => { console.error('fatal:', err); process.exit(2); });
