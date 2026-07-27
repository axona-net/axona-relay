// =====================================================================
// smoke_mesh_peer_left_live.js — the bridge's roster must NOT tear down a
//   live P2P channel (#374 bridge-independence invariant).
//
// A `peer-left` broadcast from the bridge means the peer left the BRIDGE
// (e.g. a bootstrap-nursery graduation 4200, or a transient peer↔bridge
// blip) — NOT that it left the MESH. If our DTLS/data channel to that peer
// is currently OPEN, MeshManager.onPeerLeft must keep it: channel liveness
// (the ping/pong reaper + pc-state transitions) governs teardown on its own,
// so a genuinely-departed peer is still reaped while a still-meshed peer
// survives the bridge telling us it is "gone".
//
// Before the guard, graduation was self-defeating: a graduate kept its own
// mesh, but every witness received `peer-left(graduate)` and slammed its
// healthy channel shut (mesh.onPeerLeft → _retire → dc.close()/pc.close()),
// stranding the graduate (boundPeers → 0) and forcing an immediate re-dial —
// the open→graduate→peers=0→open churn loop. The end-to-end proof over real
// node-datachannel lives in test/integration/graduation_probe.mjs.
//
// Run: node test/smoke_mesh_peer_left_live.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PEER = 'bb' + 'b7'.repeat(32);

function newMesh() { return new MeshManager({ sendSignal: () => {}, log: () => {} }); }
function fakeState(over = {}) {
  let closed = false;
  return {
    peerId: PEER, state: 'open', role: 'offerer',
    openedAt: Date.now() - 30_000,
    lastPongAt: Date.now(), pings: 5, pongs: 5, rttBuffer: [], sendFailures: 0,
    dc: { readyState: 'open', send() {}, close() { closed = true; } },
    pc: { close() { closed = true; } },
    get _closed() { return closed; },
    ...over,
  };
}

function main() {
  console.log('MeshManager onPeerLeft — bridge roster must not kill a live channel (#374)\n');

  // ── OPEN channel + peer-left → IGNORED, channel survives ────────────
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState();                    // state: 'open'
    mesh._peers.set(PEER, st);
    check('precondition: hasPeer true, channel open', mesh.hasPeer(PEER) && st.state === 'open');
    mesh.onPeerLeft(PEER);                      // the bridge says it left
    check('live channel KEPT (entry survives peer-left)', mesh._peers.has(PEER));
    check('DTLS/data channel NOT closed',       st._closed === false);
    check('onPeerLost did NOT fire for a live peer', lost.length === 0);
  }

  // ── non-open channel + peer-left → still retired (real departures) ──
  // A peer that left the bridge AND whose channel is not open is a genuine
  // departure (or a negotiation that never completed): retire as before.
  for (const deadState of ['stale', 'failed', 'signaling', 'datachannel-opening']) {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({ state: deadState });
    mesh._peers.set(PEER, st);
    mesh.onPeerLeft(PEER);
    check(`state='${deadState}': peer-left retires the entry`, !mesh._peers.has(PEER));
    check(`state='${deadState}': underlying pc/dc closed`,     st._closed === true);
  }

  // ── unknown peer + peer-left → no-op, no throw ──────────────────────
  {
    const mesh = newMesh();
    let threw = false;
    try { mesh.onPeerLeft('deadbeef'); } catch { threw = true; }
    check('peer-left for an unknown peer is a safe no-op', !threw);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
