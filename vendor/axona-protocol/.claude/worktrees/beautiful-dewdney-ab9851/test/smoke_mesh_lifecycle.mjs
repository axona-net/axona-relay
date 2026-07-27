// =====================================================================
// smoke_mesh_lifecycle.mjs — drive the REAL MeshManager connection-lifecycle
// FSM through every FAILURE state, using a fault-injectable mock
// RTCPeerConnection + a virtual clock.
//
// WHY THIS EXISTS: the connection-lifecycle is where almost every recently-
// found bug lived (closed-wedge SP-1, never-opened/responder-wedge SP-2,
// connectViaRelay flood SP-4). They shipped to "stable" because NO transport
// in the suite can make a live channel FAIL: the sim transport only
// succeeds-or-refuses-at-handshake, and the node-datachannel loopback
// integration tests never fail ICE. So the failure-half of the FSM could only
// be reached by hand-forging a `fakeState` and calling internal methods — the
// leaves were poked, the tree never walked.
//
// This harness installs a mock RTCPeerConnection the test can drive into
// never-open / failed / closed / send-throw, plus a fake clock so the 30 s
// watchdog, 5 s retry and ping/stale timers fire deterministically. It drives
// the REAL _initiateTo / _acceptFrom / _attachPc / dc.onopen / _onConnState /
// watchdog / eviction paths. Each wedge assertion FAILS if its fix is reverted.
//
// Run: node test/smoke_mesh_lifecycle.mjs
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';
import { createFakeClock } from './helpers/fake-clock.mjs';
import { installMockWebRTC, MockRTCPeerConnection } from './helpers/mock-webrtc.mjs';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
// Flush the microtask queue (the fake clock froze setTimeout, so async chains
// in _initiateTo etc. settle only via microtasks).
const flush = async () => { for (let i = 0; i < 40; i++) await Promise.resolve(); };

const ME   = 'a' + '0'.repeat(65);
const PEER = (n) => 'b' + String(n).padStart(2, '0') + '0'.repeat(63);

function newMesh() {
  const signals = [];
  const lost = [];
  const mesh = new MeshManager({ sendSignal: (to, payload) => signals.push({ to, payload }), log: () => {} });
  mesh.setMyId(ME);
  mesh.onPeerLost((id) => lost.push(id));
  return { mesh, signals, lost };
}
const lastPC = () => MockRTCPeerConnection.created.at(-1);

async function main() {
  console.log('MeshManager connection-lifecycle FSM under fault injection\n');
  const clock = createFakeClock();
  const uninstallRTC = installMockWebRTC();
  clock.install();
  try {
    // ── 1. HAPPY PATH: offerer negotiates → channel opens → live ─────────
    {
      console.log('── happy path: offerer → open ──');
      const { mesh } = newMesh();
      mesh.onPeerList([PEER(1)]);
      await flush();
      const pc = lastPC();
      check('offerer created a PeerConnection + data channel', !!pc && !!pc._dc);
      check('peer is pending (negotiating, not open)', mesh.hasPeer(PEER(1)) && !mesh.isConnected(PEER(1)));
      check('counts toward pendingNegotiations', mesh.pendingNegotiations() === 1);
      pc.simConnected();
      pc.simDcOpen();                       // dc.onopen → state 'open'
      await flush();
      check('channel OPEN after dc.onopen', mesh.isConnected(PEER(1)));
      check('no longer counted as pending', mesh.pendingNegotiations() === 0);
      check('timers are armed on the open peer', clock.pending() > 0);
      mesh.dispose();
    }

    // ── 2. SP-2a: never-opened OFFERER is reaped by the watchdog ─────────
    {
      console.log('\n── never-opened offerer → watchdog reap (SP-2) ──');
      const { mesh, lost } = newMesh();
      mesh.onPeerList([PEER(2)]);
      await flush();
      check('precondition: hasPeer true while negotiating', mesh.hasPeer(PEER(2)));
      await clock.advance(31_000);          // past NEGOTIATION_DEADLINE_MS (30 s)
      await flush();
      check('never-opened peer torn down at the deadline', !mesh.hasPeer(PEER(2)));   // ← wedge guard
      check('onPeerLost NOT fired (channel never opened)', lost.length === 0);
      check('pendingNegotiations cleared', mesh.pendingNegotiations() === 0);
      mesh.dispose();
    }

    // ── 3. SP-2b: RESPONDER whose offer never arrives is reaped ─────────
    {
      console.log('\n── responder, offer never arrives → watchdog reap (SP-2) ──');
      const { mesh } = newMesh();
      mesh.onPeerJoined(PEER(3));            // _acceptFrom: state 'new', no PC yet
      check('responder slot held while awaiting offer', mesh.hasPeer(PEER(3)));
      await clock.advance(31_000);
      await flush();
      check('stuck responder torn down (would wedge hasPeer forever pre-fix)', !mesh.hasPeer(PEER(3)));
      mesh.dispose();
    }

    // ── 4. SP-1: a live channel that goes 'closed' frees its slot ───────
    {
      console.log('\n── open channel → connectionState closed → teardown (SP-1) ──');
      const { mesh, lost } = newMesh();
      mesh.onPeerList([PEER(4)]);
      await flush();
      const pc = lastPC();
      pc.simConnected(); pc.simDcOpen(); await flush();
      check('precondition: channel open', mesh.isConnected(PEER(4)));
      pc.simClose();                         // remote close out from under us
      await flush();
      check('closed channel freed the slot (hasPeer → false)', !mesh.hasPeer(PEER(4)));  // ← wedge guard
      check('onPeerLost fired (channel had opened)', lost.includes(PEER(4)));
      mesh.dispose();
    }

    // ── 5. 'failed' → single retry → fresh PC; absolute deadline still reaps
    {
      console.log('\n── failed → retry re-initiates; absolute deadline still reaps ──');
      const { mesh } = newMesh();
      mesh.onPeerList([PEER(5)]);
      await flush();
      const before = MockRTCPeerConnection.created.length;
      lastPC().simFail();                    // connectionState 'failed'
      await flush();
      await clock.advance(5_000);            // RETRY_AFTER_MS → re-initiate
      await flush();
      check('retry created a fresh PeerConnection', MockRTCPeerConnection.created.length === before + 1);
      check('peer still negotiating after retry', mesh.hasPeer(PEER(5)));
      await clock.advance(26_000);           // cross the ORIGINAL 30 s absolute deadline
      await flush();
      check('retry loop is bounded by the absolute deadline (reaped)', !mesh.hasPeer(PEER(5)));
      mesh.dispose();
    }

    // ── 6. open channel whose sends start throwing → send-fail eviction ──
    {
      console.log('\n── open channel, dc.send throws → eviction ──');
      const { mesh, lost } = newMesh();
      mesh.onPeerList([PEER(6)]);
      await flush();
      const pc = lastPC();
      pc.simConnected(); pc.simDcOpen(); await flush();
      check('precondition: open', mesh.isConnected(PEER(6)));
      pc._dc.failSends(true);                // every ping send now throws
      await clock.advance(4_000);            // > SEND_FAIL_LIMIT(3) ping ticks (1 Hz)
      await flush();
      check('send-failing channel evicted', !mesh.hasPeer(PEER(6)));
      check('onPeerLost fired on eviction', lost.includes(PEER(6)));
      mesh.dispose();
    }

    // ── 7. pendingNegotiations cap input: many never-open peers, then reaped
    {
      console.log('\n── pendingNegotiations counts in-flight, falls as watchdog reaps ──');
      const { mesh } = newMesh();
      for (let i = 10; i < 15; i++) mesh.onPeerList([PEER(i)]);
      await flush();
      check('5 concurrent never-open negotiations counted', mesh.pendingNegotiations() === 5);
      await clock.advance(31_000);
      await flush();
      check('all reaped → pendingNegotiations back to 0', mesh.pendingNegotiations() === 0);
      mesh.dispose();
    }

    // ── 8. clean shutdown leaves no live timers ─────────────────────────
    {
      console.log('\n── dispose clears all timers (no leak) ──');
      const { mesh } = newMesh();
      mesh.onPeerList([PEER(20)]);
      await flush();
      lastPC().simConnected(); lastPC().simDcOpen(); await flush();
      const armed = clock.pending();
      mesh.dispose();
      check('open peer had live timers before dispose', armed > 0);
      check('dispose cleared every timer', clock.pending() === 0);
    }
  } finally {
    clock.uninstall();
    uninstallRTC();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('fatal:', e); process.exit(2); });
