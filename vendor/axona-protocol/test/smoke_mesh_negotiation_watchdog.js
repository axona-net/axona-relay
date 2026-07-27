// =====================================================================
// smoke_mesh_negotiation_watchdog.js — a peer that NEVER opens a data
//   channel must be torn down so it can't wedge `_peers` forever.
//
// Regression guard for the failed/never-opened wedge (the symmetric twin of
// the 'closed' wedge). A PeerConnection that fails ICE does NOT autonomously
// reach 'closed', and the open-channel death checks only apply ONCE a channel
// is open — so a responder (which gets no retry) stuck in 'failed'/'signaling'/
// 'new' would otherwise sit in `_peers` forever, keeping `hasPeer` true and
// no-op'ing `connectViaRelay`'s idempotency guard permanently: it could never
// reconnect bridgeless.
//
// Since the lifecycle consolidation, the never-opened deadline is enforced by
// the SINGLE reaper (_reapTick) — the same method that, once a channel is open,
// also does pong-timeout / send-fail eviction. This drives _reapTick / _armReaper
// / _retire directly.
//
// Contract:
//   · _reapTick on a live, never-opened entry past its deadline → _retire
//     (hasPeer→false; NO onPeerLost since the channel never opened; deadline
//     cleared so a future re-drive gets a fresh window) → 'reaped-negotiation'.
//   · an entry that opened (openedAt>0) is left alone → 'live'.
//   · a reap on a REPLACED/removed entry is a no-op → 'gone'.
//   · the deadline is ABSOLUTE per peer — preserved across a retry
//     (_retire{keepDeadline:true} keeps it; re-arm reuses it), cleared on a
//     normal _retire.
//   · _scheduleRetry refuses to schedule past the deadline (bounds the loop).
//
// Run: node test/smoke_mesh_negotiation_watchdog.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PEER = 'dd' + 'd4'.repeat(32);

function newMesh() { return new MeshManager({ sendSignal: () => {}, log: () => {} }); }
function fakeState(over = {}) {
  return {
    peerId: PEER, role: 'responder', state: 'failed',
    pc: { close() {} }, dc: null,
    since: Date.now(), openedAt: 0,
    pings: 0, pongs: 0, lastPongAt: 0, rttBuffer: [], pendingCandidates: [],
    pingTimer: null, reaperTimer: null, sendFailures: 0, retryTimer: null,
    retryUsed: false, localCand: null, remoteCand: null, pathPollTimer: null,
    ...over,
  };
}

function main() {
  console.log('MeshManager never-opened negotiation reaper (failed/responder wedge guard)\n');

  // ── responder stuck in 'failed', never opened → torn down, no peer-death ──
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({ role: 'responder', state: 'failed' });
    mesh._peers.set(PEER, st);
    mesh._negotiationDeadline.set(PEER, Date.now() - 1);   // deadline already passed
    check('precondition: hasPeer true while wedged in failed', mesh.hasPeer(PEER));
    const r = mesh._reapTick(st);
    check('never-opened peer reaped at deadline', r === 'reaped-negotiation');
    check('hasPeer → false (connectViaRelay guard clears)', !mesh.hasPeer(PEER));
    check('onPeerLost NOT fired (channel never opened)', lost.length === 0);
    check('absolute deadline cleared on teardown', !mesh._negotiationDeadline.has(PEER));
  }

  // ── an OPENED peer is left alone by the reaper's negotiation branch ───────
  {
    const mesh = newMesh();
    const st = fakeState({ state: 'open', openedAt: Date.now(), lastPongAt: Date.now() });
    mesh._peers.set(PEER, st);
    mesh._negotiationDeadline.set(PEER, Date.now() - 1);   // past, but channel is OPEN
    const r = mesh._reapTick(st);
    check('opened peer NOT torn down (openedAt>0)', mesh._peers.has(PEER) && r === 'live');
  }

  // ── a reap on a REPLACED entry is a no-op ────────────────────────────────
  {
    const mesh = newMesh();
    const stale = fakeState();
    const fresh = fakeState();
    mesh._peers.set(PEER, fresh);
    const r = mesh._reapTick(stale);       // stale !== current live entry
    check('fresh entry NOT torn down by a stale reap', mesh._peers.get(PEER) === fresh && r === 'gone');
  }

  // ── absolute deadline survives a retry, cleared on a normal retire ───────
  {
    const mesh = newMesh();
    const st1 = fakeState({ role: 'offerer' });
    mesh._peers.set(PEER, st1);
    mesh._armReaper(st1);
    const d1 = mesh._negotiationDeadline.get(PEER);
    check('arming sets an absolute deadline', typeof d1 === 'number');
    check('a real reaper timer is armed', st1.reaperTimer != null);

    // retry path: drop entry, KEEP deadline, don't fire onPeerLost
    mesh._retire(PEER, 'retry', { keepDeadline: true, notifyLost: false });
    check('retry teardown preserves the deadline', mesh._negotiationDeadline.get(PEER) === d1);

    const st2 = fakeState({ role: 'offerer' });
    mesh._peers.set(PEER, st2);
    mesh._armReaper(st2);                   // fresh state re-arms against SAME deadline
    check('re-arm after retry reuses the original deadline', mesh._negotiationDeadline.get(PEER) === d1);

    mesh._retire(PEER, 'cleanup');          // normal retire clears it
    check('normal retire clears the deadline', !mesh._negotiationDeadline.has(PEER));
  }

  // ── _scheduleRetry refuses to schedule past the deadline ─────────────────
  {
    const mesh = newMesh();
    const st = fakeState({ role: 'offerer', retryUsed: false });
    mesh._peers.set(PEER, st);
    mesh._negotiationDeadline.set(PEER, Date.now() - 1);   // past
    mesh._scheduleRetry(st);
    check('no retry scheduled past the deadline', st.retryTimer == null && st.retryUsed === false);

    // within the deadline it DOES schedule (bounded loop, not disabled)
    const st2 = fakeState({ role: 'offerer', retryUsed: false });
    mesh._peers.set(PEER, st2);
    mesh._negotiationDeadline.set(PEER, Date.now() + 60_000);
    mesh._scheduleRetry(st2);
    check('retry scheduled while within the deadline', st2.retryTimer != null);
    clearTimeout(st2.retryTimer);           // cleanup
    mesh._negotiationDeadline.delete(PEER);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
