// =====================================================================
// smoke_mesh_closed_teardown.js — a PC that reaches connectionState
//   'closed' out from under us must FREE its map slot.
//
// Regression guard for the bridgeless-reconnect wedge: before this, the
// 'closed' branch of onconnectionstatechange only set state.state='closed'
// and left the entry in _peers. That kept hasPeer(peer) === true forever,
// so connectViaRelay()'s idempotency guard (ownsPeer || isConnected ||
// hasPeer) no-op'd permanently — a peer whose relayed channel dropped to
// 'closed' could never re-establish a direct connection without the bridge.
//
// Contract (see MeshManager._onConnState):
//   · a LIVE entry reaching 'closed' is torn down → hasPeer → false (so a
//     fresh connectViaRelay/discovery can re-drive), and onPeerLost fires
//     IFF the channel had ever opened (Transport death semantics).
//   · 'failed' is NOT terminal here — it schedules a retry, entry stays.
//   · idempotent: a stale closure firing 'closed' after the entry was
//     replaced by a fresh negotiation must NOT tear down the new entry.
//
// Run: node test/smoke_mesh_closed_teardown.js
// =====================================================================

import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PEER = 'cc' + 'c3'.repeat(32);

function newMesh() { return new MeshManager({ sendSignal: () => {}, log: () => {} }); }
function fakeState(over = {}) {
  return {
    peerId: PEER, state: 'open', role: 'offerer',
    openedAt: Date.now() - 30_000,            // was genuinely open
    lastPongAt: Date.now(), pings: 5, pongs: 5, rttBuffer: [], sendFailures: 0,
    dc: { readyState: 'open', send() {}, close() {} },
    pc: { close() {} },
    ...over,
  };
}

function main() {
  console.log('MeshManager closed-PC teardown (bridgeless re-drive guard)\n');

  // ── live 'closed' entry that WAS open → torn down + onPeerLost ──────
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState();
    mesh._peers.set(PEER, st);
    check('precondition: hasPeer true while live', mesh.hasPeer(PEER));
    mesh._onConnState(st, 'closed');
    check('state marked closed',              st.state === 'closed');
    check('entry removed from _peers',        !mesh._peers.has(PEER));
    check('hasPeer → false (connectViaRelay guard clears)', !mesh.hasPeer(PEER));
    check('onPeerLost fired (channel had opened)', lost.length === 1 && lost[0] === PEER);
  }

  // ── live 'closed' entry that NEVER opened → slot freed, no peer-death
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const st = fakeState({ openedAt: 0 });    // negotiation died before opening
    mesh._peers.set(PEER, st);
    mesh._onConnState(st, 'closed');
    check('never-opened entry removed from _peers', !mesh._peers.has(PEER));
    check('hasPeer → false after never-opened close', !mesh.hasPeer(PEER));
    check('onPeerLost NOT fired (no one was using it)', lost.length === 0);
  }

  // ── idempotency: stale closure must not tear down a REPLACED entry ──
  {
    const mesh = newMesh();
    const lost = [];
    mesh.onPeerLost(id => lost.push(id));
    const stale = fakeState();
    const fresh = fakeState();                // a new negotiation for the same peer
    mesh._peers.set(PEER, fresh);             // fresh is the live entry now
    mesh._onConnState(stale, 'closed');       // stale PC fires late
    check('fresh entry NOT torn down by stale closure', mesh._peers.get(PEER) === fresh);
    check('hasPeer stays true (fresh negotiation intact)', mesh.hasPeer(PEER));
    check('no spurious onPeerLost from stale closure', lost.length === 0);
  }

  // ── 'failed' is not terminal: entry stays (retry path) ─────────────
  {
    const mesh = newMesh();
    const st = fakeState();
    mesh._peers.set(PEER, st);
    mesh._onConnState(st, 'failed');
    check('failed marks state',               st.state === 'failed');
    check('failed keeps entry for retry',     mesh._peers.has(PEER));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
