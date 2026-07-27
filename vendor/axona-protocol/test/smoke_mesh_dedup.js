// =====================================================================
// smoke_mesh_dedup.js — WebRTCTransport duplicate-identity dedup.
//
// When two distinct mesh channels (meshIds) bind the SAME peer nodeId —
// WebRTC glare, or a surviving pre-restart channel plus a fresh
// post-reconnect one — the transport must keep exactly ONE channel and
// tear the other down, deterministically (same survivor on both
// endpoints), without unbinding the surviving route or reporting the
// peer as dead.
//
// Run: node test/smoke_mesh_dedup.js
// =====================================================================

import { WebRTCTransport } from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

// Mock mesh: records disconnect() calls and can replay the resulting
// onPeerLost to the transport (as the real MeshManager._teardown would).
function mockMesh() {
  let peerLostCb = null;
  return {
    disconnected: [],
    onMessage()  { return () => {}; },
    onPeerLost(cb) { peerLostCb = cb; return () => {}; },
    disconnect(meshId, reason) {
      this.disconnected.push({ meshId, reason });
      // Mirror the real mesh: teardown fires onPeerLost for that meshId.
      if (peerLostCb) peerLostCb(meshId);
    },
  };
}

const NODE_A = 0x89aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaan;
const KEY_LO = 'aaa:bbb';   // smaller sorted-nonce key
const KEY_HI = 'ccc:ddd';   // larger

async function newTransport() {
  const mesh = mockMesh();
  const t = new WebRTCTransport({ mesh, localNodeId: 0x01n });
  await t.start();
  return { t, mesh };
}

async function testNewChannelWins() {
  console.log('\n── second channel with smaller key wins; old torn down ──');
  const { t, mesh } = await newTransport();
  const died = [];
  t.onPeerDied(id => died.push(id));

  t.bindPeer(NODE_A, 'old', KEY_HI);   // first channel (higher key)
  t.bindPeer(NODE_A, 'new', KEY_LO);   // duplicate, lower key → new wins

  check('loser "old" disconnected', mesh.disconnected.some(d => d.meshId === 'old'));
  check('winner "new" NOT disconnected', !mesh.disconnected.some(d => d.meshId === 'new'));
  check('active route is "new"', t.meshIdFor(NODE_A) === 'new');
  check('node still bound exactly once', t.boundPeers().filter(x => x === NODE_A).length === 1);
  check('no peer-died fired for the surviving identity', !died.includes(NODE_A));
  check('reverse map: new→node intact', t.nodeIdFor('new') === NODE_A);
  check('reverse map: old cleared', t.nodeIdFor('old') === null);
}

async function testExistingChannelWins() {
  console.log('\n── second channel with larger key loses; new torn down ──');
  const { t, mesh } = await newTransport();
  const died = [];
  t.onPeerDied(id => died.push(id));

  t.bindPeer(NODE_A, 'old', KEY_LO);   // first channel (lower key) → keeps winning
  t.bindPeer(NODE_A, 'new', KEY_HI);   // duplicate, higher key → new loses

  check('loser "new" disconnected', mesh.disconnected.some(d => d.meshId === 'new'));
  check('winner "old" NOT disconnected', !mesh.disconnected.some(d => d.meshId === 'old'));
  check('active route stays "old"', t.meshIdFor(NODE_A) === 'old');
  check('node still bound exactly once', t.boundPeers().filter(x => x === NODE_A).length === 1);
  check('no peer-died fired', !died.includes(NODE_A));
}

async function testOnPeerBoundFiresOnce() {
  console.log('\n── onPeerBound fires once (first bind only, not on dedup) ──');
  const { t } = await newTransport();
  const bound = [];
  t.onPeerBound(id => bound.push(id));

  t.bindPeer(NODE_A, 'old', KEY_HI);
  t.bindPeer(NODE_A, 'new', KEY_LO);   // dedup — must NOT re-fire onPeerBound

  check('onPeerBound fired exactly once', bound.filter(x => x === NODE_A).length === 1);
}

async function testDeterministicAcrossEndpoints() {
  console.log('\n── both endpoints pick the SAME survivor regardless of bind order ──');
  // Endpoint X learns "old" first then "new"; endpoint Y learns "new" first
  // then "old".  Same channelKeys (symmetric).  Both must keep the channel
  // with the smaller key.
  const X = await newTransport();
  X.t.bindPeer(NODE_A, 'chA', KEY_HI);
  X.t.bindPeer(NODE_A, 'chB', KEY_LO);

  const Y = await newTransport();
  Y.t.bindPeer(NODE_A, 'chB', KEY_LO);
  Y.t.bindPeer(NODE_A, 'chA', KEY_HI);

  // The survivor is identified by its key, not its local meshId label.
  const xWinnerKey = X.t._channelKeyByMeshId.get(X.t.meshIdFor(NODE_A));
  const yWinnerKey = Y.t._channelKeyByMeshId.get(Y.t.meshIdFor(NODE_A));
  check('X kept the smaller-key channel', xWinnerKey === KEY_LO);
  check('Y kept the smaller-key channel', yWinnerKey === KEY_LO);
  check('both endpoints agree on survivor key', xWinnerKey === yWinnerKey);
}

async function testLoserDeathDoesNotDropPeer() {
  console.log('\n── a later real death of the loser channel never unbinds winner ──');
  const { t, mesh } = await newTransport();
  const died = [];
  t.onPeerDied(id => died.push(id));

  t.bindPeer(NODE_A, 'old', KEY_HI);
  t.bindPeer(NODE_A, 'new', KEY_LO);   // old torn down (its onPeerLost already replayed)

  // Winner still routes; the duplicate's teardown produced no peer-died.
  check('winner still bound', t.meshIdFor(NODE_A) === 'new');
  check('peer never reported dead', !died.includes(NODE_A));

  // Now the WINNER genuinely dies → THAT must report peer-died + unbind.
  mesh.disconnect('new', 'real-death');
  check('winner death reported peer-died', died.includes(NODE_A));
  check('node fully unbound after winner death', t.meshIdFor(NODE_A) === null);
}

async function main() {
  console.log('Axona mesh duplicate-identity dedup smoke');
  await testNewChannelWins();
  await testExistingChannelWins();
  await testOnPeerBoundFiresOnce();
  await testDeterministicAcrossEndpoints();
  await testLoserDeathDoesNotDropPeer();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
