// =====================================================================
// smoke_mesh.js — peer.peers() / onPeerJoin / onPeerLeave smoke.
// Run: node test/smoke_mesh.js
// =====================================================================

import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { isHexId, toHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const SELF  = 'aa' + 'a1'.repeat(32);
const PEER1 = 'bb' + 'b2'.repeat(32);
const PEER2 = 'cc' + 'c3'.repeat(32);

// ── Mock engine that lets us emit events for the peer ───────────────

async function makePeer(synaptomeIds = []) {
  const synaptome = new Map();
  for (const id of synaptomeIds) synaptome.set(id, { peerId: id });
  const node = { id: SELF, alive: true, synaptome };
  const listeners = new Set();
  const engine = {
    onEvent: (cb) => { listeners.add(cb); return () => listeners.delete(cb); },
    emit: (ev) => { for (const cb of listeners) cb(ev); },
  };
  const peer = new AxonaPeer({ engine, node });
  await peer.start();   // installs the engine-event filter chain
  return { peer, engine, node, synaptome };
}

async function testPeersEmpty() {
  console.log('\n── peers() empty ──');
  const { peer } = await makePeer();
  check('peers() on empty synaptome → []',
    Array.isArray(peer.peers()) && peer.peers().length === 0);
}

async function testPeersHex() {
  console.log('\n── peers() returns hex ids ──');
  const { peer } = await makePeer([PEER1, PEER2]);
  const ids = peer.peers();
  check('peers() returns 2 ids', ids.length === 2);
  check('all are 66-char hex',  ids.every(isHexId));
  check('includes PEER1',       ids.includes(PEER1));
  check('includes PEER2',       ids.includes(PEER2));
}

function testPeersBigintCompat() {
  console.log('\n── peers() coerces bigint nodeIds to hex ──');
  // Some engines store nodeIds as bigint; AxonaPeer.peers() should
  // normalise to hex strings.
  const syn = new Map();
  const big = (1n << 263n) | 0xabcdefn;
  syn.set(big, {});
  const node = { id: SELF, alive: true, synaptome: syn };
  const engine = { onEvent: () => () => {} };
  const peer = new AxonaPeer({ engine, node });
  const ids = peer.peers();
  check('bigint key normalised to hex', ids.length === 1 && isHexId(ids[0]));
  check('hex matches toHex(bigint)',    ids[0] === toHex(big));
}

async function testOnPeerJoin() {
  console.log('\n── onPeerJoin fires on peer-joined event ──');
  const { peer, engine } = await makePeer();
  const events = [];
  const unsub = peer.onPeerJoin((id, ev) => events.push({ id, addedBy: ev.addedBy }));

  // Engine emits — needs nodeId/peerId/observerId pointing at us
  // for _eventMentionsSelf to admit it.
  engine.emit({ type: 'peer-joined', peerId: PEER1, addedBy: 'bootstrap', nodeId: SELF });
  check('handler fired once', events.length === 1);
  check('peerId is hex',      events[0].id === PEER1);
  check('addedBy carried',    events[0].addedBy === 'bootstrap');

  engine.emit({ type: 'peer-joined', peerId: PEER2, addedBy: 'ltp', nodeId: SELF });
  check('handler fires again', events.length === 2);
  check('PEER2 id seen',       events[1].id === PEER2);

  unsub();
  engine.emit({ type: 'peer-joined', peerId: PEER1, addedBy: 'x', nodeId: SELF });
  check('unsubscribe stops further events', events.length === 2);
}

async function testOnPeerLeave() {
  console.log('\n── onPeerLeave fires on peer-left event ──');
  const { peer, engine } = await makePeer();
  const ids = [];
  peer.onPeerLeave(id => ids.push(id));

  engine.emit({ type: 'peer-left', peerId: PEER1, reason: 'evicted', nodeId: SELF });
  check('handler fired once', ids.length === 1 && ids[0] === PEER1);

  // peer-joined should NOT fire onPeerLeave handlers.
  engine.emit({ type: 'peer-joined', peerId: PEER2, nodeId: SELF });
  check('peer-joined does not trigger onPeerLeave', ids.length === 1);
}

async function testFiltering() {
  console.log('\n── unrelated events ignored ──');
  const { peer, engine } = await makePeer();
  const joins = [];
  peer.onPeerJoin(id => joins.push(id));

  engine.emit({ type: 'lookup-completed', nodeId: SELF, hops: 3, found: true });
  engine.emit({ type: 'anneal-fired',     nodeId: SELF });
  check('unrelated events do not fire onPeerJoin', joins.length === 0);
}

async function testEventForOtherNodeIgnored() {
  console.log('\n── events for OTHER nodes ignored ──');
  const { peer, engine } = await makePeer();
  const joins = [];
  peer.onPeerJoin(id => joins.push(id));

  // nodeId points at a different peer — _eventMentionsSelf rejects it.
  engine.emit({ type: 'peer-joined', peerId: PEER1, nodeId: PEER2 });
  check('event for other node ignored', joins.length === 0);
}

async function testValidation() {
  console.log('\n── validation ──');
  const { peer } = await makePeer();
  let threw = false;
  try { peer.onPeerJoin('not-a-fn'); } catch { threw = true; }
  check('onPeerJoin rejects non-function', threw);

  threw = false;
  try { peer.onPeerLeave(42); } catch { threw = true; }
  check('onPeerLeave rejects non-function', threw);
}

async function main() {
  console.log('Axona mesh introspection (A5) smoke');
  await testPeersEmpty();
  await testPeersHex();
  testPeersBigintCompat();
  await testOnPeerJoin();
  await testOnPeerLeave();
  await testFiltering();
  await testEventForOtherNodeIgnored();
  await testValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
