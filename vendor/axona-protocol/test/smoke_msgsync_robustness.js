// =====================================================================
// smoke_msgsync_robustness.js — a malformed inbound frame must NOT crash a node.
//
// Reported by a host-node operator quitting many nodes at once (v2.40.0):
//   RangeError: hex id must be 66 chars, got 3
//     at fromHex → _wire → AxonaManager._onMsgSync (anti-entropy handler)
// A peer tearing down mid-shutdown delivered a truncated `fromId` (3 chars). The
// async handler's synchronous throw became a REJECTED PROMISE that the direct-
// dispatch try/catch couldn't see → unhandledRejection → Node process death.
//
// Two fixes, both covered here:
//   A. handler hardening — _onMsgSync / _onMsgSyncResp / _onKillSync parse ids
//      with _wireSafe and DROP a malformed frame instead of throwing.
//   B. dispatch boundary — AxonaPeer's direct-handler dispatch catches an async
//      handler's rejection (not just a sync throw), so NO handler can leak an
//      unhandledRejection.
//
// Run: node test/smoke_msgsync_robustness.js
// =====================================================================

import { AxonaManager }              from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }                 from '../src/dht/AxonaPeer.js';
import { SimNetwork, simTransport }  from '../src/transport/sim/index.js';
import { createNodeIdentity }        from '../src/identity/index.js';
import { toHex, fromHex }            from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const tick  = () => new Promise((r) => setTimeout(r, 0));
const flush = async () => { for (let i = 0; i < 6; i++) await tick(); };

const SELF      = BigInt('0x89' + '11'.repeat(32));
const TOPIC_BIG = BigInt('0x89' + 'ab'.repeat(32));
const T = 1_700_000_000_000;

// Minimal dht mock: records sendDirect calls so we can assert "nothing emitted",
// and CAPTURES the (guarded) handlers AxonaManager registers so we can drive a
// frame through the real registration-boundary guard.
function mockMgr() {
  const sent = [];
  const handlers = new Map();   // type → guarded handler
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: (t, h) => handlers.set(t, h),
    onDirectMessage: (t, h) => handlers.set(t, h),
    onEvent: () => () => {},
    findKClosest: async () => [SELF],
    routeMessage: async () => {},
    sendDirect: async (t, type, p) => { sent.push({ t, type, p }); return true; },
  };
  const mgr = new AxonaManager({ dht, now: () => T });
  return { mgr, sent, handlers };
}
const rootRole = () => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set(), roleCreatedAt: T, emptiedAt: 0 });

async function partA_handlers() {
  console.log('A. handler hardening — malformed frames are dropped, never thrown');

  // 1. malformed topicId (the 3-char value from the report)
  {
    const { mgr, sent } = mockMgr();
    let threw = false;
    try { await mgr._onMsgSync({ topicId: 'abc', have: [] }, { fromId: toHex(SELF) }); } catch { threw = true; }
    check('1. _onMsgSync(topicId="abc") does not throw/reject', !threw);
    check('1b. …and emits nothing', sent.length === 0);
  }

  // 2. valid topic + held message, but malformed fromId (the EXACT reported line)
  {
    const { mgr, sent } = mockMgr();
    mgr.axonRoles.set(TOPIC_BIG, { ...rootRole(), replayCache: [{ postHash: 'a'.repeat(64), json: '{}', publishId: 'p', publishTs: T }] });
    let threw = false;
    try { await mgr._onMsgSync({ topicId: toHex(TOPIC_BIG), have: [] }, { fromId: 'xyz' }); } catch { threw = true; }
    check('2. _onMsgSync(valid topic, fromId="xyz") does not throw/reject', !threw);
    check('2b. …drops it (no msgsync-resp emitted to a garbage requester)', sent.length === 0);
  }

  // 3. _onMsgSyncResp + _onKillSync with malformed topicId
  {
    const { mgr } = mockMgr();
    let threwResp = false, threwKill = false;
    try { await mgr._onMsgSyncResp({ topicId: 'x', messages: [] }, { fromId: toHex(SELF) }); } catch { threwResp = true; }
    try { await mgr._onKillSync({ topicId: '00', kills: [] }, { fromId: toHex(SELF) }); }     catch { threwKill = true; }
    check('3. _onMsgSyncResp(bad topicId) does not throw', !threwResp);
    check('3b. _onKillSync(bad topicId) does not throw', !threwKill);
  }

  // 4. a WELL-FORMED msgsync still works — sibling gets the missing message
  {
    const { mgr, sent } = mockMgr();
    const sib = BigInt('0x89' + '22'.repeat(32));
    mgr.axonRoles.set(TOPIC_BIG, { ...rootRole(), replayCache: [{ postHash: 'b'.repeat(64), json: '{}', publishId: 'p2', publishTs: T }] });
    await mgr._onMsgSync({ topicId: toHex(TOPIC_BIG), have: [] }, { fromId: toHex(sib) });
    await flush();
    const resp = sent.find((s) => s.type === 'pubsub:msgsync-resp');
    check('4. a well-formed msgsync still replies with the missing message', !!resp && resp.p.messages.length === 1);
  }
}

async function partB_dispatchBoundary() {
  console.log('\nB. dispatch boundary — an async handler rejection cannot leak as unhandledRejection');
  const network = new SimNetwork();
  const aliceId = await createNodeIdentity({ lat: 38, lng: -77 });
  const bobId   = await createNodeIdentity({ lat: 38, lng: -77.1 });
  const aliceT = simTransport({ network, identity: aliceId, heartbeatMs: 0 });
  const bobT   = simTransport({ network, identity: bobId, heartbeatMs: 0 });
  await aliceT.start(aliceId.id); await bobT.start(bobId.id);
  await aliceT.openConnection(bobId.id);
  const mk = (id, t) => new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: id.id, alive: true, transport: t }, nodeIdentity: id, transport: t });
  const alice = mk(aliceId, aliceT), bob = mk(bobId, bobT);

  // Watch for ANY unhandled rejection during the test window.
  let leaked = null;
  const onUnhandled = (err) => { leaked = err; };
  process.on('unhandledRejection', onUnhandled);

  // async handler that throws synchronously (→ rejected promise), and a sync one.
  bob.onDirectMessage('robust:asyncthrow', async () => { throw new RangeError('hex id must be 66 chars, got 3'); });
  bob.onDirectMessage('robust:syncthrow',  () => { throw new Error('sync-kaboom'); });

  await alice.sendDirect(fromHex(bobId.id), 'robust:asyncthrow', { x: 1 });
  await alice.sendDirect(fromHex(bobId.id), 'robust:syncthrow',  { x: 1 });
  for (let i = 0; i < 12; i++) await tick();   // give microtasks + the rejection a chance to fire

  process.removeListener('unhandledRejection', onUnhandled);
  check('5. a throwing async direct handler produces NO unhandledRejection', leaked === null);

  try { await aliceT.stop(); await bobT.stop(); } catch { /* */ }
}

async function partC_dispatchGuard() {
  console.log('\nC. AxonaPeer dispatch boundary — a corrupt fromId is dropped ONCE, for every handler');
  // A fake transport lets us invoke the registered onNotification callback with an
  // arbitrary fromId — exactly what a peer tearing down mid-shutdown delivers.
  const captured = new Map();
  const fakeTransport = {
    onNotification: (wt, cb) => captured.set(wt, cb),
    notify: async () => true, onRequest: () => {}, request: async () => null,
    onEvent: () => () => {}, start: async () => {}, stop: async () => {},
  };
  const id = await createNodeIdentity({ lat: 38, lng: -77 });
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: id.id, alive: true, transport: fakeTransport },
    nodeIdentity: id, transport: fakeTransport,
  });

  let calls = 0;
  peer.onDirectMessage('robust:probe', () => { calls++; });
  const cb = captured.get('direct_robust:probe');

  // 6. the reported case generalized: a 3-char sender id, dropped before the handler
  cb('xyz', { x: 1 });
  check('6. a corrupt 3-char fromId is dropped before the handler runs', calls === 0);

  // 7. a well-formed (bigint) sender id passes through to the handler
  cb(fromHex(id.id), { x: 1 });
  await tick();
  check('7. a well-formed fromId reaches the handler', calls === 1);

  // 8. a handler that itself throws a malformed-id error (a payload field the
  //    fromId check doesn't cover) is contained + classified — no crash.
  let leaked = null; const onU = (e) => { leaked = e; };
  process.on('unhandledRejection', onU);
  peer.onDirectMessage('robust:throwid', async () => { fromHex('3a'); });   // throws AXONA_BAD_ID
  captured.get('direct_robust:throwid')(fromHex(id.id), {});
  for (let i = 0; i < 10; i++) await tick();
  process.removeListener('unhandledRejection', onU);
  check('8. a handler that throws on a malformed payload id does not crash the node', leaked === null);
}

async function main() {
  console.log('Axona msgsync / direct-dispatch robustness (malformed-frame DoS guard)');
  await partA_handlers();
  await partB_dispatchBoundary();
  await partC_dispatchGuard();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
