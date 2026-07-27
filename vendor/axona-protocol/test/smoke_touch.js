// =====================================================================
// smoke_touch.js — creator-only keep-alive (Phase A #7).
//
//   1. touch.js: buildTouch / verifyTouch (+ tamper).
//   2. AxonaManager._handleTouch: creator-authorized touch resets the hold-time
//      expiry, moves the entry to the head of the queue, and bumps its eviction
//      recency so it survives eviction; a NON-creator touch is rejected; the
//      extension is bounded by the absolute ceiling.
//   3. A message acquired via replay is touchable (postHash preserved).
//   4. peer.touch input validation.
//
// Run: node test/smoke_touch.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }      from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity, computeNodeIdBigInt } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { buildTouch, verifyTouch } from '../src/pubsub/touch.js';
import { fromHex }        from '../src/utils/hexid.js';
import { TouchError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const TOKYO  = { lat: 35.6762, lng: 139.6503 };
const TOPIC_DESC = { region: 'useast', name: 'cats' };   // structured topic for envelopes/peer
const TOPIC_HEX = '89' + 'ab'.repeat(32);
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T = 1_700_000_000_000;
const HOLD_MS = 24 * 60 * 60 * 1000;
const CEIL_MS = 48 * 60 * 60 * 1000;

function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
function mkManager() { return new AxonaManager({ dht: stubDht(), now: () => T }); }

async function aliceMsg(identity) {
  const env  = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi', identity, ts: T, seq: T });
  return { env, json: JSON.stringify(env) };
}

// v0.3 owner anchor for an AUTHOR: the topic anchor's low-256 bits are
// sha256(pubkey) — exactly what computeNodeIdBigInt(pubkey, lat, lng) yields —
// so the owner-gate (sha256(signerPubkey) === anchor suffix) recognizes this
// author as the owner. Replaces the old `fromHex(identity.id)`.
const ownerAnchor = (author, { lat, lng }) => computeNodeIdBigInt(author.pubkey, lat, lng);

async function testTouchObject() {
  console.log('\n── touch.js build / verify / tamper ──');
  const alice = await createAuthorIdentity();
  const touch = await buildTouch({ topicId: TOPIC_HEX, msgId: 'a'.repeat(64), ts: T, seq: T, identity: alice });
  check('touch carries its domain kind', touch.kind === 'axona:pubsub-touch:v1');
  check('touch signerPubkey is alice', touch.signerPubkey === alice.pubkeyHex);
  check('verifyTouch(intact) → ok', (await verifyTouch(touch)).ok === true);
  check('tampered msgId → ok:false',
    (await verifyTouch({ ...touch, msgId: 'b'.repeat(64) })).ok === false);
  const noSig = { ...touch }; delete noSig.signature;
  check('missing signature → ok:false', (await verifyTouch(noSig)).ok === false);
}

async function testOwnerTouchResetsAndHeads() {
  console.log('\n── OWNED topic: the owner touch resets hold, moves to head, bumps recency ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  const aliceAnchor = await ownerAnchor(alice, LONDON);          // owned: anchor low-256 = sha256(alice pubkey) ≠ 0
  // Entry near expiry; another (untouched) entry sits ahead of it.
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true, children: new Map(),
    replayCache: [
      { json: '{}', publishId: 'x', publishTs: T, postHash: 'c'.repeat(64), seq: 5, publisher: aliceAnchor },
      { json, publishId: 'p1', publishTs: T, postHash: env.msgId, seq: 1,
        expiresAt: T + 1000, ceilingAt: T + CEIL_MS, publisher: aliceAnchor },
    ],
  });

  // Alice IS the owner (her pubkey hashes to the anchor suffix).
  await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));

  const cache = am.axonRoles.get(TOPIC_BIG).replayCache;
  const e = cache.find(x => x.postHash === env.msgId);
  check('owner accepted: hold-time expiry reset to now + hold', e.expiresAt === T + HOLD_MS);
  check('touchedTs stamped to the touch ts', e.touchedTs === T);
  check('moved to the head of the queue', cache[0].postHash === env.msgId);
}

async function testTouchBoundedByCeiling() {
  console.log('\n── touch cannot extend past the absolute ceiling ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  // Ceiling is only 1s away — a 24h hold must clamp to it.
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true, children: new Map(),
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, seq: 1,
                    expiresAt: T, ceilingAt: T + 1000 }],
  });
  await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  const e = am.axonRoles.get(TOPIC_BIG).replayCache[0];
  check('expiry clamped to ceilingAt (not now + 24h)', e.expiresAt === T + 1000);
}

async function testTouchedSurvivesEviction() {
  console.log('\n── a touched message is evicted LAST (head of queue) ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  const role = { isRoot: true, children: new Map(), replayCache: [], maxMessages: 2 };
  am.axonRoles.set(TOPIC_BIG, role);

  am._addToReplayCache(role, { json, publishId: 'a', publishTs: T, postHash: env.msgId, seq: 1 });
  await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  // Two newer (higher-seq) but UNTOUCHED messages arrive; queue cap is 2.
  am._addToReplayCache(role, { json: '{}', publishId: 'b', publishTs: T, postHash: 'b'.repeat(64), seq: 2 });
  am._addToReplayCache(role, { json: '{}', publishId: 'c', publishTs: T, postHash: 'd'.repeat(64), seq: 3 });

  check('queue bounded to maxMessages', role.replayCache.length === 2);
  check('touched (older) message survived eviction',
    role.replayCache.some(e => e.postHash === env.msgId));
  check('an untouched message was evicted instead',
    !role.replayCache.some(e => e.postHash === 'b'.repeat(64)));
}

async function testTouchAfterReplayAcquisition() {
  console.log('\n── a message acquired via replay is touchable (postHash preserved) ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  am.axonRoles.set(TOPIC_BIG, { isRoot: true, children: new Map(), replayCache: [] });
  am._onReplayBatch({ topicId: TOPIC_HEX, messages: [{ json, publishId: 'r1', publishTs: T, postHash: env.msgId }] }, {});

  const verdict = await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  check('touch consumed', verdict === 'consumed');
  check('replay-acquired entry was touched', am.axonRoles.get(TOPIC_BIG).replayCache[0].touchedTs === T);
}

async function testOwnedTopicNonOwnerRejected() {
  console.log('\n── OWNED topic: a non-owner cannot touch ──');
  const alice = await createAuthorIdentity();   // owner (anchor)
  const bob   = await createAuthorIdentity();    // stranger
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  const aliceAnchor = await ownerAnchor(alice, LONDON);
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true, children: new Map(),
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, seq: 1,
                    expiresAt: T + 1000, ceilingAt: T + CEIL_MS, publisher: aliceAnchor }],
  });
  await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: bob }));
  const e = am.axonRoles.get(TOPIC_BIG).replayCache[0];
  check('expiry NOT extended (bob is not the owner)', e.expiresAt === T + 1000);
  check('not stamped with touchedTs', e.touchedTs === undefined);
}

async function testOpenTopicAnyoneCanTouch() {
  console.log('\n── UNOWNED (open) topic: anyone may touch ──');
  const alice = await createAuthorIdentity();   // published the message
  const carol = await createAuthorIdentity();    // a stranger, not the author
  const am = mkManager();
  const { env, json } = await aliceMsg(alice);
  const synthAnchor = fromHex('89' + '0'.repeat(64));   // synthetic regional anchor → low-256 = 0 → UNOWNED
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true, children: new Map(),
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, seq: 1,
                    expiresAt: T + 1000, ceilingAt: T + CEIL_MS, publisher: synthAnchor }],
  });
  // Carol is neither the author nor an owner — but the topic is unowned.
  await am._handleTouch(TOPIC_BIG,
    await buildTouch({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: carol }));
  const e = am.axonRoles.get(TOPIC_BIG).replayCache[0];
  check('stranger accepted on an open topic: expiry extended', e.expiresAt === T + HOLD_MS);
  check('touchedTs stamped', e.touchedTs === T);
}

async function testPeerValidation() {
  console.log('\n── peer.touch input validation ──');
  const node   = await createNodeIdentity(LONDON);
  const author = await createAuthorIdentity();
  const am = mkManager();
  const peer = new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node });

  // Bad msgId is rejected before any signer check (msgId validated first).
  let e1 = null; try { await peer.touch(TOPIC_DESC, 'not-hex', { signWith: author }); } catch (e) { e1 = e; }
  check('bad msgId → TouchError(TOUCH_INVALID_MSGID)',
    e1 instanceof TouchError && e1.code === ErrorCodes.TOUCH_INVALID_MSGID);

  // v0.3: a touch must be signed (no node-key fallback) — omitting signWith is
  // the analogue of the old "no identity" case.
  let e2 = null; try { await peer.touch(TOPIC_DESC, 'a'.repeat(64)); } catch (e) { e2 = e; }
  check('no signWith → TouchError(TOUCH_SIGN_FAILED)',
    e2 instanceof TouchError && e2.code === ErrorCodes.TOUCH_SIGN_FAILED);
}

async function main() {
  console.log('Axona touch / keep-alive (Phase A #7) smoke');
  await testTouchObject();
  await testOwnerTouchResetsAndHeads();
  await testTouchBoundedByCeiling();
  await testTouchedSurvivesEviction();
  await testTouchAfterReplayAcquisition();
  await testOwnedTopicNonOwnerRejected();
  await testOpenTopicAnyoneCanTouch();
  await testPeerValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
