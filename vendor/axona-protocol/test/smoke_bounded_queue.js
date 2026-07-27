// =====================================================================
// smoke_bounded_queue.js — bounded replay queue + per-publisher quota
//                          (Phase A #4).
//
//   1. Deterministic eviction by signed (seq, ts, msgId) — NOT insertion
//      order — so replicas converge; maxMessages=1 = retained slot.
//   2. Per-publisher quota on OPEN topics: one signer can't flush the queue.
//   3. Owned topics: no quota (single owner may fill their own queue).
//   4. _openTopicQuota only fires for public-derived topic ids.
//
// Run: node test/smoke_bounded_queue.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { fromHex }        from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
const am = new AxonaManager({ dht: stubDht(), now: () => 1_700_000_000_000 });

function entry(seq, signer, ts = 0) {
  return { json: '{}', publishId: `p${seq}-${signer}`, postHash: `${signer}:${seq}`, seq, ts, signerPubkey: signer };
}
const seqs = (cache) => cache.map(e => e.seq).sort((a, b) => a - b);

function testOrderedEviction() {
  console.log('\n── eviction is by seq, not insertion order ──');
  const role = { replayCache: [], maxMessages: 3 };
  for (const s of [5, 1, 3, 2]) am._addToReplayCache(role, entry(s, 'A'));
  // FIFO would have dropped seq 5 (first in); ordered eviction drops seq 1 (lowest).
  check('bounded to maxMessages', role.replayCache.length === 3);
  check('lowest seq (1) evicted', !role.replayCache.some(e => e.seq === 1));
  check('survivors are the 3 highest seqs', JSON.stringify(seqs(role.replayCache)) === '[2,3,5]');
}

function testRetainedSlot() {
  console.log('\n── maxMessages = 1 is a retained / latest-value slot ──');
  const role = { replayCache: [], maxMessages: 1 };
  am._addToReplayCache(role, entry(10, 'A'));
  am._addToReplayCache(role, entry(20, 'A'));
  am._addToReplayCache(role, entry(15, 'A'));   // older seq than 20 → ignored on eviction
  check('exactly one slot', role.replayCache.length === 1);
  check('retains the highest seq (20)', role.replayCache[0].seq === 20);
}

function testPerPublisherQuotaOpen() {
  console.log('\n── open-topic per-publisher quota self-limits a flooder ──');
  const role = { replayCache: [], maxMessages: 8 };
  const quotaPerPublisher = 2;                  // ceil(8/4)
  for (const s of [1, 2, 3, 4, 5]) am._addToReplayCache(role, entry(s, 'FLOOD'), { quotaPerPublisher });
  for (const s of [6, 7])          am._addToReplayCache(role, entry(s, 'BOB'),   { quotaPerPublisher });

  const flood = role.replayCache.filter(e => e.signerPubkey === 'FLOOD');
  const bob   = role.replayCache.filter(e => e.signerPubkey === 'BOB');
  check('flooder capped at quota (2)', flood.length === 2);
  check('flooder keeps its highest seqs (4,5)', JSON.stringify(seqs(flood)) === '[4,5]');
  check('other publisher unaffected', bob.length === 2);
  check('legit publisher not evicted by the flood', bob.every(e => [6, 7].includes(e.seq)));
}

function testOwnedTopicNoQuota() {
  console.log('\n── owned topic (no quota): owner may fill their own queue ──');
  const role = { replayCache: [], maxMessages: 8 };
  for (const s of [1, 2, 3, 4, 5]) am._addToReplayCache(role, entry(s, 'OWNER'));  // no quota opt
  check('all 5 owner messages kept (under global max)', role.replayCache.length === 5);
}

async function testOpenTopicDetection() {
  console.log('\n── _openTopicQuota fires only for OPEN (write:open) signed descriptors ──');
  const alice = await createAuthorIdentity();
  const role = { replayCache: [] };

  // Open topic: the SIGNED descriptor's write policy is 'open' (anyone publishes).
  const openDesc = { region: 'useast', name: 'room', write: 'open' };
  const pubHex = await deriveTopicId(openDesc);
  const pubEnv = await buildEnvelope({ topic: openDesc, message: 'x', sign: false, ts: 1, seq: 1 });
  const qOpen  = await am._openTopicQuota(role, JSON.stringify(pubEnv), BigInt('0x' + pubHex));
  check('open topic → quota number', typeof qOpen === 'number' && qOpen === Math.ceil(am.replayCacheSize / 4));

  // Owned topic: descriptor names an owner + write:'owner' → no quota.
  const ownDesc = { region: 'useast', owner: alice.authorId, name: 'room', write: 'owner' };
  const ownHex = await deriveTopicId(ownDesc);
  const ownEnv = await buildEnvelope({ topic: ownDesc, message: 'x', identity: alice, ts: 1, seq: 1 });
  const qOwned = await am._openTopicQuota(role, JSON.stringify(ownEnv), BigInt('0x' + ownHex));
  check('owned topic → no quota (null)', qOwned === null);
}

function testLiveCacheCount() {
  console.log('\n── _liveCacheCount: current_count = live, non-expired entries ──');
  const now = 1_700_000_000_000;
  const live1 = { ...entry(1, 'A') };                       // no expiresAt → live
  const live2 = { ...entry(2, 'A'), expiresAt: now + 1000 }; // future → live
  const dead  = { ...entry(3, 'A'), expiresAt: now - 1000 }; // past → expired
  const role  = { replayCache: [live1, live2, dead] };
  check('counts only live entries (2 of 3)', am._liveCacheCount(role) === 2);
  check('empty role → 0', am._liveCacheCount({ replayCache: [] }) === 0);
  check('missing role → 0', am._liveCacheCount(undefined) === 0);
}

async function testMetricsResponseShape() {
  console.log('\n── metricsResp carries current_count + subscribers ──');
  const requester = (await createNodeIdentity({ lat: 0, lng: 0 })).id;  // 66-char hex node id
  let sent = null;
  const dht = {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, routeMessage: async () => {},
    sendDirect: async (_to, _type, payload) => { sent = payload; return true; },
  };
  const now = 1_700_000_000_000;
  const am2 = new AxonaManager({ dht, now: () => now });
  const topicId = 42n;
  // metrics() is owner-only (v3.5.0): the cached posts must be anchored at the
  // requester for the gate to pass, so this verifies the OWNER's response shape.
  const requesterBig = fromHex(requester);
  const role = {
    isRoot: true,
    children: new Map([[10n, {}], [11n, {}], [12n, {}]]),   // 3 direct subscribers
    replayCache: [
      { ...entry(1, 'A'), publisher: requesterBig },                       // live (no expiresAt)
      { ...entry(2, 'A'), publisher: requesterBig, expiresAt: now - 1 },   // expired
    ],
  };
  am2.axonRoles.set(topicId, role);
  const ok = am2._maybeRespondMetrics(
    { requesterId: requester, requestId: 'r1', postHashes: null }, role, topicId);
  check('responded', ok === true);
  check('subscribers = 3 (direct children)', sent?.subscribers === 3);
  check('current_count = 1 (one live, one expired)', sent?.current_count === 1);
}

async function testMetricsOwnership() {
  console.log('\n── metrics gate (v3.5.0): owner-only — open/synthetic/public refused ──');
  const now = 1_700_000_000_000;
  const ownerId    = (await createNodeIdentity({ lat: 38, lng: -77 })).id;   // real anchor
  const ownerBig   = fromHex(ownerId);
  const strangerId = (await createNodeIdentity({ lat: 0,  lng: 0   })).id;
  const synthBig   = fromHex('89' + '0'.repeat(64));   // synthetic regional anchor (low 256 = 0)

  function respond(anchorBig, requesterId) {
    let sent = null;
    const dht = { ...stubDht(), sendDirect: async (_t, _y, p) => { sent = p; return true; } };
    const am2 = new AxonaManager({ dht, now: () => now });
    const role = {
      replayCache: [{ ...entry(1, 'A'), publisher: anchorBig }],
      children: new Map([[1n, {}]]),
    };
    am2.axonRoles.set(99n, role);
    const ok = am2._maybeRespondMetrics({ requesterId, requestId: 'r', postHashes: null }, role, 99n);
    return { ok, sent };
  }

  check('owned topic: non-owner is blocked', respond(ownerBig, strangerId).ok === false);
  check('owned topic: owner is allowed',     respond(ownerBig, ownerId).ok === true);
  // v3.5.0: open/synthetic/public topics no longer answer the scatter-gather at
  // all — their live state is read by subscribing to metricTopic(T) instead.
  check('synthetic/unowned anchor: refused (read via metricTopic)', respond(synthBig, strangerId).ok === false);
}

async function main() {
  console.log('Axona bounded queue + per-publisher quota (Phase A #4) smoke');
  testOrderedEviction();
  testRetainedSlot();
  testPerPublisherQuotaOpen();
  testOwnedTopicNoQuota();
  testLiveCacheCount();
  await testMetricsResponseShape();
  await testMetricsOwnership();
  await testOpenTopicDetection();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
