// =====================================================================
// smoke_kill.js — creator-only message retraction (Phase A #2).
//
//   1. kill.js: buildKill / verifyKill (+ tamper).
//   2. AxonaManager._handleKill: creator-authorized retraction removes the
//      message, tombstones it, delivers a delete marker; a NON-creator kill
//      is rejected; a tombstone blocks resurrection at publish ingress.
//   3. peer.kill input validation.
//
// Run: node test/smoke_kill.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }      from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { buildKill, verifyKill } from '../src/pubsub/kill.js';
import { KillError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const TOPIC_DESC = { region: 'useast', name: 'cats' };   // structured topic for envelopes/peer
const TOPIC_HEX = '89' + 'ab'.repeat(32);          // 66-char hex topic id (raw AM addressing)
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T = 1_700_000_000_000;                       // fixed test clock

function stubDht() {
  return {
    getSelfId:       () => 1n,
    onRoutedMessage: () => {},
    onDirectMessage: () => {},
    onEvent:         () => () => {},
    sendDirect:      async () => true,
    routeMessage:    async () => {},
  };
}

function mkManager() {
  const am = new AxonaManager({ dht: stubDht(), now: () => T });
  const deliveries = [];
  am.onPubsubDelivery((topicId, json) => { try { deliveries.push(JSON.parse(json)); } catch {} });
  return { am, deliveries };
}

// Seed a root role hosting one signed message from `signerId`, with self as
// a subscriber child (so a purge delivers locally).
async function seedRoleWithMessage(am, signerId) {
  const env = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi', identity: signerId, ts: T, seq: T });
  const json = JSON.stringify(env);
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true,
    children: new Map([[am.nodeId, {}]]),     // self is a subscriber
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
  });
  return env;
}

async function testKillObject() {
  console.log('\n── kill.js build / verify / tamper ──');
  const alice = await createAuthorIdentity();
  const kill  = await buildKill({ topicId: TOPIC_HEX, msgId: 'a'.repeat(64), ts: T, seq: T, identity: alice });
  check('kill carries domain kind', kill.kind === 'axona:pubsub-kill:v1');
  check('kill signerPubkey is alice', kill.signerPubkey === alice.pubkeyHex);
  const v = await verifyKill(kill);
  check('verifyKill(intact) → ok', v.ok === true && v.signerPubkey === alice.pubkeyHex);

  const tampered = { ...kill, msgId: 'b'.repeat(64) };
  check('tampered msgId → ok:false', (await verifyKill(tampered)).ok === false);
  const noSig = { ...kill }; delete noSig.signature;
  check('missing signature → ok:false', (await verifyKill(noSig)).ok === false);
}

async function testAuthorizedKill() {
  console.log('\n── creator-authorized kill retracts + tombstones + purges ──');
  const alice = await createAuthorIdentity();
  const { am, deliveries } = mkManager();
  const env = await seedRoleWithMessage(am, alice);

  const kill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice });
  const verdict = await am._handleKill(TOPIC_BIG, kill);

  check('kill consumed', verdict === 'consumed');
  check('message removed from replay cache',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
  check('msgId tombstoned', am._isTombstoned(env.msgId) === true);
  check('delete marker delivered to subscriber',
    deliveries.some(d => d.deleted === true && d.msgId === env.msgId));
  // v0.3 envelopes carry a topic DESCRIPTOR (object), not a topic-name string,
  // so _handleKill's name-only delete marker is null; the marker still routes
  // the retraction to subscribers keyed on msgId (the load-bearing field).
  check('delete marker delivered with no string topic name', deliveries.some(d => d.deleted === true && d.topic === null));
}

async function testTombstoneBlocksResurrection() {
  console.log('\n── tombstone blocks a re-published (resurrected) message ──');
  const alice = await createAuthorIdentity();
  const { am } = mkManager();
  const env = await seedRoleWithMessage(am, alice);
  const json = JSON.stringify(env);

  await am._handleKill(TOPIC_BIG, await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  check('cache empty after kill', am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);

  // A lagging replica re-gossips the same message (fresh publishId).
  await am._onPublish({ topicId: TOPIC_HEX, publisher: null, json, publishId: 'p2', publishTs: T, postHash: env.msgId }, {});
  check('resurrected publish was dropped (cache still empty)',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
}

async function testKillAfterReplayAcquisition() {
  console.log('\n── a message acquired via REPLAY is still killable (postHash preserved) ──');
  const alice = await createAuthorIdentity();
  const { am, deliveries } = mkManager();
  const env  = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi', identity: alice, ts: T, seq: T });
  const json = JSON.stringify(env);

  // Relay hosts the topic but holds NOTHING yet; it acquires the message the
  // way a freshly-subscribed relay does — through a replay batch.
  am.axonRoles.set(TOPIC_BIG, { isRoot: true, children: new Map([[am.nodeId, {}]]), replayCache: [] });
  am._onReplayBatch(
    { topicId: TOPIC_HEX, messages: [{ json, publishId: 'r1', publishTs: T, postHash: env.msgId, publisher: null }] },
    {});
  check('replay populated the cache', am.axonRoles.get(TOPIC_BIG).replayCache.length === 1);
  check('replay-cached entry retained its postHash',
    am.axonRoles.get(TOPIC_BIG).replayCache[0].postHash === env.msgId);

  // Creator kills it — must match and remove despite being replay-acquired.
  const verdict = await am._handleKill(TOPIC_BIG,
    await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  check('kill consumed', verdict === 'consumed');
  check('replay-acquired message removed from cache',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
  check('delete marker delivered', deliveries.some(d => d.deleted === true && d.msgId === env.msgId));
}

async function testReplayDoesNotResurrectKilled() {
  console.log('\n── a replay batch cannot resurrect a killed message (tombstone guard) ──');
  const alice = await createAuthorIdentity();
  const { am } = mkManager();
  const env  = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi', identity: alice, ts: T, seq: T });
  const json = JSON.stringify(env);

  // Host + hold the message, then kill it (tombstone set, cache emptied).
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true, children: new Map(),
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
  });
  await am._handleKill(TOPIC_BIG,
    await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  check('killed: cache empty + tombstoned',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 0 && am._isTombstoned(env.msgId) === true);

  // A lagging replica replays the killed message with a fresh publishId.
  am._onReplayBatch(
    { topicId: TOPIC_HEX, messages: [{ json, publishId: 'r9', publishTs: T, postHash: env.msgId, publisher: null }] },
    {});
  check('replay did NOT resurrect the killed message',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
}

async function testUnauthorizedKillRejected() {
  console.log('\n── a non-creator cannot kill someone else’s message ──');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();
  const { am, deliveries } = mkManager();
  const env = await seedRoleWithMessage(am, alice);     // alice authored it

  const bobKill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: bob });
  await am._handleKill(TOPIC_BIG, bobKill);

  check('message NOT removed (bob is not the creator)',
    am.axonRoles.get(TOPIC_BIG).replayCache.length === 1);
  check('not tombstoned', am._isTombstoned(env.msgId) === false);
  check('no delete delivered', deliveries.length === 0);
}

async function testPeerValidation() {
  console.log('\n── peer.kill input validation ──');
  const node   = await createNodeIdentity({ lat: 51.5074, lng: -0.1278 });
  const author = await createAuthorIdentity();
  const am = new AxonaManager({ dht: stubDht(), now: () => T });
  const peer = new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node });

  // Bad msgId is rejected before any signer check (msgId validated first).
  let e1 = null; try { await peer.kill(TOPIC_DESC, 'not-hex', { signWith: author }); } catch (e) { e1 = e; }
  check('bad msgId → KillError(KILL_INVALID_MSGID)',
    e1 instanceof KillError && e1.code === ErrorCodes.KILL_INVALID_MSGID);

  // v0.3: a kill must be signed by an author key (no node-key fallback) — omitting
  // signWith is the analogue of the old "no identity" case.
  let e2 = null; try { await peer.kill(TOPIC_DESC, 'a'.repeat(64)); } catch (e) { e2 = e; }
  check('no signWith → KillError(KILL_SIGN_FAILED)',
    e2 instanceof KillError && e2.code === ErrorCodes.KILL_SIGN_FAILED);
}

async function testKillRemovesAllDuplicates() {
  console.log('\n── SP-11: kill removes EVERY copy of duplicate-content publishes ──');
  const alice = await createAuthorIdentity();
  const { am } = mkManager();
  // Identical content re-gossiped → same msgId/postHash, distinct wire publishIds,
  // cached as separate entries. A single splice would leave a duplicate alive.
  const env  = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi', identity: alice, ts: T, seq: T });
  const json = JSON.stringify(env);
  am.axonRoles.set(TOPIC_BIG, {
    isRoot: true,
    children: new Map([[am.nodeId, {}]]),
    replayCache: [
      { json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null },
      { json, publishId: 'p2', publishTs: T, postHash: env.msgId, publisher: null },
    ],
  });
  check('seeded two duplicate-content copies', am.axonRoles.get(TOPIC_BIG).replayCache.length === 2);
  await am._handleKill(TOPIC_BIG, await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
  check('one kill removed BOTH copies', am.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
}

async function main() {
  console.log('Axona kill / retraction (Phase A #2) smoke');
  await testKillObject();
  await testAuthorizedKill();
  await testTombstoneBlocksResurrection();
  await testKillAfterReplayAcquisition();
  await testReplayDoesNotResurrectKilled();
  await testUnauthorizedKillRejected();
  await testKillRemovesAllDuplicates();
  await testPeerValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
