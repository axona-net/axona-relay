// =====================================================================
// smoke_unpub.js — owner-only topic-queue removal (Phase A #3), v0.3.
//
//   1. unpub.js: buildUnpub / verifyUnpub (+ tamper).
//   2. AxonaManager._handleUnpub: owner-authorized unpub clears the queue
//      (and tombstones the msgIds); {destroy} removes the role; a NON-owner
//      unpub is rejected.
//   3. peer.unpub validation (open topic, missing signer).
//
// v0.3 ownership model: a topic's OWNER is an AUTHOR identity, and the
// owner field IS the Author ID (64-hex pubkey). An unpub is authorized
// self-authenticatingly when (a) signerPubkey === ownerNodeId and (b) the
// owner-only descriptor { region, owner, name, write:'owner' } derives the
// routed topic id. The region byte is recovered from the topic id, so it
// need not travel in the unpub.
//
// Run: node test/smoke_unpub.js
// =====================================================================

import { AxonaManager }        from '../src/pubsub/AxonaManager.js';
import { AxonaPeer }           from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }       from '../src/pubsub/envelope.js';
import { resolveTopic }        from '../src/pubsub/post.js';
import { buildUnpub, verifyUnpub } from '../src/pubsub/unpub.js';
import { UnpubError, ErrorCodes }  from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const T = 1_700_000_000_000;

function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
function mkManager() { return new AxonaManager({ dht: stubDht(), now: () => T }); }

// Resolve an owner-only topic for `owner` (an author) and `name`. Region is
// explicit ('useast') — a topic's region is always a real cell, never derived
// from the author key.
async function ownedTopic(owner, name) {
  return resolveTopic({ region: 'useast', owner: owner.authorId, name, write: 'owner' });
}

// Seed a root role for `owner`'s owned topic `name` with one cached message.
async function seed(am, owner, name) {
  const t = await ownedTopic(owner, name);
  const topicBig = BigInt('0x' + t.topicId);
  const env = await buildEnvelope({
    topic: { region: t.region, owner: t.owner, name, write: 'owner' },
    message: 'hi', identity: owner, ts: T, seq: T,
  });
  am.axonRoles.set(topicBig, {
    isRoot: true, children: new Map(),
    replayCache: [{ json: JSON.stringify(env), publishTs: T, postHash: env.msgId, publisher: null }],
  });
  return { topicHex: t.topicId, topicBig, env };
}

async function testUnpubObject() {
  console.log('\n── unpub.js build / verify / tamper ──');
  const alice = await createAuthorIdentity();
  const t = await ownedTopic(alice, 'cats');
  const u = await buildUnpub({ topicId: t.topicId, topicName: 'cats', ownerNodeId: alice.authorId, destroy: false, ts: T, seq: T, identity: alice });
  check('carries domain kind', u.kind === 'axona:pubsub-unpub:v1');
  check('destroy flag present', u.destroy === false);
  check('ownerNodeId is the Author ID', u.ownerNodeId === alice.authorId);
  check('signerPubkey === owner (self-authorized)', u.signerPubkey === alice.authorId);
  check('verifyUnpub(intact) → ok', (await verifyUnpub(u)).ok === true);
  check('tampered topicName → ok:false', (await verifyUnpub({ ...u, topicName: 'dogs' })).ok === false);
  check('flipped destroy → ok:false', (await verifyUnpub({ ...u, destroy: true })).ok === false);
}

async function testOwnerUnpubClears() {
  console.log('\n── owner unpub clears the queue + tombstones ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { topicHex, topicBig, env } = await seed(am, alice, 'cats');

  const u = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.authorId, destroy: false, ts: T, seq: T, identity: alice });
  const verdict = await am._handleUnpub(topicBig, u);

  check('unpub consumed', verdict === 'consumed');
  check('queue cleared', am.axonRoles.get(topicBig).replayCache.length === 0);
  check('role kept (non-destroy)', am.axonRoles.has(topicBig) === true);
  check('msgId tombstoned (no resurrection)', am._isTombstoned(env.msgId) === true);
}

async function testDestroyRemovesRole() {
  console.log('\n── unpub { destroy } removes the role entirely ──');
  const alice = await createAuthorIdentity();
  const am = mkManager();
  const { topicHex, topicBig } = await seed(am, alice, 'cats');

  const u = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.authorId, destroy: true, ts: T, seq: T, identity: alice });
  await am._handleUnpub(topicBig, u);
  check('role deleted', am.axonRoles.has(topicBig) === false);
}

async function testNonOwnerRejected() {
  console.log('\n── a non-owner cannot unpub someone else’s topic ──');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();
  const am = mkManager();
  const { topicHex, topicBig } = await seed(am, alice, 'cats');   // alice owns it

  // (a) bob claims alice as owner but signs with HIS key → signerPubkey ≠ ownerNodeId.
  const forged = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: alice.authorId, destroy: false, ts: T, seq: T, identity: bob });
  await am._handleUnpub(topicBig, forged);
  check('forged owner (bob signs as alice) rejected — queue intact',
    am.axonRoles.get(topicBig).replayCache.length === 1);

  // (b) bob names HIMSELF as owner → resolveTopic({owner:bob,...}) ≠ alice's topicId.
  const bobOwn = await buildUnpub({ topicId: topicHex, topicName: 'cats', ownerNodeId: bob.authorId, destroy: false, ts: T, seq: T, identity: bob });
  await am._handleUnpub(topicBig, bobOwn);
  check('bob-as-self rejected — queue still intact',
    am.axonRoles.get(topicBig).replayCache.length === 1);
}

async function testPeerValidation() {
  console.log('\n── peer.unpub input validation ──');
  const node  = await createNodeIdentity(LONDON);
  const owner = await createAuthorIdentity();
  const am = mkManager();
  const peer = new AxonaPeer({ engine: { onEvent: () => () => {} }, node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node });

  // Open topics have no owner → cannot be unpubbed.
  let e1 = null;
  try { await peer.unpub({ region: 'useast', name: 'cats' }, { signWith: owner }); } catch (e) { e1 = e; }
  check('open topic → UnpubError(UNPUB_PUBLIC_TOPIC)',
    e1 instanceof UnpubError && e1.code === ErrorCodes.UNPUB_PUBLIC_TOPIC);

  // Owner-only topic but no signer → can't sign the unpub.
  let e2 = null;
  try { await peer.unpub({ owner: owner.authorId, name: 'feed', write: 'owner' }); } catch (e) { e2 = e; }
  check('no signer → UnpubError(UNPUB_SIGN_FAILED)',
    e2 instanceof UnpubError && e2.code === ErrorCodes.UNPUB_SIGN_FAILED);

  // Owner-only topic but the signer is NOT the owner → rejected (publisher-side).
  const other = await createAuthorIdentity();
  let e3 = null;
  try { await peer.unpub({ owner: owner.authorId, name: 'feed', write: 'owner' }, { signWith: other }); } catch (e) { e3 = e; }
  check('signer ≠ owner → UnpubError(UNPUB_SIGN_FAILED)',
    e3 instanceof UnpubError && e3.code === ErrorCodes.UNPUB_SIGN_FAILED);
}

async function main() {
  console.log('Axona unpub / owner queue removal (Phase A #3) smoke — v0.3');
  await testUnpubObject();
  await testOwnerUnpubClears();
  await testDestroyRemovesRole();
  await testNonOwnerRejected();
  await testPeerValidation();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
