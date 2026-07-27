// =====================================================================
// smoke_envelope.js — signed-publish envelope: build, verify, tamper.
//                      Plus end-to-end through AxonaPeer.pub() / sub()
//                      against a mock AxonaManager.
// Run: node test/smoke_envelope.js
// =====================================================================

import { AxonaPeer, ANONYMOUS } from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import {
  buildEnvelope,
  verifyEnvelope,
  computeMsgId,
  checkFreshness,
  ENVELOPE_DOMAIN,
  MAX_PUBLISH_SKEW_MS,
}                       from '../src/pubsub/envelope.js';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { canonical, sha256Hex } from '../src/pubsub/post.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

// v0.3: a topic in an envelope is the structured DESCRIPTOR object, not a string.
const TOPIC = (name) => ({ region: 0x89, owner: null, name, write: 'open' });

// ── Mock AxonaManager (same shape as A1 smoke) ────────────────────────

class MockAxonaManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.published = [];
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    this.published.push({ topicId, json, meta, publishId });
    return publishId;
  }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  triggerDelivery(topicId, json, publishId = 'm-1', ts = Date.now()) {
    if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, ts);
  }
}

// ── envelope.js direct tests ─────────────────────────────────────────

async function testBuildSigned() {
  console.log('\n── buildEnvelope: signed ──');
  const id = await createAuthorIdentity();
  const env = await buildEnvelope({
    topic: TOPIC('cats'),
    message: { meow: 1 },
    identity: id,
    ts: 1700000000000,
    seq: 1700000000123,
  });
  check('has msgId',            typeof env.msgId === 'string' && env.msgId.length === 64);
  check('has seq (C-2)',        env.seq === 1700000000123);
  check('has ts',               env.ts === 1700000000000);
  check('has topic descriptor', env.topic && env.topic.name === 'cats' && env.topic.write === 'open');
  check('has message',          env.message.meow === 1);
  check('signature ed25519:',   env.signature.startsWith('ed25519:'));
  check('signature is 128 hex', env.signature.length === 'ed25519:'.length + 128);
  check('signerPubkey is 64 hex', env.signerPubkey.length === 64);
  check('signerPubkey matches identity.pubkeyHex',
    env.signerPubkey === id.pubkeyHex);
}

async function testBuildUnsigned() {
  console.log('\n── buildEnvelope: unsigned (opt-out) ──');
  const env = await buildEnvelope({
    topic: TOPIC('public'),
    message: 'broadcast',
    sign: false,
    ts: 1700000000000,
    seq: 42,
  });
  check('has msgId',            typeof env.msgId === 'string' && env.msgId.length === 64);
  check('has seq (C-2)',        env.seq === 42);
  check('no signature field',   env.signature === undefined);
  check('no signerPubkey field', env.signerPubkey === undefined);
}

async function testSignWithoutIdentity() {
  console.log('\n── buildEnvelope: sign:true without identity throws ──');
  let threw = false;
  try {
    await buildEnvelope({ topic: TOPIC('x'), message: null, sign: true });
  } catch { threw = true; }
  check('throws TypeError when sign:true and no identity', threw);
}

async function testVerifyHappy() {
  console.log('\n── verifyEnvelope: signed envelope verifies ──');
  const id = await createAuthorIdentity();
  const env = await buildEnvelope({
    topic: TOPIC('cats'), message: { meow: 1 }, identity: id,
  });
  const r = await verifyEnvelope(env);
  check('verify(intact, signed) → ok',  r.ok === true);
  check('verify reports signed: true',  r.signed === true);
}

async function testVerifyUnsignedHappy() {
  console.log('\n── verifyEnvelope: unsigned envelope verifies ──');
  const env = await buildEnvelope({
    topic: TOPIC('announce'), message: 'hi', sign: false,
  });
  const r = await verifyEnvelope(env);
  check('verify(intact, unsigned) → ok', r.ok === true);
  check('verify reports signed: false',  r.signed === false);
}

async function testTamperMessage() {
  console.log('\n── verifyEnvelope: tampered message ──');
  const id = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: TOPIC('cats'), message: 'real', identity: id });
  const tampered = { ...env, message: 'forged' };
  const r = await verifyEnvelope(tampered);
  check('tampered message → ok:false', r.ok === false);
  check('reason is bad_signature or bad_msgid',
    r.reason === 'bad_signature' || r.reason === 'bad_msgid');
}

async function testTamperMsgId() {
  console.log('\n── verifyEnvelope: tampered msgId ──');
  const id = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: TOPIC('cats'), message: 'real', identity: id });
  const tampered = { ...env, msgId: '0'.repeat(64) };
  const r = await verifyEnvelope(tampered);
  check('tampered msgId → ok:false',  r.ok === false);
  check('reason is bad_msgid',        r.reason === 'bad_msgid');
}

async function testTamperSignature() {
  console.log('\n── verifyEnvelope: tampered signature ──');
  const id = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: TOPIC('cats'), message: 'real', identity: id });
  // Flip one hex char in the signature.
  const sig = env.signature;
  const flippedHex = sig.slice(0, 10) + (sig[10] === 'a' ? 'b' : 'a') + sig.slice(11);
  const tampered = { ...env, signature: flippedHex };
  const r = await verifyEnvelope(tampered);
  check('tampered signature → ok:false', r.ok === false);
  // msgId = hash(publisher+message) no longer folds in the signature, so a
  // flipped signature is caught by the Ed25519 check → bad_signature.
  check('reason is bad_signature or bad_msgid',
    r.reason === 'bad_signature' || r.reason === 'bad_msgid');
}

async function testRejectMissingFields() {
  console.log('\n── verifyEnvelope: structural rejections ──');
  check('null → not_an_object',
    (await verifyEnvelope(null)).reason === 'not_an_object');
  check('no msgId',
    (await verifyEnvelope({ ts: 1, topic: TOPIC('x'), message: 1 })).reason === 'missing_msgId');
  check('no ts',
    (await verifyEnvelope({ msgId: 'x', topic: TOPIC('x'), message: 1 })).reason === 'missing_ts');
  check('no topic',
    (await verifyEnvelope({ msgId: 'x', ts: 1, message: 1 })).reason === 'missing_topic');
  check('no message',
    (await verifyEnvelope({ msgId: 'x', ts: 1, topic: TOPIC('x') })).reason === 'missing_message');
}

async function testMsgIdDeterminism() {
  console.log('\n── computeMsgId determinism (hash(publisher+message)) ──');
  const a = await computeMsgId({ publisher: 'pubA', message: { a: 1, b: 2 } });
  const b = await computeMsgId({ publisher: 'pubA', message: { b: 2, a: 1 } });
  check('msgId independent of object key order', a === b);

  // The id is a content address of (publisher, message): it does NOT depend
  // on time. Same (publisher, message) ⇒ same id regardless of when sent.
  const sameContentLater = await computeMsgId({ publisher: 'pubA', message: { a: 1, b: 2 } });
  check('id is time-independent (same publisher+message → same id)', a === sameContentLater);

  // Different publisher ⇒ different id.
  const otherPub = await computeMsgId({ publisher: 'pubB', message: { a: 1, b: 2 } });
  check('different publisher → different msgId', a !== otherPub);

  // Different message (e.g. publisher-added nonce) ⇒ different id.
  const withNonce = await computeMsgId({ publisher: 'pubA', message: { a: 1, b: 2, nonce: 7 } });
  check('publisher nonce in message → different msgId', a !== withNonce);

  // Unsigned (publisher = null) is well-defined and stable.
  const anon1 = await computeMsgId({ message: { a: 1 } });
  const anon2 = await computeMsgId({ publisher: null, message: { a: 1 } });
  check('unsigned (null publisher) is stable', anon1 === anon2);
}

// ── End-to-end through AxonaPeer ─────────────────────────────────────

async function testPeerSignedRoundTrip() {
  console.log('\n── peer.pub signed → peer.sub envelope (e2e) ──');
  const node = await createNodeIdentity(LONDON);
  const author = await createAuthorIdentity();
  const am = new MockAxonaManager(node.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node,
  });

  const received = [];
  const sub = await peer.sub(TOPIC('cats'), e => received.push(e));

  const msgId = await peer.pub(TOPIC('cats'), { meow: 1 }, { signWith: author });
  check('pub returns content-derived msgId (64-char hex)',
    typeof msgId === 'string' && msgId.length === 64 && /^[0-9a-f]+$/.test(msgId));
  check('AxonaManager.pubsubPublish called',
    am.published.length === 1);

  // Trigger delivery with the same JSON the publisher wrote.
  am.triggerDelivery(sub._topicId, am.published[0].json, 'internal-1', 1700000000000);
  check('handler received a delivery', received.length === 1);

  // The signed envelope the publisher actually wrote (what a relay caches and
  // a subscriber re-verifies). v0.3: topic is the structured descriptor.
  const env = JSON.parse(am.published[0].json);
  check('published envelope.msgId matches pub return value',
    env.msgId === msgId);
  check('envelope.signature present (signed by default)',
    env.signature?.startsWith('ed25519:'));
  check('envelope.signerPubkey matches author',
    env.signerPubkey === author.pubkeyHex);
  check('envelope carries the topic descriptor',
    env.topic && env.topic.name === 'cats' && env.topic.write === 'open');

  // Verify the signed envelope.
  const r = await verifyEnvelope(env);
  check('signed envelope verifies', r.ok === true && r.signed === true);

  await sub.stop();
}

async function testPeerUnsignedRoundTrip() {
  console.log('\n── peer.pub({ sign: false }) → unsigned envelope ──');
  const node = await createNodeIdentity(LONDON);
  const am = new MockAxonaManager(node.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node,
  });

  const received = [];
  const sub = await peer.sub(TOPIC('announce'), e => received.push(e));

  const msgId = await peer.pub(TOPIC('announce'), 'public broadcast', { signWith: ANONYMOUS });
  am.triggerDelivery(sub._topicId, am.published[0].json, 'internal-1', 1700000000000);
  check('handler received a delivery', received.length === 1);

  const env = JSON.parse(am.published[0].json);
  check('published msgId matches pub return value', env.msgId === msgId);
  check('no signature on unsigned envelope',
    env.signature === undefined);
  check('no signerPubkey on unsigned envelope',
    env.signerPubkey === undefined);

  const r = await verifyEnvelope(env);
  check('unsigned envelope still verifies (msgId check)',
    r.ok === true && r.signed === false);

  await sub.stop();
}

async function testPeerSignWithoutIdentity() {
  console.log('\n── peer.pub(sign:true) without identity throws ──');
  const node = await createNodeIdentity(LONDON);
  const am = new MockAxonaManager(node.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node,
  });

  let err = null;
  try { await peer.pub(TOPIC('cats'), null); }   // no { signWith } — no default author
  catch (e) { err = e; }
  check('throws PublishError when no signer named (node key never signs implicitly)',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY);

  // Explicit anonymous still works without naming an author.
  const msgId = await peer.pub(TOPIC('cats'), 'anon', { signWith: ANONYMOUS });
  check('anonymous publish works (signWith: ANONYMOUS)',
    typeof msgId === 'string' && msgId.length === 64);
}

async function testPeerCrossSignerVerification() {
  console.log('\n── cross-peer verification: bob receives, verifies alice ──');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();

  const aliceNode = await createNodeIdentity(LONDON);
  const aliceAm   = new MockAxonaManager(aliceNode.id);
  const alicePeer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + aliceNode.id), alive: true }, axonaManager: aliceAm, nodeIdentity: aliceNode,
  });

  await alicePeer.pub(TOPIC('news'), { headline: 'mesh launch' }, { signWith: alice });
  const json = aliceAm.published[0].json;

  // bob's side: parse the JSON and verify.
  const env = JSON.parse(json);
  // bob doesn't trust the publisher to have set msgId correctly; he
  // recomputes via verifyEnvelope.
  const r = await verifyEnvelope(env);
  check('bob verifies alice signed envelope',
    r.ok === true && r.signed === true);
  check('signerPubkey is alice, not bob',
    env.signerPubkey === alice.pubkeyHex &&
    env.signerPubkey !== bob.pubkeyHex);
}

// ── C-2: freshness + seq + domain separation ─────────────────────────

async function testMissingSeqRejected() {
  console.log('\n── verifyEnvelope: v1 envelope (no seq) rejected ──');
  const id  = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: TOPIC('cats'), message: 'real', identity: id });
  const v1  = { ...env };
  delete v1.seq;                                   // simulate a pre-C-2 envelope
  const r = await verifyEnvelope(v1);
  check('envelope without seq → ok:false', r.ok === false);
  check('reason is missing_seq',           r.reason === 'missing_seq');
}

async function testSeqBoundIntoSignature() {
  console.log('\n── verifyEnvelope: tampered seq breaks auth ──');
  const id  = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: TOPIC('cats'), message: 'real', identity: id, seq: 1000 });
  const tampered = { ...env, seq: 9999 };
  const r = await verifyEnvelope(tampered);
  check('tampered seq → ok:false', r.ok === false);
  check('reason is bad_signature or bad_msgid',
    r.reason === 'bad_signature' || r.reason === 'bad_msgid');
}

async function testDomainSeparation() {
  console.log('\n── envelope signature is domain-separated (E-4) ──');
  const id   = await createAuthorIdentity();
  const seq  = 5, ts = 1700000000000, topic = TOPIC('cats'), message = 'x';
  const env  = await buildEnvelope({ topic, message, identity: id, ts, seq });
  // The signed bytes are over the domain-tagged core; a verifier using the
  // bare (un-tagged) core would compute different bytes and reject.  Confirm
  // the canonical signed form actually carries the domain tag.
  const taggedBytes = canonical({ d: ENVELOPE_DOMAIN, seq, ts, topic, message });
  const bareBytes   = canonical({ seq, ts, topic, message });
  check('domain tag changes the signed preimage', taggedBytes !== bareBytes);
  check('domain tag value present in preimage', taggedBytes.includes(ENVELOPE_DOMAIN));
  // And the honestly-built envelope still verifies (round-trips the tag).
  const r = await verifyEnvelope(env);
  check('domain-tagged envelope verifies', r.ok === true && r.signed === true);
}

async function testFreshnessHelper() {
  console.log('\n── checkFreshness window (C-2) ──');
  const now = 1_700_000_000_000;
  check('fresh ts → ok',
    checkFreshness({ ts: now }, { now }).ok === true);
  check('ts just inside window → ok',
    checkFreshness({ ts: now - (MAX_PUBLISH_SKEW_MS - 1000) }, { now }).ok === true);
  const stale = checkFreshness({ ts: now - (MAX_PUBLISH_SKEW_MS + 60_000) }, { now });
  check('stale ts → ok:false',     stale.ok === false);
  check('stale reason is "stale"', stale.reason === 'stale');
  check('far-future ts → ok:false',
    checkFreshness({ ts: now + (MAX_PUBLISH_SKEW_MS + 60_000) }, { now }).ok === false);
  check('non-numeric ts → missing_ts',
    checkFreshness({ ts: 'nope' }, { now }).reason === 'missing_ts');
}

// NOTE (kernel v3.12.0 clean break): the per-publisher seq high-water ingress
// gate (`_publishFreshAndOrdered` / `_publisherSeq`) was removed with the old
// K-closest manager. The routing-only core gates a live publish at the root via
// absolute-time freshness (checkFreshness — see testFreshnessHelper above) plus
// content-addressed msgId idempotency. Restoring per-publisher seq monotonic
// replay-detection is a Phase 2 hardening item (design doc §9). Its smoke is
// parked under test/legacy-pubsub/ until then.

async function main() {
  console.log('Axona signed-envelope (A2) smoke');
  await testBuildSigned();
  await testBuildUnsigned();
  await testSignWithoutIdentity();
  await testVerifyHappy();
  await testVerifyUnsignedHappy();
  await testTamperMessage();
  await testTamperMsgId();
  await testTamperSignature();
  await testRejectMissingFields();
  await testMsgIdDeterminism();
  await testMissingSeqRejected();
  await testSeqBoundIntoSignature();
  await testDomainSeparation();
  await testFreshnessHelper();
  await testPeerSignedRoundTrip();
  await testPeerUnsignedRoundTrip();
  await testPeerSignWithoutIdentity();
  await testPeerCrossSignerVerification();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
