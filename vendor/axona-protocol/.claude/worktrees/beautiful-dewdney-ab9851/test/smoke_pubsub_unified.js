// =====================================================================
// smoke_pubsub_unified.js — verify AxonaPeer.pub() / sub() / stop()
//                            against a mock AxonaManager — v0.3 API.
// Run: node test/smoke_pubsub_unified.js
//
// Covers the unified-API contract (A1 surface), v0.3:
//   - STRUCTURED topic descriptors ({ region, name } open; { owner, name,
//     write:'owner' } owned) — strings are rejected
//   - BigInt topic-id derivation via deriveTopicIdBig (matches publisher)
//   - msgId (64-hex content hash) returned from pub()
//   - { signWith: author } required; { signWith: ANONYMOUS } = unsigned;
//     omitting signWith → PUBLISH_NO_PUBLISH_IDENTITY
//   - envelope descriptor in the wire ({region,owner,name,write}); meta
//     carries postHash (=msgId), NOT publishId
//   - Subscription handle with .stop()
//   - delivery dispatch shape: { msgId, ts, topic, message }
//   - `since` modes seed lastSeenTs correctly
//   - input validation
//
// Full pub/sub end-to-end against the production AxonaManager + Engine
// is exercised by the dht-sim regression suite — those validate the
// wiring layer. The SIGNED-envelope semantics (region-required, owner-only
// pre-check, descriptor in envelope) live in smoke_pubsub_v3.mjs.
// =====================================================================

import { AxonaPeer, ANONYMOUS } from '../src/dht/AxonaPeer.js';
import { Subscription }    from '../src/dht/Subscription.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { PublishError, SubscribeError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// Structured open-topic descriptors (region required for an open topic).
const CATS = { region: 'useast', name: 'cats' };

// ── MockAxonaManager: implements just the surface AxonaPeer needs ─────

class MockAxonaManager {
  constructor() {
    this.published = [];            // [{ topicId, json, meta }]
    this.subscribed = [];           // topicIds (BigInt)
    this.unsubscribed = [];         // topicIds (BigInt)
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `internal:${++this._publishCounter}`;
    this.published.push({ topicId, json, meta, publishId });
    return publishId;
  }
  pubsubSubscribe(topicId) { this.subscribed.push(topicId); }
  pubsubUnsubscribe(topicId) { this.unsubscribed.push(topicId); }
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }

  // Test helper: simulate a delivery.
  triggerDelivery(topicId, json, publishId = 'internal:1', publishTs = Date.now()) {
    if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
  }
}

// ── MockNode + mockEngine just enough for AxonaPeer constructor ──────

async function makePeer({ withAxonaManager = true } = {}) {
  const id      = await createNodeIdentity({ lat: 38, lng: -78 });
  const node    = { id: BigInt('0x' + id.id), alive: true };
  const engine  = { onEvent: (_cb) => () => {}, simEpoch: 0 };
  const am = withAxonaManager ? new MockAxonaManager() : null;
  const peer = new AxonaPeer({ engine, node, axonaManager: am, nodeIdentity: id });
  return { peer, am };
}

let AUTHOR;   // shared author identity for signed publishes

// ── Tests ────────────────────────────────────────────────────────────

async function testPubBasics() {
  console.log('\n── peer.pub() basics ──');
  const { peer, am } = await makePeer();
  const msgId = await peer.pub(CATS, { meow: 1 }, { signWith: AUTHOR });

  check('pub resolves with msgId string (64-hex content hash)',
    typeof msgId === 'string' && msgId.length === 64);
  check('AxonaManager.pubsubPublish called once', am.published.length === 1);
  // Kernel passes BigInt topicId to AxonaManager.
  check('topicId is bigint', typeof am.published[0].topicId === 'bigint');

  const expected = await deriveTopicIdBig({ region: 'useast', name: 'cats', write: 'open' });
  check('topicId = deriveTopicIdBig({region,name})',
    am.published[0].topicId === expected);

  // Envelope is JSON-serialized: { msgId, seq, ts, topic, message, signerPubkey }
  const env = JSON.parse(am.published[0].json);
  check('json envelope has msgId',   env.msgId === msgId);
  check('json envelope carries the topic DESCRIPTOR',
    env.topic && env.topic.name === 'cats' && env.topic.write === 'open' && env.topic.owner === null);
  check('json envelope has message', env.message.meow === 1);
  check('json envelope signed by the author', env.signerPubkey === AUTHOR.authorId);
  // v0.3: meta carries postHash (=msgId), NOT publishId / publisher.
  check('meta.postHash === msgId',   am.published[0].meta.postHash === msgId);
  check('meta carries no publishId', am.published[0].meta.publishId === undefined);
  check('meta carries no publisher', am.published[0].meta.publisher === undefined);
}

async function testAnonymousPub() {
  console.log('\n── peer.pub() anonymous (signWith: ANONYMOUS) ──');
  const { peer, am } = await makePeer();
  const msgId = await peer.pub(CATS, { meow: 1 }, { signWith: ANONYMOUS });
  check('anonymous pub resolves with msgId', typeof msgId === 'string' && msgId.length === 64);
  const env = JSON.parse(am.published[0].json);
  check('anonymous → no signerPubkey', env.signerPubkey === undefined);
}

async function testPubValidation() {
  console.log('\n── peer.pub() validation ──');
  const { peer } = await makePeer();

  // Omitting signWith is an error — no default author, no node-key fallback.
  let err = null;
  try { await peer.pub(CATS, { x: 1 }); } catch (e) { err = e; }
  check('no signer → PUBLISH_NO_PUBLISH_IDENTITY',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY);

  // String topic (legacy) is rejected — topics are structured descriptors now.
  err = null;
  try { await peer.pub('cats', { x: 1 }, { signWith: AUTHOR }); } catch (e) { err = e; }
  check('string topic → PUBLISH_INVALID_TOPIC',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_INVALID_TOPIC);

  // Empty name → invalid topic.
  err = null;
  try { await peer.pub({ region: 'useast', name: '' }, { x: 1 }, { signWith: AUTHOR }); } catch (e) { err = e; }
  check('empty topic name → PUBLISH_INVALID_TOPIC',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_INVALID_TOPIC);

  // Open topic with no region → defaults to the publisher's own node region
  // (never global, never author-derived). The peer supplies its node-ID region
  // byte as the fallback, so the publish succeeds.
  err = null;
  let defMsgId = null;
  try { defMsgId = await peer.pub({ name: 'cats' }, { x: 1 }, { signWith: AUTHOR }); } catch (e) { err = e; }
  check('open topic without region → defaults to node region (no throw)',
    !err && typeof defMsgId === 'string');

  // Cyclic payload explodes during envelope build (msgId hashing).
  err = null;
  const cyclic = {}; cyclic.self = cyclic;
  try { await peer.pub(CATS, cyclic, { signWith: ANONYMOUS }); } catch (e) { err = e; }
  check('un-stringifiable payload → PublishError',
    err instanceof PublishError &&
    (err.code === ErrorCodes.PUBLISH_SIGN_FAILED ||
     err.code === ErrorCodes.PUBLISH_INVALID_MESSAGE));

  // Oversize: fails LOUD with PUBLISH_PAYLOAD_TOO_LARGE (O-5).
  err = null;
  try { await peer.pub(CATS, 'x'.repeat(256 * 1024 + 16), { signWith: ANONYMOUS }); } catch (e) { err = e; }
  check('oversize message → PUBLISH_PAYLOAD_TOO_LARGE',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_PAYLOAD_TOO_LARGE);

  // 32 KiB still over the WebRTC-interop floor (~16 KiB reliable limit, O-5).
  err = null;
  try { await peer.pub(CATS, 'z'.repeat(32 * 1024), { signWith: ANONYMOUS }); } catch (e) { err = e; }
  check('32 KiB message → PUBLISH_PAYLOAD_TOO_LARGE (16 KiB reliable limit, O-5)',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_PAYLOAD_TOO_LARGE);

  // A small message (well under the interop floor) still publishes.
  const okId = await peer.pub(CATS, 'y'.repeat(8 * 1024), { signWith: ANONYMOUS });
  check('under-limit (8 KiB) message publishes', typeof okId === 'string' && okId.length === 64);
}

async function testPubNoManager() {
  console.log('\n── peer.pub() without AxonaManager ──');
  const { peer } = await makePeer({ withAxonaManager: false });
  let err = null;
  try { await peer.pub(CATS, null, { signWith: ANONYMOUS }); } catch (e) { err = e; }
  check('no AxonaManager → PublishError', err instanceof PublishError);
}

async function testSubReceives() {
  console.log('\n── peer.sub() receives delivery as envelope ──');
  const { peer, am } = await makePeer();
  const received = [];
  const sub = await peer.sub(CATS, env => received.push(env));

  check('sub returns Subscription', sub instanceof Subscription);
  check('sub.topicName preserved',  sub.topicName === 'cats');
  // AxonaManager.pubsubSubscribe is called with the BigInt topicId.
  check('AxonaManager.pubsubSubscribe called',
    am.subscribed.length === 1 && am.subscribed[0] === sub.topicIdBig);

  // Synthesize a well-formed v0.3 envelope (topic is the signed descriptor
  // object { region, owner, name, write } → clean-parse path).
  const env = { msgId: '0'.repeat(64), ts: 1234567,
    topic: { region: 'useast', owner: null, name: 'cats', write: 'open' }, message: { hi: 1 } };
  am.triggerDelivery(sub.topicIdBig, JSON.stringify(env), 'internal:7', 999);

  check('handler invoked once', received.length === 1);
  check('envelope.msgId is content-derived (from envelope, not publishId)',
    received[0].msgId === env.msgId);
  // Root time is the single ordering authority (v4.9.1): the delivered `ts` is the
  // root's monotonic delivery stamp (999 here), NOT the publisher's signed claim
  // (1234567) — so every subscriber orders identically regardless of publisher clocks.
  check('envelope.ts is the ROOT delivery stamp, not the publisher claim', received[0].ts === 999);
  check('envelope.topic', received[0].topic?.name === 'cats');
  check('envelope.message', received[0].message.hi === 1);
}

async function testMultipleSubsSameTopic() {
  console.log('\n── multiple subs to same topic both receive ──');
  const { peer, am } = await makePeer();
  const a = [], b = [];
  const subA = await peer.sub(CATS, e => a.push(e));
  const subB = await peer.sub(CATS, e => b.push(e));

  check('AxonaManager.pubsubSubscribe called twice', am.subscribed.length === 2);

  const payload = JSON.stringify({ msgId: '1'.repeat(64), ts: 1, topic: 'cats', message: 'meow' });
  am.triggerDelivery(subA.topicIdBig, payload, 'internal:1', 1);

  check('handler A received', a.length === 1);
  check('handler B received', b.length === 1);
}

async function testSubStopUnsubscribes() {
  console.log('\n── sub.stop() unsubscribes only when last handler gone ──');
  const { peer, am } = await makePeer();
  const a = [], b = [];
  const subA = await peer.sub(CATS, e => a.push(e));
  const subB = await peer.sub(CATS, e => b.push(e));

  await subA.stop();
  check('unsubscribe not called yet (subB still active)', am.unsubscribed.length === 0);
  check('subA.stopped', subA.stopped);

  // Stopped sub no longer receives.
  const payload = JSON.stringify({ msgId: '2'.repeat(64), ts: 2, topic: 'cats', message: 'x' });
  am.triggerDelivery(subA.topicIdBig, payload);
  check('subA handler no longer fires after stop', a.length === 0);
  check('subB handler still fires', b.length === 1);

  // stop is idempotent.
  await subA.stop();
  check('repeat stop is no-op', am.unsubscribed.length === 0);

  await subB.stop();
  check('after last handler stops: unsubscribe called',
    am.unsubscribed.length === 1 && am.unsubscribed[0] === subB.topicIdBig);
}

async function testSubValidation() {
  console.log('\n── peer.sub() validation ──');
  const { peer } = await makePeer();

  let err = null;
  try { await peer.sub({ region: 'useast', name: '' }, () => {}); } catch (e) { err = e; }
  check('empty topic name → PublishError(PUBLISH_INVALID_TOPIC)',
    err instanceof PublishError && err.code === ErrorCodes.PUBLISH_INVALID_TOPIC);

  err = null;
  try { await peer.sub(CATS, 'not-a-fn'); } catch (e) { err = e; }
  check('non-fn handler → SubscribeError(HANDLER_MISSING)',
    err instanceof SubscribeError && err.code === ErrorCodes.SUBSCRIBE_HANDLER_MISSING);
}

async function testSinceModes() {
  console.log('\n── peer.sub({since}) modes ──');
  const { peer, am } = await makePeer();

  // _lastSeenTsByTopic is keyed by BigInt topicId (kernel-internal form).
  const sub1 = await peer.sub({ region: 'useast', name: 'a' }, () => {});
  const ts1 = am._lastSeenTsByTopic.get(sub1.topicIdBig);
  check('default since: lastSeenTs ≈ now (live tail)',
    typeof ts1 === 'number' && ts1 > 0 && ts1 <= Date.now());

  const sub2 = await peer.sub({ region: 'useast', name: 'b' }, () => {}, { since: 'all' });
  check("since:'all' → lastSeenTs = 0",
    am._lastSeenTsByTopic.get(sub2.topicIdBig) === 0);

  // v4.3.0: 'latest' floors at NOW (future tail); the newest retained entry is
  // delivered out-of-band via the replayLatest flag, not via the ts-floor — so
  // the floor is no longer the now-1s window that missed older current values.
  const sub3 = await peer.sub({ region: 'useast', name: 'c' }, () => {}, { since: 'latest' });
  const ts3 = am._lastSeenTsByTopic.get(sub3.topicIdBig);
  check("since:'latest' → lastSeenTs ≈ now",
    typeof ts3 === 'number' && Math.abs(ts3 - Date.now()) < 100);

  const sub4 = await peer.sub({ region: 'useast', name: 'd' }, () => {}, { since: 42 });
  check('since:<number> → lastSeenTs set exactly',
    am._lastSeenTsByTopic.get(sub4.topicIdBig) === 42);

  let err = null;
  try { await peer.sub({ region: 'useast', name: 'e' }, () => {}, { since: 'invalid' }); } catch (e) { err = e; }
  check('since: invalid string → SubscribeError', err instanceof SubscribeError);
}

async function testEngineFallback() {
  console.log('\n── AxonaManager fallback via engine.axonaManagerFor() ──');
  const id   = await createNodeIdentity({ lat: 38, lng: -78 });
  const node = { id: BigInt('0x' + id.id), alive: true };
  const am   = new MockAxonaManager();
  const engine = { onEvent: () => () => {}, axonaManagerFor: (n) => (n === node ? am : null) };
  const peer = new AxonaPeer({ engine, node, nodeIdentity: id });

  const msgId = await peer.pub({ region: 'useast', name: 'topic' }, { hi: 1 }, { signWith: AUTHOR });
  check('engine.axonaManagerFor wired through',
    typeof msgId === 'string' && am.published.length === 1);
}

async function testEngineFallbackMapShape() {
  console.log('\n── AxonaManager fallback via engine._axonaManagers Map ──');
  const id   = await createNodeIdentity({ lat: 38, lng: -78 });
  const node = { id: BigInt('0x' + id.id), alive: true };
  const am   = new MockAxonaManager();
  const engine = { onEvent: () => () => {}, _axonaManagers: new Map([[node.id, am]]) };
  const peer = new AxonaPeer({ engine, node, nodeIdentity: id });

  await peer.pub({ region: 'useast', name: 'topic' }, { hi: 1 }, { signWith: AUTHOR });
  check('engine._axonaManagers Map wired through', am.published.length === 1);
}

async function main() {
  console.log('Axona AxonaPeer.pub() / sub() smoke — v0.3 structured topics');
  AUTHOR = await createAuthorIdentity();
  await testPubBasics();
  await testAnonymousPub();
  await testPubValidation();
  await testPubNoManager();
  await testSubReceives();
  await testMultipleSubsSameTopic();
  await testSubStopUnsubscribes();
  await testSubValidation();
  await testSinceModes();
  await testEngineFallback();
  await testEngineFallbackMapShape();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
