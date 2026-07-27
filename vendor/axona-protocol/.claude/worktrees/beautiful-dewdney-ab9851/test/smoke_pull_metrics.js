// =====================================================================
// smoke_pull_metrics.js — peer.pull(msgId, {topic}) against a mock
// AxonaManager that implements requestPull.
//
// NOTE (v4.3.0): peer.metrics() no longer scatter-gathers relay counters
// (requestMetrics was removed). The publish-based metrics path is covered
// by smoke_metrics_publish.mjs. This smoke now exercises pull + topic-id
// read-handle behaviour only.
// Run: node test/smoke_pull_metrics.js
// =====================================================================

import { AxonaPeer }       from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }   from '../src/pubsub/envelope.js';
import { deriveTopicId, deriveTopicIdBig } from '../src/pubsub/post.js';
import { fromHex }         from '../src/utils/hexid.js';
import { PullError, ErrorCodes } from '../src/errors.js';

// v0.3: topics are structured descriptors. Use a stable open topic for the
// happy-path / metrics tests (no per-publisher anchoring anymore — dedup is the
// content-addressed msgId).
const CATS = { region: 'useast', name: 'cats' };
const NEWS = { region: 'useast', name: 'news' };

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };

// ── MockAxonaManager with replay cache + counter store ────────────────

class MockAxonaManager {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
    // Replay cache: topicId → [{ json, postHash, publisher }]
    this._replay   = new Map();
    // Counters: topicId → [{ post_hash, delivery_count, pull_count, reshare_count }]
    this._counters = new Map();
  }
  pubsubPublish(topicId, json, meta) {
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    if (!this._replay.has(topicId)) this._replay.set(topicId, []);
    this._replay.get(topicId).push({
      json, postHash: meta?.postHash, publisher: meta?.publisher,
    });
    // Seed counters: bump publishes.
    if (!this._counters.has(topicId)) this._counters.set(topicId, new Map());
    const byHash = this._counters.get(topicId);
    if (meta?.postHash && !byHash.has(meta.postHash)) {
      byHash.set(meta.postHash, {
        post_hash: meta.postHash, delivery_count: 0, pull_count: 0, reshare_count: 0,
      });
    }
    return publishId;
  }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }

  async requestPull(topicId, postHash, { timeoutMs = 1000 } = {}) {
    const cache = this._replay.get(topicId);
    if (!cache) return null;
    for (let i = cache.length - 1; i >= 0; i--) {
      if (cache[i].postHash === postHash) {
        // Bump pull_count.
        const ctr = this._counters.get(topicId)?.get(postHash);
        if (ctr) ctr.pull_count++;
        try { return JSON.parse(cache[i].json); }
        catch { return null; }
      }
    }
    return null;
  }
  async requestMetrics(topicId, _postHashes, { timeoutMs = 500 } = {}) {
    const byHash = this._counters.get(topicId);
    if (!byHash) return [];
    const entries = [...byHash.values()].map(c => ({ ...c }));
    return [{
      responderId: this.nodeId,
      entries,
      timestamp: Date.now(),
      subscribers: (this._replay.get(topicId)?.length ?? 0),  // proxy
      current_count: (this._replay.get(topicId)?.length ?? 0), // live retained
    }];
  }

  // Test helper: bump delivery_count for a post (simulates a relay
  // forwarding the publish to a subscriber).
  _bumpDelivery(topicId, postHash, by = 1) {
    const c = this._counters.get(topicId)?.get(postHash);
    if (c) c.delivery_count += by;
  }
}

// ── Setup helper ─────────────────────────────────────────────────────

async function setupPeer() {
  const node     = await createNodeIdentity(LONDON);
  const author   = await createAuthorIdentity();
  const am       = new MockAxonaManager(node.id);
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node,
  });
  return { peer, am, author };
}

// ── Tests ────────────────────────────────────────────────────────────

async function testPullHappy() {
  console.log('\n── peer.pull() happy path ──');
  const { peer, am, author } = await setupPeer();

  const msgId = await peer.pub(CATS, { meow: 1 }, { signWith: author });
  check('pub succeeded', typeof msgId === 'string');
  // Kernel passes BigInt topicId to AxonaManager now.
  check('replay cache populated with postHash',
    am._replay.get(await deriveTopicIdBig(CATS))?.[0]?.postHash === msgId);

  const pulled = await peer.pull(msgId, { topic: CATS });
  check('pull returned envelope',
    pulled !== null && pulled.msgId === msgId);
  check('pulled envelope.message matches',
    pulled.message.meow === 1);
  check('pulled envelope is signed',
    pulled.signature?.startsWith('ed25519:'));
}

async function testPullMiss() {
  console.log('\n── peer.pull() miss returns null ──');
  const { peer } = await setupPeer();

  // Unknown msgId — not in cache.
  const result = await peer.pull('0'.repeat(64), { topic: CATS });
  check('miss returns null', result === null);
}

async function testPullValidation() {
  console.log('\n── peer.pull() validation ──');
  const { peer } = await setupPeer();

  let err = null;
  try { await peer.pull('short', { topic: CATS }); }
  catch (e) { err = e; }
  check('short msgId → PullError',
    err instanceof PullError && err.code === ErrorCodes.PULL_INVALID_MSGID);

  // v0.3: a malformed/absent topic descriptor is rejected at resolution.
  err = null;
  try { await peer.pull('0'.repeat(64), {}); }   // no topic descriptor
  catch (e) { err = e; }
  check('missing topic → throws', err !== null);

  err = null;
  try { await peer.pull('0'.repeat(64), { topic: { name: 'cats' } }); }   // region omitted
  catch (e) { err = e; }
  check('open topic without region → node-region default (no region error)',
    err === null || !/region/i.test(String(err && err.message)));
}

async function testPullBumpsCounter() {
  console.log('\n── peer.pull() bumps pull_count ──');
  const { peer, am, author } = await setupPeer();

  const msgId = await peer.pub(CATS, 'hi', { signWith: author });
  await peer.pull(msgId, { topic: CATS });

  // BigInt topicId is the kernel-internal key.
  const topicIdBig = await deriveTopicIdBig(CATS);
  const ctr = am._counters.get(topicIdBig).get(msgId);
  check('pull_count incremented', ctr.pull_count === 1);

  // Pull again.
  await peer.pull(msgId, { topic: CATS });
  check('pull_count = 2 after second pull', ctr.pull_count === 2);
}

async function testCrossPublisherIsolation() {
  console.log('\n── owner-topic isolation: distinct owners → distinct topic ids ──');
  const alice = await setupPeer();

  // v0.3: open topics are no longer publisher-scoped, but OWNER topics are —
  // alice's owned feed and bob's owned feed derive distinct topic ids, so a
  // pull against the wrong owner's topic misses (different replay cache entry).
  const aliceFeed = { owner: alice.author.authorId, name: 'feed', write: 'owner' };
  const bobAuthor = await createAuthorIdentity();
  const bobFeed   = { owner: bobAuthor.authorId, name: 'feed', write: 'owner' };

  const m = await alice.peer.pub(aliceFeed, { headline: 'launch' }, { signWith: alice.author });
  const pulled = await alice.peer.pull(m, { topic: aliceFeed });
  check('alice pulls from her own owned feed',
    pulled !== null && pulled.message.headline === 'launch');

  // Same msgId addressed to BOB's owner-topic space → null (different topic id).
  const bobView = await alice.peer.pull(m, { topic: bobFeed });
  check('pull against a different owner-topic returns null',
    bobView === null);
}

async function testReadByTopicId() {
  console.log('\n── read by topic ID (shareable handle) ──');
  const { peer, author } = await setupPeer();

  const msgId = await peer.pub(CATS, { meow: 9 }, { signWith: author });
  const idHex = await deriveTopicId(CATS);                       // 66-hex read handle
  check('topic id is 66 hex', /^[0-9a-f]{66}$/.test(idHex));

  // pull by the raw id (no descriptor) returns the same message
  const pulled = await peer.pull(msgId, { topic: idHex });
  check('pull by topic id returns the message', pulled?.message?.meow === 9);

  // publishing with a bare id is rejected — the id is a read handle, not a write credential
  let err = null;
  try { await peer.pub(idHex, 'x', { signWith: author }); } catch (e) { err = e; }
  check('pub by bare topic id is rejected', err !== null && /read-only handle/.test(err.message));

  // a non-hex string is rejected with a helpful message
  err = null;
  try { await peer.sub('not-an-id', () => {}); } catch (e) { err = e; }
  check('sub with a non-id string is rejected', err !== null && /hex topic ID/.test(err.message));
}

async function main() {
  console.log('Axona pull/metrics (A3) smoke');
  await testPullHappy();
  await testPullMiss();
  await testPullValidation();
  await testPullBumpsCounter();
  await testCrossPublisherIsolation();
  await testReadByTopicId();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
