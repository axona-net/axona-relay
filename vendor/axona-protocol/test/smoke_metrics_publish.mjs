// =====================================================================
// smoke_metrics_publish.mjs — v4.3.0 metrics-via-publish.
//
// peer.metrics(topic) no longer scatter-gathers the K roots. The topic's
// root PUBLISHES a signed snapshot to metricTopic(dataId) (the relay fleet
// runs this loop); metrics() briefly subscribes to that OPEN metric topic
// and returns the freshest replayed snapshot. This holds for BOTH open and
// owned data topics — an owned topic's activity metrics are public in
// v4.3.0, so anyone can subscribe to an owned topicID's metrics.
//
//   1. open topic: metrics() reads the published snapshot
//   2. envelope signer is preferred over the self-asserted body `signer`
//   3. metrics() cleans up its temporary subscription (pubsubUnsubscribe)
//   4. owned topic: its (public) snapshot reads identically
//   5. no snapshot → stale:true, zeros, signer:null
//
// Run: node test/smoke_metrics_publish.mjs
// =====================================================================
import assert from 'node:assert';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { deriveTopicId } from '../src/pubsub/post.js';
import { metricTopic } from '../src/pubsub/metrics.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';

let n = 0; const ok = (m) => console.log(`  ok ${++n} - ${m}`);
const delay = (ms) => new Promise(r => setTimeout(r, ms));

class MockAM {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this._lastSeenTsByTopic = new Map();
    this._cb = null;
    this.subscribed = [];
    this.unsubscribed = [];
  }
  pubsubPublish() { return 'pub'; }
  pubsubSubscribe(topicId) { this.subscribed.push(topicId); }
  pubsubUnsubscribe(topicId) { this.unsubscribed.push(topicId); }
  onPubsubDelivery(cb) { this._cb = cb; }
  deliver(topicId, json) { this._cb?.(topicId, json, 'pub', Date.now()); }
}

async function mkPeer() {
  const node = await createNodeIdentity({ lat: 38, lng: -78 });
  const engine = { onEvent: () => () => {}, simEpoch: 0 };
  const am = new MockAM(node.id);
  const peer = new AxonaPeer({ engine, node: { id: BigInt('0x' + node.id) }, axonaManager: am, nodeIdentity: node });
  return { peer, am };
}

// Build a metric snapshot envelope exactly as the relay metrics-loop would:
// the OPEN metricTopic(dataId) descriptor, signed by the publishing relay.
async function snapshotEnv(dataId, snapshot, signer) {
  const mt = metricTopic(dataId);
  return buildEnvelope({
    topic: { region: mt.region, owner: null, name: mt.name, write: 'open' },
    message: snapshot, seq: 1, identity: signer, sign: true,
  });
}

// Drive a metrics() read while delivering a snapshot mid-window.
async function readWithSnapshot(peer, am, dataId, snapshot, signer, timeoutMs = 1500) {
  const before = am.subscribed.length;
  const p = peer.metrics(dataId, { timeoutMs });
  await delay(30);                                  // let sub() register the metric topic
  const mtId = am.subscribed[am.subscribed.length - 1];
  assert.equal(am.subscribed.length, before + 1, 'metrics() subscribed to exactly one metric topic');
  const env = await snapshotEnv(dataId, snapshot, signer);
  am.deliver(mtId, JSON.stringify(env));
  return { m: await p, mtId };
}

const relay = await createAuthorIdentity();

// ── 1+2+3. open topic: read snapshot, prefer envelope signer, clean up ──
{
  const { peer, am } = await mkPeer();
  const dataId = await deriveTopicId({ region: 'useast', name: 'lobby' });
  const snapshot = { v: 1, topic: dataId, ts: 1234, by: 'relay-node', signer: 'self-asserted', current_count: 3, subscribers: 7, bytes: 512 };
  const { m, mtId } = await readWithSnapshot(peer, am, dataId, snapshot, relay);

  assert.equal(m.current_count, 3, 'current_count from snapshot');
  assert.equal(m.subscribers, 7, 'subscribers from snapshot');
  assert.equal(m.bytes, 512, 'bytes from snapshot');
  assert.equal(m.ts, 1234, 'ts from snapshot');
  assert.equal(m.stale, false, 'not stale — a snapshot was seen');
  assert.equal(m.signer, relay.authorId, 'envelope signer preferred over self-asserted body field');
  ok('open topic: metrics() reads the published snapshot (envelope signer wins)');

  assert.ok(am.unsubscribed.includes(mtId), 'metrics() tore down its temporary metric subscription');
  ok('metrics() cleans up its temporary subscription');
}

// ── 4. owned topic: its public snapshot reads identically ──
{
  const { peer, am } = await mkPeer();
  const owner = await createAuthorIdentity();
  const ownedId = await deriveTopicId({ region: 'useast', owner: owner.authorId, name: 'feed', write: 'owner' });
  const snapshot = { v: 1, topic: ownedId, ts: 99, by: 'relay-node', current_count: 1, subscribers: 2, bytes: 64 };
  const { m } = await readWithSnapshot(peer, am, ownedId, snapshot, relay);

  assert.equal(m.current_count, 1, 'owned-topic current_count');
  assert.equal(m.subscribers, 2, 'owned-topic subscribers');
  assert.equal(m.bytes, 64, 'owned-topic bytes');
  assert.equal(m.stale, false);
  ok('owned topic: metrics() reads its (public) snapshot — anyone can subscribe');
}

// ── 5. no snapshot → stale ──
{
  const { peer } = await mkPeer();
  const dataId = await deriveTopicId({ region: 'useast', name: 'never-rooted' });
  const m = await peer.metrics(dataId, { timeoutMs: 150 });   // nothing delivered
  assert.equal(m.stale, true, 'stale:true when no snapshot seen');
  assert.equal(m.current_count, 0);
  assert.equal(m.subscribers, 0);
  assert.equal(m.bytes, 0);
  assert.equal(m.signer, null, 'no signer when no snapshot');
  ok('no snapshot → stale:true, zeros, signer:null');
}

console.log(`\nsmoke_metrics_publish: ${n} checks passed`);
