// =====================================================================
// smoke_metrics_kfanout.js — peer.metrics() must aggregate across the WHOLE
// root cohort, not trust any single root's snapshot.
//
// Lineage: this smoke originally guarded the pre-v4.10.1 scatter-gather
// requestMetrics() against reading only the routed root. That path is gone —
// metrics are demand-driven (v4.12.0): each cohort root publishes its own
// signed snapshot to metricTopic(dataId), and peer.metrics() collects them
// over a window and AGGREGATES. The invariant survives in new clothes:
// one root's view is a PARTIAL cohort view, so
//   - `subscribers` is SUMMED (each root reports only its own children)
//   - `current_count` / `seq` / `bytes` are MAXED (they converge via
//     anti-entropy; max tolerates a lagging member)
//   - one node publishing twice must not double-count (freshest per `by` wins)
//
// Run: node test/smoke_metrics_kfanout.js
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

// A metric snapshot envelope exactly as a cohort root's metrics loop builds it:
// the OPEN metricTopic(dataId) descriptor, signed by the publishing root.
async function snapshotEnv(dataId, snapshot, signer) {
  const mt = metricTopic(dataId);
  return buildEnvelope({
    topic: { region: mt.region, owner: null, name: mt.name, write: 'open' },
    message: snapshot, seq: 1, identity: signer, sign: true,
  });
}

// Drive one metrics() read, delivering each cohort snapshot mid-window.
async function readWithSnapshots(peer, am, dataId, snaps, timeoutMs = 600) {
  const p = peer.metrics(dataId, { timeoutMs });
  await delay(30);                                  // let sub() register the metric topic
  const mtId = am.subscribed[am.subscribed.length - 1];
  for (const { snapshot, signer } of snaps) {
    const env = await snapshotEnv(dataId, snapshot, signer);
    am.deliver(mtId, JSON.stringify(env));
  }
  return p;
}

const rootA = await createAuthorIdentity();
const rootB = await createAuthorIdentity();
const rootC = await createAuthorIdentity();

// ── 1. three cohort roots: subscribers summed, counters maxed ──
{
  const { peer, am } = await mkPeer();
  const dataId = await deriveTopicId({ region: 'useast', name: 'kfanout-cohort' });
  const m = await readWithSnapshots(peer, am, dataId, [
    { snapshot: { v: 1, topic: dataId, ts: 100, by: 'root-a', current_count: 3, seq: 9,  subscribers: 2, bytes: 512 }, signer: rootA },
    { snapshot: { v: 1, topic: dataId, ts: 110, by: 'root-b', current_count: 3, seq: 9,  subscribers: 5, bytes: 512 }, signer: rootB },
    { snapshot: { v: 1, topic: dataId, ts: 120, by: 'root-c', current_count: 2, seq: 7,  subscribers: 1, bytes: 300 }, signer: rootC }, // lagging member
  ]);

  assert.equal(m.cohortSize, 3, 'three distinct roots reported');
  assert.equal(m.subscribers, 8, 'subscribers = 2+5+1 summed across the cohort');
  assert.equal(m.current_count, 3, 'current_count maxed — lagging root C tolerated');
  assert.equal(m.seq, 9, 'seq maxed — the true high-water');
  assert.equal(m.bytes, 512, 'bytes maxed');
  assert.equal(m.ts, 120, 'ts from the newest snapshot');
  assert.equal(m.stale, false);
  ok('cohort of 3: subscribers summed, counters maxed, laggard tolerated');
}

// ── 2. one root publishing twice must not double-count; freshest wins ──
{
  const { peer, am } = await mkPeer();
  const dataId = await deriveTopicId({ region: 'useast', name: 'kfanout-dedupe' });
  const m = await readWithSnapshots(peer, am, dataId, [
    { snapshot: { v: 1, topic: dataId, ts: 100, by: 'root-a', current_count: 4, seq: 4, subscribers: 6, bytes: 100 }, signer: rootA },
    { snapshot: { v: 1, topic: dataId, ts: 200, by: 'root-a', current_count: 5, seq: 5, subscribers: 2, bytes: 120 }, signer: rootA }, // fresher: subs dropped 6→2
    { snapshot: { v: 1, topic: dataId, ts:  50, by: 'root-a', current_count: 9, seq: 9, subscribers: 9, bytes: 900 }, signer: rootA }, // stale replay — must be ignored
    { snapshot: { v: 1, topic: dataId, ts: 150, by: 'root-b', current_count: 5, seq: 5, subscribers: 3, bytes: 120 }, signer: rootB },
  ]);

  assert.equal(m.cohortSize, 2, 'two distinct roots — repeats collapse per `by`');
  assert.equal(m.subscribers, 5, 'subscribers = 2+3 — freshest per root, no double-count, stale replay ignored');
  assert.equal(m.current_count, 5, 'current_count from live snapshots only');
  assert.equal(m.ts, 200, 'ts from the newest live snapshot');
  ok('per-root dedupe: freshest snapshot wins, repeats never double-count');
}

// ── 3. snapshot without `by` falls back to the envelope signer as cohort key ──
{
  const { peer, am } = await mkPeer();
  const dataId = await deriveTopicId({ region: 'useast', name: 'kfanout-bykey' });
  const m = await readWithSnapshots(peer, am, dataId, [
    { snapshot: { v: 1, topic: dataId, ts: 100, current_count: 1, seq: 1, subscribers: 1, bytes: 10 }, signer: rootA },
    { snapshot: { v: 1, topic: dataId, ts: 110, current_count: 1, seq: 1, subscribers: 1, bytes: 10 }, signer: rootB },
  ]);

  assert.equal(m.cohortSize, 2, 'distinct envelope signers count as distinct cohort members');
  assert.equal(m.subscribers, 2, 'both counted');
  assert.equal(m.signer, rootB.authorId, 'signer = envelope signer of the newest snapshot');
  ok('missing `by` keys on the envelope signer — cohort still resolves');
}

console.log(`\nsmoke_metrics_kfanout: ${n} checks passed`);
