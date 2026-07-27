// =====================================================================
// repro_howard_chunks.mjs — reproduce Howard's chunked-transfer report.
//
//   "Send a count + 4 chunks under a unique topic, then subscribe in the
//    SAME session. USUALLY get only the last chunk; rarely all five. A
//    second browser open throughout gets all five (live). Reload either
//    browser → ZERO on that subscription."
//
// We drive real AxonaManager instances through a MockNet whose sendDirect
// dispatches to the target's registered handler (incl. self — matching the
// AxonaPeer default-dht self-dispatch at AxonaPeer.js:2380). Public topic,
// monotonic seq, distinct messages — exactly the app's shape.
//
//   node test/repro_howard_chunks.mjs
// =====================================================================
import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { deriveIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { toHex, fromHex } from '../src/utils/hexid.js';

const tick  = () => new Promise((r) => setTimeout(r, 0));
const flush = async () => { for (let i = 0; i < 12; i++) await tick(); };

// MockNet: sendDirect dispatches to the target manager's handler, self
// included (mirrors AxonaPeer default-dht). findKClosest = XOR-nearest over
// the live node set. Clock advances per call so seq/ts are monotonic + fresh.
class MockNet {
  constructor() { this.mgrs = new Map(); this.clock = 1_700_000_000_000; }
  now() { return this.clock; }
  kclosest(target, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }
  makeDht(selfId) {
    const net = this, direct = new Map(), routed = new Map();
    return {
      getSelfId: () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent: () => () => {},
      findKClosest: async (target, K) => net.kclosest(target, K),
      routeMessage: async () => {},
      sendDirect: async (target, type, payload) => {
        const m = net.mgrs.get(target);
        if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _direct: direct, _routed: routed,
    };
  }
  spawn(selfId, label) {
    const dht = this.makeDht(selfId);
    const net = this;
    const mgr = new AxonaManager({ dht, now: () => net.now() });
    mgr._dht = dht; mgr._label = label;
    this.mgrs.set(selfId, mgr);
    return mgr;
  }
}

// Collect app deliveries per manager.
function sink(mgr) {
  const got = [];
  mgr.onPubsubDelivery((topicId, json) => {
    let msg = json; try { msg = JSON.parse(json).message; } catch {}
    got.push(msg);
  });
  return got;
}

async function publishN(net, pub, topicBig, msgs, idEnv) {
  for (const message of msgs) {
    net.clock += 1000;                         // 1s apart, like Howard
    const env = await buildEnvelope({ topic: idEnv.topic, message, identity: idEnv.identity, ts: net.clock, seq: net.clock });
    pub.pubsubPublish(topicBig, JSON.stringify(env), { postHash: env.msgId, publisher: null });
    await flush();
  }
}

async function main() {
  const MSGS = ['count:5', 'chunk-1', 'chunk-2', 'chunk-3', 'chunk-4'];
  const TOPIC = 'howard-unique-' + 'transfer';
  const topicHex = await deriveTopicId(null, TOPIC);   // PUBLIC topic (publisher:null)
  const topicBig = fromHex(topicHex);
  const idA = await deriveIdentity({ lat: 51.5, lng: -0.12 });
  const idEnv = { topic: TOPIC, identity: idA };

  function report(label, got) {
    const ok = got.length === MSGS.length;
    console.log(`  ${ok ? '✓' : '✗'} ${label}: received ${got.length}/${MSGS.length} → [${got.join(', ')}]`);
    return ok;
  }

  // ── Scenario A: stable mesh, publisher subscribes in same session ──
  // Publisher A + 4 infra roots near the topic (R = 5). A is the publisher
  // AND subscribes after sending all 5 (Howard's same-session case).
  {
    const net = new MockNet();
    // 4 roots clustered nearest the topic, publisher slightly farther.
    const roots = [0x1n, 0x2n, 0x3n, 0x4n].map(x => net.spawn(topicBig ^ x, 'root'));
    const A = net.spawn(topicBig ^ 0x100n, 'A-pub');   // publisher's identity id is its own; topic id is separate
    const gotA = sink(A);
    await publishN(net, A, topicBig, MSGS, idEnv);
    A.pubsubSubscribe(topicBig);                       // subscribe AFTER all sent
    await flush();
    report('A. same-session publisher-subscriber (stable mesh)', gotA);
  }

  // ── Scenario B: second subscriber live throughout ──
  {
    const net = new MockNet();
    [0x1n, 0x2n, 0x3n, 0x4n].map(x => net.spawn(topicBig ^ x, 'root'));
    const A = net.spawn(topicBig ^ 0x100n, 'A-pub');
    const B = net.spawn(topicBig ^ 0x200n, 'B-sub');
    const gotB = sink(B);
    B.pubsubSubscribe(topicBig);                       // live subscriber, before publishing
    await flush();
    await publishN(net, A, topicBig, MSGS, idEnv);
    report('B. live concurrent subscriber', gotB);
  }

  // ── Scenario C: fresh node subscribes after the fact (reload) ──
  {
    const net = new MockNet();
    [0x1n, 0x2n, 0x3n, 0x4n].map(x => net.spawn(topicBig ^ x, 'root'));
    const A = net.spawn(topicBig ^ 0x100n, 'A-pub');
    await publishN(net, A, topicBig, MSGS, idEnv);
    const F = net.spawn(topicBig ^ 0x300n, 'F-reload');  // brand-new peer, empty state
    const gotF = sink(F);
    F.pubsubSubscribe(topicBig);
    await flush();
    report('C. fresh node subscribe-after (reload)', gotF);
  }

  // ── Scenario D: CHURN — root set changes between each publish ──
  // Models a freshly-forming web mesh where K-closest is unstable: spawn a
  // new nearer root between publishes (epoch churn), then subscribe.
  {
    const net = new MockNet();
    const A = net.spawn(topicBig ^ 0x100n, 'A-pub');
    const gotA = sink(A);
    let near = 0x80n;
    for (const message of MSGS) {
      net.spawn(topicBig ^ near, 'root');   // a new, nearer root joins
      near = near >> 1n || 0x1n;
      // invalidate every manager's k-closest cache (peer join → epoch bump)
      for (const m of net.mgrs.values()) m.invalidateKClosestCache?.();
      net.clock += 1000;
      const env = await buildEnvelope({ topic: TOPIC, message, identity: idA, ts: net.clock, seq: net.clock });
      A.pubsubPublish(topicBig, JSON.stringify(env), { postHash: env.msgId, publisher: null });
      await flush();
    }
    A.pubsubSubscribe(topicBig);
    await flush();
    report('D. same-session subscribe under root-set churn', gotA);
  }
}
main().catch((e) => { console.error('repro threw:', e?.stack || e); process.exit(2); });
