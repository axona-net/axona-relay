// =====================================================================
// smoke_pubsub_bounds.js — bounded-state robustness (#4).
//
// AxonaManager keeps several dedup/metrics/replay maps. Three of them had no
// entry-COUNT bound, so a long-lived node leaked memory slowly:
//   · _counters  — post-level metrics, one inner entry per post EVER seen on
//                  every hosted topic, never evicted.
// And one had a SUBTLE replay weakness: the per-publisher seq high-water map
// (_publisherSeq, the C-2 freshness gate) was capped, but a plain Map.set on an
// existing key keeps its ORIGINAL insertion position — so the FIFO cap evicted
// the longest-ACTIVE publishers first, reopening their replay window. The fix
// LRU-touches (delete+re-insert) on accept so the cap evicts idle publishers.
//
// This proves: the shared _capStore evicts the oldest half at the cap; the
// _publisherSeq high-water survives eviction for a continuously-active early
// publisher; and _counters stays bounded across many topics/posts.
//
// Run: node test/smoke_pubsub_bounds.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);
const big = (h) => BigInt('0x' + h);

function makeManager() {
  const dht = {
    getSelfId: () => big(hex(0x01)),
    onRoutedMessage: () => {}, onDirectMessage: () => {},
    routeMessage: () => {}, sendDirect: async () => true, findKClosest: undefined,
  };
  return new AxonaManager({ dht });
}

function main() {
  console.log('pub/sub bounded-state robustness\n');

  // ── _capStore: drops the oldest half once over cap ─────────────────
  console.log('── _capStore eviction ──');
  {
    const m = makeManager();
    const store = new Map();
    for (let i = 0; i < 10; i++) store.set(`k${i}`, i);
    m._capStore(store, 8);                       // size 10 > 8 → drop oldest 4
    check('size reduced below cap', store.size === 6);
    check('oldest entries evicted (k0..k3 gone)',
      !store.has('k0') && !store.has('k1') && !store.has('k2') && !store.has('k3'));
    check('newest entries kept (k4..k9)', store.has('k4') && store.has('k9'));
    m._capStore(store, 8);                       // under cap → no-op
    check('no-op when under cap', store.size === 6);
    // works on a Set too (keys() aliases values())
    const set = new Set();
    for (let i = 0; i < 10; i++) set.add(i);
    m._capStore(set, 8);
    check('Set is capped too', set.size === 6 && !set.has(0) && set.has(9));
  }

  // ── _publisherSeq LRU: an active early publisher survives eviction ─
  console.log('\n── _publisherSeq replay-window survives eviction (LRU) ──');
  {
    const m = makeManager();
    m._publisherSeqCap = 8;                      // small cap for the test
    const PUB = 'PUB_ACTIVE';
    const now = m._now();
    // The active publisher is seen FIRST (oldest insertion)...
    const env0 = { signature: 's', signerPubkey: PUB, ts: now, seq: now };
    m._publishFreshAndOrdered(JSON.stringify(env0), now);
    // ...then many OTHER publishers arrive, repeatedly, over the cap.
    for (let i = 0; i < 30; i++) {
      const k = `OTHER_${i}`;
      const e = { signature: 's', signerPubkey: k, ts: now, seq: now };
      m._publishFreshAndOrdered(JSON.stringify(e), now);
      // the active publisher keeps publishing (advancing its high-water)
      const ea = { signature: 's', signerPubkey: PUB, ts: now, seq: now + i + 1 };
      m._publishFreshAndOrdered(JSON.stringify(ea), now);
    }
    check('map stayed bounded at the cap', m._publisherSeq.size <= 8);
    check('active publisher NOT evicted (high-water retained)', m._publisherSeq.has(PUB));
    // A replay of the active publisher with a stale seq is still rejected.
    const replay = { signature: 's', signerPubkey: PUB, ts: now, seq: now - 1_000_000 };
    const v = m._publishFreshAndOrdered(JSON.stringify(replay), now);
    check('stale replay from the active publisher is rejected', v.ok === false && v.reason === 'replay_seq');
  }

  // ── _counters: bounded across many topics and posts ────────────────
  console.log('\n── _counters bounded ──');
  {
    const m = makeManager();
    m._countersTopicCap = 16;
    m._countersPostCap  = 16;
    // many posts on ONE topic
    const T = big(hex(0x09));
    for (let i = 0; i < 100; i++) m._counterFor(T, `post${i}`);
    check('posts-per-topic bounded', m._counters.get(T).size <= 16);
    // many topics (keep each id byte < 256 so hex() stays a valid 264-bit id)
    for (let i = 0; i < 100; i++) m._counterFor(big(hex((i + 20) & 0xff)), 'p');
    check('topic count bounded', m._counters.size <= 16);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main();
