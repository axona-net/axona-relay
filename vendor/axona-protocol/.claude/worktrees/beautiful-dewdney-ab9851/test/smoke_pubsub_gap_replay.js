// =====================================================================
// smoke_pubsub_gap_replay.js — gap-safe replay (v2.37).
//
// The "occasional missing message that never recovers" bug: replay-on-subscribe
// filtered by a single lastSeenTs high-water, so a message MISSED below that
// water was masked forever — even when the subscriber re-subscribed to a root
// that held it. The fix: the subscriber reports the postHashes it HOLDS (`have`)
// and the root replays exactly the complement.
//
//   1. lastSeenTs path masks a gap older than the high-water  (the bug).
//   2. `have` digest backfills exactly the missing message    (the fix).
//   3. a subscriber that holds everything pulls nothing        (no redundant re-send).
//   4. an entry with no postHash is always replayed            (can't dedup by content).
//   5. end-to-end through the subscribe-k ingress.
//
// Run: node test/smoke_pubsub_gap_replay.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { toHex }        from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const T     = 1_700_000_000_000;
const TOPIC = BigInt('0x' + '89' + 'cd'.repeat(32));
const ph    = (i) => i.toString(16).padStart(64, '0');     // postHash as 64-char hex

function mk() {
  const sent = [];
  const dht = {
    getSelfId: () => 1n,
    onRoutedMessage: () => {}, onDirectMessage: () => {}, onEvent: () => () => {},
    sendDirect: async (target, type, payload) => { sent.push({ target, type, payload }); return true; },
    routeMessage: async () => {},
    findKClosest: undefined,
  };
  const am = new AxonaManager({ dht, now: () => T });
  return { am, sent };
}

// Seed a root with n cached publishes, seq 0..n-1, oldest→newest by publishTs.
function seedRole(am, n) {
  const cache = [];
  for (let i = 0; i < n; i++) {
    cache.push({ json: JSON.stringify({ seq: i }), publishId: 'p' + i, publishTs: T - (n - i) * 1000, postHash: ph(i), publisher: null });
  }
  am.axonRoles.set(TOPIC, { isRoot: true, children: new Map(), replayCache: cache });
  return cache;
}
const replayBatches = (sent) => sent.filter((s) => s.type === 'pubsub:replay-batch');
const seqsIn = (batch) => batch.payload.messages.map((m) => JSON.parse(m.json).seq).sort((a, b) => a - b);

async function main() {
  console.log('Axona gap-safe replay (v2.37)');

  // ── 1. lastSeenTs masks a gap (documents the bug the fix removes) ──
  {
    const { am, sent } = mk();
    seedRole(am, 5);                                   // seq 0..4, ts T-5000..T-1000
    // Subscriber missed seq 2 but received seq 4 ⇒ high-water = ts(seq4) = T-1000.
    await am._maybeSendReplay(TOPIC, am.axonRoles.get(TOPIC), 2n, T - 1000 /* no have */);
    const b = replayBatches(sent);
    check('1. lastSeenTs path replays nothing newer than the water (gap stays masked)',
      b.length === 0);
  }

  // ── 2. have digest backfills exactly the missing message ──
  {
    const { am, sent } = mk();
    seedRole(am, 5);
    const have = [ph(0), ph(1), ph(3), ph(4)];         // holds all but seq 2
    await am._maybeSendReplay(TOPIC, am.axonRoles.get(TOPIC), 2n, T - 1000, have);
    const b = replayBatches(sent);
    check('2a. exactly one replay batch sent', b.length === 1);
    check('2b. it contains ONLY the missing seq 2', b.length === 1 && JSON.stringify(seqsIn(b[0])) === '[2]');
  }

  // ── 3. holds everything ⇒ nothing re-sent ──
  {
    const { am, sent } = mk();
    seedRole(am, 5);
    await am._maybeSendReplay(TOPIC, am.axonRoles.get(TOPIC), 2n, 0, [ph(0), ph(1), ph(2), ph(3), ph(4)]);
    check('3. up-to-date subscriber triggers no replay (no redundant bandwidth)',
      replayBatches(sent).length === 0);
  }

  // ── 4. an entry with no postHash can't be content-deduped → always replayed ──
  {
    const { am, sent } = mk();
    const cache = seedRole(am, 3);
    cache.push({ json: JSON.stringify({ seq: 99 }), publishId: 'pX', publishTs: T - 500, postHash: null, publisher: null });
    await am._maybeSendReplay(TOPIC, am.axonRoles.get(TOPIC), 2n, 0, [ph(0), ph(1), ph(2)]);
    const b = replayBatches(sent);
    check('4. postHash-less entry is always replayed', b.length === 1 && seqsIn(b[0]).includes(99));
  }

  // ── 5. end-to-end through the subscribe-k ingress (wire `have` threading) ──
  {
    const { am, sent } = mk();
    seedRole(am, 5);
    const subHex = toHex(2n);
    await am._onSubscribeDirect(
      { topicId: toHex(TOPIC), subscriberId: subHex, peerRoots: [], have: [ph(0), ph(1), ph(2), ph(4)] },  // missing seq 3
      { fromId: subHex });
    const b = replayBatches(sent);
    check('5. subscribe-k with `have` replays only the gap (seq 3)',
      b.length === 1 && JSON.stringify(seqsIn(b[0])) === '[3]');
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
