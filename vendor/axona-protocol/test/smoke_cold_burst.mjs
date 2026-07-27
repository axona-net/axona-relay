// smoke_cold_burst.mjs — cold-publish burst (v4.11.0).
//
// A publish from a freshly-joined (not-yet-integrated) node is the worst case for
// the one-shot greedy PUB: it strands and never re-homes. Waiting is harmful —
// OUTBOUND traffic is what integrates a newcomer. So while COLD (few neighbours),
// pubsubPublish re-sends the SAME envelope a few times over the first ~second
// (idempotent; root dedups by msgId). It must:
//   1. COLD publisher (neighbours < threshold) → initial + a TWO-PHASE burst
//      (5 fast @200ms ≈ 1s, then 5 slow @400ms ≈ 2s more)
//   2. WARM publisher: the FIRST publish to a topic → two sends (200ms apart);
//      a REPEAT publish to the same topic → exactly one send (no re-send)
//   3. burst stops early once the publish is confirmed (_confirmPending)
//   4. stop() cancels any in-flight burst timers
//
// Run: node test/smoke_cold_burst.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const T_PUB = 'pubsub:pub';
const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');
const delay = (ms) => new Promise(r => setTimeout(r, ms));

const SELF = REG | 0x11n, TOPIC = REG | 0xabcn;

function mk({ neighbours }) {
  const pubs = [];
  const nbrs = Array.from({ length: neighbours }, (_, i) => idHex(REG | BigInt(0x100 + i)));
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (_target, type, _payload) => { if (type === T_PUB) pubs.push(1); },
    neighbors: () => nbrs,
    bridgeId: () => null,
    findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht, now: () => Date.now() });
  am.nodeId = SELF;
  return { am, pubs };
}

function publish(am, msgId) {
  const json = JSON.stringify({ msgId, message: 'hi', topic: { name: 't' } });
  am.pubsubPublish(TOPIC, json, { postHash: msgId });
}

// 1. COLD → two-phase burst (initial + 5 fast @200ms + 5 slow @400ms)
{
  const { am, pubs } = mk({ neighbours: 2 });     // < COLD_PEER_THRESHOLD (8)
  publish(am, 'a'.repeat(64));
  ok('cold: initial send fires immediately', pubs.length === 1, `(${pubs.length})`);
  await delay(1300);                              // fast phase: 5 × 200ms + margin
  const afterFast = pubs.length;
  ok('cold: fast phase re-sends the envelope', afterFast >= 5, `(after ~1s=${afterFast})`);
  await delay(2200);                              // slow phase: 5 × 400ms + margin
  ok('cold: slow phase adds more re-sends', pubs.length > afterFast, `(after ~3s=${pubs.length})`);
  ok('cold: burst bounded (≤ initial + 5 fast + 5 slow)', pubs.length <= 11, `(total sends=${pubs.length})`);
  am.stop();
}

// 2. WARM → first publish to a topic re-sends once; a repeat publish does not
{
  const { am, pubs } = mk({ neighbours: 20 });    // ≥ threshold (warm)
  publish(am, 'b'.repeat(64));                    // FIRST publish to this topic
  ok('warm first-publish: immediate send', pubs.length === 1, `(${pubs.length})`);
  await delay(400);                               // FIRST_PUBLISH_RESEND_MS + margin
  ok('warm first-publish: re-sends once (two sends total)', pubs.length === 2, `(total sends=${pubs.length})`);
  const before = pubs.length;
  publish(am, 'B'.repeat(64));                    // REPEAT publish to the same topic
  await delay(400);
  ok('warm repeat-publish: exactly one send (no re-send)', pubs.length === before + 1, `(delta=${pubs.length - before})`);
  am.stop();
}

// 3. COLD but confirmed mid-burst → stops early
{
  const { am, pubs } = mk({ neighbours: 2 });
  const msgId = 'c'.repeat(64);
  publish(am, msgId);
  await delay(350);                               // ~1 burst tick in
  am._confirmPending(TOPIC, msgId);               // publisher observed its own msgId → stop
  const atConfirm = pubs.length;
  await delay(900);
  ok('confirmed mid-burst → no further re-sends', pubs.length === atConfirm, `(at confirm=${atConfirm}, final=${pubs.length})`);
  ok('confirmed early → fewer than a full burst', pubs.length < 11, `(total sends=${pubs.length})`);
  am.stop();
}

// 4. stop() cancels in-flight burst timers
{
  const { am, pubs } = mk({ neighbours: 2 });
  publish(am, 'd'.repeat(64));
  await delay(250);
  am.stop();
  const atStop = pubs.length;
  await delay(900);
  ok('stop() cancels the remaining burst', pubs.length === atStop, `(at stop=${atStop}, final=${pubs.length})`);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_cold_burst: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
