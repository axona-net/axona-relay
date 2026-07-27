// =====================================================================
// smoke_adaptive_renewal.mjs — the churn re-home fix: adaptive subscriber renewal.
//
// A subscriber re-homes only when it renews, so the renewal interval IS the orphan
// window after its relay/root churns. The fix: renew FAST after subscribe / after a
// relay change (re-pin), backing off ×RENEW_BACKOFF toward the renewMs ceiling while
// stable. This pins the logic deterministically with a controllable clock:
//
//   1. fresh subscription starts at renewFastMs
//   2. each renewal backs the interval off ×1.5, capped at the renewMs ceiling
//   3. a re-pin (DELIVER from a NEW relay) snaps the interval back to fast
//   4. a same-relay delivery does NOT reset (keeps backing off)
//
//   node test/smoke_adaptive_renewal.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

const idHex = (big) => big.toString(16).padStart(66, '0');
const lc = (h) => String(h).toLowerCase();

let clock = 1_000_000;
const SELF  = 0x42n << 248n;
const TOPIC = (0x42n << 248n) | 0xabcn;
const RENEW_CEIL = 8000, RENEW_FAST = 1000;

const sends = [];
// A neighbour XOR-CLOSER to the topic than self, so the reachable-root fallback
// (v4.9.1) does NOT fire — this test exercises renewal BACKOFF, which requires the
// node to stay a subscriber (a node that is itself the closest reachable peer would
// correctly self-claim root after the unattached window and stop renewing).
const CLOSER = (0x42n << 248n) | 0x800n;   // ^TOPIC = 0x2bc  <  self ^TOPIC = 0xabc
const dht = {
  getSelfId: () => SELF,
  routeMessage: (target, type, payload) => { sends.push({ type, payload }); },
  onRoutedMessage: () => {},
  neighbors: () => [CLOSER],
};
const mgr = new AxonaManager({ dht, now: () => clock, renewMs: RENEW_CEIL, renewFastMs: RENEW_FAST });
const ivOf = () => mgr.mySubscriptions.get(TOPIC)?.interval;
const tick = async () => { await mgr.refreshTick(); };

async function main() {
  console.log('Axona adaptive subscriber renewal (churn re-home fix)');

  // 1. fresh subscription starts fast
  mgr.pubsubSubscribe(TOPIC);
  check(`1. fresh subscription interval = renewFastMs (${ivOf()} = ${RENEW_FAST})`, ivOf() === RENEW_FAST);

  // 2. while UNATTACHED (no upstream pin yet — a fresh or stranded subscriber),
  //    renewals stay at the fast floor. Backoff is gated on attachment (v4.8.3
  //    liveness re-route): a subscriber that hasn't heard back must keep retrying
  //    fast, not back off into a long orphan window. (Backoff once attached: §3.)
  const got = [];
  for (let i = 0; i < 8; i++) {
    clock += ivOf();                 // advance exactly one current-interval
    await tick();                    // a renewal fires; unattached → stays fast
    got.push(ivOf());
  }
  check(`2a. unattached renewals stay at the fast floor: [${got.join(',')}]`,
        got.every((v) => v === RENEW_FAST));
  check('2b. no backoff while unattached', got[got.length - 1] === RENEW_FAST);

  // 3. a re-pin (deliver from a NEW relay) snaps back to fast
  const relayA = lc(idHex((0x42n << 248n) | 0x1n));
  const relayB = lc(idHex((0x42n << 248n) | 0x2n));
  mgr._onDeliver({ topicId: idHex(TOPIC), from: relayA, msgs: [] }, { targetId: SELF });
  check('3a. first deliver pins upstream to relay A + resets interval to fast', ivOf() === RENEW_FAST);
  // back it off again, then deliver from a DIFFERENT relay → reset again
  clock += ivOf(); await tick();                       // interval → 1500
  check('3b. interval backed off after a stable renewal', ivOf() === 1500);
  mgr._onDeliver({ topicId: idHex(TOPIC), from: relayB, msgs: [] }, { targetId: SELF });
  check('3c. deliver from a NEW relay (re-home) snaps interval back to fast', ivOf() === RENEW_FAST);

  // 4. a SAME-relay deliver does NOT reset (no spurious fast-renew when nothing moved)
  clock += ivOf(); await tick();                       // interval → 1500
  mgr._onDeliver({ topicId: idHex(TOPIC), from: relayB, msgs: [] }, { targetId: SELF });
  check('4. same-relay deliver does NOT reset the interval (stays 1500)', ivOf() === 1500);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
