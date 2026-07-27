// smoke_handoff_ack_honesty.mjs — a HANDOFFACK must claim only what is HELD (#402).
//
// The gap this fences: _ingestStampedBatch returned void. Its per-message helper
// had five silent early-returns (malformed JSON — no log at all; failed B-4
// verify; bad-clock stamp; dedup; tombstone) plus a shape guard that skipped
// with no log whatsoever. The HANDOFF path then sent { topicId } regardless, so
// a heir that REJECTED half the batch acked byte-identically to one that took
// all of it. The leaver added the topic to _handoffAcked, which exempts it from
// the retry rounds AND from the Phase C cohort spray, then departed — dropping
// the last copy of exactly the messages that were rejected.
//
// Live signature that prompted this: prod 4.43.0 vs 4.44.0 departure runs showed
// PARTIAL deliveries (published 5, received 4/2/1) — not zeros. Partial history
// is a transfer that reported success while incomplete. Scaling the ack WINDOW
// (4.44.0) cannot fix an ack that is untruthful; 4.44.0 measured 544 unacked on
// 396 axons with a 7x-larger budget than 4.43.0's 248 on 747.
//
// The contract under test:
//   1. dedup and tombstone count as HELD — the receiver's state correctly
//      accounts for those messages (that is why they are not stored again).
//   2. malformed / unverifiable / bad-clock count as REJECTED.
//   3. HANDOFFACK carries { held, sent }.
//   4. A leaver seeing held < sent does NOT mark the topic acked.
//   5. A legacy ack ({ topicId } only) is still accepted — mixed-fleet safe.
//
// Run: node test/smoke_handoff_ack_honesty.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const __LOC = regionCenter('useast');

function makeManager(selfBig) {
  const sent = [];
  const warnings = [];
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    neighbors: () => [],
    bridgeId: () => null,
    findKClosest: async () => [selfBig ^ 0xFFn],
    routeMessage: (target, type, payload) => sent.push({ type, payload }),
  };
  const am = new AxonaManager({ dht, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
  const origLog = am._log;
  am._log = function (lvl, tag, data) {
    if (lvl === 'warn') warnings.push({ tag, data });
    return origLog?.call(this, lvl, tag, data);
  };
  return { am, sent, warnings };
}

function emptyRole(topicBig) {
  return {
    topicId: topicBig, isRoot: true,
    cache: [], cacheIds: new Set(), children: new Set(), subscribers: new Map(),
    tombstones: new Map(), replicas: new Map(), lastTs: 0, seq: 0,
  };
}

async function main() {
  console.log('HANDOFFACK honesty — an ack claims only what is held (#402)\n');
  const ident = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const SELF = BigInt('0x' + ident.id);
  const T = SELF ^ 0x1234n;

  // ── 1. The tally distinguishes held from rejected ──────────────────────
  {
    const { am, warnings } = makeManager(SELF);
    const role = emptyRole(T);
    // Every one of these is unverifiable or unshaped, so all must be REJECTED.
    // (A genuinely signed message needs the full publish path; the verify-fail
    // branch is the one that matters here — it is the branch that used to be
    // invisible to the leaver.)
    const msgs = [
      { msgId: 'a'.repeat(64), publishTs: 1, json: '{"not":"an envelope"}' },  // verify fail
      { msgId: 'b'.repeat(64), publishTs: 2, json: 'NOT JSON AT ALL' },        // malformed
      { msgId: 'c'.repeat(64), publishTs: 3 },                                  // unshaped (no json)
      null,                                                                     // unshaped
    ];
    const tally = await am._ingestStampedBatch(role, msgs);
    check('tally reports sent = batch length', tally.sent === 4, JSON.stringify(tally));
    check('all four unusable messages counted REJECTED', tally.rejected === 4, JSON.stringify(tally));
    check('none counted as held', tally.held === 0, JSON.stringify(tally));
    check('the previously-SILENT shape skip now logs', warnings.some(w => w.tag === 'drop-unshaped-stamped'),
      warnings.map(w => w.tag).join(','));
    check('the previously-SILENT malformed-JSON skip now logs', warnings.some(w => w.tag === 'drop-malformed-stamped'),
      warnings.map(w => w.tag).join(','));
  }

  // ── 2. Dedup counts as HELD, not rejected ──────────────────────────────
  // NOTE ON ORDER: _ingestStamped verifies BEFORE it dedups, so this needs a
  // genuinely signed envelope — a fake body is rejected at verification and
  // never reaches the cacheIds branch. (That ordering also means an
  // already-held message pays a full Ed25519 verify on every replay; a real
  // cost on the join-storm path, tracked separately, not changed here.)
  // Envelope v3: `topic` is the signed DESCRIPTOR { region, owner, name, write },
  // NOT a hex id — a hex string fails verify as `missing_topic`.
  {
    const { am } = makeManager(SELF);
    const role = emptyRole(T);
    const env = await buildEnvelope({
      topic: { region: 'useast', owner: null, name: 'ack-honesty-test', write: 'open' },
      message: { hello: 'world' }, ts: 5, seq: 1, identity: ident,
    });
    const stamped = { msgId: env.msgId, publishTs: 5, json: JSON.stringify(env), seq: 1 };

    const first = await am._ingestStampedBatch(role, [stamped]);
    check('a valid signed message is HELD on first ingest', first.held === 1 && first.rejected === 0,
      JSON.stringify(first));

    const again = await am._ingestStampedBatch(role, [stamped]);
    check('re-ingesting the SAME message is HELD, not rejected (dedup ≠ loss)',
      again.held === 1 && again.rejected === 0, JSON.stringify(again));
  }

  // ── 3. A SHORT ack does NOT mark the topic acked ────────────────────────
  {
    const { am, warnings } = makeManager(SELF);
    am._handoffAcked = new Set();
    const topicHex = T.toString(16);
    am._onHandoffAck({ topicId: topicHex, held: 2, sent: 5 }, { targetId: SELF });
    check('held(2) < sent(5) ⇒ topic left UNACKED (retry + cohort spray still run)',
      am._handoffAcked.size === 0, `size=${am._handoffAcked.size}`);
    check('short ack raises handoff-ack-short with the shortfall',
      warnings.some(w => w.tag === 'handoff-ack-short' && w.data?.missing === 3),
      JSON.stringify(warnings.filter(w => w.tag === 'handoff-ack-short')));
  }

  // ── 4. A COMPLETE ack marks it acked ───────────────────────────────────
  {
    const { am } = makeManager(SELF);
    am._handoffAcked = new Set();
    am._onHandoffAck({ topicId: T.toString(16), held: 5, sent: 5 }, { targetId: SELF });
    check('held == sent ⇒ acked', am._handoffAcked.size === 1, `size=${am._handoffAcked.size}`);
  }

  // ── 5. MIXED FLEET: a legacy ack with no counters is still accepted ─────
  {
    const { am } = makeManager(SELF);
    am._handoffAcked = new Set();
    am._onHandoffAck({ topicId: T.toString(16) }, { targetId: SELF });
    check('legacy ack (no held/sent) still accepted — pre-4.45.0 heirs keep working',
      am._handoffAcked.size === 1, `size=${am._handoffAcked.size}`);
  }

  // ── 6. An empty handoff is vacuously complete (KEEPALIVE shape) ─────────
  {
    const { am } = makeManager(SELF);
    const tally = await am._ingestStampedBatch(emptyRole(T), []);
    check('empty batch ⇒ sent 0, held 0, rejected 0 (KEEPALIVE is not a failure)',
      tally.sent === 0 && tally.held === 0 && tally.rejected === 0, JSON.stringify(tally));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch(e => { console.error('FAIL:', e); process.exit(2); });
