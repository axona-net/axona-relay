// smoke_handoff_scaling.mjs — mass-leaver handoff scaling (review 2026-07-25).
//
// The gap this fences: the Phase B round ack window was a FLAT HANDOFF_ACK_MS
// while heir-side ingest is O(topics received). A mass leaver (burst publisher
// or a confirm-subscriber that accrued dozens of roles) pushes K HANDOFFs into
// a few heirs at once; each heir works through the ingest queue (time-sliced),
// so the i-th ack arrives at O(i) — far past a flat 700ms window at K≈68. All
// those topics were counted unacked ("unacked usually meant ACKED LATE", Phase
// C note) and fell through to a single unconfirmed fallback send: sole-copy
// history riding one fire-and-forget packet. Prod signature: 68/68
// handoff-unacked warns on a ~68-role leaver (2026-07-25, kernel 4.41).
//
// The fix under test: window = HANDOFF_ACK_MS + HANDOFF_ACK_PER_TOPIC_MS ×
// round-batch (capped at HANDOFF_ACK_MAX_MS), early-exit on all-acked
// (pre-existing), stall-exit when no NEW ack lands for a base window.
//
// The pre-existing burst smoke covers (many topics × instant acks) and (one
// topic × no acks); this one covers the PRODUCT (many × slow) that prod hit.
//
// Run: node test/smoke_handoff_scaling.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { HANDOFF_ACK_MS, HANDOFF_ACK_PER_TOPIC_MS, HANDOFF_ACK_MAX_MS } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const __LOC = regionCenter('useast');

// Mocked-dht manager in the house style: findKClosest is LOCAL (cheap), the
// heir acks the i-th delivered HANDOFF after ackDelayFor(i) ms (herd model —
// an heir draining a queue), and unacked warnings are counted off _log.
function makeManager({ selfBig, ackDelayFor }) {
  const handlers = new Map();
  let delivered = 0;
  const state = { sent: 0, acked: 0, warnings: 0 };
  let amRef = null;
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: (t, h) => handlers.set(t, h),
    neighbors: () => [],
    bridgeId: () => null,
    findKClosest: async () => [selfBig, selfBig ^ 0xFFn, selfBig ^ 0xFF00n],   // local table read
    routeMessage: (target, type, payload) => {
      if (type !== 'pubsub:handoff') return;
      state.sent++;
      const d = ackDelayFor(delivered++);
      if (d === Infinity) return;
      setTimeout(() => {
        state.acked++;
        try { amRef._onHandoffAck({ topicId: payload.topicId }, { targetId: selfBig }); } catch { /* post-teardown */ }
      }, d);
    },
  };
  const am = new AxonaManager({ dht, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
  amRef = am;
  const origLog = am._log;
  am._log = function (lvl, tag, data) {
    if (String(tag).includes('unacked')) state.warnings++;
    return origLog?.call(this, lvl, tag, data);
  };
  return { am, state };
}

function plantRoots(am, selfBig, K) {
  for (let i = 0; i < K; i++) {
    const t = selfBig ^ BigInt(0x1000 + i);
    am.axonRoles.set(t, {
      topicId: t, isRoot: true,
      cache: [{ msgId: 'm' + i, publishTs: i + 1, json: '{}', seq: 1 }],
      cacheIds: new Set(), children: new Set(), subscribers: new Map(),
      tombstones: new Map(), replicas: new Map(),
    });
  }
}

async function arm(K, ackDelayFor) {
  const ident = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const SELF = BigInt('0x' + ident.id);
  const { am, state } = makeManager({ selfBig: SELF, ackDelayFor });
  plantRoots(am, SELF, K);
  const t0 = Date.now();
  await am.pubsubLeaveHandoff();
  return { ms: Date.now() - t0, ...state };
}

async function main() {
  console.log('mass-leaver handoff scaling (window ∝ batch, progress-aware)\n');
  const herd = (i) => 150 + 18 * i;    // i-th delivered HANDOFF acks at 150+18i ms (K=68 ⇒ last ≈1.4s)

  // ── 1. THE PROD CASE: 68 roots, herd-delayed acks ─────────────────────
  {
    const K = 68;
    const r = await arm(K, herd);
    const window0 = Math.min(HANDOFF_ACK_MAX_MS, HANDOFF_ACK_MS + HANDOFF_ACK_PER_TOPIC_MS * K);
    check(`all ${K} handed off with ZERO unacked warnings (was 68/68 pre-fix)`, r.warnings === 0, `${r.warnings}`);
    check(`every ack consumed inside the scaled round-0 window (${window0}ms)`, r.acked === K && r.sent === K, `sent=${r.sent} acked=${r.acked}`);
    check(`no retry round fired (round 0 sufficed)`, r.sent === K, `${r.sent}`);
    check(`wall bounded by one scaled window (+slack), took ${r.ms}ms`, r.ms < window0 + 500, `${r.ms}ms`);
  }

  // ── 2. Small batch stays snappy (window barely grows) ─────────────────
  {
    const r = await arm(8, herd);
    check('K=8 herd: all acked, no warnings', r.warnings === 0 && r.acked === 8, `acked=${r.acked} warns=${r.warnings}`);
    check(`K=8 herd: fast (${r.ms}ms < 1200ms)`, r.ms < 1200, `${r.ms}ms`);
  }

  // ── 3. Dead-silent heirs: stall-exit keeps the no-ack path bounded ─────
  //    (an heir that never acks must not hold the window to its full scaled
  //    cap — no progress for a base window ⇒ move on to the retry round /
  //    Phase C fallback, preserving the old bounded-departure property)
  {
    const K = 40;
    const t0 = Date.now();
    const r = await arm(K, () => Infinity);
    const flatBound = 2 * (HANDOFF_ACK_MS + 200) + 1500;   // ~2 stall windows + overhead
    check(`no-ack path still bounded via stall-exit (${r.ms}ms < ${flatBound}ms)`, r.ms < flatBound, `${r.ms}ms`);
    check(`all ${K} fell through to Phase C exactly once each`, r.warnings === K, `${r.warnings}`);
    void t0;
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
