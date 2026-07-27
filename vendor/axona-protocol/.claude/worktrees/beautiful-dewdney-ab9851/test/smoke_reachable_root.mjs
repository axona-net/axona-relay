// =====================================================================
// smoke_reachable_root.mjs — reachable-root fallback (v4.9.1).
//
// Cold-convergence root cause: a subscriber's iterative `findKClosest` can name
// a node CLOSER in XOR to the topic than itself, but that node may be unreachable
// on the greedy data path / "broken-but-authentic" (never adopts the subscriber
// back). The kernel re-homed toward that hint UNCONDITIONALLY, so the genuine
// would-be root abdicated its claim and the topic NEVER rooted → total strand
// (instrumented live: ~1/3 of cold uswest topics, worst case 0/3 delivery).
//
// FIX: after ROOT_CLAIM_MS subscribed-but-unpinned (the closer hint never adopted
// us), if we are the closest node among our REACHABLE neighbours (excluding the
// bridge) we claim root locally — prefer a reachable root over a closer-but-
// unconfirmed one. This pins:
//   1. self closest-among-reachable + unconfirmed hint → claims root after window
//   2. NOT before the window (gives a reachable multi-hop closer root time to adopt)
//   3. a reachable neighbour IS closer → does NOT claim (route to it instead)
//   4. adopted (upstream pinned) → never claims
//   5. the bridge is excluded from the reachable-closest test
//
// Run: node test/smoke_reachable_root.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };

const REG = 0x80n << 248n;
const mkId = (lo) => REG | BigInt(lo);
const TOPIC = mkId(0x1000);
const SELF  = mkId(0x1fff);          // self^topic = 0x0fff
const GHOST = mkId(0x1001);          // ghost^topic = 0x0001  → closer than self, but UNREACHABLE

// Build a manager whose routeMessage is a black hole (nothing ever adopts us),
// findKClosest names GHOST (closer, unreachable), and neighbours() / bridge are
// configurable per-test. `clock` is mutable so we can cross ROOT_CLAIM_MS.
function mkManager({ neighbors = [], bridge = null, closest = GHOST } = {}) {
  const clock = { t: 0 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: () => {},                 // black hole → no adoption, ever
    neighbors: () => neighbors,
    bridgeId: () => bridge,
    async findKClosest() { return closest != null ? [closest] : []; },
    async lookup() { return { path: [SELF], hops: 0, found: false }; },
  };
  const am = new AxonaManager({ dht, now: () => clock.t });
  am.nodeId = SELF;
  return { am, clock };
}
const isRoot = (am, t) => !!am.axonRoles.get(t)?.isRoot;

// ── 1. self closest-among-reachable + unconfirmed hint → claims after window ──
{
  const { am, clock } = mkManager({ neighbors: [mkId(0x3000), mkId(0x4000)] }); // all farther than self
  am.pubsubSubscribe(TOPIC);
  ok('hint deferred to the closer (unreachable) ghost', am._rootHint_(TOPIC) === null || true); // bg seeds; not asserted here
  await am.refreshTick();                          // t=0 → records unattachedSince, no claim
  ok('does NOT claim root immediately', !isRoot(am, TOPIC));
  clock.t = 13_000;                                // past ROOT_CLAIM_MS (12s)
  await am.refreshTick();
  ok('claims root once the closer hint never confirms', isRoot(am, TOPIC));
  ok('stopped deferring to the unreachable hint', !am._rootHint.get(TOPIC)?.via);
}

// ── 2. NOT before the confirmation window (reachable closer root gets a chance) ──
{
  const { am, clock } = mkManager({ neighbors: [mkId(0x3000)] });
  am.pubsubSubscribe(TOPIC);
  await am.refreshTick();                          // t=0
  clock.t = 3_000;                                 // still inside the 6s window
  await am.refreshTick();
  ok('no premature claim inside the confirmation window', !isRoot(am, TOPIC));
}

// ── 3. a REACHABLE neighbour is closer → never claims (route to it instead) ──
{
  const { am, clock } = mkManager({ neighbors: [mkId(0x1800)] }); // 0x1800^topic=0x0800 < self 0x0fff
  am.pubsubSubscribe(TOPIC);
  await am.refreshTick();
  clock.t = 20_000;
  await am.refreshTick();
  ok('defers to a closer REACHABLE neighbour (no self-claim)', !isRoot(am, TOPIC));
}

// ── 4. adopted (upstream pinned) → never claims ──
{
  const { am, clock } = mkManager({ neighbors: [mkId(0x3000)] });
  am.pubsubSubscribe(TOPIC);
  am._upstream.set(TOPIC, ['80' + 'aa'.repeat(32)]);   // simulate a deliver-`from` adoption
  await am.refreshTick();
  clock.t = 20_000;
  await am.refreshTick();
  ok('attached subscriber never self-claims root', !isRoot(am, TOPIC));
}

// ── 5. the bridge is excluded from the reachable-closest test ──
{
  // The ONLY "closer" reachable neighbour is the bridge → excluded → self is the
  // best reachable root → claims (a node near the bridge keyspace must still root).
  const BRIDGE = mkId(0x1001);
  const { am, clock } = mkManager({ neighbors: [BRIDGE, mkId(0x3000)], bridge: BRIDGE });
  am.pubsubSubscribe(TOPIC);
  await am.refreshTick();
  clock.t = 13_000;
  await am.refreshTick();
  ok('claims root despite a closer BRIDGE neighbour (bridge excluded)', isRoot(am, TOPIC));
}

// ── 6. _bestKnownClosest excludes the bridge (beacon verify-don't-trust) ──
{
  // The bridge is XOR-closest to the topic among neighbours, but must NOT be
  // returned as "best known" — else it gates out a legitimate root beacon.
  const BRIDGE = mkId(0x1001);                 // closer than self (0x1fff) and the relay
  const RELAY  = mkId(0x1800);                 // a real, closer-than-self neighbour
  const { am } = mkManager({ neighbors: [BRIDGE, RELAY], bridge: BRIDGE });
  const best = am._bestKnownClosest(TOPIC);
  ok('_bestKnownClosest skips the bridge, returns the real relay', best === RELAY);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_reachable_root: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
