// smoke_root_claim.mjs — the root-claim state machine's decision table (Phase 1).
//
// rootClaim.js is now the ONLY place role.isRoot changes. This drives the
// transitions and guards directly (mocked dht) and asserts the two invariants
// the table enforces (INVARIANTS.md I-1/I-2), plus the observability contract:
// every flip emits exactly one structured `root-transition` log.
//
// Run: node test/smoke_root_claim.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

function makeManager({ selfBig, neighbors = [], bridge = null } = {}) {
  const routed = [];
  const logs = [];
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => routed.push({ target, type, payload }),
    neighbors: () => neighbors.map(idHex),
    bridgeId: () => bridge,
  };
  const am = new AxonaManager({ dht, emitLog: (lvl, ev, ctx) => logs.push({ lvl, ev, ctx }) });
  return { am, rc: am._rootClaim, routed, logs };
}
const transitions = (logs) => logs.filter(l => l.ev === 'pubsub:root-transition');

async function main() {
  console.log('root-claim state machine — decision table\n');
  const SELF = (0x80n << 248n) | 0x1000n;
  const T    = SELF ^ 0x10n;              // topic near self
  const NEAR = T ^ 0x1n;                  // strictly closer to T than self
  const FAR  = T ^ (0x1n << 200n);        // much farther from T than self

  // ── guard: liveCloserRoot (I-2: never defer to farther/ghost) ─────────
  {
    const { am, rc } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    check('no beacon → no defer', rc.liveCloserRoot(T) === null);
    am._rootBeacons.set(T, { root: idHex(FAR), at: am._now(), exp: am._now() + 50_000 });
    check('FARTHER beaconed root → never defer', rc.liveCloserRoot(T) === null);
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now(), exp: am._now() + 50_000 });
    check('closer + channel-verified neighbour → defer', rc.liveCloserRoot(T) === idHex(NEAR));
  }
  {
    // ghost semantics: closer but NOT a neighbour
    const { am, rc } = makeManager({ selfBig: SELF, neighbors: [] });
    const now = am._now();
    am._rootBeacons.set(T, { root: idHex(NEAR), at: now - 60_000, exp: now + 20_000 });
    check('closer non-neighbour, stale beacon → ghost, no defer (strict)', rc.liveCloserRoot(T) === null);
    check('…and no defer on the loose gate either (corpse-freshness cut)',
      rc.liveCloserRoot(T, { requireReachable: false }) === null);
    am._rootBeacons.set(T, { root: idHex(NEAR), at: now, exp: now + 50_000 });
    check('closer non-neighbour, FRESH beacon → loose gate defers (PUB/KILL)',
      rc.liveCloserRoot(T, { requireReachable: false }) === idHex(NEAR));
    check('…but the strict gate still refuses (channel evidence required)',
      rc.liveCloserRoot(T) === null);
    am._rootBeacons.set(T, { root: idHex(NEAR), at: now - 60_000, exp: now + 20_000, verified: true });
    check('verified pointer beats reachability (network-confirmed)', rc.liveCloserRoot(T) === idHex(NEAR));
  }

  // ── become / promote / demote round-trip (I-1) ────────────────────────
  {
    const { am, rc, logs } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    const role = rc.become(T, 'sub-terminal');
    check('become creates the role AS root', role.isRoot === true && am.axonRoles.get(T) === role);
    check('become armed the early self-verify', role.formedAt > 0 && role.lastVerify === 0);
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now(), exp: am._now() + 50_000 });
    check('demote yields to the strictly-closer live root', rc.demote(T, idHex(NEAR), 'beacon-closer') === true);
    check('…role is no longer root, upstream pinned', role.isRoot === false && am._upstream.get(T)?.[0] === idHex(NEAR));
    check('demote of a non-root role is a no-op', rc.demote(T, idHex(NEAR), 'again') === false);
    check('demote toward self is refused', rc.demote(T, idHex(SELF), 'self') === false);
    // promotion is beacon-gated: the closer live root blocks the re-take (no flap)
    rc.promote(role, { via: [], topicId: idHex(T) }, { isTerminal: true });
    check('terminal promotion deferred while a closer live root beacons (no flap)', role.isRoot === false);
    am._rootBeacons.delete(T);
    rc.promote(role, { via: [], topicId: idHex(T) }, { isTerminal: true });
    check('promotion proceeds once the beacon is gone', role.isRoot === true);
    const tr = transitions(logs);
    check('every flip emitted exactly one root-transition', tr.length === 3, `${tr.length}`);
    check('transitions carry why-codes',
      tr.map(l => l.ctx.why).join(',') === 'sub-terminal,beacon-closer,terminal-promote');
  }

  // ── handoffArrived: leaver rules (I-2) ────────────────────────────────
  {
    const { am, rc } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    const role = rc.become(T, 'handoff-heir');
    // leaver is closer and still-beaconing — the ghost must be purged, no defer back
    am._rootBeacons.set(T, { root: idHex(NEAR), at: am._now(), exp: am._now() + 50_000 });
    rc.handoffArrived(T, idHex(NEAR));
    check('heir keeps the claim (never defers back to the leaver)', role.isRoot === true);
    check('leaver ghost beacon purged', !am._rootBeacons.has(T));
    // a DIFFERENT closer live root → the heir yields to it
    const NEAR2 = T ^ 0x2n;
    const { am: am2, rc: rc2 } = makeManager({ selfBig: SELF, neighbors: [NEAR2] });
    const role2 = rc2.become(T, 'handoff-heir');
    am2._rootBeacons.set(T, { root: idHex(NEAR2), at: am2._now(), exp: am2._now() + 50_000 });
    rc2.handoffArrived(T, idHex(NEAR));
    check('heir yields to a closer live root that is NOT the leaver', role2.isRoot === false);
  }

  // ── meshBare / selfClosestReachable / claimReachable ──────────────────
  {
    const BRIDGE = SELF ^ (0x1n << 100n);
    const { rc } = makeManager({ selfBig: SELF, neighbors: [BRIDGE], bridge: BRIDGE });
    check('bridge-only mesh is BARE (alone-in-the-dark)', rc.meshBare() === true);
    const { rc: rc2 } = makeManager({ selfBig: SELF, neighbors: [BRIDGE, FAR], bridge: BRIDGE });
    check('any non-bridge neighbour → meshed', rc2.meshBare() === false);
    check('self closest among reachable (farther neighbour)', rc2.selfClosestReachable(T) === true);
    const { rc: rc3 } = makeManager({ selfBig: SELF, neighbors: [NEAR] });
    check('closer reachable neighbour → self NOT closest', rc3.selfClosestReachable(T) === false);
  }
  {
    const { am, rc } = makeManager({ selfBig: SELF, neighbors: [FAR] });
    am._rootHint.set(T, { via: idHex(NEAR), at: am._now() });
    am._unattachedSince.set(T, am._now() - 10_000);
    const role = rc.claimReachable(T);
    check('reachable-fallback claims root', role.isRoot === true);
    check('…and stops deferring to the unreachable hint', !am._rootHint.has(T) && !am._unattachedSince.has(T));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
