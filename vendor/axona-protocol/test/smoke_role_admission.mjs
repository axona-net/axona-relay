// smoke_role_admission.mjs — a node must be able to say "no" to a role (v4.46.0).
//
// THE GAP THIS FENCES. The NEUROMORPHIC layer has had capacity discipline for a
// long time: a declared degree budget (AxonaDomain.MAX_SYNAPTOME = 50), a real
// refusal at the budget (NeuronNode.addIncomingSynapse simply returns), and —
// under a cap — a stratified breadth-before-depth fill so a capped node spreads
// coverage instead of exhausting itself on whatever it saw first
// (utils/geo.js buildXorRoutingTable). The AXONIC layer had NONE of the three:
// roles accrued without limit, a node could never decline, and a joining node
// was handed a bulk backlog while still mid-handshake.
//
// Prod, 2026-07-26, kernel 4.45.0, region eagle, 9 relays on three 961 MB
// droplets: role counts 325 / 431 / 523 and climbing past 720, memory 86–92 %,
// and five of nine relays locked out of the bridge in state=upgrade-required
// (actually a client-hello timeout — the join-storm spiral of #332/#338).
//
// THE CONTRACT UNDER TEST
//   1. MAX_ROLES is a real ceiling — saturated() flips at it.
//   2. Grace refuses MANAGEMENT but never transport; seated() is not a bare
//      timer (a node past its clock with no mesh is still not seated).
//   3. TWO TIERS. 'bridge' is HARD and the floor must never override it.
//      'not-seated' / 'saturated' / 'paced' are SOFT and the floor MUST.
//   4. THE FLOOR. A terminus with no alternative is admitted anyway and says so
//      (admitted-despite). Without this, a fleet-wide restart — every node in
//      grace at once — leaves every topic unrooted. That is worse than no gate.
//   5. A HANDOFF is judged by admitPushedRole, which refuses ONLY on capacity.
//      Grace must NOT refuse a handoff: grace is 90 s, the leaver's ack window
//      is ≤5 s, so a grace refusal is not a deferral — it is the last copy of
//      someone's history hitting the floor.
//   6. Pacing bounds NEW roles per tick so a backlog lands over seconds instead
//      of in one event-loop-blocking burst.
//
// Run: node test/smoke_role_admission.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createNodeIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
import { MAX_ROLES, ROLE_GRACE_MS, ROLE_ADMIT_PER_TICK } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const __LOC = regionCenter('useast');

// clock is injected so grace is testable without sleeping 90 s
function makeManager(selfBig, { neverRoot = false, meshed = true, ...opts } = {}) {
  const logs = [];
  let t = 1_000_000;
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    // a routable non-bridge neighbour ⇒ meshBare() false ⇒ meshed
    neighbors: () => (meshed ? [selfBig ^ 0xABCn] : []),
    bridgeId: () => null,
    findKClosest: async () => [selfBig ^ 0xFFn],
    routeMessage: () => {},
  };
  const am = new AxonaManager({
    dht, now: () => t, neverRoot, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, ...opts,
  });
  const origLog = am._log;
  am._log = function (lvl, tag, data) { logs.push({ lvl, tag, data }); return origLog?.call(this, lvl, tag, data); };
  return { am, logs, advance: (ms) => { t += ms; }, now: () => t };
}

async function main() {
  console.log('Axonic admission control — a node must be able to say "no" (v4.46.0)\n');
  const ident = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const SELF = BigInt('0x' + ident.id);
  const T = (n) => SELF ^ BigInt(n + 1);

  // ── 1. Grace: refuses management, and is NOT a bare timer ───────────────
  console.log('── 1. grace ──');
  {
    const { am, advance } = makeManager(SELF);
    check('a brand-new node is NOT seated', am.seated() === false);
    check('  → canAcceptRole refuses with not-seated',
      am.canAcceptRole().why === 'not-seated', JSON.stringify(am.canAcceptRole()));
    check('  → and it is SOFT (floor may override)', am.canAcceptRole().hard === false);
    advance(ROLE_GRACE_MS + 1);
    check('past ROLE_GRACE_MS with a meshed neighbour it IS seated', am.seated() === true);
  }
  {
    // The clause that matters: prod's locked-out relays showed mesh 0 open / 58
    // bound. A bare timer would hand roles to exactly those nodes.
    const { am, advance } = makeManager(SELF, { meshed: false });
    advance(ROLE_GRACE_MS * 10);
    check('clock long expired but NO mesh ⇒ still not seated (not a bare timer)', am.seated() === false);
  }

  // ── 2. MAX_ROLES is a real ceiling ──────────────────────────────────────
  console.log('\n── 2. saturation ──');
  {
    const { am, advance } = makeManager(SELF);
    advance(ROLE_GRACE_MS + 1);
    check('an empty node is not saturated', am.saturated() === false);
    for (let i = 0; i < MAX_ROLES; i++) am.axonRoles.set(T(i), { topicId: T(i) });
    check(`at MAX_ROLES (${MAX_ROLES}) it declares saturated`, am.saturated() === true);
    check('  → canAcceptRole refuses with saturated', am.canAcceptRole().why === 'saturated');
    check('  → and it is SOFT', am.canAcceptRole().hard === false);
  }

  // ── 3. The bridge fence is HARD ─────────────────────────────────────────
  console.log('\n── 3. the bridge fence (HARD) ──');
  {
    const { am, advance, logs } = makeManager(SELF, { neverRoot: true });
    advance(ROLE_GRACE_MS + 1);                       // seated, empty, healthy…
    const v = am.canAcceptRole();
    check('a bridge refuses even seated and empty', v.ok === false && v.why === 'bridge');
    check('  → and the refusal is HARD', v.hard === true);
    // hasAlternative=false is the floor case — a HARD reason must survive it.
    check('THE FLOOR MUST NOT OVERRIDE IT', am.admitRole(T(0), false) === false);
    check('  → logged as a hard refusal', logs.some(l => l.tag === 'role-refused' && l.data?.hard === true));
    check('  → and NOT logged as admitted-despite', !logs.some(l => l.tag === 'admitted-despite'));
    check('_becomeRoot therefore returns null (callers must handle it)',
      am._becomeRoot(T(1), 'sub-terminal') === null);
    check('  → and no role was created', am.axonRoles.size === 0);
  }

  // ── 4. The floor — soft refusals must not partition the network ─────────
  console.log('\n── 4. the floor ──');
  {
    const { am, logs } = makeManager(SELF);            // in grace: soft refusal
    check('WITH an alternative, a soft refusal stands', am.admitRole(T(0), true) === false);
    check('WITHOUT an alternative, it is FLOORED', am.admitRole(T(1), false) === true);
    check('  → and says so loudly', logs.some(l => l.tag === 'admitted-despite' && l.lvl === 'warn'));
    const role = am._becomeRoot(T(2), 'pub-terminal');
    check('a terminus in grace still gets its role (no data loss)', role !== null && role.isRoot === true);
  }

  // ── 5. HANDOFF: capacity may refuse, grace may NOT ──────────────────────
  console.log('\n── 5. pushed roles (HANDOFF) ──');
  {
    const { am } = makeManager(SELF);                  // in grace
    check('a node in GRACE still accepts a handoff (grace 90s ≫ ack window 5s)',
      am.admitPushedRole(T(0)) === true);
  }
  {
    const { am, advance, logs } = makeManager(SELF);
    advance(ROLE_GRACE_MS + 1);
    for (let i = 0; i < MAX_ROLES; i++) am.axonRoles.set(T(i), { topicId: T(i) });
    check('a SATURATED node refuses a handoff ("I cannot" is honest)',
      am.admitPushedRole(T(999)) === false);
    check('  → warns so the operator can see it', logs.some(l => l.tag === 'role-refused' && l.data?.pushed === true));
  }
  {
    const { am, advance } = makeManager(SELF, { neverRoot: true });
    advance(ROLE_GRACE_MS + 1);
    check('a bridge refuses a handoff too', am.admitPushedRole(T(0)) === false);
  }

  // ── 6. Paced admission — breadth over time, not one burst ───────────────
  console.log('\n── 6. pacing ──');
  {
    const { am, advance } = makeManager(SELF, { refreshIntervalMs: 5_000 });
    advance(ROLE_GRACE_MS + 1);
    let admitted = 0;
    for (let i = 0; i < ROLE_ADMIT_PER_TICK * 4; i++) if (am.admitRole(T(i), true)) admitted++;
    check(`at most ROLE_ADMIT_PER_TICK (${ROLE_ADMIT_PER_TICK}) admitted in one tick`,
      admitted === ROLE_ADMIT_PER_TICK, `admitted=${admitted}`);
    check('  → the rest are refused as paced', am.canAcceptRole().why === 'paced');
    advance(5_001);
    check('the next tick reopens the window', am.canAcceptRole().ok === true);
  }

  // ── 7. Observability ────────────────────────────────────────────────────
  console.log('\n── 7. observability ──');
  {
    const { am } = makeManager(SELF);
    am.admitRole(T(0), true);                          // refused: not-seated
    const a = am.inspectAdmission();
    check('inspectAdmission reports the budget', a.maxRoles === MAX_ROLES);
    check('inspectAdmission counts the refusal', a.refusals['not-seated'] === 1, JSON.stringify(a.refusals));
    check('inspectAdmission reports grace remaining', a.graceRemainingMs > 0);
  }

  console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'}  (${passed} passed, ${failed} failed)`);
  if (failed) process.exit(1);
}
main().catch((e) => { console.error(e); process.exit(1); });
