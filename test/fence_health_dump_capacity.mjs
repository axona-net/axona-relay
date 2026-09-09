// =====================================================================
// fence_health_dump_capacity.mjs — the health dump carries CAPACITY, and
// survives a kernel that has none.
//
// Why this exists. On 2026-09-09, diagnosing #36, three prod droplets were
// each running SIX relays on ONE core at load 15-19 (node CPU 91-96% of that
// core). One relay shed peers 74 -> 19 in 25 minutes on `pc-closed` evictions
// — 49 in 30 minutes against 1-6 for its siblings on the same box — and was
// the only node in the fleet emitting replicate-all-failed. That starvation
// had to be inferred from `ps` and load average, because `health-dump` emitted
// roles and nothing else, while the kernel had been MEASURING it since 4.47.0
// (helloPressure = tick lag / HELLO_DEADLINE_MS) and the dump discarded it.
//
// The dump logic also has a history: its first cut read `h.roles` and
// `r.topicId` when the contract is `h.axonRoles` and `r.topic`, so it emitted
// `roles:0 seated:[]` unconditionally — and that empty output was then read as
// "the relay just restarted" rather than as a broken reader. It shipped because
// nothing could test it: it lived inside a SIGUSR1 handler in the entry point,
// and importing that starts a relay. Hence src/healthdump.js, and hence this.
// =====================================================================
import { buildHealthDump } from '../src/healthdump.js';

let pass = 0, fail = 0;
const ok = (cond, name, got) => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${got !== undefined ? ` — got ${JSON.stringify(got)}` : ''}`); }
};

// A faithful health() shape (AxonaPeer.js:3246-3258, AxonaManager.js:712-723).
const full = {
  synaptomeSize: 61,
  peers: new Array(61).fill('p'),
  subscriptions: 2,
  axonRoles: [
    { topic: 'jokes-abcdef0123', isRoot: true,  children: 3, cacheSize: 12 },
    { topic: 'council',          isRoot: false, children: 0, cacheSize: 0  },
  ],
  admission: {
    roles: 2, maxRoles: 96, seated: true, saturated: false,
    capacity: {
      roles: 2, subscriptions: 2, overdue: 0, obligations: 4, overdueFrac: 0,
      unserviced: 0, worstAgeMs: 1200, worstObligation: 'renewal',
      servicePressure: 0.007,
      tickLagMs: 3, tickLagMaxMs: 4200, tickLagWindow: 30, tickLagPeakMs: 9100,
      tickDurMs: 12, tickStalls: 2,
      helloPressure: 0.84,
    },
  },
};

console.log('A. a healthy, complete shape');
{
  const d = buildHealthDump(full);
  ok(d.peers === 61, 'peers from the array length');
  ok(d.roles === 2, 'roles from axonRoles, not h.roles');
  ok(d.rooted === 1, 'rooted counts isRoot');
  // 'jokes-abcdef0123'.slice(0,12) === 'jokes-abcdef' — 12 chars, not 11.
  ok(d.seated[0].topic === 'jokes-abcdef', 'topic truncated to 12 and read from r.topic', d.seated[0].topic);
  ok(d.seated[0].kids === 3, 'kids read from r.children');
  ok(d.helloPressure === 0.84, 'helloPressure surfaced', d.helloPressure);
  ok(d.servicePressure === 0.007, 'servicePressure surfaced', d.servicePressure);
  ok(d.tickLagMaxMs === 4200, 'rolling tick lag surfaced', d.tickLagMaxMs);
  ok(d.tickStalls === 2, 'tickStalls surfaced', d.tickStalls);
  ok(d.saturated === false, 'admission verdict surfaced', d.saturated);
  ok(d.worstObligation === 'renewal', 'worstObligation surfaced', d.worstObligation);
}

// THE TRAP. Zero is the HEALTHY reading for both pressures, and it is a real
// value, not an absence. `cap.helloPressure || null` would print null for a
// perfectly healthy relay — turning the best possible reading into "unknown"
// and making a starved node indistinguishable from an idle one.
console.log('B. zero pressure is a VALUE, not an absence');
{
  const idle = structuredClone(full);
  idle.admission.capacity.helloPressure = 0;
  idle.admission.capacity.servicePressure = 0;
  idle.admission.capacity.tickLagMaxMs = 0;
  idle.admission.capacity.tickStalls = 0;
  const d = buildHealthDump(idle);
  ok(d.helloPressure === 0,   'helloPressure 0 stays 0, never null',   d.helloPressure);
  ok(d.servicePressure === 0, 'servicePressure 0 stays 0, never null', d.servicePressure);
  ok(d.tickLagMaxMs === 0,    'tickLagMaxMs 0 stays 0, never null',    d.tickLagMaxMs);
  ok(d.tickStalls === 0,      'tickStalls 0 stays 0, never null',      d.tickStalls);
}

// A relay may run against a kernel with no capacity telemetry. Losing the role
// data too — because the reader threw on the way past — is the failure this
// separates.
console.log('C. a kernel with no admission/capacity still yields roles');
{
  const older = { ...full, admission: undefined };
  let d, threw = null;
  try { d = buildHealthDump(older); } catch (e) { threw = e; }
  ok(threw === null, 'no throw when admission is absent', threw && threw.message);
  ok(d.roles === 2, 'roles still reported', d && d.roles);
  ok(d.helloPressure === null, 'helloPressure reports null, not 0', d && d.helloPressure);
  ok(d.saturated === null, 'saturated reports null', d && d.saturated);

  const noCap = structuredClone(full); delete noCap.admission.capacity;
  const d2 = buildHealthDump(noCap);
  ok(d2.saturated === false, 'admission still read when only capacity is missing', d2.saturated);
  ok(d2.tickLagMaxMs === null, 'capacity fields null when capacity is missing', d2.tickLagMaxMs);
}

// The original defect, pinned: h.roles must never be mistaken for the roles.
console.log('D. the shape mistake that shipped once');
{
  const wrong = { synaptomeSize: 5, roles: [{ topicId: 'x' }, { topicId: 'y' }] };  // no axonRoles
  const d = buildHealthDump(wrong);
  ok(d.roles === 0, 'h.roles is NOT read as axonRoles', d.roles);
  ok(Array.isArray(d.seated) && d.seated.length === 0, 'seated empty, not throwing');
  ok(d.peers === 5, 'falls back to synaptomeSize when peers is absent', d.peers);
}

console.log('E. nothing at all');
{
  let threw = null, d;
  try { d = buildHealthDump(undefined); } catch (e) { threw = e; }
  ok(threw === null, 'undefined health does not throw', threw && threw.message);
  ok(d.roles === 0 && d.peers === null, 'degrades to nulls/zeros');
}

console.log(`\n${fail === 0 ? '✓' : '✗'} ${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);
