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
  lookahead: {
    calls: 120, bypassedAtDestination: 40, probingCalls: 80,
    probesEmitted: 5600, probesPerCall: 70, usefulProbeRate: 0.0125,
  },
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
  // THE TOPIC IS THE WHOLE ID AND IT COMES FROM r.topic.
  //
  // This fence used to assert slice(0,12), and the truncation was deliberate
  // while the dump was something a human read on one node. It became wrong the
  // moment the dump was used as INVENTORY: a union of 12-hex prefixes across
  // nodes counts distinct prefixes, which is not the same object as distinct
  // topics. I published a 250-topic union built from prefixes before Aster
  // caught it at council 655.
  //
  // The OTHER half of the original assertion is the one that must never relax:
  // the field is r.topic, not r.topicId. Reading r.topicId is the mistake that
  // shipped once and emitted seated:[] unconditionally.
  ok(d.seated[0].topic === 'jokes-abcdef0123', 'topic is the FULL id, read from r.topic', d.seated[0].topic);
  ok(d.seated[0].kids === 3, 'kids read from r.children');
  ok(d.helloPressure === 0.84, 'helloPressure surfaced', d.helloPressure);
  ok(d.servicePressure === 0.007, 'servicePressure surfaced', d.servicePressure);
  ok(d.tickLagMaxMs === 4200, 'rolling tick lag surfaced', d.tickLagMaxMs);
  ok(d.tickStalls === 2, 'tickStalls surfaced', d.tickStalls);
  ok(d.saturated === false, 'admission verdict surfaced', d.saturated);
  ok(d.worstObligation === 'renewal', 'worstObligation surfaced', d.worstObligation);
  // The emit-side census is the ONLY place the probe question can be answered —
  // a browser emits zero probes, so if the relay dump drops this field there is
  // nowhere else to read it. It was dropped once: health() carried `lookahead`
  // while this builder emitted an explicit field list that did not include it.
  ok(d.lookahead && d.lookahead.usefulProbeRate === 0.0125,
     'lookahead census surfaced whole', d.lookahead);
  ok(d.lookahead.probesEmitted === 5600, 'probesEmitted carried', d.lookahead?.probesEmitted);
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
  ok(buildHealthDump({ ...full, lookahead: undefined }).lookahead === null,
     'lookahead reports null when the kernel has no census');
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

// =====================================================================
// F. THE PER-ROLE ROW IS CARRIED WHOLE, NOT RE-NARROWED.
//
// AxonaManager.inspectRoles() always computed nature, holder, subscribers and
// the replica stamps. AxonaPeer.health() copied four fields and dropped the
// rest one line later, and this dump could only pass on what it was given. The
// consequence was not abstract: a relay serves no /diag, so "do I hold a role
// with no subscribers and no messages" — the question David asked at council
// 648 — was answerable on the two bridges and on none of the other 52 nodes.
//
// These fences fail if anyone re-narrows that row again.
// =====================================================================
console.log('F. the per-role row survives the trip');
{
  const h = { synaptomeSize: 3, axonRoles: [
    { topic: 'aabbccddeeff00', isRoot: false, nature: 'backup', holder: false,
      subscribers: 0, children: 0, cacheSize: 0, lastReplicaAgeMs: 3794 },
    { topic: '112233445566', isRoot: true, nature: 'root', holder: true,
      subscribers: 4, children: 2, cacheSize: 61, lastReplicaAgeMs: null },
  ] };
  const d = buildHealthDump(h);
  const [backup, root] = d.seated;
  ok(backup.subs === 0, 'subscribers=0 is carried as subs, not dropped', backup.subs);
  ok(root.subs === 4, 'a non-zero subscriber count is carried', root.subs);
  ok(backup.nature === 'backup' && root.nature === 'root', 'nature is carried');
  ok(backup.replicaAgeMs === 3794, 'lastReplicaAgeMs is carried', backup.replicaAgeMs);
  ok(root.replicaAgeMs === null, 'a never-stamped replica stays null, not 0', root.replicaAgeMs);

  // The exact shape the question turns on, representable end to end.
  const quiet = d.seated.filter((r) => r.subs === 0 && r.cache === 0);
  ok(quiet.length === 1 && quiet[0].nature === 'backup',
     'a zero-subscriber zero-cache role is identifiable AND its nature is visible');
}

// ZERO AND ABSENT MUST NOT COLLAPSE. subs=0 is a measurement; subs=null is "this
// kernel did not tell me". A reader that cannot tell them apart would count an
// old kernel's silence as an empty role — the false zero this project keeps
// paying for.
console.log('G. an old kernel degrades to null, never to zero');
{
  const old = { synaptomeSize: 3, axonRoles: [
    { topic: 'deadbeef0000', isRoot: true, children: 1, cacheSize: 7 },   // pre-4.100.0 shape
  ] };
  const d = buildHealthDump(old);
  const r = d.seated[0];
  ok(r.subs === null, 'absent subscribers is null, NOT 0', r.subs);
  ok(r.nature === null, 'absent nature is null', r.nature);
  ok(r.replicaAgeMs === null, 'absent replica age is null', r.replicaAgeMs);
  ok(r.cache === 7 && r.kids === 1, 'the fields an old kernel DOES send still arrive');
  ok(Object.keys(r).length === 7, 'the row shape is identical on both kernels', Object.keys(r));
}

// An empty inventory and an unreadable one must never print the same.
console.log('H. empty is not the same fact as unreadable');
{
  const none    = buildHealthDump({ synaptomeSize: 2, axonRoles: [],  axonRolesComplete: true  });
  const broken  = buildHealthDump({ synaptomeSize: 2, axonRoles: [],  axonRolesComplete: false });
  const ancient = buildHealthDump({ synaptomeSize: 2, axonRoles: [] });   // kernel too old to say
  ok(none.roles === 0 && none.rolesComplete === true,   'genuinely zero roles reads complete=true');
  ok(broken.roles === 0 && broken.rolesComplete === false, 'unreadable roles reads complete=false');
  ok(ancient.rolesComplete === null, 'a kernel that cannot say reads null, not false', ancient.rolesComplete);
  ok(none.roles === broken.roles && none.rolesComplete !== broken.rolesComplete,
     'the two are distinguishable ONLY by the flag — which is why it exists');
}

console.log(`\n${fail === 0 ? '✓' : '✗'} ${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);
