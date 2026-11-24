// =====================================================================
// fence_health_dump_return_route.mjs — the health dump carries the kernel's
// return-route counters, whole, and survives a kernel that has none.
//
// Why this exists. On 2026-11-24 a subscriber no relay held a channel to went
// unfed for nine hours. The relay's replies to it travel by a return route the
// kernel records and counts. The counts were in the process and in no log, so
// "its way back expired" was an argument from two constants. Kernel 4.161.0
// puts the counters in health(); this fence holds the dump to passing them on.
// =====================================================================
import { buildHealthDump } from '../src/healthdump.js';

let pass = 0, fail = 0;
const ok = (cond, name, got) => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fail++; console.log(`  ✗ ${name}${got !== undefined ? ` — got ${JSON.stringify(got)}` : ''}`); }
};

const RR = {
  pubsub:  { armed: true, live: 4, recorded: 120, used: 90, delivered: 61, fellBack: 29 },
  routing: { honoured: 300, unavailable: 12, originated: 5, delivered: 280, fellBack: 20 },
};
const base = { synaptomeSize: 3, peers: ['a', 'b', 'c'], subscriptions: 0, axonRoles: [] };

console.log('A. the counters pass through whole');
{
  const d = buildHealthDump({ ...base, returnRoute: RR });
  ok(d.returnRoute && d.returnRoute.pubsub && d.returnRoute.routing, 'both halves are present', d.returnRoute);
  ok(JSON.stringify(d.returnRoute) === JSON.stringify(RR), 'nothing is renamed, dropped or flattened', d.returnRoute);
  ok(d.returnRoute?.pubsub?.used === 90 && d.returnRoute?.pubsub?.delivered === 61 && d.returnRoute?.pubsub?.fellBack === 29,
     'used, delivered and fellBack travel together', d.returnRoute?.pubsub);
}

console.log('B. a kernel older than 4.161.0 prints null, and the key is still there');
{
  const d = buildHealthDump({ ...base });
  ok('returnRoute' in d, 'the key is present');
  ok(d.returnRoute === null, 'and reads null, not {} and not zeros', d.returnRoute);
}

console.log('C. an option that is off is carried as off, not as absent');
{
  const off = { pubsub: { armed: false, live: 0, recorded: 0, used: 0, delivered: 0, fellBack: 0 },
                routing: { honoured: 0, unavailable: 0, originated: 0, delivered: 0, fellBack: 0 } };
  const d = buildHealthDump({ ...base, returnRoute: off });
  ok(d.returnRoute !== null && d.returnRoute.pubsub.armed === false, 'armed:false survives', d.returnRoute);
}

console.log('D. a null health object does not throw');
{
  let threw = null, d;
  try { d = buildHealthDump(null); } catch (e) { threw = e; }
  ok(threw === null, 'buildHealthDump(null) returns', threw && threw.message);
  ok(d && d.returnRoute === null, 'with returnRoute null', d && d.returnRoute);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
