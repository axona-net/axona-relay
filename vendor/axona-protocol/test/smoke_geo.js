// smoke_geo.js — geographic + routing-table utilities (src/utils/geo.js).
//
// geo.js carries the simulator's physics (haversine distance → propagation
// latency), the globe rendering math (lat/lng ↔ unit-sphere XYZ), continent
// classification, stats, and the XOR-bucket routing-table builders shared by
// every sim protocol. It also re-exports the hexid.js keyspace surface for
// legacy importers. This smoke pins:
//   1. haversine known values (equator degree, London–Paris), the antipodal
//      maximum, pole degeneracy, and antimeridian crossing;
//   2. latency derivation (propagation ∝ distance, + hop cost, RTT doubling)
//      and setLatencyParams round-trip — RESTORED afterward for hermeticity;
//   3. lat/lng ↔ XYZ: unit-norm, pole convention, round-trip stability;
//   4. continentOf on known cities + open-ocean null (incl. the OC-before-AS
//      box ordering that keeps Sydney out of Asia);
//   5. deterministic _collectBucket / buildXorRoutingTable behaviour on a
//      tiny id-sorted population (contiguous slice per bucket, full coverage
//      uncapped, one-per-bucket breadth under a budget cap);
//   6. reservoirSample filtering (alive + excludeIds) and computeStats.
//
// Run: node test/smoke_geo.js
import {
  EARTH_RADIUS_KM, MAX_GREAT_CIRCLE_KM,
  haversine, propagationDelay, messageLatency, roundTripLatency,
  setLatencyParams, getLatencyParams,
  latLngToXYZ, xyzToLatLng, randomU32,
  _collectBucket, buildXorRoutingTable, buildIntraCellTable, buildInterCellTable,
  reservoirSample, continentOf, CONTINENT_NAMES, computeStats,
  ID_BITS, toHex,   // re-exported hexid surface
} from '../src/utils/geo.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const near = (a, b, tol) => Math.abs(a - b) <= tol;

console.log('geo — distance, latency, sphere math, buckets\n');

// ── 1. haversine known values + edges ─────────────────────────────────
{
  const degKm = EARTH_RADIUS_KM * Math.PI / 180;   // ~111.195 km per equatorial degree
  check('one equatorial degree ≈ 111.195 km', near(haversine(0, 0, 0, 1), degKm, 1e-6),
    `${haversine(0, 0, 0, 1)}`);
  const lonPar = haversine(51.5074, -0.1278, 48.8566, 2.3522);
  check(`London–Paris ≈ 343 km (got ${lonPar.toFixed(1)})`, lonPar > 335 && lonPar < 352);
  check('zero distance for identical points', haversine(40.7, -74, 40.7, -74) === 0);
  check('antipodal (0,0)↔(0,180) hits MAX_GREAT_CIRCLE_KM',
    near(haversine(0, 0, 0, 180), MAX_GREAT_CIRCLE_KM, 1e-6));
  check('pole-to-pole also antipodal', near(haversine(90, 0, -90, 0), MAX_GREAT_CIRCLE_KM, 1e-6));
  check('at a pole, longitude is degenerate (distance 0)',
    near(haversine(90, 0, 90, 137), 0, 1e-6));
  check('antimeridian crossing: (0,179.5)↔(0,-179.5) is 1°, not 359°',
    near(haversine(0, 179.5, 0, -179.5), degKm, 1e-6));
  check('symmetric', haversine(10, 20, -30, 140) === haversine(-30, 140, 10, 20));
}

// ── 2. latency derivation + param round-trip (restored) ───────────────
{
  const dflt = getLatencyParams();
  check('default params: 150 ms max propagation, 10 ms hop cost',
    dflt.maxPropagation === 150 && dflt.hopCost === 10, JSON.stringify(dflt));

  const a = { lat: 0, lng: 0 }, b = { lat: 0, lng: 180 }, mid = { lat: 0, lng: 90 };
  check('antipodal propagation = full 150 ms', near(propagationDelay(a, b), 150, 1e-9));
  check('propagation scales linearly (quarter-circumference → 75 ms)',
    near(propagationDelay(a, mid), 75, 1e-9));
  check('messageLatency adds the hop cost', near(messageLatency(a, b), 160, 1e-9));
  check('co-located nodes still pay the hop cost', near(messageLatency(a, a), 10, 1e-9));
  check('roundTripLatency doubles one-way (antipodal RTT 320 ms)',
    near(roundTripLatency(a, b), 320, 1e-9));

  setLatencyParams(300, 5);
  check('setLatencyParams takes effect (antipodal one-way 305 ms)',
    near(messageLatency(a, b), 305, 1e-9) &&
    getLatencyParams().maxPropagation === 300 && getLatencyParams().hopCost === 5);
  setLatencyParams(150, 10);                       // RESTORE defaults (hermeticity)
  check('params restored to defaults', near(roundTripLatency(a, b), 320, 1e-9));
}

// ── 3. lat/lng ↔ unit-sphere XYZ ──────────────────────────────────────
{
  const np = latLngToXYZ(90, 0);
  check('north pole maps to (0, 1, 0) — Y-up convention',
    near(np.x, 0, 1e-12) && near(np.y, 1, 1e-12) && near(np.z, 0, 1e-12));
  const sp = latLngToXYZ(-90, 45);
  check('south pole maps to (0, -1, 0) regardless of lng',
    near(sp.x, 0, 1e-12) && near(sp.y, -1, 1e-12) && near(sp.z, 0, 1e-12));

  const pts = [[51.5074, -0.1278], [-33.8688, 151.2093], [35.6762, 139.6503],
               [40.7128, -74.006], [0, 0], [-89.9, 12.3]];
  check('output always lies on the unit sphere', pts.every(([lat, lng]) => {
    const { x, y, z } = latLngToXYZ(lat, lng);
    return near(Math.hypot(x, y, z), 1, 1e-12);
  }));
  check('radius parameter scales the vector',
    near(Math.hypot(...Object.values(latLngToXYZ(12, 34, 2.5))), 2.5, 1e-12));
  check('xyzToLatLng(latLngToXYZ(p)) round-trips (non-degenerate points)',
    pts.every(([lat, lng]) => {
      const { x, y, z } = latLngToXYZ(lat, lng);
      const back = xyzToLatLng(x, y, z);
      return near(back.lat, lat, 1e-9) && near(back.lng, lng, 1e-9);
    }));
  const wrapped = xyzToLatLng(...Object.values(latLngToXYZ(0, 180)));
  check('lng normalised to [-180, 180) (180 comes back as ±180 meridian)',
    near(Math.abs(wrapped.lng), 180, 1e-9) && near(wrapped.lat, 0, 1e-9));
}

// ── 4. continent classification ───────────────────────────────────────
{
  check('New York → NA', continentOf(40.7128, -74.006) === 'NA');
  check('São Paulo → SA', continentOf(-23.5505, -46.6333) === 'SA');
  check('Berlin → EU', continentOf(52.52, 13.405) === 'EU');
  check('Nairobi → AF', continentOf(-1.2921, 36.8219) === 'AF');
  check('Tokyo → AS', continentOf(35.6762, 139.6503) === 'AS');
  check('Sydney → OC (box order shields it from the AS box)',
    continentOf(-33.8688, 151.2093) === 'OC');
  check('open mid-Pacific → null', continentOf(0, -140) === null);
  check('deep Antarctic → null', continentOf(-75, 0) === null);
  check('CONTINENT_NAMES covers all six codes',
    ['NA', 'SA', 'EU', 'AF', 'AS', 'OC'].every(c => typeof CONTINENT_NAMES[c] === 'string'));
  check('classification is stable for a fixed input',
    continentOf(48.8566, 2.3522) === continentOf(48.8566, 2.3522));
}

// ── 5. XOR-bucket builders on a tiny deterministic population ─────────
{
  // 20 nodes with ids 1..20, sorted; self = 0 (not in the population).
  const sorted = Array.from({ length: 20 }, (_, i) => ({ id: BigInt(i + 1) }));
  const SELF = 0n;

  // Bucket b relative to self=0 is exactly the contiguous slice [2^b, 2^(b+1)-1].
  const b3 = _collectBucket(SELF, sorted, 3, 10).map(n => n.id).sort((a, b) => a < b ? -1 : 1);
  check('_collectBucket b=3 returns exactly ids 8..15',
    b3.length === 8 && b3[0] === 8n && b3[7] === 15n, `${b3}`);
  check('_collectBucket b=0 returns exactly id 1',
    _collectBucket(SELF, sorted, 0, 10).map(n => n.id).join() === '1');
  const b4 = _collectBucket(SELF, sorted, 4, 10).map(n => n.id);
  check('_collectBucket b=4 clips to the population (ids 16..20)',
    b4.length === 5 && b4.every(id => id >= 16n && id <= 20n));
  check('_collectBucket k-caps an over-full bucket with distinct picks', (() => {
    const picked = _collectBucket(SELF, sorted, 3, 3).map(n => n.id);
    return picked.length === 3 && new Set(picked).size === 3 &&
           picked.every(id => id >= 8n && id <= 15n);
  })());
  check('top bucket (opposite half of keyspace) is empty for this population',
    _collectBucket(SELF, sorted, ID_BITS - 1, 10).length === 0);
  check('top bucket from a high self covers the low half', (() => {
    const highSelf = 1n << BigInt(ID_BITS - 1);
    const got = _collectBucket(highSelf, sorted, ID_BITS - 1, 25).map(n => n.id);
    return got.length === 20;                       // all 20 low ids are in the opposite half
  })());

  // Uncapped build: every node lands in exactly one bucket → full coverage, no self.
  const full = buildXorRoutingTable(SELF, sorted, 10);
  check('uncapped buildXorRoutingTable covers all 20 peers exactly once',
    full.length === 20 && new Set(full.map(n => n.id)).size === 20);
  check('…and never includes self', full.every(n => n.id !== SELF));

  // Budget-capped: 5 non-empty buckets (b=0..4), maxTotal=5 → phase-1 breadth
  // guarantees exactly one peer from each occupied bucket.
  const capped = buildXorRoutingTable(SELF, sorted, 3, 5);
  const strata = capped.map(n => n.id < 2n ? 0 : Math.floor(Math.log2(Number(n.id))));
  check('capped build (maxTotal=5): one peer per occupied bucket',
    capped.length === 5 && new Set(strata).size === 5, `${capped.map(n => n.id)}`);
  check('capped build respects the hard budget', buildXorRoutingTable(SELF, sorted, 3, 7).length === 7);

  // Intra/inter split partitions the buckets at the boundary.
  const intra = buildIntraCellTable(SELF, sorted, 10, 3);   // buckets 0..2 → ids 1..7
  check('buildIntraCellTable collects only low buckets (ids 1..7)',
    intra.length === 7 && intra.every(n => n.id <= 7n));
  const inter = buildInterCellTable(SELF, sorted, 10, 3);   // buckets 3.. → ids 8..20
  check('buildInterCellTable collects the rest (ids 8..20)',
    inter.length === 13 && inter.every(n => n.id >= 8n));
}

// ── 6. reservoirSample + computeStats + misc ──────────────────────────
{
  const pool = Array.from({ length: 10 }, (_, i) => ({ id: i, alive: i !== 3 }));
  const got = reservoirSample(pool, 5, new Set([0, 1]));
  check('reservoirSample returns the requested count', got.length === 5);
  check('…only live nodes, none excluded',
    got.every(n => n.alive && n.id !== 0 && n.id !== 1 && n.id !== 3));
  check('…distinct picks', new Set(got.map(n => n.id)).size === 5);
  check('…clips to availability (ask 99, 7 eligible)',
    reservoirSample(pool, 99, new Set([0, 1])).length === 7);

  check('computeStats([]) → null', computeStats([]) === null);
  const s = computeStats([10, 1, 5, 3, 8, 2, 9, 4, 7, 6]);   // 1..10 shuffled
  check('computeStats known values (n=10)',
    s.count === 10 && s.mean === 5.5 && s.min === 1 && s.max === 10 &&
    s.median === 6 && s.p25 === 3 && s.p75 === 8 && s.p95 === 10 && s.p99 === 10,
    JSON.stringify(s));
  const input = [3, 1, 2];
  computeStats(input);
  check('computeStats does not mutate its input', input.join() === '3,1,2');

  const u = randomU32();
  check('randomU32 yields a u32', Number.isInteger(u) && u >= 0 && u <= 0xFFFFFFFF);
  check('hexid surface re-exported (ID_BITS=264, toHex live)',
    ID_BITS === 264 && toHex(1n).length === 66);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
