// smoke_join_storm.mjs — task #332 / invariant I-11: bulk work never starves
// liveness. Live-reproduced failure (testnet 2026-07-17, both 4.25.0 and
// 4.26.0): a (re)joining relay receives its region's whole role mass — every
// root that now sees it in its K-closest cohort fires a full-state REPLICATE
// within one tick — and the receiver's event loop drowns in per-message
// signature verifies; its mesh keepalives miss, peers evict it, the mesh
// dissolves and never re-forms. Three legs, each pinned here:
//   1. SENDER: at most REPLICATE_FULL_BUDGET full pushes per tick, round-robin
//      cursor so a newcomer is seeded over ticks, never in one burst.
//   2. RECEIVER: REPLICATE ingest drains through a bounded, time-sliced queue —
//      the event loop stays responsive DURING a storm (measured), the queue
//      overflow-drops (bounded memory) and all surviving state converges.
//   3. MESH: a starved mesh (peers < MESH_REWARM_MIN for MESH_REWARM_TICKS)
//      triggers reintegrate(), rate-limited by the cooldown.
//
// Run: node test/smoke_join_storm.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import {
  REPLICATE_FULL_BUDGET, INGEST_QUEUE_MAX,
  MESH_REWARM_MIN, MESH_REWARM_TICKS, MESH_REWARM_COOLDOWN_MS,
} from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const REG = 0x87n << 248n, idHexPad = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x100n, NEWCOMER = REG | 0x200n;
const settle = async (rounds = 8) => { for (let i = 0; i < rounds; i++) await new Promise(r => setImmediate(r)); };

function mk({ neighbors = [NEWCOMER], kClosest = [SELF, NEWCOMER], replicas = 1, reintegrate = null, clock = { t: 1_000_000 } } = {}) {
  const sends = []; const logs = [];
  const dht = {
    getSelfId: () => SELF, onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => (typeof neighbors === 'function' ? neighbors() : neighbors),
    bridgeId: () => null,
    findKClosest: async () => kClosest,
    ...(reintegrate ? { reintegrate } : {}),
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: replicas });
  am.nodeId = SELF;
  am.setLogSink((level, type, data) => logs.push({ level, type, data }));
  return { am, sends, logs, clock };
}

console.log('join-storm (I-11) — sender budget, receiver slicing, mesh re-warm\n');

// ── 1. SENDER: full-push budget + round-robin seeding ──────────────────────
{
  const ROLES = 3 * REPLICATE_FULL_BUDGET + 5;    // needs 4 ticks to seed
  const { am, sends, clock } = mk();
  for (let i = 0; i < ROLES; i++) {
    const t = REG | (0x1000n + BigInt(i));
    const role = makeRole(t, true);
    role.cache.push({ msgId: 'm' + i, publishTs: 100 + i, json: '{}', seq: 1 });
    am.axonRoles.set(t, role);
  }
  am._replicateRoots(); await settle();
  const fulls = () => sends.filter(s => s.payload.msgs && s.payload.msgs.length > 0).length;
  const t1 = fulls();
  ok(`tick 1 full pushes ≤ budget (${t1}/${REPLICATE_FULL_BUDGET}, roles=${ROLES})`,
    t1 > 0 && t1 <= REPLICATE_FULL_BUDGET, `got ${t1}`);
  // The storm shape this prevents: without the budget every one of the ROLES
  // roles would have full-pushed at the newcomer in tick 1.
  ok('tick 1 did NOT firehose every role at the newcomer', t1 < ROLES);
  for (let tick = 2; tick <= 5; tick++) { clock.t += 5_000; am._replicateRoots(); await settle(); }
  const seeded = [...am.axonRoles.values()].filter(r => r.replicas.size > 0).length;
  ok(`newcomer fully seeded across later ticks (${seeded}/${ROLES})`, seeded === ROLES, `seeded=${seeded}`);
  ok('cursor rotated (deferred roles were not starved)', (am._replicateCursor ?? 0) > 0);
}

// ── 2. RECEIVER: the storm — liveness measured DURING bulk verified ingest ──
{
  const TOPICS = 150, PER = 3;                    // 450 real Ed25519 verifies
  const author = await createAuthorIdentity();
  const payloads = [];
  for (let i = 0; i < TOPICS; i++) {
    const t = REG | (0x9000n + BigInt(i));
    const topic = { name: 'storm-' + i, region: 135, owner: null, write: 'open' };
    const msgs = [];
    for (let j = 0; j < PER; j++) {
      const env = await buildEnvelope({ topic, message: `storm ${i}/${j}`, ts: 1000 + j, seq: j + 1, identity: author });
      msgs.push({ msgId: env.msgId, publishTs: 1000 + j, json: JSON.stringify(env), seq: j + 1 });
    }
    payloads.push({ topicId: idHexPad(t), from: idHexPad(NEWCOMER), msgs, dels: [] });
  }
  const { am } = mk({ replicas: 2 });
  // Liveness probe: a 10ms heartbeat during the storm; drift = how long the
  // event loop kept it waiting. Inline ingest of 450 verifies stalls this for
  // the whole burst; sliced ingest must keep worst-case drift bounded.
  let maxDrift = 0, last = Date.now();
  const hb = setInterval(() => { const d = Date.now() - last - 10; if (d > maxDrift) maxDrift = d; last = Date.now(); }, 10);
  const t0 = Date.now();
  for (const p of payloads) am._onReplicate(p, { targetId: SELF });   // the burst: all arrive "at once"
  const verdictQueued = (am._ingestQueue?.length ?? 0) > 0;
  await am._ingestIdle();
  clearInterval(hb);
  const took = Date.now() - t0;
  ok('burst was queued, not processed inline', verdictQueued);
  ok(`event loop stayed live during the storm (max heartbeat drift ${maxDrift}ms < 250ms; ingest took ${took}ms)`,
    maxDrift < 250, `drift=${maxDrift}ms`);
  const adopted = [...am.axonRoles.values()].filter(r => r.cache.length === PER).length;
  ok(`all ${TOPICS} topics fully adopted after the queue drained (${adopted})`, adopted === TOPICS, `adopted=${adopted}`);
  ok('every adopted role is a BACKUP with verified bodies', [...am.axonRoles.values()].every(r => r.backupOf !== null));
}

// ── 3. RECEIVER: bounded queue — overflow drops, memory stays capped ───────
{
  const { am, logs } = mk();
  let processed = 0;
  // Fill synchronously so the pump can't drain between pushes (its first slice
  // consumes one item synchronously before suspending — hence ±1 tolerance;
  // the invariant is CONSERVATION under a bounded queue, not an exact count).
  const SUBMITTED = INGEST_QUEUE_MAX + 50;
  for (let i = 0; i < SUBMITTED; i++) am._ingestEnqueue(() => { processed++; });
  const dropped = am._ingestDropped ?? 0;
  // Exact count is scheduling-dependent (inline fast-path + the pump's first
  // synchronous slice each consume a few before the queue fills); the pinned
  // invariants are: overflow HAPPENED, it's bounded by the excess, and nothing
  // else was lost (conservation, next check).
  ok(`overflow dropped the excess (${dropped} ≲ 50), queue stayed bounded`, dropped >= 40 && dropped <= 50, `dropped=${dropped}`);
  await am._ingestIdle();
  ok(`conservation: processed + dropped === submitted (${processed} + ${dropped} = ${SUBMITTED})`, processed + dropped === SUBMITTED);
  ok('overflow was logged (I-6: no silent drop)', logs.some(l => l.type === 'pubsub:ingest-overflow'));
}

// ── 4. MESH: starved mesh triggers reintegrate, cooldown-limited ───────────
{
  let calls = 0;
  let meshPeers = [];
  const clock = { t: 1_000_000 };
  const { am } = mk({ neighbors: () => meshPeers, reintegrate: () => { calls++; }, clock });
  for (let i = 0; i < MESH_REWARM_TICKS - 1; i++) { await am.refreshTick(); clock.t += 5_000; }
  ok('no rewarm before the starvation threshold', calls === 0);
  await am.refreshTick();
  ok('rewarm fires after MESH_REWARM_TICKS starved ticks', calls === 1);
  for (let i = 0; i < 4; i++) { clock.t += 5_000; await am.refreshTick(); }
  ok('cooldown suppresses immediate re-fire', calls === 1);
  clock.t += MESH_REWARM_COOLDOWN_MS + 1_000;
  for (let i = 0; i < MESH_REWARM_TICKS; i++) { await am.refreshTick(); clock.t += 5_000; }
  ok('still-starved mesh re-fires after the cooldown', calls === 2);
  meshPeers = [NEWCOMER, REG | 0x300n, REG | 0x400n];
  clock.t += MESH_REWARM_COOLDOWN_MS + 1_000;
  for (let i = 0; i < MESH_REWARM_TICKS + 1; i++) { await am.refreshTick(); clock.t += 5_000; }
  ok('healthy mesh never triggers rewarm', calls === 2);
}

console.log(`\nResult: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
