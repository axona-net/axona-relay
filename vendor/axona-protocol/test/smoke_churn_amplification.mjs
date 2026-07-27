// smoke_churn_amplification.mjs — churn must not multiply topic state (v4.24.1).
//
// Field incident (task #333, 2026-07-16): an identical 8-scenario overnight soak
// ran 31 cycles flap-free on kernel 4.22.1 but collapsed the relay backbone twice
// on 4.24.0 (~3h cadence; post-collapse epoch 4/106 ok). Mechanism: 4.24.0's
// leave-handoff Phase C sprayed REPLICATE to the K-closest cohort for every topic
// whose HANDOFFACK missed the (load-sensitive) ack window. Each sprayed node became
// a BACKUP of the now-departed leaver and began subscribing toward the topic; on a
// busy mesh those SUBs strand → duplicate sub-terminal roots → every duplicate root
// replicates its full cache to ITS cohort each tick → role bloat → ingest storms →
// heartbeat evictions → mesh collapse (#332 feedback loop).
//
// The fix keeps the durability that motivated 4.24.0 but deletes the amplifier:
// a departing node NEVER sends REPLICATE (never plants backup roles whose "root"
// is definitionally gone). The unacked fallback is a single extra HANDOFF to the
// runner-up candidate — the recipient becomes a proper holder through the normal
// handoff path — so the worst case plants ≤2 holders per topic, matching 4.22.1's
// one-heir footprint plus one alternative.
//
// This smoke drives real peers over the sim transport through the field shape:
// a burst PUBLISHER (the alert-bot pattern — roots its own fresh topics, cache
// full) churns out with its HANDOFFACKs dropped (the exact field trigger: acks
// that never land inside the window). Asserts:
//   1. leavers send ZERO REPLICATE (the amplifier is gone),
//   2. leaver handoff volume is bounded (heir tries + ≤1 fallback per topic),
//   3. standing-fleet role population stays bounded (no monotone blow-up),
//   4. durability RETAINED: a fresh since:'all' reader recovers every churned
//      topic's history after its publisher is long gone.
//
// Run: node test/smoke_churn_amplification.mjs
import {
  AxonaPeer, AxonaDomain, NeuronNode, SimNetwork, simTransport,
  createNodeIdentity, createAuthorIdentity, regionCenter,
} from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const __LOC = regionCenter('useast');

// Global per-verb send counters, split by sender class. A churner's counters
// split again at the moment leave() begins: `churner` (alive — its eager
// on-publish replication is LEGITIMATE root behaviour, publish-rate-bounded,
// present on stable 4.22.1) vs `leaving` (departure path — where the 4.24.0
// cohort spray lived and where REPLICATE must now be zero).
const sends = { standing: {}, churner: {}, leaving: {} };
function instrument(peer, cls) {
  const am = peer._requireAxonaManager ? peer._requireAxonaManager() : peer._axonaManager;
  const state = { cls };
  const orig = am._route.bind(am);
  am._route = (target, verb, payload) => {
    sends[state.cls][verb] = (sends[state.cls][verb] || 0) + 1;
    return orig(target, verb, payload);
  };
  return { am, state };
}

async function makePeer(net, cls, sponsor = null) {
  const identity = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const transport = simTransport({ network: net, identity, heartbeatMs: 0 });
  await transport.start(identity.id);
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: __LOC.lat, lng: __LOC.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: identity, transport });
  await peer.join(sponsor ?? undefined);              // sponsor path seeds the synaptome
  const { am, state } = instrument(peer, cls);
  return { peer, am, state, identity };
}

const totalRoles = (fleet) => fleet.reduce((n, p) => n + (p.peer._axonaManager?.axonRoles.size ?? 0), 0);

async function main() {
  console.log('churn amplification — leavers must not multiply topic state\n');
  const net = new SimNetwork();

  // Standing fleet: 8 relay-like peers (default rootReplicas) that inherit
  // whatever the churners leave behind.
  const fleet = [await makePeer(net, 'standing')];
  for (let i = 1; i < 8; i++) fleet.push(await makePeer(net, 'standing', fleet[0].identity.id));
  await sleep(800);                                     // synaptomes gossip/settle
  const author = await createAuthorIdentity();

  // ── churn: 5 rounds of the alert-bot pattern — join, MINT+PUBLISH 10 fresh
  // topics (publisher self-roots them, cache full), then leave() with every
  // HANDOFFACK dropped at the leaver (deterministic stand-in for acks arriving
  // later than the window under load). ──────────────────────────────────────
  const churned = [];   // [{T, msg}] all topics whose only holder departed
  for (let round = 0; round < 5; round++) {
    const c = await makePeer(net, 'churner', fleet[0].identity.id);
    for (let i = 0; i < 10; i++) {
      const T = { region: 'useast', name: `churn-amp-r${round}-${i}` };
      const msg = `m-r${round}-${i}`;
      await c.peer.pub(T, msg, { signWith: author });
      churned.push({ T, msg });
    }
    await sleep(500);                                   // roles settle on the churner
    const dud = { add() {}, has: () => false };
    Object.defineProperty(c.am, '_handoffAcked', { get: () => dud, set() {}, configurable: true });
    c.state.cls = 'leaving';                          // departure path — spray territory
    await c.peer.leave({ timeoutMs: 6000 });
  }
  await sleep(2500);                                    // ticks run; planted state acts

  const after = totalRoles(fleet);
  const replicateFromLeavers = sends.leaving['pubsub:replicate'] || 0;
  const handoffFromLeavers = sends.leaving['pubsub:handoff'] || 0;
  console.log(`  · roles across standing fleet after churn: ${after}`);
  console.log(`  · leaver sends: HANDOFF=${handoffFromLeavers} REPLICATE=${replicateFromLeavers}`);

  // 1. The amplifier: a departing node must never plant backup roles.
  check('leavers sent ZERO REPLICATE (no backup planting from departures)',
    replicateFromLeavers === 0, `${replicateFromLeavers} sends`);

  // 2. Leaver handoff volume bounded: HANDOFF_TRIES(2) heir sends + ≤1 fallback
  //    per topic = ≤3 × 10 topics × 5 rounds = 150 hard cap.
  check('leaver handoff sends bounded (≤150 for 5×10 ack-dropped churns)',
    handoffFromLeavers > 0 && handoffFromLeavers <= 150, `${handoffFromLeavers} sends`);

  // 3. Standing-fleet role population bounded: 50 churned topics × (1 holder
  //    + ≤1 fallback holder + ≤2 legitimate backups once stable) ≤ 200. The
  //    4.24.0 spray path blows past this as flap dynamics multiply roles.
  check('standing-fleet roles bounded after churn (≤200)', after <= 200, `${after} roles`);

  // 4. Durability retained: a fresh reader recovers EVERY churned topic's
  //    history — the protection 4.24.0 was built for must survive the fix.
  const reader = await makePeer(net, 'standing', fleet[0].identity.id);
  const got = new Set();
  for (const { T } of churned) {
    await reader.peer.sub(T, (env) => {
      const m = env?.message ?? env;
      got.add(String(typeof m === 'object' ? JSON.stringify(m) : m));
    }, { since: 'all' });
  }
  await sleep(3000);
  const recovered = churned.filter(({ msg }) => [...got].some(g => g.includes(msg))).length;
  check(`durability retained: fresh since:'all' reader recovers all ${churned.length} churned topics`,
    recovered === churned.length, `${recovered}/${churned.length}`);

  for (const p of [...fleet, reader]) await p.peer.stop();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
