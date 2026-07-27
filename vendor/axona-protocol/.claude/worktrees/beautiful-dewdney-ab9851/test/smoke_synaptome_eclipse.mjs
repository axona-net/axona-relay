// =====================================================================
// smoke_synaptome_eclipse.mjs — adversarial eclipse properties of near-quota
// synaptome maintenance (Synaptome-Maintenance-v0.1).
//
// Near-quota maintenance actively dials the victim's K XOR-nearest peers, so it
// is an eclipse-sensitive surface: an attacker who positions ids near the victim
// could try to fill its successor clique. This test verifies the maintenance loop
// itself adds NO new eclipse leverage beyond raw keyspace proximity:
//
//   1. NO NEW TRUST — a "near" peer that cannot complete a first-party handshake
//      (phantom / unbindable) is NEVER admitted, even if it is XOR-nearest. The
//      refill routes through _considerCandidate → openConnection → axona/4 bind.
//   2. NO AMPLIFICATION (fairness) — maintenance connects to the GENUINELY nearest
//      verified peers, so it never displaces a nearer honest node in favour of a
//      farther attacker. Honest nodes interleaved in the near keyspace are kept;
//      capture is bounded by the attacker's true share of the nearest-K slots.
//   3. BUDGET BOUND — under a flood of near candidates, one pass dials at most
//      maxPerTick (no dial-storm / connection-exhaustion DoS).
//
// SCOPE / honest limit: the COST of acquiring keyspace proximity (so an attacker
// can't cheaply own the nearest-K) is E-1 (pubkey-derived id + memory-hard PoW),
// which is SEPARATE and currently at difficulty 0 on testnet. This test does not
// re-prove E-1; it proves the maintenance loop grants no leverage on top of it.
//
// Run: node test/smoke_synaptome_eclipse.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));
const xcmp  = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

async function makePeer(net, domain, lat, lng, maintain = null) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport, synaptomeMaintain: maintain });
  await peer.start();
  return { peer, id, transport, node, big: fromHex(id.id) };
}

async function main() {
  console.log('Axona synaptome-maintenance — adversarial eclipse properties');
  const net = new SimNetwork();
  const domain = new AxonaDomain();
  const KNEAR = 5;

  // Victim with maintenance ON.
  const V = await makePeer(net, domain, 0, 0, { kNear: KNEAR, intervalMs: 999999, maxPerTick: 3 });

  // Build an honest population + an attacker cohort whose ids are GROUND near V
  // (worst case: the attacker can select near ids — i.e. the no-PoW regime).
  // We over-generate identities and bucket by XOR distance to V.
  const honest = [];
  for (let i = 0; i < 14; i++) honest.push(await makePeer(net, domain, (i*17)%80-40, (i*53)%360-180, null));

  // Generate a large candidate pool, take the ones XOR-NEAREST to V as attackers.
  const pool = [];
  for (let i = 0; i < 120; i++) { const id = await createNodeIdentity({ lat: (i*7)%80-40, lng: (i*97)%360-180 }); pool.push(id); }
  pool.sort((a, b) => xcmp(V.big ^ fromHex(a.id), V.big ^ fromHex(b.id)));
  const attackerIds = pool.slice(0, 6);   // the 6 ids nearest to V = the attacker cohort

  // Start the attacker cohort as REAL bound sim nodes (they exist + handshake).
  const attackers = [];
  for (const id of attackerIds) {
    const transport = simTransport({ network: net, identity: id, heartbeatMs: 0 });
    await transport.start(id.id);
    const node = new NeuronNode({ id: fromHex(id.id), lat: 1, lng: 1 });
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
    await peer.start();
    attackers.push({ peer, id, transport, node, big: fromHex(id.id) });
  }
  const attackerBig = new Set(attackers.map(a => a.big));

  // Fully mesh honest+attacker so findKClosest can discover the neighbourhood,
  // and give V a single sponsor (fresh-join shape).
  const all = [...honest, ...attackers];
  for (let i = 0; i < all.length; i++)
    for (let j = i + 1; j < all.length; j++)
      await all[i].transport.openConnection(all[j].id.id);
  await V.transport.openConnection(honest[0].id.id);
  await wait(20);

  // ── 1. NO NEW TRUST: a phantom near id (no live node) is never admitted ──
  {
    // craft an id numerically adjacent to V (nearest possible) with NO node behind it
    const phantom = V.big ^ 1n;
    const before = V.node.synaptome.size;
    await V.peer._considerCandidate(phantom, 'maintain');   // the exact refill path
    await wait(10);
    check('1. phantom near id (unbindable) NOT admitted', !V.node.synaptome.has(phantom) && V.node.synaptome.size === before);
  }

  // ── 3. BUDGET BOUND: one pass under the near-sybil flood dials ≤ maxPerTick ─
  {
    const before = V.node.synaptome.size;
    await V.peer._maintainSynaptome();
    await wait(20);
    const opened = V.node.synaptome.size - before;
    check('3. one maintenance pass bounded by maxPerTick', opened <= 3, `(opened=${opened})`);
  }

  // converge the near quota over a few passes
  for (let k = 0; k < 6; k++) { await V.peer._maintainSynaptome(); await wait(20); }

  // Ground truth: V's K_NEAR XOR-nearest over ALL live peers (honest + attacker).
  const liveBig = all.map(p => p.big);
  const trueNearest = liveBig.slice().sort((a, b) => xcmp(V.big ^ a, V.big ^ b)).slice(0, KNEAR);

  // ── 2. NO AMPLIFICATION: V's near clique == the genuinely nearest verified ──
  {
    const clique = trueNearest.filter(id => V.node.synaptome.has(id));
    check('2a. maintenance filled the genuinely nearest-K (fair)', clique.length === KNEAR,
      `(${clique.length}/${KNEAR})`);

    // The load-bearing eclipse property: NO honest node nearer than a connected
    // attacker was displaced. Equivalently, for every connected attacker, every
    // honest node strictly nearer to V is ALSO connected.
    let noDisplacement = true;
    for (const id of V.node.synaptome.keys()) {
      if (!attackerBig.has(id)) continue;                 // a connected attacker
      const dA = V.big ^ id;
      for (const h of honest) {
        if ((V.big ^ h.big) < dA && !V.node.synaptome.has(h.big)) { noDisplacement = false; break; }
      }
      if (!noDisplacement) break;
    }
    check('2b. no nearer honest peer displaced by a farther attacker', noDisplacement);

    // Report the capture fraction — bounded by the attacker's keyspace share, NOT 100%.
    const captured = trueNearest.filter(id => attackerBig.has(id)).length;
    console.log(`     · near-clique capture = ${captured}/${KNEAR} (= attacker share of V's nearest keyspace; bounded by E-1 cost, not by maintenance)`);
    // With honest nodes also present near V, the clique is not fully attacker-owned.
    check('2c. clique not fully captured while honest near-peers exist', captured < KNEAR);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
