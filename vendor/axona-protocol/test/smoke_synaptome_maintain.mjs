// =====================================================================
// smoke_synaptome_maintain.mjs — Synaptome-Maintenance-v0.1 near-quota refill.
//
// A peer with maintenance ON must, on its own, fill the K_NEAR XOR-nearest
// "successor" quota — the structural fix for greedy routing's last-mile strand.
// Covers:
//   1. OPT-IN: default (flag off) installs no timer and _maintainSynaptome is a no-op.
//   2. FILL: a peer that only knows a sponsor refills to hold its K_NEAR nearest.
//   3. BOUND: a single pass opens at most maxPerTick connections.
//   4. VERIFIED PATH: refill routes through _considerCandidate (first-party verify),
//      so the synapses it adds are real bound channels (eclipse-safe by construction).
//
// Run: node test/smoke_synaptome_maintain.mjs
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
  console.log('Axona synaptome-maintenance smoke (near-quota refill)');

  // ── 1. opt-in: off by default ─────────────────────────────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    const p = await makePeer(net, domain, 10, 20, null);   // flag omitted
    check('1. default: no maintenance timer installed', p.peer._maintainTimer == null);
    const r = await p.peer._maintainSynaptome();
    check('1. default: _maintainSynaptome is a no-op', r === 0);
  }

  // ── 2+3+4. fill the near quota from a single sponsor ───────────────────
  {
    const net = new SimNetwork(); const domain = new AxonaDomain();
    // 12 fully-meshed base peers (so the nearest set is discoverable + reachable).
    const base = [];
    for (let i = 0; i < 12; i++) base.push(await makePeer(net, domain, (i*17)%80-40, (i*53)%360-180, null));
    for (let i = 0; i < base.length; i++)
      for (let j = i + 1; j < base.length; j++)
        await base[i].transport.openConnection(base[j].id.id);
    await wait(20);

    // The test node knows ONLY a sponsor (like a fresh join with no self-integrate).
    const KNEAR = 5;
    const t = await makePeer(net, domain, 5, 5, { kNear: KNEAR, intervalMs: 999999, maxPerTick: 2 });
    await t.transport.openConnection(base[0].id.id);   // sponsor only
    await wait(10);
    check('2. pre-refill: holds ~only the sponsor', t.node.synaptome.size <= 2, `(size=${t.node.synaptome.size})`);

    // The K_NEAR globally-nearest live peers to t (ground truth).
    const nearestTruth = base.map(b => b.big).sort((a, b) => xcmp(t.big ^ a, t.big ^ b)).slice(0, KNEAR);

    // 3. one pass opens at most maxPerTick.
    const before = t.node.synaptome.size;
    await t.peer._maintainSynaptome();
    await wait(20);
    const opened1 = t.node.synaptome.size - before;
    check('3. one pass bounded by maxPerTick', opened1 <= 2, `(opened=${opened1})`);

    // 2. converge over a few passes → holds all K_NEAR nearest.
    for (let k = 0; k < 5; k++) { await t.peer._maintainSynaptome(); await wait(20); }
    const have = nearestTruth.filter(id => t.node.synaptome.has(id));
    check('2. near-quota filled: holds all K_NEAR XOR-nearest', have.length === KNEAR, `(${have.length}/${KNEAR})`);

    // 4. the added synapses are real bound channels (verified path, not phantom).
    const bound = t.transport.boundPeers();
    const allBound = have.every(id => bound.some(b => b === id));
    check('4. refilled synapses are first-party-bound channels', allBound);

    // 4b. steady state: a further pass is a no-op (quota already full).
    const r = await t.peer._maintainSynaptome();
    check('4b. quota full → further pass attempts 0', r === 0, `(attempted=${r})`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
