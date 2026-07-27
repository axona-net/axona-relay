// =====================================================================
// smoke_standalone_lookup.mjs — Phase 5e milestone smoke.
//
// Proves that AxonaPeer.lookup() works through Transport.sim, end to
// end, against a shared AxonaDomain — no AxonaEngine, no god's-eye
// nodeMap, no simulator-specific orchestration.
//
// This is the milestone that the engine→peer cleanup (Phases 5a–5d)
// was building towards.  Before this:
//
//   · per-peer state (`_routedHandlers`, `_directHandlers`,
//     `_nodeStats`) lived on the engine, keyed by node (5a + 5b)
//   · shared state + config (`simEpoch`, `_emaHops`, `MAX_HOPS`,
//     `EPSILON`, …) lived on the engine, with the peer reaching in
//     via `this._engine.X` (5c)
//   · the engine was a required constructor argument (5d)
//
// After:
//
//   · `new AxonaPeer({ domain, node, transport })` is enough to
//     spin up a working peer
//   · N peers share a single `AxonaDomain` for simEpoch + EMA stats
//   · `Transport.sim` (the kernel's own in-process transport) carries
//     `lookup_step` requests between peers; the kernel's `_lookupStep`
//     receiver re-runs the per-hop routing logic on each forwarder
//     and forwards via `transport.send('lookup_step', …)`
//   · `peer.lookup(targetKey)` returns `{ found, hops, path, time }`
//     identical in shape to the engine-driven path
//
// Scenario: N=10 peers, full-mesh synaptomes (every peer knows every
// other peer at distance 1), source=peer[0], target=peer[N-1].id.
// Expect: found=true, hops≈1 (direct synapse hit) or 2.
//
// Run:  node test/smoke_standalone_lookup.mjs
// =====================================================================

import {
  AxonaPeer, AxonaDomain,
  NeuronNode, Synapse,
  SimNetwork, simTransport,
  clz264,
} from '../src/index.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// ── Helpers ──────────────────────────────────────────────────────────

/** Build N peers wired to a shared SimNetwork + AxonaDomain.
 *  Returns array of { id, node, transport, peer } records. */
async function buildMesh(N, domain) {
  const network = new SimNetwork();
  const peers = [];
  for (let i = 0; i < N; i++) {
    // Deterministic ids: 1, 2, 3, ... .  Routing math is BigInt XOR
    // so any 264-bit value works; small numbers keep traces readable.
    const id = BigInt(i + 1);
    const node = new NeuronNode({ id, lat: 51.5 + i * 0.1, lng: -0.1 });
    node.alive = true;
    node.temperature = domain.T_INIT;
    const transport = simTransport({ network, heartbeatMs: 0 });
    await transport.start(id);
    node.transport = transport;
    const peer = new AxonaPeer({
      domain,
      node,
      transport,
    });
    await peer.start();
    peers.push({ id, node, transport, peer });
  }
  return { network, peers };
}

// Note: each peer's `start()` now wires the receiver-side
// 'lookup_step' handler automatically as long as a transport is
// attached to `node.transport`.  No manual wiring needed here.

/** Manually populate each peer's synaptome with synapses to all
 *  other peers.  Skips bootstrap entirely — every peer is one hop
 *  from every other peer.  Verifies the routing layer in isolation. */
function fullMeshSynaptomes(peers) {
  for (const { node } of peers) {
    for (const { node: other } of peers) {
      if (other.id === node.id) continue;
      const stratum = clz264(node.id ^ other.id);
      const syn = new Synapse({
        peerId:    other.id,
        latencyMs: 42,
        stratum,
      });
      syn.weight = 0.7;
      syn._addedBy = 'smoke-bootstrap';
      node.synaptome.set(other.id, syn);
    }
  }
}

/** Each transport needs to know it's "connected" to the others for
 *  some transports' liveness checks; simTransport.openConnection is
 *  the bilateral admission gate. */
async function openMesh(peers) {
  for (const a of peers) {
    for (const b of peers) {
      if (a.id === b.id) continue;
      await a.transport.openConnection(b.id);
    }
  }
}

// ── Scenarios ────────────────────────────────────────────────────────

async function testConstruction() {
  console.log('\n── construction: { domain } alone works ──');
  const domain = new AxonaDomain();
  check('AxonaDomain exposes simEpoch counter',
    typeof domain.simEpoch === 'number');
  check('AxonaDomain has MAX_HOPS default',
    typeof domain.MAX_HOPS === 'number' && domain.MAX_HOPS > 0);
  check('AxonaDomain.onEvent is a function',
    typeof domain.onEvent === 'function');

  const network = new SimNetwork();
  const node = new NeuronNode({ id: 1n, lat: 51.5, lng: -0.1 });
  node.alive = true;
  node.temperature = domain.T_INIT;
  const transport = simTransport({ network, heartbeatMs: 0 });
  await transport.start(1n);
  node.transport = transport;

  let threw = false;
  try {
    new AxonaPeer({ domain, node, transport });
  } catch { threw = true; }
  check('AxonaPeer accepts { domain } without engine', !threw);

  await transport.stop();
}

async function testFullMeshLookup() {
  console.log('\n── 10-peer full-mesh lookup over Transport.sim ──');
  const N = 10;
  const domain = new AxonaDomain();
  const { peers } = await buildMesh(N, domain);

  await openMesh(peers);
  fullMeshSynaptomes(peers);
  // (lookup_step handler is auto-installed by peer.start())

  const source = peers[0];
  const target = peers[N - 1];

  check('source has synapses to N-1 peers',
    source.node.synaptome.size === N - 1);
  check('source transport connected to target',
    source.transport.isConnected(target.id));

  const result = await source.peer.lookup(target.id);
  check('lookup returned a result',           result != null);
  check('lookup.found === true',              result?.found === true);
  check('lookup.hops >= 1',                   result?.hops >= 1);
  check('lookup.hops <= MAX_HOPS',            result?.hops <= domain.MAX_HOPS);
  check('lookup.path includes target.id',     result?.path?.includes(target.id));
  check('lookup.time is a number',            typeof result?.time === 'number');

  check('domain.simEpoch advanced (>= 1)',    domain.simEpoch >= 1);

  // Teardown
  for (const { transport } of peers) await transport.stop();
}

async function testTwoHopLookup() {
  console.log('\n── 3-peer line topology: source → relay → target ──');
  // Topology: peer[0] knows peer[1] only; peer[1] knows peer[2] only.
  // Lookup from peer[0] to peer[2] must hop via peer[1].
  const domain = new AxonaDomain();
  const network = new SimNetwork();
  const peers = [];
  for (let i = 0; i < 3; i++) {
    const id = BigInt(i + 1);
    const node = new NeuronNode({ id, lat: 51.5 + i, lng: -0.1 });
    node.alive = true;
    node.temperature = domain.T_INIT;
    const transport = simTransport({ network, heartbeatMs: 0 });
    await transport.start(id);
    node.transport = transport;
    const peer = new AxonaPeer({ domain, node, transport });
    await peer.start();
    peers.push({ id, node, transport, peer });
  }

  // Synapse 0 → 1, 1 → {0, 2}, 2 → 1
  const synOf = (selfNode, other) => {
    const stratum = clz264(selfNode.id ^ other.id);
    const syn = new Synapse({ peerId: other.id, latencyMs: 30, stratum });
    syn.weight = 0.7;
    syn._addedBy = 'smoke-bootstrap';
    return syn;
  };
  peers[0].node.synaptome.set(peers[1].id, synOf(peers[0].node, peers[1].node));
  peers[1].node.synaptome.set(peers[0].id, synOf(peers[1].node, peers[0].node));
  peers[1].node.synaptome.set(peers[2].id, synOf(peers[1].node, peers[2].node));
  peers[2].node.synaptome.set(peers[1].id, synOf(peers[2].node, peers[1].node));

  // Mesh open
  await peers[0].transport.openConnection(peers[1].id);
  await peers[1].transport.openConnection(peers[2].id);
  // and the reverse (simTransport admission is bilateral)
  await peers[2].transport.openConnection(peers[1].id);
  await peers[1].transport.openConnection(peers[0].id);

  // (lookup_step handler is auto-installed by peer.start())

  const result = await peers[0].peer.lookup(peers[2].id);
  check('two-hop lookup found',                  result?.found === true);
  check('two-hop lookup hops >= 1',              result?.hops >= 1);
  check('two-hop path includes relay (peer[1])', result?.path?.includes(peers[1].id));
  check('two-hop path ends with target',         result?.path?.[result.path.length - 1] === peers[2].id);

  for (const { transport } of peers) await transport.stop();
}

async function main() {
  console.log('Phase 5e — standalone AxonaPeer.lookup() over Transport.sim');
  console.log('(no AxonaEngine, no god\'s-eye nodeMap; shared AxonaDomain only)\n');

  await testConstruction();
  await testFullMeshLookup();
  await testTwoHopLookup();

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('Phase 5e smoke threw:', err);
  process.exit(2);
});
