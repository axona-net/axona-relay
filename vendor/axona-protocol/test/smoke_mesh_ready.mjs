// =====================================================================
// smoke_mesh_ready.mjs — peer.ready() mesh-readiness signal (v4.8.2).
//
// Subscribing the instant after join() (synaptome = just the bridge) strands the
// SUB in a not-yet-formed mesh and heals only over slow renewal cycles. ready()
// lets an app await convergence first. It must:
//   1. resolve FAST via 'minPeers' once a healthy mesh formed,
//   2. resolve via 'stable' in a SMALL mesh that can't reach minPeers (never
//      hang waiting for an unreachable count — the bug in a fixed synapse gate),
//   3. resolve 'timeout' (ready:false) for a truly isolated peer — never hang.
//
// Run: node test/smoke_mesh_ready.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label} ${extra}`); ok ? passed++ : failed++; };

async function makePeer(network, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, transport, node };
}

// ── 1. healthy mesh → ready() resolves fast via minPeers ─────────────────
async function testHealthy() {
  console.log('\n── ready() resolves fast (minPeers) in a healthy mesh ──');
  const net = new SimNetwork(); const domain = new AxonaDomain();
  const base = [];
  for (let i = 0; i < 6; i++) base.push(await makePeer(net, domain, (i * 17) % 80 - 40, (i * 53) % 360 - 180));
  for (let i = 0; i < base.length; i++)
    for (let j = i + 1; j < base.length; j++)
      await base[i].transport.openConnection(base[j].id.id);
  const r = await base[0].peer.ready({ minPeers: 3, timeoutMs: 5000, stableMs: 1500 });
  check('ready:true via minPeers', r.ready && r.reason === 'minPeers', JSON.stringify(r));
  check('resolved quickly (< stableMs, no needless wait)', r.ms < 1500, `(${r.ms}ms)`);
  check('reports the real peer count', r.peers >= 3, `(peers=${r.peers})`);
}

// ── 2. tiny mesh that can't reach minPeers → resolves via 'stable' ───────
async function testTinyMesh() {
  console.log('\n── ready() resolves via stable in a small mesh (never hangs on unreachable minPeers) ──');
  const net = new SimNetwork(); const domain = new AxonaDomain();
  const a = await makePeer(net, domain, 51, -0.1);
  const b = await makePeer(net, domain, 35, 139);
  await a.transport.openConnection(b.id.id);     // a has exactly 1 synapse
  const t0 = Date.now();
  const r = await a.peer.ready({ minPeers: 4, timeoutMs: 5000, stableMs: 300, pollMs: 50 });
  check('ready:true via stable (not timeout)', r.ready && r.reason === 'stable', JSON.stringify(r));
  check('resolved well before timeout', Date.now() - t0 < 2000, `(${r.ms}ms)`);
  check('reports the achievable count (1), not the unreachable floor', r.peers === 1, `(peers=${r.peers})`);
}

// ── 3. isolated peer → ready:false via timeout, never hangs ──────────────
async function testIsolated() {
  console.log('\n── ready() reports not-ready for an isolated peer (timeout, never hangs) ──');
  const net = new SimNetwork(); const domain = new AxonaDomain();
  const solo = await makePeer(net, domain, 0, 0);   // no connections
  // stableMs > timeoutMs so the (n>0) stable path can't mask a true isolation;
  // n stays 0 so neither minPeers nor stable fire → clean timeout.
  const r = await solo.peer.ready({ minPeers: 4, timeoutMs: 600, stableMs: 5000, pollMs: 50 });
  check('ready:false via timeout', r.ready === false && r.reason === 'timeout', JSON.stringify(r));
  check('reports zero peers', r.peers === 0);
}

async function main() {
  console.log('Axona mesh-ready smoke (v4.8.2)');
  await testHealthy();
  await testTinyMesh();
  await testIsolated();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
