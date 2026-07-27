// =====================================================================
// smoke_mesh_signal.js — AxonaPeer peer-relayed-signaling wiring.
//
// Proves the kernel half of bridgeless connection (design:
// axona-docs/implementation/Peer-Relayed-Signaling-v0.1.md §3): a peer's
// outbound signal-relay sink routes a `mesh:signal` over the mesh to a
// target it has no direct channel to, and the TERMINAL peer (and only it)
// hands the opaque payload to its transport's deliverMeshSignal ingress.
//
// Topology: a 3-peer line A↔B↔C over Transport.sim (A has no synapse to C).
// We attach the web-transport relay surface (setSignalRelay /
// deliverMeshSignal) onto each sim transport BEFORE peer.start(), so the
// peer registers its relay exactly as it would on a real web transport.
//
//   node test/smoke_mesh_signal.js
// =====================================================================

import {
  AxonaPeer, AxonaDomain, NeuronNode, Synapse,
  SimNetwork, simTransport, clz264,
} from '../src/index.js';
import { toHex, fromHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// Build a peer over Transport.sim with the web-transport relay surface
// (setSignalRelay + deliverMeshSignal) attached so AxonaPeer wires its
// relay on start(), and we can observe terminal delivery.
async function makePeer(network, domain, id) {
  const node = new NeuronNode({ id, lat: 51 + Number(id), lng: -0.1 });
  node.alive = true;
  node.temperature = domain.T_INIT;
  const transport = simTransport({ network, heartbeatMs: 0 });
  await transport.start(id);
  node.transport = transport;

  // Web-transport relay surface (the bits AxonaPeer.start() looks for).
  const delivered = [];               // { from, signal } terminal captures
  transport.setSignalRelay = (fn) => { transport.__relayFn = fn; };
  transport.deliverMeshSignal = (fromHexId, signal) => { delivered.push({ from: fromHexId, signal }); };

  const peer = new AxonaPeer({ domain, node, transport });
  await peer.start();
  return { id, node, transport, peer, delivered };
}

function linkSyn(a, b) {
  const stratum = clz264(a.node.id ^ b.node.id);
  const syn = new Synapse({ peerId: b.node.id, latencyMs: 20, stratum });
  syn.weight = 0.7; syn._addedBy = 'smoke';
  a.node.synaptome.set(b.node.id, syn);
}

async function main() {
  console.log('AxonaPeer peer-relayed signaling (mesh:signal over the mesh)\n');
  const domain  = new AxonaDomain();
  const network = new SimNetwork();

  const A = await makePeer(network, domain, 1n);
  const B = await makePeer(network, domain, 2n);
  const C = await makePeer(network, domain, 3n);

  // Line topology A↔B↔C (no A↔C synapse). 1^3=2 > 2^3=1, so greedy A→C
  // progresses through B.
  linkSyn(A, B); linkSyn(B, A);
  linkSyn(B, C); linkSyn(C, B);
  for (const [x, y] of [[A, B], [B, A], [B, C], [C, B]]) {
    await x.transport.openConnection(y.node.id);
  }

  check('A registered a relay sink on start()', typeof A.transport.__relayFn === 'function');
  check('A has no direct synapse to C', !A.node.synaptome.has(C.node.id));

  const cHex = toHex(C.node.id);
  const signal = { kind: 'sdp-offer', sdp: 'v=0\r\na=fingerprint:sha-256 AB:CD\r\n' };

  // Invoke A's relay sink exactly as the web transport's sendSignal would.
  const took = A.transport.__relayFn(cHex, signal);
  check('relay sink took ownership (A is meshed)', took === true);

  // Give the async lookup-gated route a few ticks to land.
  await sleep(200);

  check('C (terminal) received the relayed signal', C.delivered.length === 1);
  if (C.delivered.length === 1) {
    check('C signal.from is A (hex)', fromHex(C.delivered[0].from) === A.node.id);
    check('C signal payload preserved', C.delivered[0].signal?.sdp === signal.sdp);
  }
  check('B (intermediary) did NOT consume the signal', B.delivered.length === 0);
  check('A (origin) did NOT deliver to itself', A.delivered.length === 0);

  // Negative: relaying to self is not taken (sink → bridge fallback).
  check('relay sink declines self-target', A.transport.__relayFn(toHex(A.node.id), signal) === false);

  // Negative: an unmeshed peer declines (cold bootstrap → bridge).
  const lonely = await makePeer(network, domain, 99n);
  check('unmeshed peer declines relay (bridge fallback)',
    lonely.transport.__relayFn(cHex, signal) === false);

  for (const p of [A, B, C, lonely]) { try { await p.transport.stop(); } catch {} }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => { console.error('smoke_mesh_signal threw:', err); process.exit(2); });
