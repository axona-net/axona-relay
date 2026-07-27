// =====================================================================
// mesh_multipeer.mjs — Tier-2 headless multi-peer mesh integration test.
//
// This is the harness the demo bug needed.  The CBV-asymmetry regression
// lived in the WebRTC-mesh axona/4 handshake — code that ONLY runs once
// two browser peers open a real RTCDataChannel between them and exchange
// hello / hello-sig.  No unit test reached it (the inline closure was
// untestable), and the Peer UI's getPeers() view showed open data
// channels as "connected" while NOTHING was bound.  This harness drives
// the real path end-to-end, headless:
//
//   • boots the REAL axona-bridge (sibling repo) on a throwaway port
//   • brings up N real webTransport peers, each with a genuine Ed25519
//     identity, using node-datachannel as the RTCPeerConnection impl
//   • lets them discover each other via the bridge and negotiate real
//     WebRTC data channels over localhost
//   • asserts the ROUTING TRUTH the UI hid:
//       – every peer binds N-1 mesh peers      (transport.boundPeers)
//       – every peer's synaptome holds N-1 peers (peer.health)
//       – health().meshDegraded is false everywhere
//       – a public-topic publish from peer 0 reaches all other peers
//
// On the OLD asymmetric-tag kernel this fails at the bind assertion:
// data channels open, hellos cross, but the CBVs never match so
// boundPeers stays empty.  That is exactly the signal we want.
//
// OPT-IN: not part of `npm test`.  Run with `npm run test:mesh`.
// Requires the optional native dep node-datachannel and the sibling
// axona-bridge checkout; SKIPs cleanly (exit 0) if either is absent so
// CI without the native module stays green.
//
//   node test/integration/mesh_multipeer.mjs
// =====================================================================

import { spawn }            from 'node:child_process';
import { fileURLToPath }    from 'node:url';
import { dirname, resolve } from 'node:path';
import { existsSync } from 'node:fs';

const __dirname    = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT  = resolve(__dirname, '..', '..');          // axona-protocol/
const BRIDGE_ROOT  = resolve(KERNEL_ROOT, '..', 'axona-bridge'); // sibling

const N            = 3;            // peers
const BRIDGE_PORT  = 19090;
const BRIDGE_URL   = `ws://localhost:${BRIDGE_PORT}`;
const PEER_VERSION = '9.9.9';      // above any MIN_PEER_VERSION gate

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── 0. Optional prerequisites — skip gracefully ─────────────────────
let polyfill;
try {
  polyfill = await import('node-datachannel/polyfill');
} catch {
  console.log('SKIP: node-datachannel not installed — `npm i -D node-datachannel` to run this harness.');
  process.exit(0);
}
if (!existsSync(resolve(BRIDGE_ROOT, 'src', 'server.js'))) {
  console.log(`SKIP: sibling axona-bridge not found at ${BRIDGE_ROOT}.`);
  process.exit(0);
}
// node-datachannel ships a spec RTCPeerConnection; MeshManager reads the
// global at connection time, so installing it here is enough — no kernel
// change required.
globalThis.RTCPeerConnection = polyfill.RTCPeerConnection;

// ── 1. Kernel under test (local working source, not a vendored copy) ─
const { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity } =
  await import('../../src/index.js');
const { webTransport } =
  await import('../../src/transport/web/index.js');

// ── 2. Boot the real bridge on a throwaway port ─────────────────────
function startBridge() {
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_ROOT,
    env: {
      ...process.env,
      PORT: String(BRIDGE_PORT),
      LOG_LEVEL: process.env.VERBOSE ? 'info' : 'warn',
      MIN_PEER_VERSION: '0.0.1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let ready = false;
  child.stdout.on('data', (c) => {
    const s = c.toString();
    if (s.includes('"event":"listen"')) ready = true;
    if (process.env.VERBOSE) process.stdout.write('[bridge] ' + s);
  });
  child.stderr.on('data', (c) => { if (process.env.VERBOSE) process.stderr.write('[bridge] ' + c.toString()); });
  return { child, ready: () => ready };
}

async function waitFor(predicate, { timeoutMs = 15000, everyMs = 200, label = 'condition' } = {}) {
  const t0 = Date.now();
  while (!predicate()) {
    if (Date.now() - t0 > timeoutMs) return false;
    await sleep(everyMs);
  }
  return true;
}

// Spread peers across a few regions so node ids differ in the geo prefix
// too (closer to production than all-same-region).
const REGIONS = [
  { lat: 40.71, lng: -74.0 },   // NYC
  { lat: 51.50, lng: -0.12 },   // London
  { lat: 35.68, lng: 139.69 },  // Tokyo
  { lat: -33.87, lng: 151.21 }, // Sydney
  { lat: 37.77, lng: -122.42 }, // SF
];

async function makePeer(i) {
  const region   = REGIONS[i % REGIONS.length];
  const identity = await createNodeIdentity(region);
  const logs     = [];
  const transport = webTransport({
    bridgeUrl:   BRIDGE_URL,
    identity:    { ...identity, id: identity.id },  // kernel id is already hex
    peerVersion: PEER_VERSION,
    reconnect:   false,
    log: (e, d) => { logs.push([e, d]); if (process.env.VERBOSE) console.log(`[p${i}] ${e}`, d ?? ''); },
  });
  const node   = new NeuronNode({ id: BigInt('0x' + identity.id), lat: region.lat, lng: region.lng });
  node.transport = transport;
  const domain = new AxonaDomain({ k: 20 });
  const peer   = new AxonaPeer({ domain, node, identity, transport });
  return { i, identity, transport, node, peer, logs };
}

async function main() {
  console.log(`Tier-2 multi-peer mesh: ${N} real webTransport peers + real bridge + node-datachannel\n`);
  const bridge = startBridge();
  if (!(await waitFor(bridge.ready, { timeoutMs: 8000, label: 'bridge-listen' }))) {
    bridge.child.kill('SIGKILL');
    console.error('FAIL: bridge did not start');
    process.exit(2);
  }

  const peers = [];
  let exitCode = 1;
  try {
    for (let i = 0; i < N; i++) peers.push(await makePeer(i));

    // Start transports first (bridge handshake), then the AxonaPeers.
    for (const p of peers) await p.transport.start();
    for (const p of peers) await p.peer.start();
    console.log(`  · ${N} peers connected to bridge; waiting for mesh to converge…`);

    // ── Wait for full-mesh binding ──────────────────────────────────
    // Real WebRTC negotiation over localhost is fast but not instant;
    // give it a generous window.  Success = every peer has bound the
    // other N-1 over the mesh.
    // Mesh-only bind count (excludes the bridge link) — this is what
    // "every peer bound the other N-1" actually means.
    const meshBoundOf = (p) => p.peer.health().transport?.meshBound ?? 0;
    const allBound = () => peers.every(p => meshBoundOf(p) >= N - 1);
    const converged = await waitFor(allBound, { timeoutMs: 25000, everyMs: 250, label: 'full-mesh' });

    console.log('\n── routing truth (the view the UI hid) ──');
    for (const p of peers) {
      const h     = p.peer.health();
      const bound = h.transport?.meshBound ?? 0;
      check(`peer ${p.i}: bound ${bound}/${N - 1} mesh peers`, bound >= N - 1);
      check(`peer ${p.i}: synaptome holds ≥${N - 1}`, h.synaptomeSize >= N - 1);
      check(`peer ${p.i}: meshDegraded === false`, h.meshDegraded === false);
      if (process.env.VERBOSE) console.log(`     health.transport`, h.transport);
    }
    check('mesh converged within timeout', converged);

    // ── Pub/sub reaches every peer ──────────────────────────────────
    // Public-topic mode so all peers derive the same topic id without
    // needing each other's publisher id.
    console.log('\n── pub/sub over the authenticated mesh ──');
    const TOPIC = 'tier2/mesh/smoke';
    const received = peers.map(() => []);
    await Promise.all(peers.map((p, idx) =>
      p.peer.sub(TOPIC, (env) => { received[idx].push(env); }, { publisher: null })));
    await sleep(500);  // let subscriptions propagate

    const payload = `hello-mesh-${process.pid}`;
    await peers[0].peer.pub(TOPIC, payload, { publisher: null });

    // Every OTHER peer should receive it.  (Publisher's own delivery is
    // not asserted — apps don't echo to the local publisher.)
    const others = peers.slice(1);
    const gotIt = await waitFor(
      () => others.every((_, k) => received[k + 1].some(e => (e?.message ?? e?.body ?? e) === payload)),
      { timeoutMs: 12000, everyMs: 250, label: 'pubsub-fanout' });
    for (const p of others) {
      const got = received[p.i].some(e => (e?.message ?? e?.body ?? e) === payload);
      check(`peer ${p.i} received the publish`, got);
    }
    check('publish reached all subscribers', gotIt);

    exitCode = failed === 0 ? 0 : 1;
  } catch (err) {
    console.error('\nharness threw:', err);
    exitCode = 2;
  } finally {
    for (const p of peers) { try { await p.transport.stop?.(); } catch {} }
    bridge.child.kill('SIGTERM');
    // node-datachannel keeps a worker thread alive; force a clean exit.
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch {}
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  await sleep(100);
  process.exit(exitCode);
}

main().catch((err) => { console.error('fatal:', err); process.exit(2); });
