// =====================================================================
// mesh_signal_split.mjs — W1b: how does a NEW node reach the mesh?
//
// Stands up a settled mesh of M real webTransport peers (real bridge, real
// node-datachannel WebRTC), then connects ONE fresh joiner and reads *its*
// signaling split from transport.signalStats() (W1a instrumentation):
//   • bridgePeers  — distinct peers the joiner signalled to via the BRIDGE
//                    (bridge-introduced edges — unavoidable for bootstrap)
//   • meshPeers    — distinct peers it signalled via the MESH relay
//                    (connectViaRelay / peer-relayed — the bridgeless path)
//   • bridgeMsgFraction — share of signalling MESSAGES that hit the bridge
//
// Claim under test: "with a well-connected mesh, a joiner should reach almost
// all of its neighbours peer-to-peer — the bridge only carries the first
// edge(s) + genuine NAT/ICE failures." Sweeps M ∈ {3,6,12} → the
// bridge-fallback-vs-mesh-size curve. MEASUREMENT harness (prints a table),
// not a hard pass/fail smoke — a high bridge share is a finding (motivates the
// bridge bootstrap-nursery, W2), not a test failure.
//
// OPT-IN. Requires node-datachannel + the sibling axona-bridge checkout;
// SKIPs cleanly (exit 0) if either is absent.
//   node test/integration/mesh_signal_split.mjs
// =====================================================================

import { spawn }            from 'node:child_process';
import { fileURLToPath }    from 'node:url';
import { dirname, resolve } from 'node:path';
import { existsSync } from 'node:fs';

const __dirname   = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT = resolve(__dirname, '..', '..');
const BRIDGE_ROOT = resolve(KERNEL_ROOT, '..', 'axona-bridge');
const BRIDGE_PORT = 19091;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const MESH_SIZES  = (process.env.MESH_SIZES || '3,6,12').split(',').map(Number);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

let polyfill;
try { polyfill = await import('node-datachannel/polyfill'); }
catch { console.log('SKIP: node-datachannel not installed.'); process.exit(0); }
if (!existsSync(resolve(BRIDGE_ROOT, 'src', 'server.js'))) {
  console.log(`SKIP: sibling axona-bridge not found at ${BRIDGE_ROOT}.`); process.exit(0);
}
globalThis.RTCPeerConnection = polyfill.RTCPeerConnection;

const { connect } = await import('../../src/connect.js');

function startBridge() {
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_ROOT,
    env: { ...process.env, PORT: String(BRIDGE_PORT),
           LOG_LEVEL: process.env.VERBOSE ? 'info' : 'warn', MIN_PEER_VERSION: '0.0.1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let ready = false;
  child.stdout.on('data', (c) => { if (c.toString().includes('"event":"listen"')) ready = true;
    if (process.env.VERBOSE) process.stdout.write('[bridge] ' + c); });
  child.stderr.on('data', (c) => { if (process.env.VERBOSE) process.stderr.write('[bridge] ' + c); });
  return { child, ready: () => ready };
}

async function waitFor(pred, { timeoutMs = 30000, everyMs = 250 } = {}) {
  const t0 = Date.now();
  while (!pred()) { if (Date.now() - t0 > timeoutMs) return false; await sleep(everyMs); }
  return true;
}

// Spread across regions so ids differ in the geo prefix (production-like).
const REGIONS = [
  { lat: 40.71, lng: -74.0 }, { lat: 51.50, lng: -0.12 }, { lat: 35.68, lng: 139.69 },
  { lat: -33.87, lng: 151.21 }, { lat: 37.77, lng: -122.42 }, { lat: 48.85, lng: 2.35 },
  { lat: 1.35, lng: 103.82 }, { lat: -23.55, lng: -46.63 }, { lat: 55.75, lng: 37.62 },
  { lat: 19.43, lng: -99.13 }, { lat: 28.61, lng: 77.21 }, { lat: -26.20, lng: 28.04 },
  { lat: 59.33, lng: 18.07 },
];
let regionCursor = 0;
const nextPeer = () => connect({
  bridge: BRIDGE_URL, location: REGIONS[regionCursor++ % REGIONS.length],
  author: false, ready: false, web: { peerVersion: '9.9.9', reconnect: false },
});
const meshBoundOf = (c) => { try { return c.peer.health().transport?.meshBound ?? 0; } catch { return 0; } };

async function runOne(M) {
  regionCursor = 0;
  const base = [];
  for (let i = 0; i < M; i++) base.push(await nextPeer());
  // settle the base mesh (best-effort full convergence)
  const converged = await waitFor(() => base.every(c => meshBoundOf(c) >= M - 1), { timeoutMs: 35000 });
  const baseDeg = base.map(meshBoundOf);

  // NOW the joiner — a fresh node reaching an already-settled mesh.
  const joiner = await nextPeer();
  // let it integrate: bind as many as it will, up to the mesh size, or settle out
  await waitFor(() => meshBoundOf(joiner) >= M, { timeoutMs: 20000 });
  await sleep(3000);   // let late signalling flush

  const s = joiner.transport.signalStats();
  const bound = meshBoundOf(joiner);
  const row = {
    meshSize: M, baseConverged: converged, baseDegreeMin: Math.min(...baseDeg), baseDegreeMax: Math.max(...baseDeg),
    joinerBound: bound,
    meshPeers: s.meshPeers, bridgePeers: s.bridgePeers,
    meshMsgs: s.meshMsgs, bridgeMsgs: s.bridgeMsgs, dropMsgs: s.dropMsgs,
    bridgeMsgFraction: s.bridgeMsgFraction,
  };
  for (const c of [joiner, ...base]) { try { await c.disconnect(); } catch { /* */ } }
  await sleep(1500);
  return row;
}

async function main() {
  console.log(`W1b mesh-signal-split — joiner into settled mesh of M ∈ {${MESH_SIZES.join(', ')}}\n`);
  const bridge = startBridge();
  if (!(await waitFor(bridge.ready, { timeoutMs: 8000 }))) { bridge.child.kill('SIGKILL'); console.error('FAIL: bridge did not start'); process.exit(2); }
  const rows = [];
  try {
    for (const M of MESH_SIZES) {
      console.log(`── M=${M}: standing up mesh + joiner …`);
      rows.push(await runOne(M));
    }
    console.log('\n=== JOINER SIGNALING SPLIT (how a new node reached the mesh) ===');
    console.log('M  baseConv baseDeg  joinerBound  meshPeers bridgePeers  meshMsg bridgeMsg  bridgeMsgFrac');
    for (const r of rows) {
      console.log(
        `${String(r.meshSize).padEnd(2)} ${String(r.baseConverged).padEnd(8)} ${(`${r.baseDegreeMin}-${r.baseDegreeMax}`).padEnd(8)} ` +
        `${String(r.joinerBound).padEnd(12)} ${String(r.meshPeers).padEnd(9)} ${String(r.bridgePeers).padEnd(12)} ` +
        `${String(r.meshMsgs).padEnd(8)} ${String(r.bridgeMsgs).padEnd(10)} ${r.bridgeMsgFraction}`);
    }
    console.log('\nReading: meshPeers ≫ bridgePeers and bridgeMsgFraction→0 as M grows ⇒ the mesh self-signals new edges (claim holds).');
    console.log('bridgePeers ≈ joinerBound ⇒ the bridge is doing the introducing (mesh-relay under-used) ⇒ motivates the bootstrap-nursery (W2).');
  } catch (err) { console.error('harness threw:', err); process.exitCode = 2; }
  finally {
    bridge.child.kill('SIGTERM');
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch { /* */ }
  }
  await sleep(150);
  process.exit(process.exitCode || 0);
}
main().catch((e) => { console.error('fatal:', e); process.exit(2); });
