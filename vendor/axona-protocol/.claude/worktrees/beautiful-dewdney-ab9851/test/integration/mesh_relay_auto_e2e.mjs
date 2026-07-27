// =====================================================================
// mesh_relay_auto_e2e.mjs — AUTONOMOUS bridgeless connect, bridge killed.
//
// mesh_relay_e2e.mjs proves connectViaRelay() works when called explicitly.
// THIS proves the node does it BY ITSELF on peer discovery, with the feature
// at its shipping DEFAULT — i.e. that a deployment genuinely does not depend
// on the bridge to form new direct connections.
//
//   · peers are built WITHOUT passing meshRelay → relies on the kernel default
//     (meshRelay = true as of v2.19.0).
//   · A↔C is triggered by a real `triadic_introduce` from B (peer-driven
//     discovery), NOT by calling connectViaRelay. The autonomous chain:
//       B → triadic_introduce(C) → A.onNotification → _considerCandidate(C)
//         → openConnection(C) fails (no bridge binding) → connectViaRelay(C)
//         → mesh:signal relayed A→B→C → axona/4 binds → onPeerBound admits.
//
// SCENARIO: bootstrap mesh via a real bridge → KILL the bridge → sever A↔C and
// drop the stale synapse → B introduces C to A → assert A autonomously forms
// an authenticated direct channel to C AND admits it to the routing table,
// with the bridge dead.
//
//   npm run test:mesh-relay-auto-e2e
// =====================================================================

import { spawn }            from 'node:child_process';
import { fileURLToPath }    from 'node:url';
import { dirname, resolve } from 'node:path';
import { existsSync, rmSync } from 'node:fs';

const __dirname    = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT  = resolve(__dirname, '..', '..');
const BRIDGE_ROOT  = resolve(KERNEL_ROOT, '..', 'axona-bridge');
const BRIDGE_PORT  = 19092;
const BRIDGE_URL   = `ws://localhost:${BRIDGE_PORT}`;
const PEER_VERSION = '9.9.9';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function waitFor(predicate, { timeoutMs = 20000, everyMs = 200 } = {}) {
  const t0 = Date.now();
  while (!(await predicate())) { if (Date.now() - t0 > timeoutMs) return false; await sleep(everyMs); }
  return true;
}

let polyfill;
try { polyfill = await import('node-datachannel/polyfill'); }
catch { console.log('SKIP: node-datachannel not installed.'); process.exit(0); }
if (!existsSync(resolve(BRIDGE_ROOT, 'src', 'server.js'))) {
  console.log(`SKIP: sibling axona-bridge not found at ${BRIDGE_ROOT}.`); process.exit(0);
}
globalThis.RTCPeerConnection = polyfill.RTCPeerConnection;

const { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity } = await import('../../src/index.js');
const { webTransport } = await import('../../src/transport/web/index.js');

function startBridge() {
  const identityPath = `/tmp/axona-bridge-auto-${process.pid}.json`;
  try { rmSync(identityPath, { force: true }); } catch {}
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_ROOT,
    env: { ...process.env, PORT: String(BRIDGE_PORT), BRIDGE_IDENTITY_PATH: identityPath,
           LOG_LEVEL: process.env.VERBOSE ? 'info' : 'warn', MIN_PEER_VERSION: '0.0.1' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let ready = false;
  child.stdout.on('data', (c) => { if (c.toString().includes('"event":"listen"')) ready = true; });
  return { child, identityPath, ready: () => ready };
}

const REGIONS = [{ lat: 40.71, lng: -74.0 }, { lat: 51.50, lng: -0.12 }, { lat: 35.68, lng: 139.69 }];
const NAMES = ['A', 'B', 'C'];

async function makePeer(i) {
  const region   = REGIONS[i];
  const identity = await createNodeIdentity(region);
  // meshRelay NOT passed — relies on the kernel default (v2.19.0 = on).
  const transport = webTransport({
    bridgeUrl: BRIDGE_URL, identity: { ...identity, id: identity.id },
    peerVersion: PEER_VERSION, reconnect: false, log: () => {},
  });
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: region.lat, lng: region.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, identity, transport });
  return { name: NAMES[i], identity, transport, node, peer,
           big: BigInt('0x' + identity.id), hex: identity.id };
}

async function main() {
  console.log('AUTONOMOUS bridgeless connect (default flag): A forms A↔C on discovery, bridge DEAD\n');
  const bridge = startBridge();
  if (!(await waitFor(bridge.ready, { timeoutMs: 8000 }))) {
    bridge.child.kill('SIGKILL'); console.error('FAIL: bridge did not start'); process.exit(2);
  }
  const peers = [];
  let exitCode = 1;
  try {
    for (let i = 0; i < 3; i++) peers.push(await makePeer(i));
    const [A, B, C] = peers;
    for (const p of peers) await p.transport.start();
    for (const p of peers) await p.peer.start();

    check('meshRelay is ON by default (capability advertised)',
      typeof A.transport.hasCapability === 'function' && A.transport.hasCapability('mesh-relay'));

    console.log('── bootstrap mesh via bridge ──');
    const meshBound = (p) => p.peer.health().transport?.meshBound ?? 0;
    check('mesh converged via bridge', await waitFor(() => peers.every(p => meshBound(p) >= 2), { timeoutMs: 30000 }));

    console.log('── kill bridge ──');
    bridge.child.kill('SIGKILL');
    check('bridge sockets closed', await waitFor(() => peers.every(p => p.transport.bridgeState !== 'open'), { timeoutMs: 10000 }));

    console.log('── sever A↔C, drop stale synapse (A knows C\'s id, no channel) ──');
    const acMesh = A.transport.webrtc.meshIdFor(C.big);
    const caMesh = C.transport.webrtc.meshIdFor(A.big);
    if (acMesh) A.transport.mesh.disconnect(acMesh, 'sever');
    if (caMesh) C.transport.mesh.disconnect(caMesh, 'sever');
    A.node.synaptome.delete(C.big);
    C.node.synaptome.delete(A.big);
    check('A↔C severed', await waitFor(() =>
      !A.transport.webrtc.isConnected(C.big) && !C.transport.webrtc.isConnected(A.big), { timeoutMs: 10000 }));
    check('A↔B & B↔C relay legs live',
      A.transport.webrtc.isConnected(B.big) && B.transport.webrtc.isConnected(C.big));

    // ── AUTONOMOUS TRIGGER: B introduces C to A (real triadic_introduce). ──
    console.log('── B → triadic_introduce(C) → A connects to C ON ITS OWN, bridge dead ──');
    await B.transport.notify(A.big, 'triadic_introduce', { peerId: C.big });

    const formed = await waitFor(() =>
      A.transport.webrtc.isConnected(C.big) && C.transport.webrtc.isConnected(A.big), { timeoutMs: 30000 });
    check('A↔C direct channel formed AUTONOMOUSLY on discovery (bridge dead)', formed);
    check('axona/4 bound A↔C end-to-end',
      A.transport.webrtc.boundPeers().includes(C.big) && C.transport.webrtc.boundPeers().includes(A.big));
    check('C re-admitted into A\'s synaptome (discovery → live routing peer)',
      await waitFor(() => A.node.synaptome.has(C.big), { timeoutMs: 8000 }));
    check('bridge still dead (relay carried it, not a reconnect)',
      A.transport.bridgeState !== 'open' && bridge.child.killed === true);

    exitCode = failed === 0 ? 0 : 1;
  } catch (err) { console.error('\nharness threw:', err); exitCode = 2; }
  finally {
    for (const p of peers) { try { await p.transport.stop?.(); } catch {} }
    try { bridge.child.kill('SIGKILL'); } catch {}
    try { rmSync(bridge.identityPath, { force: true }); } catch {}
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch {}
  }
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  await sleep(100);
  process.exit(exitCode);
}
main().catch((err) => { console.error('fatal:', err); process.exit(2); });
