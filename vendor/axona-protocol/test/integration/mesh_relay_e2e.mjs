// =====================================================================
// mesh_relay_e2e.mjs — END-TO-END proof that nodes do NOT depend on the
//   bridge to form new direct connections to each other.
//
// This composes the FULL PRODUCTIZED kernel path (not a hand-wired relay):
//   real webTransport({meshRelay:true}) → real MeshManager + axona/4 →
//   AxonaPeer routing (route_msg / lookup / mesh:signal) → connectViaRelay
// over real WebRTC (node-datachannel), with a REAL axona-bridge that is then
// KILLED mid-session.
//
// SCENARIO (3 real peers A,B,C + a real bridge):
//   1. BOOTSTRAP: all three join via the bridge and form an authenticated
//      mesh. (Proves the bridge bootstrap path still works.)
//   2. KILL THE BRIDGE: terminate the bridge process; wait until every
//      peer's bridge socket is closed. From here NO signaling can flow
//      through the bridge — it does not exist.
//   3. REACH THE PRODUCTION PRECONDITION: sever the A↔C direct channel and
//      drop the stale synapse on both ends, so A holds C's nodeId but has
//      NO channel and NO synapse to C — exactly the state a node is in when
//      it discovers a peer via gossip/triadic introduction and wants to
//      connect (design §3.3). A and C each still hold their channel to B.
//   4. BRIDGELESS CONNECT: A.connectViaRelay(C) — the offer/answer/ICE are
//      relayed A→B→C and back as `mesh:signal` over the live A↔B / B↔C data
//      channels, with the bridge dead.
//
// ASSERTS (the go/no-go gate):
//   · bootstrap mesh formed (bridge worked)
//   · bridge process is dead AND every peer's bridge socket is closed
//     → the bridge physically cannot carry the A↔C signaling
//   · precondition: A↔C not connected, A has no synapse to C, A↔B & B↔C live
//   · A↔C real RTCDataChannel OPENS in BOTH directions, bridge dead
//   · axona/4 binds A↔C end-to-end (mutual signed handshake over the new
//     channel) — i.e. authenticated, not just "a pipe opened"
//   · live ping/pong RTT is measured over the A↔C channel (bytes flowing
//     directly, not via B)
//   · the bridge is STILL dead at the end (no silent reconnect)
//
// Because the bridge process is dead and A↔C had no prior channel, the only
// possible conduit for the SDP/ICE was the peer relay through B. A passing
// run is therefore direct proof that connection formation does not depend on
// the bridge.
//
// OPT-IN: `npm run test:mesh-relay-e2e`. Requires node-datachannel + the
// sibling axona-bridge checkout; SKIPs cleanly (exit 0) if either is absent.
// =====================================================================

import { spawn }            from 'node:child_process';
import { fileURLToPath }    from 'node:url';
import { dirname, resolve } from 'node:path';
import { existsSync } from 'node:fs';

const __dirname    = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT  = resolve(__dirname, '..', '..');
const BRIDGE_ROOT  = resolve(KERNEL_ROOT, '..', 'axona-bridge');

const BRIDGE_PORT  = 19091;
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
  while (!(await predicate())) {
    if (Date.now() - t0 > timeoutMs) return false;
    await sleep(everyMs);
  }
  return true;
}

// ── 0. Optional prerequisites — skip gracefully ─────────────────────
let polyfill;
try { polyfill = await import('node-datachannel/polyfill'); }
catch {
  console.log('SKIP: node-datachannel not installed.');
  process.exit(0);
}
if (!existsSync(resolve(BRIDGE_ROOT, 'src', 'server.js'))) {
  console.log(`SKIP: sibling axona-bridge not found at ${BRIDGE_ROOT}.`);
  process.exit(0);
}
globalThis.RTCPeerConnection = polyfill.RTCPeerConnection;

const { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity } = await import('../../src/index.js');
const { webTransport } = await import('../../src/transport/web/index.js');

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

const REGIONS = [{ lat: 40.71, lng: -74.0 }, { lat: 51.50, lng: -0.12 }, { lat: 35.68, lng: 139.69 }];
const NAMES = ['A', 'B', 'C'];

async function makePeer(i) {
  const region   = REGIONS[i];
  const identity = await createNodeIdentity(region);
  const logs = [];
  const transport = webTransport({
    bridgeUrl: BRIDGE_URL, identity: { ...identity, id: identity.id },
    peerVersion: PEER_VERSION, reconnect: false, meshRelay: true,   // ← feature under test
    log: (e, d) => { logs.push([Date.now(), e, d]); if (process.env.VERBOSE) console.log(`[${NAMES[i]}] ${e}`, d ?? ''); },
  });
  const node   = new NeuronNode({ id: BigInt('0x' + identity.id), lat: region.lat, lng: region.lng });
  node.transport = transport;
  const domain = new AxonaDomain({ k: 20 });
  const devents = [];
  domain.onEvent?.((e) => { if (e && /mesh-signal|relay|route/.test(e.type || '')) devents.push(`${e.type}${e.to ? '→' + String(e.to).slice(0,6) : ''}`); });
  const peer   = new AxonaPeer({ domain, node, identity, transport });
  return { name: NAMES[i], identity, transport, node, peer, logs, domain, devents,
           big: BigInt('0x' + identity.id), hex: identity.id };
}

async function main() {
  console.log('END-TO-END bridgeless connection: A↔C formed via relay through B with the bridge DEAD\n');
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

    // ── Phase 1: bootstrap mesh via the bridge ──────────────────────
    console.log('── phase 1: bootstrap authenticated mesh via the bridge ──');
    const meshBound = (p) => p.peer.health().transport?.meshBound ?? 0;
    const converged = await waitFor(() => peers.every(p => meshBound(p) >= 2), { timeoutMs: 30000 });
    check('all 3 peers bound the mesh via the bridge', converged);
    check('A↔C connected after bootstrap', A.transport.webrtc.isConnected(C.big));

    // ── Phase 2: KILL the bridge ────────────────────────────────────
    console.log('── phase 2: kill the bridge process ──');
    bridge.child.kill('SIGKILL');
    const socketsClosed = await waitFor(
      () => peers.every(p => p.transport.bridgeState !== 'open'), { timeoutMs: 10000 });
    check('every peer\'s bridge socket is closed', socketsClosed);
    check('bridge child process is dead', bridge.child.killed === true);

    // ── Phase 3: production precondition — A has no channel/synapse to C
    console.log('── phase 3: sever A↔C + drop stale synapse (discovered-but-unconnected state) ──');
    const acMesh = A.transport.webrtc.meshIdFor(C.big);
    const caMesh = C.transport.webrtc.meshIdFor(A.big);
    if (acMesh) A.transport.mesh.disconnect(acMesh, 'e2e-sever');
    if (caMesh) C.transport.mesh.disconnect(caMesh, 'e2e-sever');
    A.node.synaptome.delete(C.big);   // simulate "knows C's id, has no synapse"
    C.node.synaptome.delete(A.big);
    const severed = await waitFor(() =>
      !A.transport.webrtc.isConnected(C.big) && !C.transport.webrtc.isConnected(A.big), { timeoutMs: 10000 });
    check('A↔C channel severed', severed);
    check('A has no synapse to C', !A.node.synaptome.has(C.big));
    check('A↔B relay leg still live', A.transport.webrtc.isConnected(B.big));
    check('B↔C relay leg still live', B.transport.webrtc.isConnected(C.big));
    check('bridge socket closed at reconnect time (cannot carry signaling)',
      A.transport.bridgeState !== 'open' && C.transport.bridgeState !== 'open');

    // ── DIAGNOSTICS ─────────────────────────────────────────────────
    const evlog = [];
    for (const p of peers) p.peer.onEvent((e) => {
      if (e && typeof e.type === 'string' && /mesh-signal|relay|route|signal/.test(e.type)) {
        evlog.push(`${p.name}:${e.type}${e.to ? ' to=' + String(e.to).slice(0, 8) : ''}`);
      }
    });
    const diagLk = await A.peer.lookup(C.big);
    const diagLk2 = await C.peer.lookup(A.big);
    console.log(`  · diag A.lookup(C): found=${diagLk?.found} hops=${diagLk?.hops} | C.lookup(A): found=${diagLk2?.found} hops=${diagLk2?.hops}`);
    console.log(`  · diag A.synaptome=[${[...A.node.synaptome.keys()].map(k => String(k).slice(0,6)).join(',')}] B.synaptome size=${B.node.synaptome.size} has A=${B.node.synaptome.has(A.big)} has C=${B.node.synaptome.has(C.big)}`);

    // ── Phase 4: bridgeless connect A→C, relayed through B ──────────
    console.log('── phase 4: A.connectViaRelay(C) — SDP/ICE relayed A→B→C, bridge dead ──');
    const initiated = A.transport.connectViaRelay(C.hex);
    check('connectViaRelay initiated', initiated === true);

    const reformed = await waitFor(() =>
      A.transport.webrtc.isConnected(C.big) && C.transport.webrtc.isConnected(A.big), { timeoutMs: 30000 });
    check('A↔C REAL DataChannel re-opened in BOTH directions (bridge dead)', reformed);
    if (!reformed) {
      console.log(`  · diag events: ${evlog.join(' | ') || '(none)'}`);
      console.log(`  · diag A.mesh peers: ${JSON.stringify(A.transport.mesh.getPeers().map(p => ({ id: String(p.peerId).slice(0,8), st: p.state, role: p.role })))}`);
      console.log(`  · diag C.mesh peers: ${JSON.stringify(C.transport.mesh.getPeers().map(p => ({ id: String(p.peerId).slice(0,8), st: p.state, role: p.role })))}`);
      const rel = /signal|ice|offer|answer|pc-state|dc-|initiate|accept|unknown|stats/i;
      const dump = (p) => p.logs.filter(([, e]) => rel.test(e)).slice(-18).map(([, e, d]) => `${e}${d?.peerId ? '(' + String(d.peerId).slice(0,6) + ')' : ''}${d?.kind ? ':' + d.kind : ''}${d?.ice ? ':' + d.ice : ''}${d?.pc ? ':' + d.pc : ''}`).join(' ');
      console.log(`  · diag A.log: ${dump(A)}`);
      console.log(`  · diag C.log: ${dump(C)}`);
      console.log(`  · diag domain events A=[${A.devents.join(',')}] B=[${B.devents.join(',')}] C=[${C.devents.join(',')}]`);
    }

    const bound = await waitFor(() =>
      A.transport.webrtc.boundPeers().includes(C.big) &&
      C.transport.webrtc.boundPeers().includes(A.big), { timeoutMs: 10000 });
    check('axona/4 bound A↔C end-to-end (authenticated over the relayed channel)', bound);

    // Live ping/pong RTT proves bytes flow over the A↔C channel directly.
    const rttLive = await waitFor(() => A.transport.getLatency(C.big) >= 0, { timeoutMs: 8000 });
    check('live RTT measured over the direct A↔C channel (bytes flowing)', rttLive);

    check('bridge STILL dead at end (no silent reconnect carried it)',
      A.transport.bridgeState !== 'open' && bridge.child.killed === true);

    exitCode = failed === 0 ? 0 : 1;
  } catch (err) {
    console.error('\nharness threw:', err); exitCode = 2;
  } finally {
    for (const p of peers) { try { await p.transport.stop?.(); } catch {} }
    try { bridge.child.kill('SIGKILL'); } catch {}
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch {}
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  await sleep(100);
  process.exit(exitCode);
}

main().catch((err) => { console.error('fatal:', err); process.exit(2); });
