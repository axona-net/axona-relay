// =====================================================================
// graduation_probe.mjs — watch the mesh die when a node leaves the bridge.
//
// #374, the existential question: when a fully-meshed node's bridge link
// closes (a graduation 4200, or any bridge departure), does its WebRTC
// mesh SURVIVE — as bridge-independence requires — or does it collapse?
//
// This is the instrumented probe David asked for. It:
//   • boots the REAL axona-bridge on a throwaway port
//   • brings up N real webTransport peers over real node-datachannel
//   • waits until the mesh is FULLY converged (every peer bound N-1)
//   • designates peer 0 the PROBE and, once it is fully integrated,
//     has it EXPLICITLY close its own bridge socket (mesh untouched by
//     the close handler — reconnect:false, ordinary close code)
//   • then samples boundPeers on every peer every 250ms and captures the
//     mesh-layer `teardown` logs (with their reason) on every peer
//
// EXPECTED (the bug): the probe's bound mesh does NOT survive. Within a
// couple seconds the OTHER peers each log `teardown reason=peer-left`
// for the probe and close their live, healthy DTLS channels to it —
// because the bridge broadcast `peer-left(probe)` on the socket close
// (axona-bridge server.js ws.on('close')) and mesh.onPeerLeft retires
// unconditionally (mesh.js) even though the channel is still open. The
// probe's boundPeers collapses toward 0; in prod its graduation-watch
// then re-dials the bridge → the open→graduate→peers=0→open churn loop.
//
// A bridge-independent mesh would keep every channel: a peer leaving the
// BRIDGE is not a peer leaving the MESH.
//
// OPT-IN: not part of `npm test`. Requires node-datachannel + the sibling
// axona-bridge checkout; SKIPs cleanly (exit 0) if either is absent.
//
//   node test/integration/graduation_probe.mjs           # concise
//   VERBOSE=1 node test/integration/graduation_probe.mjs  # full logs
// =====================================================================

import { spawn }             from 'node:child_process';
import { fileURLToPath }     from 'node:url';
import { dirname, resolve }  from 'node:path';
import { existsSync, rmSync } from 'node:fs';

const __dirname   = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT = resolve(__dirname, '..', '..');
const BRIDGE_ROOT = resolve(KERNEL_ROOT, '..', 'axona-bridge');

const N            = 3;          // peers — probe + 2 witnesses (enough to show
                                 // witnesses retiring a live channel on peer-left;
                                 // the graduation floor is irrelevant here since
                                 // reconnect:false isolates the cut)
const PROBE        = 0;          // which peer severs its bridge link
const BRIDGE_PORT  = 19097;
const BRIDGE_URL   = `ws://localhost:${BRIDGE_PORT}`;
const PEER_VERSION = '9.9.9';
const WATCH_MS     = 9000;       // how long to watch after the cut
const SAMPLE_MS    = 250;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const now   = (t0) => `${String(Date.now() - t0).padStart(5)}ms`;

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

// ── 1. Kernel under test (local working source) ─────────────────────
const { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity } =
  await import('../../src/index.js');
const { webTransport } = await import('../../src/transport/web/index.js');

// ── 2. Boot the real bridge ─────────────────────────────────────────
function startBridge() {
  const identityPath = `/tmp/axona-bridge-grad-${process.pid}.json`;
  try { rmSync(identityPath, { force: true }); } catch {}
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_ROOT,
    env: {
      ...process.env,
      PORT: String(BRIDGE_PORT),
      BRIDGE_IDENTITY_PATH: identityPath,
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
  return { child, identityPath, ready: () => ready };
}

async function waitFor(pred, { timeoutMs = 15000, everyMs = 200 } = {}) {
  const t = Date.now();
  while (!pred()) { if (Date.now() - t > timeoutMs) return false; await sleep(everyMs); }
  return true;
}

const REGIONS = [
  { lat: 40.71, lng: -74.0 }, { lat: 51.50, lng: -0.12 },
  { lat: 35.68, lng: 139.69 }, { lat: -33.87, lng: 151.21 },
  { lat: 37.77, lng: -122.42 },
];

// Short, stable label for a hex nodeId (first 6 chars).
const tag = (hex) => (hex ? String(hex).slice(0, 6) : '??????');

async function makePeer(i, t0) {
  const region   = REGIONS[i % REGIONS.length];
  const identity = await createNodeIdentity(region);
  const idHex    = identity.id;
  const teardowns = [];   // captured mesh `teardown` events (the smoking gun)
  const transport = webTransport({
    bridgeUrl:   BRIDGE_URL,
    identity:    { ...identity, id: idHex },
    peerVersion: PEER_VERSION,
    reconnect:   false,    // isolate the cut: no auto-redial to muddy the picture
    log: (e, d) => {
      if (e === 'teardown') teardowns.push({ at: Date.now() - t0, ...d });
      if (process.env.VERBOSE) console.log(`[p${i} ${tag(idHex)}] ${e}`, d ?? '');
    },
  });
  const node   = new NeuronNode({ id: BigInt('0x' + idHex), lat: region.lat, lng: region.lng });
  node.transport = transport;
  const domain = new AxonaDomain({ k: 20 });
  const peer   = new AxonaPeer({ domain, node, identity, transport });
  return { i, idHex, tag: tag(idHex), transport, node, peer, teardowns };
}

// Mesh-bound count straight from the transport (excludes the bridge link).
const boundOf = (p) => {
  try { return p.transport.webrtc.boundPeers().length; } catch { return -1; }
};

async function main() {
  const t0 = Date.now();
  console.log(`#374 graduation probe — ${N} real peers + real bridge + node-datachannel`);
  console.log(`Question: does peer ${PROBE}'s WebRTC mesh survive when it leaves the bridge?\n`);

  const bridge = startBridge();
  if (!(await waitFor(bridge.ready, { timeoutMs: 8000 }))) {
    bridge.child.kill('SIGKILL');
    console.error('FAIL: bridge did not start'); process.exit(2);
  }

  const peers = [];
  let exitCode = 2;
  try {
    for (let i = 0; i < N; i++) peers.push(await makePeer(i, t0));
    for (const p of peers) await p.transport.start();
    for (const p of peers) await p.peer.start();
    console.log(`· ${N} peers on the bridge; converging mesh…`);

    const allBound = () => peers.every(p => boundOf(p) >= N - 1);
    const converged = await waitFor(allBound, { timeoutMs: 25000, everyMs: 250 });
    if (!converged) {
      console.error('FAIL: mesh never fully converged — cannot run the probe.');
      for (const p of peers) console.error(`   peer ${p.i} ${p.tag}: bound ${boundOf(p)}/${N - 1}`);
      throw new Error('no full mesh');
    }
    const probe = peers[PROBE];
    console.log(`· mesh converged — every peer bound ${N - 1}/${N - 1}`);
    console.log(`  baseline boundPeers: ${peers.map(p => `p${p.i}=${boundOf(p)}`).join('  ')}`);

    // ── THE CUT ─────────────────────────────────────────────────────
    console.log(`\n[${now(t0)}] ✂️  peer ${PROBE} (${probe.tag}) closes its OWN bridge socket — mesh left intact.\n`);
    probe.transport.__probeCloseBridgeSocket(1000, 'probe-graduation');

    // ── WATCH ───────────────────────────────────────────────────────
    console.log('time     ' + peers.map(p => `p${p.i}`).map(s => s.padStart(4)).join(' ') + '   probe-mesh');
    const deadline = Date.now() + WATCH_MS;
    let probeFloor = boundOf(probe);
    while (Date.now() < deadline) {
      const row = peers.map(p => String(boundOf(p)).padStart(4)).join(' ');
      const pm  = boundOf(probe);
      probeFloor = Math.min(probeFloor, pm);
      console.log(`${now(t0)}  ${row}   ${pm === 0 ? '💀 0' : pm}`);
      if (pm <= 0) break;               // fully stranded — seen enough
      await sleep(SAMPLE_MS);
    }

    // ── VERDICT ─────────────────────────────────────────────────────
    const probeFinal = boundOf(probe);
    // Who tore down the probe, and why?
    const peerLeftKills = [];
    for (const p of peers) {
      for (const td of p.teardowns) {
        if (td.peerId && td.reason) {
          peerLeftKills.push({ by: p.i, byTag: p.tag, ...td });
        }
      }
    }
    console.log('\n── teardown log (the mechanism) ──');
    if (peerLeftKills.length === 0) console.log('  (none captured)');
    for (const k of peerLeftKills.sort((a, b) => a.at - b.at)) {
      console.log(`  [${String(k.at).padStart(5)}ms] peer ${k.by} (${k.byTag}) tore down a channel — reason="${k.reason}" role=${k.role} wasOpen(state=${k.state})`);
    }

    const survived   = probeFinal >= N - 1;
    const collapsed  = probeFloor <= 0;
    const byPeerLeft = peerLeftKills.some(k => k.reason === 'peer-left');

    console.log('\n── verdict ──');
    console.log(`  probe boundPeers: baseline ${N - 1} → min ${probeFloor} → final ${probeFinal}`);
    if (survived) {
      console.log('  ✅ MESH SURVIVED the bridge departure — bridge-independence holds.');
      exitCode = 0;   // fixed
    } else {
      console.log('  ❌ MESH COLLAPSED — the probe lost its live channels after leaving the bridge.');
      if (byPeerLeft) {
        console.log('     Cause: witnesses received the bridge\'s `peer-left(probe)` broadcast and');
        console.log('     retired healthy DTLS channels (mesh.onPeerLeft → _retire). The bridge roster');
        console.log('     killed a live mesh — the exact bridge-dependence flaw #374 is about.');
      }
      exitCode = collapsed ? 1 : 3;   // 1 = full repro, 3 = partial degrade
    }
  } catch (err) {
    console.error('\nprobe threw:', err?.message || err);
    exitCode = 2;
  } finally {
    for (const p of peers) { try { await p.transport.stop?.(); } catch {} }
    bridge.child.kill('SIGTERM');
    try { rmSync(bridge.identityPath, { force: true }); } catch {}
    try { polyfill.RTCPeerConnection?.cleanup?.(); } catch {}
  }

  console.log(`\nexit ${exitCode}  (0=survived/fixed · 1=collapsed/repro · 3=partial · 2=error)`);
  await sleep(100);
  process.exit(exitCode);
}

main().catch((err) => { console.error('fatal:', err); process.exit(2); });
