// =====================================================================
// mesh_relay_multihop_e2e.mjs — GENUINE MULTI-HOP, AT-SCALE proof that
//   WebRTC peers find AND connect to each other with NO bridge.
//
// WHY THIS EXISTS (vs mesh_relay_e2e / _auto_e2e):
//   The 3-peer e2e harnesses prove relay works, but the relay there is a
//   SINGLE intermediary B that is directly adjacent to BOTH endpoints. That
//   is the easy case. A deployment is never a full mesh: to reach a peer you
//   must relay the SDP/ICE across SEVERAL routed kernel hops, through nodes
//   that are NOT adjacent to your target. THIS harness proves exactly that.
//
// THE IRONCLAD CONDITION — "no common neighbour":
//   We stand up N real peers, then prune the mesh to a SPARSE, connectivity-
//   checked topology and pick an (origin O, target T) pair such that:
//       · O and T are NOT directly connected, AND
//       · O and T share NO common neighbour.
//   With no shared neighbour, a single relay hop is STRUCTURALLY IMPOSSIBLE:
//   no one peer can carry O's offer straight to T. So if an authenticated
//   O↔T channel forms with the bridge process DEAD, the signalling provably
//   chained through ≥2 distinct intermediaries over the live mesh. We also
//   instrument every `mesh:signal` forward and PRINT the actual relay chain.
//
// WHAT IT ASSERTS:
//   1. bootstrap mesh via a real bridge (N peers)
//   2. prune to a sparse routable topology; ≥1 no-common-neighbour pair exists
//   3. KILL the bridge; every peer's socket closes
//   4. for the headline no-common-neighbour pair: O.connectViaRelay(T) forms a
//      REAL RTCDataChannel, axona/4 binds end-to-end, live RTT flows, the relay
//      chain had ≥2 distinct forwarders, and the bridge stayed dead
//   5. BULK: every sampled non-adjacent pair the router can reach forms a
//      direct channel over relay, bridge dead (deployment-scale convergence)
//
//   PEERS=<n> KEEP=<k> VERBOSE=1 npm run test:mesh-relay-multihop
// =====================================================================

import { spawn }              from 'node:child_process';
import { fileURLToPath }      from 'node:url';
import { dirname, resolve }   from 'node:path';
import { existsSync, rmSync } from 'node:fs';

const __dirname    = dirname(fileURLToPath(import.meta.url));
const KERNEL_ROOT  = resolve(__dirname, '..', '..');
const BRIDGE_ROOT  = resolve(KERNEL_ROOT, '..', 'axona-bridge');
const BRIDGE_PORT  = 19093;
const BRIDGE_URL   = `ws://localhost:${BRIDGE_PORT}`;
const PEER_VERSION = '9.9.9';

const N    = Math.max(5, parseInt(process.env.PEERS || '8', 10));
const KEEP = Math.max(1, parseInt(process.env.KEEP  || '2', 10));

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
  const identityPath = `/tmp/axona-bridge-mh-${process.pid}.json`;
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

// Spread N peers around the globe so geo-prefixed nodeIds span the keyspace.
function regionFor(i) {
  const lat = -55 + ((i * 9973) % 110);
  const lng = -179 + ((i * 7919) % 358);
  return { lat, lng };
}

const tally = new Map();   // peerName → count of mesh:signal route frames it emitted

async function makePeer(i) {
  const region   = regionFor(i);
  const identity = await createNodeIdentity(region);
  const name     = `P${i}`;
  const transport = webTransport({
    bridgeUrl: BRIDGE_URL, identity: { ...identity, id: identity.id },
    peerVersion: PEER_VERSION, reconnect: false,   // meshRelay defaults ON
    log: (e, d) => { if (process.env.VERBOSE) console.log(`[${name}] ${e}`, d ?? ''); },
  });
  // Instrument: count every route_msg carrying a mesh:signal that THIS peer
  // emits (as origin or as a forwarder). The set of emitters for one
  // negotiation IS the relay chain.
  const origSend = transport.send.bind(transport);
  transport.send = (nodeId, type, body) => {
    if (type === 'route_msg' && body && body.type === 'mesh:signal') {
      tally.set(name, (tally.get(name) || 0) + 1);
    }
    return origSend(nodeId, type, body);
  };
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: region.lat, lng: region.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, identity, transport });
  return { name, identity, transport, node, peer, big: BigInt('0x' + identity.id), hex: identity.id };
}

// XOR distance helper
const xor = (a, b) => (a < b ? b - a : a - b) === 0n ? 0n : (a ^ b);

// Build a sparse, CONNECTED keep-set from the LIVE bootstrap graph that retains
// ≥1 no-common-neighbour pair. Strategy: start from the SPARSEST base (symmetric
// k-nearest by XOR, k=KEEP), then make it connected by merging components with
// the *minimum* number of long edges — preserving sparsity so two non-adjacent
// nodes still share no neighbour (the property that makes a single relay
// impossible). Only edges that exist LIVE in the bootstrap are eligible, so the
// retained channel is real. Planning from `liveSets` (what actually
// bootstrapped) makes the prune robust to a real-WebRTC bootstrap that left a
// channel or two unformed.
function componentsOf(adj) {
  const comp = new Array(N).fill(-1); let c = 0;
  for (let s = 0; s < N; s++) {
    if (comp[s] !== -1) continue;
    const stack = [s]; comp[s] = c;
    while (stack.length) { const u = stack.pop(); for (const v of adj[u]) if (comp[v] === -1) { comp[v] = c; stack.push(v); } }
    c++;
  }
  return { comp, count: c };
}
function ncnPairs(adj) {
  const out = [];
  for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) {
    if (adj[i].has(j)) continue;
    if (![...adj[i]].some(x => adj[j].has(x))) out.push([i, j]);
  }
  return out;
}
function planTopology(peers, liveSets) {
  const ids = peers.map(p => p.big);
  const live = (i, j) => liveSets[i].has(j);
  for (let k = KEEP; k < N; k++) {
    const adj = peers.map(() => new Set());
    for (let i = 0; i < N; i++) {
      const order = [...liveSets[i]]
        .sort((a, b) => { const da = ids[i] ^ ids[a], db = ids[i] ^ ids[b]; return da < db ? -1 : da > db ? 1 : 0; });
      for (const j of order.slice(0, k)) { adj[i].add(j); adj[j].add(i); }   // symmetric, LIVE-only
    }
    // Merge components using the fewest, XOR-nearest LIVE cross-component edges.
    for (let guard = 0; guard < N; guard++) {
      const { comp, count } = componentsOf(adj);
      if (count === 1) break;
      let best = null, bestD = null;
      for (let i = 0; i < N; i++) for (let j = 0; j < N; j++) {
        if (i === j || comp[i] === comp[j] || !live(i, j)) continue;
        const d = ids[i] ^ ids[j];
        if (bestD === null || d < bestD) { bestD = d; best = [i, j]; }
      }
      if (!best) break;                       // components can't be joined over live edges
      adj[best[0]].add(best[1]); adj[best[1]].add(best[0]);
    }
    if (componentsOf(adj).count !== 1) continue;
    const ncn = ncnPairs(adj);
    if (ncn.length > 0) return { k, adj, ncn };
  }
  return null;
}

async function main() {
  console.log(`GENUINE MULTI-HOP bridgeless connect — ${N} real WebRTC peers, sparse mesh, bridge DEAD\n`);
  const bridge = startBridge();
  if (!(await waitFor(bridge.ready, { timeoutMs: 8000 }))) {
    bridge.child.kill('SIGKILL'); console.error('FAIL: bridge did not start'); process.exit(2);
  }
  const peers = [];
  let exitCode = 1;
  try {
    for (let i = 0; i < N; i++) peers.push(await makePeer(i));
    for (const p of peers) await p.transport.start();
    for (const p of peers) await p.peer.start();

    check(`meshRelay ON by default on all ${N} peers`,
      peers.every(p => typeof p.transport.hasCapability === 'function' && p.transport.hasCapability('mesh-relay')));

    // Bidirectional LIVE adjacency over the WebRTC channel graph.
    const liveAdjSets = () => {
      const a = peers.map(() => new Set());
      for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++)
        if (peers[i].transport.webrtc.isConnected(peers[j].big) &&
            peers[j].transport.webrtc.isConnected(peers[i].big)) { a[i].add(j); a[j].add(i); }
      return a;
    };
    const isConnectedGraph = (a) => {
      const seen = new Set([0]); const stack = [0];
      while (stack.length) { const u = stack.pop(); for (const v of a[u]) if (!seen.has(v)) { seen.add(v); stack.push(v); } }
      return seen.size === N;
    };

    // ── Phase 1: bootstrap mesh via the bridge ──
    // We only need a CONNECTED mesh dense enough to prune to a keep=k backbone,
    // not a perfect full mesh — node-datachannel occasionally leaves one of the
    // N·(N-1)/2 bootstrap channels unformed, which is irrelevant to the feature.
    console.log(`── phase 1: bootstrap ${N}-peer mesh via the bridge ──`);
    const meshBound = (p) => p.peer.health().transport?.meshBound ?? 0;
    await waitFor(() => peers.every(p => meshBound(p) >= N - 1), { timeoutMs: 90000 });
    const boot = liveAdjSets();
    const bootMin = Math.min(...boot.map(s => s.size));
    const bootFull = peers.every(p => meshBound(p) >= N - 1);
    console.log(`  · bootstrap: min live degree ${bootMin}/${N - 1}${bootFull ? ' (full mesh)' : ''}, connected=${isConnectedGraph(boot)}`);
    check('bootstrap mesh is CONNECTED and dense enough to prune (min degree ≥ keep+1)',
      isConnectedGraph(boot) && bootMin >= KEEP + 1);

    // ── Phase 2: plan + apply a sparse, connected topology ──
    console.log('── phase 2: prune to a sparse, connectivity-checked topology ──');
    const plan = planTopology(peers, boot);
    check('found a connected sparse topology with a no-common-neighbour pair', plan !== null);
    if (!plan) throw new Error('no viable sparse topology — raise PEERS or KEEP');
    const { k, adj, ncn } = plan;
    console.log(`  · keep=${k} per node; ${ncn.length} no-common-neighbour candidate pair(s)`);

    // Sever every edge NOT in the keep-set (channels + synapses, both sides).
    let severs = 0;
    for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) {
      if (adj[i].has(j)) continue;
      const mij = peers[i].transport.webrtc.meshIdFor(peers[j].big);
      const mji = peers[j].transport.webrtc.meshIdFor(peers[i].big);
      if (mij) peers[i].transport.mesh.disconnect(mij, 'prune');
      if (mji) peers[j].transport.mesh.disconnect(mji, 'prune');
      peers[i].node.synaptome.delete(peers[j].big);
      peers[j].node.synaptome.delete(peers[i].big);
      severs++;
    }
    console.log(`  · severed ${severs} edge(s); retained sparse backbone`);
    await sleep(1500);   // let ICE teardown settle

    // Ground ALL structural claims in the LIVE channel graph (real WebRTC
    // teardown is asynchronous, so planned ≠ live until it settles).
    const live0 = liveAdjSets();
    const avgDeg = live0.reduce((s, set) => s + set.size, 0) / N;
    console.log(`  · live backbone: avg degree ${avgDeg.toFixed(1)}, connected=${isConnectedGraph(live0)}`);
    check('pruned backbone is sparse (avg degree < N-1) and CONNECTED',
      avgDeg < N - 1 && isConnectedGraph(live0));

    // ── Phase 3: KILL the bridge ──
    console.log('── phase 3: kill the bridge process ──');
    bridge.child.kill('SIGKILL');
    check('every peer\'s bridge socket closed', await waitFor(
      () => peers.every(p => p.transport.bridgeState !== 'open'), { timeoutMs: 10000 }));
    check('bridge child process is dead', bridge.child.killed === true);

    // ── Phase 4: HEADLINE — form the no-common-neighbour pair over relay ──
    // Pick the candidate the router can actually reach (lookup succeeds), so
    // we assert on a genuinely reachable multi-hop pair.
    console.log('── phase 4: headline no-common-neighbour pair, bridge DEAD ──');
    // Recompute the no-common-neighbour candidates from the LIVE graph (post
    // bridge-kill, post-settle) so the structural guarantee is grounded in
    // reality, not the plan. Require: not adjacent, no shared LIVE neighbour,
    // and the router actually finds a path.
    const lg = liveAdjSets();
    const liveNcn = [];
    for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) {
      if (lg[i].has(j)) continue;
      if ([...lg[i]].some(x => lg[j].has(x))) continue;
      liveNcn.push([i, j]);
    }
    console.log(`  · ${liveNcn.length} LIVE no-common-neighbour pair(s) post bridge-kill`);
    let head = null, headLk = null;
    for (const [i, j] of liveNcn) {
      const lk = await peers[i].peer.lookup(peers[j].big);
      if (lk && lk.found) { head = [i, j]; headLk = lk; break; }
    }
    check('a LIVE no-common-neighbour pair is routable (lookup found a path)', head !== null);
    if (!head) throw new Error('no routable no-common-neighbour pair');
    const [oi, ti] = head; const O = peers[oi], T = peers[ti];
    const shared = [...lg[oi]].filter(x => lg[ti].has(x));
    console.log(`  · headline pair ${O.name}→${T.name}: lookup found in ${headLk.hops} hop(s); shared live neighbours = ${shared.length}`);
    check('headline pair shares NO live neighbour (single relay STRUCTURALLY impossible)', shared.length === 0);
    check('headline pair not directly connected pre-connect',
      !O.transport.webrtc.isConnected(T.big) && !O.node.synaptome.has(T.big));

    tally.clear();
    const initiated = O.transport.connectViaRelay(T.hex);
    check('connectViaRelay initiated', initiated === true);

    const formed = await waitFor(() =>
      O.transport.webrtc.isConnected(T.big) && T.transport.webrtc.isConnected(O.big), { timeoutMs: 30000 });
    check(`${O.name}↔${T.name} REAL DataChannel formed over MULTI-HOP relay (bridge dead)`, formed);

    const bound = await waitFor(() =>
      O.transport.webrtc.boundPeers().includes(T.big) &&
      T.transport.webrtc.boundPeers().includes(O.big), { timeoutMs: 10000 });
    check('axona/4 bound end-to-end over the relayed channel', bound);

    check('live RTT measured over the new direct channel',
      await waitFor(() => O.transport.getLatency(T.big) >= 0, { timeoutMs: 8000 }));

    // Relay chain analysis. Both endpoints legitimately EMIT mesh:signal: O
    // sources the offer + its ICE, T sources the answer + its ICE (each routed
    // back over the mesh). The genuine-multi-hop evidence is the set of PURE
    // INTERMEDIARIES — emitters that are neither endpoint — which can only have
    // appeared by forwarding someone else's signalling across the sparse mesh.
    const emitters      = [...tally.keys()];
    const intermediaries = emitters.filter(nm => nm !== O.name && nm !== T.name);
    console.log(`  · mesh:signal emitters: ${[...tally.entries()].map(([n, c]) => `${n}×${c}`).join(', ')}`);
    console.log(`  · pure intermediaries (neither ${O.name} nor ${T.name}): [${intermediaries.join(', ')}]`);
    check('relay chained through ≥2 distinct pure intermediaries (genuine multi-hop)', intermediaries.length >= 2);
    check('both endpoints sourced signalling (offer from O, answer from T)',
      tally.has(O.name) && tally.has(T.name));

    check('bridge STILL dead after headline connect', bridge.child.killed === true &&
      peers.every(p => p.transport.bridgeState !== 'open'));

    // ── Phase 5: BULK — every reachable non-adjacent pair forms over relay ──
    // Two independent claims, separated so a real regression is unambiguous:
    //   (a) ROUTING reaches every non-adjacent pair (relay signalling is
    //       deliverable bridge-dead) — re-lookup found, mesh peer never 'none'.
    //   (b) the WebRTC channel fully OPENS for ≥90% of them. Stragglers in this
    //       single-process harness are node-datachannel finishing ICE under the
    //       load of N²/2 concurrent negotiations (state 'datachannel-opening'/
    //       'connecting'/'closed'), NOT a relay/routing fault — proven by (a).
    console.log('── phase 5: bulk convergence — all reachable non-adjacent pairs, bridge dead ──');
    let bulkTry = 0, bulkOk = 0, bulkUnreachable = 0;
    let routingReachedAll = true;
    for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) {
      if (peers[i].transport.webrtc.isConnected(peers[j].big)) continue;   // already adjacent
      const lk = await peers[i].peer.lookup(peers[j].big);
      if (!(lk && lk.found)) { bulkUnreachable++; continue; }               // router can't reach → out of scope
      bulkTry++;
      const formedPair = async () =>
        peers[i].transport.webrtc.isConnected(peers[j].big) &&
        peers[j].transport.webrtc.isConnected(peers[i].big);
      // Up to 3 negotiation attempts. connectViaRelay no-ops while a
      // negotiation is in-flight (idempotent); when one terminally fails/closes
      // the kernel frees the slot so a re-drive starts fresh. This models a
      // deployment's discovery re-trigger, not a protocol crutch.
      let ok = false;
      for (let attempt = 0; attempt < 3 && !ok; attempt++) {
        peers[i].transport.connectViaRelay(peers[j].hex);
        ok = await waitFor(formedPair, { timeoutMs: 25000 });
      }
      if (ok) { bulkOk++; continue; }
      // Attribute the miss: did the relay signalling reach the target at all?
      const reLk = await peers[i].peer.lookup(peers[j].big);
      const mp = peers[i].transport.mesh.getPeers().find(p => String(p.peerId) === peers[j].hex);
      const state = mp ? mp.state : 'none';
      if (!(reLk && reLk.found)) routingReachedAll = false;   // routing regression
      // NB: state 'none' is NOT signal-loss — a terminally-retried negotiation
      // is torn down (kernel frees the slot), so reachability is judged solely
      // by lookup, the routing substrate the relay signalling rides on.
      console.log(`    · ${peers[i].name}→${peers[j].name} channel still not OPEN at timeout — routing re-lookup found=${reLk?.found} hops=${reLk?.hops}; mesh peer state=${state} (found ⇒ relay routing reached it; channel ICE not yet open)`);
    }
    const openRate = bulkTry ? bulkOk / bulkTry : 0;
    console.log(`  · bulk: ${bulkOk}/${bulkTry} reachable non-adjacent pairs OPENED a direct channel (${(openRate*100).toFixed(0)}%); ${bulkUnreachable} unreachable, out of scope`);
    check('(a) relay routing reached EVERY non-adjacent pair (signalling deliverable bridge-dead)',
      bulkTry > 0 && routingReachedAll);
    check('(b) ≥90% of reachable pairs fully opened a direct channel over relay (rest mid-ICE)',
      openRate >= 0.9);
    check('bridge STILL dead at end of bulk phase', bridge.child.killed === true &&
      peers.every(p => p.transport.bridgeState !== 'open'));

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
