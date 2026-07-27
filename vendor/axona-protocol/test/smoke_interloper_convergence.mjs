// =====================================================================
// smoke_interloper_convergence.mjs — split-root convergence, both variants
// (tasks #353 warm-interloper + #352 cold watcher-first).
//
// Field capture (prod, 2026-07-18): a fresh joiner whose node id is XOR-closer
// to a warm topic than the standing root becomes root BY ROUTING the moment
// any topic message reaches it. Its publishes stamp locally; the standing
// root's seated subscribers see nothing. The incumbent's escape is
// _verifyRoots (periodic iterative lookup → demote → re-home), which #354
// restored on standalone peers.
//
// This smoke proves END-TO-END over a real mini-mesh (standalone AxonaPeers,
// default dht adapter — the exact code path that was dead pre-4.28.1):
//
//  A. WARM (#353): topic with a standing root, seated subscribers, and
//     history. A strictly-closer joiner appears (wired into the topic
//     neighborhood, as its own bootstrap would place it) and new messages
//     start stamping at it. Assert the system CONVERGES:
//       a1. the joiner ends up the (sole) root,
//       a2. the former root demotes,
//       a3. seated subscribers receive every post-interloper message
//           (live or via re-home replay),
//       a4. the new root's cache unions the FULL history (old + new),
//       a5. a late subscriber (since:'all') replays the full history.
//  B. COLD (#352): watcher subscribes FIRST on a fresh topic, publisher
//     appears later from elsewhere. Assert the watcher receives every
//     message and a late subscriber replays all of them.
//
// Timing: the incumbent's steady-state verify cadence is ROOT_VERIFY_MS
// (45 s). To keep the smoke fast we rewind the standing root's verify clock
// after the interloper mints (models "verify due now"); INTERLOPER_NATURAL=1
// skips the rewind and measures the true convergence latency instead.
//
// Run: node test/smoke_interloper_convergence.mjs
//      REPS=5 node test/smoke_interloper_convergence.mjs   (methodology gate)
// =====================================================================

import {
  AxonaPeer, AxonaDomain, NeuronNode, Synapse, SimNetwork, simTransport,
  createNodeIdentity, createAuthorIdentity, deriveTopicId, clz264,
} from '../src/index.js';
import { buildXorRoutingTable } from '../src/utils/geo.js';

const wait = (ms) => new Promise(r => setTimeout(r, ms));
const REPS = Math.max(1, parseInt(process.env.REPS || '1', 10));
const NATURAL = process.env.INTERLOPER_NATURAL === '1';
const N = 30, K = 8;
const LAT = 38, LNG = -77;                       // one S2 cell → region-lock consistent

let passed = 0, failed = 0;
const check = (l, c, extra = '') => {
  console.log(`  ${c ? '✓' : '✗'} ${l}${c ? '' : '  ' + extra}`);
  c ? passed++ : failed++;
};

async function makePeer(network, domain) {
  const identity = await createNodeIdentity({ lat: LAT, lng: LNG });
  const transport = simTransport({ network, identity, heartbeatMs: 0 });
  await transport.start(identity.id);
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: LAT, lng: LNG });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });
  await peer.start();
  peer._requireAxonaManager?.('interloper-smoke');
  return { peer, node, hex: identity.id, big: node.id };
}

function linkTables(peers, k) {
  const sorted = peers.map(p => p.node).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  for (const p of peers) {
    for (const cand of buildXorRoutingTable(p.node.id, sorted, k, Infinity)) {
      if (cand.id === p.node.id || p.node.synaptome.has(cand.id)) continue;
      const syn = new Synapse({ peerId: cand.id, latencyMs: 1, stratum: clz264(p.node.id ^ cand.id) });
      syn.weight = 0.5; syn.inertia = 0;
      p.node.synaptome.set(cand.id, syn);
    }
  }
}

async function openChannels(peers, byBig) {
  for (const p of peers) {
    for (const peerBig of p.node.synaptome.keys()) {
      const t = byBig.get(peerBig);
      if (t) { try { await p.peer._transport.openConnection(t.hex); } catch { /* */ } }
    }
  }
}

const drive = async (peers, rounds = 1) => {
  for (let r = 0; r < rounds; r++) {
    const ticks = [];
    for (const p of peers) {
      const am = p.peer._axonaManager;
      if (am) { try { const t = am.refreshTick(); if (t?.catch) ticks.push(t.catch(() => {})); } catch { /* */ } }
    }
    await Promise.all(ticks);
    await wait(250);
  }
};

const rootsOf = (peers, topicBig) =>
  peers.filter(p => p.peer._axonaManager?.axonRoles?.get(topicBig)?.isRoot);

async function scenarioWarm(rep) {
  console.log(`\n── A. warm interloper (#353) — rep ${rep + 1}/${REPS} ──`);
  const network = new SimNetwork();
  const domain = new AxonaDomain({ k: K });
  const peers = [];
  for (let i = 0; i < N; i++) peers.push(await makePeer(network, domain));
  const byBig = new Map(peers.map(p => [p.big, p]));
  linkTables(peers, K);
  await openChannels(peers, byBig);
  await wait(150);

  const topic = { region: 'useast', name: `interloper-${rep}-${Math.floor(Math.random() * 1e9)}` };
  const topicBig = BigInt('0x' + await deriveTopicId(topic));
  const author = await createAuthorIdentity();

  // Seated subscribers: the three peers FARTHEST from the topic (pure leaves).
  const byDist = peers.slice().sort((a, b) => ((a.big ^ topicBig) < (b.big ^ topicBig) ? -1 : 1));
  const subsPeers = byDist.slice(-3);
  const recv = new Map(subsPeers.map(p => [p.hex, new Set()]));
  for (const s of subsPeers) {
    await s.peer.sub(topic, (env) => recv.get(s.hex).add(env.message ?? env.msgId));
    const am = s.peer._axonaManager; if (am) am.refreshIntervalMs = 1500;
  }
  await drive(peers, 4);

  // Warm history from a mid-distance publisher.
  const publisher = byDist[Math.floor(N / 2)];
  for (const m of ['m1', 'm2', 'm3']) await publisher.peer.pub(topic, m, { signWith: author });
  await drive(peers, 4);

  const preOk = subsPeers.every(s => ['m1', 'm2', 'm3'].every(m => recv.get(s.hex).has(m)));
  check('warm baseline: 3/3 delivered to all seated subscribers', preOk,
    JSON.stringify(subsPeers.map(s => [...recv.get(s.hex)])));
  const standing = rootsOf(peers, topicBig);
  check('warm baseline: exactly one standing root', standing.length === 1, `roots=${standing.length}`);
  const r0 = standing[0] ?? byDist[0];

  // ── the interloper: strictly closer to the topic than anyone alive ──
  let interloper = null;
  for (let tries = 0; tries < 400 && !interloper; tries++) {
    const cand = await makePeer(network, domain);
    if ((cand.big ^ topicBig) < (byDist[0].big ^ topicBig)) interloper = cand;
    else { try { await cand.peer.stop?.(); } catch { /* */ } }
  }
  check('minted a strictly-closest joiner', !!interloper);
  if (!interloper) return { converged: false };

  // Wire it the way the PROD capture had it — asymmetrically:
  //   · gossip has spread the joiner into the TOPIC NEIGHBORHOOD's routing
  //     tables (hood → interloper one-way entries + channels), so iterative
  //     lookups and routed traffic find it and stamp there (the mint);
  //   · the joiner's OWN neighbor set holds only its few random bootstrap
  //     contacts — NOT the standing root — so its beacons/replicate push
  //     never reach the incumbent ("push-only union misses the standing
  //     root"). Channels are symmetric at the transport; TABLES are not.
  // The STANDING ROOT itself is excluded from the gossip wiring: prod gossip
  // fills the candidate pool, not the synaptome (B-3), so the incumbent has
  // no local channel-verified knowledge of the joiner — the ONLY way it can
  // discover it is the iterative verify lookup (#354's restored path).
  const hood = byDist.slice(0, K + 1).filter(p => p !== r0).slice(0, K);
  for (const nb of hood) {
    const back = new Synapse({ peerId: interloper.big, latencyMs: 1, stratum: clz264(nb.big ^ interloper.big) });
    back.weight = 0.5; back.inertia = 0;
    nb.node.synaptome.set(interloper.big, back);
    try { await nb.peer._transport.openConnection(interloper.hex); } catch { /* */ }
  }
  // Bootstrap contacts: 3 random FAR peers (model: bridge intro + first ICE
  // completions), reciprocal like any real channel.
  const contacts = byDist.slice(-Math.floor(N / 2)).sort(() => 0.5 - Math.random()).slice(0, 3);
  for (const nb of contacts) {
    const syn = new Synapse({ peerId: nb.big, latencyMs: 1, stratum: clz264(interloper.big ^ nb.big) });
    syn.weight = 0.5; syn.inertia = 0;
    interloper.node.synaptome.set(nb.big, syn);
    const back = new Synapse({ peerId: interloper.big, latencyMs: 1, stratum: clz264(nb.big ^ interloper.big) });
    back.weight = 0.5; back.inertia = 0;
    nb.node.synaptome.set(interloper.big, back);
    try { await interloper.peer._transport.openConnection(nb.hex); } catch { /* */ }
  }
  const all = peers.concat([interloper]);
  await wait(150);

  const tMint = Date.now();
  // The captured loss mode, verbatim: the joiner PUBLISHES — being topic-
  // closest, its posts stamp at its own local claim (the mint), and the
  // standing root's seated subscribers see nothing until the incumbent
  // discovers the closer claimant and the transition (demote → re-home →
  // lw-union) carries the stranded messages across.
  for (const m of ['m4', 'm5', 'm6']) await interloper.peer.pub(topic, m, { signWith: author });
  await drive(all, 2);
  const strandedRole = interloper.peer._axonaManager?.axonRoles?.get(topicBig);
  check('mint: joiner stamped its own publishes locally',
    !!strandedRole && (strandedRole.cache || []).length >= 3,
    `cache=${(strandedRole?.cache || []).length}`);

  if (!NATURAL) {
    // Fast-forward the incumbent's verify clock (due-now), keeping mechanism
    // identical; INTERLOPER_NATURAL=1 measures the real 45 s cadence instead.
    const role = r0.peer._axonaManager?.axonRoles?.get(topicBig);
    if (role) { role.lastVerify = 1; role.formedAt = 1; }
  }

  // Converge: allow up to 60 s natural / 20 s accelerated.
  const deadline = Date.now() + (NATURAL ? 60_000 : 20_000);
  let converged = false, tConverged = null;
  while (Date.now() < deadline && !converged) {
    await drive(all, 1);
    const late = subsPeers.every(s => ['m4', 'm5', 'm6'].every(m => recv.get(s.hex).has(m)));
    const iRole = interloper.peer._axonaManager?.axonRoles?.get(topicBig);
    const r0Role = r0.peer._axonaManager?.axonRoles?.get(topicBig);
    if (late && iRole?.isRoot && !(r0Role?.isRoot)) { converged = true; tConverged = Date.now(); }
  }

  const iRole = interloper.peer._axonaManager?.axonRoles?.get(topicBig);
  const r0Role = r0.peer._axonaManager?.axonRoles?.get(topicBig);
  check('a1. joiner holds the root claim', iRole?.isRoot === true);
  check('a2. former root demoted', !(r0Role?.isRoot), `r0 still root`);
  for (const s of subsPeers) {
    check(`a3. seated subscriber ${s.hex.slice(0, 8)} got m4..m6`,
      ['m4', 'm5', 'm6'].every(m => recv.get(s.hex).has(m)),
      `has ${[...recv.get(s.hex)].join(',')}`);
  }
  const cacheIds = new Set((iRole?.cache || []).map(c => { try { return JSON.parse(c.json).message ?? JSON.parse(c.json); } catch { return null; } }));
  const cacheHas = (m) => (iRole?.cache || []).some(c => (c.json || '').includes(m));
  check('a4. new root cache unions full history m1..m6',
    ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].every(cacheHas),
    `cache=${(iRole?.cache || []).length} ids=${[...cacheIds].join(',')}`);

  const late = byDist.slice(-4, -3)[0];
  const lateRecv = new Set();
  await late.peer.sub(topic, (env) => lateRecv.add(env.message ?? env.msgId), { since: 'all' });
  await drive(all, 4);
  check('a5. late subscriber replays 6/6',
    ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].every(m => lateRecv.has(m)),
    `got ${[...lateRecv].join(',')}`);

  if (converged) console.log(`  · converged ${(tConverged - tMint) / 1000}s after mint${NATURAL ? ' (natural cadence)' : ' (accelerated)'}`);
  for (const p of all) { try { await p.peer.stop?.(); } catch { /* */ } }
  return { converged };
}

async function scenarioCold(rep) {
  console.log(`\n── B. cold watcher-first (#352) — rep ${rep + 1}/${REPS} ──`);
  const network = new SimNetwork();
  const domain = new AxonaDomain({ k: K });
  const peers = [];
  for (let i = 0; i < N; i++) peers.push(await makePeer(network, domain));
  const byBig = new Map(peers.map(p => [p.big, p]));
  linkTables(peers, K);
  await openChannels(peers, byBig);
  await wait(150);

  const topic = { region: 'useast', name: `cold-${rep}-${Math.floor(Math.random() * 1e9)}` };
  const topicBig = BigInt('0x' + await deriveTopicId(topic));
  const author = await createAuthorIdentity();
  const byDist = peers.slice().sort((a, b) => ((a.big ^ topicBig) < (b.big ^ topicBig) ? -1 : 1));

  // Watcher FIRST (farthest peer), before any publish exists.
  const watcher = byDist[byDist.length - 1];
  const wRecv = new Set();
  await watcher.peer.sub(topic, (env) => wRecv.add(env.message ?? env.msgId));
  const wam = watcher.peer._axonaManager; if (wam) wam.refreshIntervalMs = 1500;
  await drive(peers, 4);

  // Publisher appears later, from a different corner.
  const publisher = byDist[Math.floor(N / 3)];
  for (const m of ['c1', 'c2', 'c3']) await publisher.peer.pub(topic, m, { signWith: author });

  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline && !['c1', 'c2', 'c3'].every(m => wRecv.has(m))) await drive(peers, 1);

  for (const m of ['c1', 'c2', 'c3']) {
    check(`b1. watcher received ${m}`, wRecv.has(m), `has ${[...wRecv].join(',')}`);
  }
  const roots = rootsOf(peers, topicBig);
  check('b2. exactly one root after convergence', roots.length === 1, `roots=${roots.length}`);

  const late = byDist[byDist.length - 2];
  const lateRecv = new Set();
  await late.peer.sub(topic, (env) => lateRecv.add(env.message ?? env.msgId), { since: 'all' });
  await drive(peers, 4);
  check('b3. late subscriber replays 3/3', ['c1', 'c2', 'c3'].every(m => lateRecv.has(m)),
    `got ${[...lateRecv].join(',')}`);

  for (const p of peers) { try { await p.peer.stop?.(); } catch { /* */ } }
}

// C. EPHEMERAL interloper (#353's captured shape, strictest): the joiner
// publishes and is otherwise INERT — it never runs a refreshTick (an
// ephemeral script node dies before its first beacon/sweep), and the
// incumbent's verify clock is NOT rewound. The only mechanism that can heal
// the seated subscribers fast is the EAGER stamp-time replicate reaching the
// standing root — which requires the cohort to come from the ITERATIVE view,
// not the joiner's thin local table. Then the joiner DIES; history must
// survive at the incumbent.
async function scenarioEphemeral(rep) {
  console.log(`\n── C. ephemeral interloper, eager-push heal (#353) — rep ${rep + 1}/${REPS} ──`);
  const network = new SimNetwork();
  const domain = new AxonaDomain({ k: K });
  const peers = [];
  for (let i = 0; i < N; i++) peers.push(await makePeer(network, domain));
  const byBig = new Map(peers.map(p => [p.big, p]));
  linkTables(peers, K);
  await openChannels(peers, byBig);
  await wait(150);

  const topic = { region: 'useast', name: `ephemeral-${rep}-${Math.floor(Math.random() * 1e9)}` };
  const topicBig = BigInt('0x' + await deriveTopicId(topic));
  const author = await createAuthorIdentity();
  const byDist = peers.slice().sort((a, b) => ((a.big ^ topicBig) < (b.big ^ topicBig) ? -1 : 1));
  const subsPeers = byDist.slice(-3);
  const recv = new Map(subsPeers.map(p => [p.hex, new Set()]));
  for (const s of subsPeers) {
    await s.peer.sub(topic, (env) => recv.get(s.hex).add(env.message ?? env.msgId));
    const am = s.peer._axonaManager; if (am) am.refreshIntervalMs = 1500;
  }
  await drive(peers, 4);
  const publisher = byDist[Math.floor(N / 2)];
  for (const m of ['m1', 'm2', 'm3']) await publisher.peer.pub(topic, m, { signWith: author });
  await drive(peers, 4);
  const r0 = rootsOf(peers, topicBig)[0] ?? byDist[0];

  let interloper = null;
  for (let tries = 0; tries < 400 && !interloper; tries++) {
    const cand = await makePeer(network, domain);
    if ((cand.big ^ topicBig) < (byDist[0].big ^ topicBig)) interloper = cand;
    else { try { await cand.peer.stop?.(); } catch { /* */ } }
  }
  check('minted a strictly-closest joiner', !!interloper);
  if (!interloper) return;
  const hood = byDist.slice(0, K + 1).filter(p => p !== r0).slice(0, K);
  for (const nb of hood) {
    const back = new Synapse({ peerId: interloper.big, latencyMs: 1, stratum: clz264(nb.big ^ interloper.big) });
    back.weight = 0.5; back.inertia = 0;
    nb.node.synaptome.set(interloper.big, back);
    try { await nb.peer._transport.openConnection(interloper.hex); } catch { /* */ }
  }
  const contacts = byDist.slice(-Math.floor(N / 2)).sort(() => 0.5 - Math.random()).slice(0, 3);
  for (const nb of contacts) {
    const syn = new Synapse({ peerId: nb.big, latencyMs: 1, stratum: clz264(interloper.big ^ nb.big) });
    syn.weight = 0.5; syn.inertia = 0;
    interloper.node.synaptome.set(nb.big, syn);
    const back = new Synapse({ peerId: interloper.big, latencyMs: 1, stratum: clz264(nb.big ^ interloper.big) });
    back.weight = 0.5; back.inertia = 0;
    nb.node.synaptome.set(interloper.big, back);
    try { await interloper.peer._transport.openConnection(nb.hex); } catch { /* */ }
  }
  await wait(150);

  // The joiner publishes — and that's ALL it ever does. No ticks (drive()
  // below excludes it), no verify rewind at r0. The 8 s window is far below
  // ROOT_VERIFY_MS: only the eager iterative-union push can make this pass.
  for (const m of ['m4', 'm5', 'm6']) await interloper.peer.pub(topic, m, { signWith: author });
  const deadline = Date.now() + 8_000;
  const healed = () => subsPeers.every(s => ['m4', 'm5', 'm6'].every(m => recv.get(s.hex).has(m)));
  while (Date.now() < deadline && !healed()) await drive(peers, 1);
  for (const s of subsPeers) {
    check(`c1. seated subscriber ${s.hex.slice(0, 8)} healed within 8s (eager push)`,
      ['m4', 'm5', 'm6'].every(m => recv.get(s.hex).has(m)),
      `has ${[...recv.get(s.hex)].join(',')}`);
  }

  // Ephemeral death: the joiner disappears. Full history must survive.
  try { await interloper.peer.stop?.(); } catch { /* */ }
  await drive(peers, 2);
  const late = byDist.slice(-4, -3)[0];
  const lateRecv = new Set();
  await late.peer.sub(topic, (env) => lateRecv.add(env.message ?? env.msgId), { since: 'all' });
  await drive(peers, 4);
  check('c2. after joiner death a late subscriber replays 6/6',
    ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].every(m => lateRecv.has(m)),
    `got ${[...lateRecv].join(',')}`);
  for (const p of peers) { try { await p.peer.stop?.(); } catch { /* */ } }
}

for (let rep = 0; rep < REPS; rep++) {
  await scenarioWarm(rep);
  await scenarioCold(rep);
  await scenarioEphemeral(rep);
}
console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'}  (${passed} passed, ${failed} failed)`);
process.exit(failed === 0 ? 0 : 1);
