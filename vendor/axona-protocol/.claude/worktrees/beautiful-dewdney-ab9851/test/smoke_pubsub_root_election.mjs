// =====================================================================
// smoke_pubsub_root_election.mjs — roots stay canonical; overload recruits.
//
// Regression for the root-election fix (kernel v3.10.0). The intended axon-tree
// shape: a topic has a FIXED ~rootSetSize set of roots (the topicID-closest
// nodes); subscribers just subscribe; an overloaded root RECRUITS a close-by
// sub-axon that attaches UNDER it. Subscribers must NOT mint new roots.
//
// Before the fix three paths let non-closest nodes become roots:
//   1. subscribe-k receipt self-promoted with no proximity gate,
//   2. the routed terminal-subscribe minted a root with no gate,
//   3. recruited relays (adopt-subscribers) were created parentId:null and a
//      stray subscribe-k flipped them to isInRootSet — masquerading as roots.
// Result: the root set ballooned to dozens, load smeared, recruitment never
// fired. This test asserts the corrected behaviour.
//
//   node test/smoke_pubsub_root_election.mjs
// =====================================================================

import {
  AxonaPeer, AxonaDomain, NeuronNode, Synapse, SimNetwork, simTransport,
  createNodeIdentity, createAuthorIdentity, deriveTopicId, clz264,
} from '../src/index.js';
import { buildXorRoutingTable } from '../src/utils/geo.js';

const wait = (ms) => new Promise(r => setTimeout(r, ms));
let passed = 0, failed = 0;
const check = (l, c) => { if (c) { console.log(`  ✓ ${l}`); passed++; } else { console.log(`  ✗ ${l}`); failed++; } };

const N = 150, SUBS = 80, K = 20, R = 5;   // SUBS ≫ maxDirectSubs(20) ⇒ recruitment must fire
const LAT = 38, LNG = -77;
console.log(`\n── pub/sub root election — N=${N} SUBS=${SUBS} rootSetSize=${R} ──`);

const network = new SimNetwork();
const domain  = new AxonaDomain({ k: K });
const peers = [];
for (let i = 0; i < N; i++) {
  const identity = await createNodeIdentity({ lat: LAT, lng: LNG });
  const transport = simTransport({ network, identity, heartbeatMs: 0 });
  await transport.start(identity.id);
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: LAT, lng: LNG });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });
  await peer.start();
  peer._requireAxonaManager?.('root-election-smoke');
  peers.push({ peer, node, hex: identity.id, big: node.id, author: null });
}
const byBig = new Map(peers.map(p => [p.big, p]));

// Seed a navigable XOR mesh + open channels (same recipe as the harness).
const sorted = peers.map(p => p.node).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
for (const p of peers) {
  for (const cand of buildXorRoutingTable(p.node.id, sorted, K, Infinity)) {
    if (cand.id === p.node.id || p.node.synaptome.has(cand.id)) continue;
    const syn = new Synapse({ peerId: cand.id, latencyMs: 1, stratum: clz264(p.node.id ^ cand.id) });
    syn.weight = 0.5; syn.inertia = 0;
    p.node.synaptome.set(cand.id, syn);
  }
}
for (const p of peers) {
  for (const peerBig of p.node.synaptome.keys()) {
    const t = byBig.get(peerBig);
    if (t) { try { await p.peer._transport.openConnection(t.hex); } catch { /* */ } }
  }
}
await wait(150);

const topic = { region: 'useast', name: 'root-election' };
const topicBig = BigInt('0x' + await deriveTopicId(topic));

// Canonical truth: the rootSetSize closest peers to the topic.
const trueRoots = new Set((await peers[0].peer.findKClosest(topicBig, R)).map(b => b.toString(16)));

// Subscribe SUBS peers; drive refreshTick deterministically (no wall-clock timer).
const subs = peers.slice(0, SUBS);
const recv = new Map();
for (const s of subs) {
  recv.set(s.big, 0);
  await s.peer.sub(topic, () => recv.set(s.big, recv.get(s.big) + 1));
  const am = s.peer._axonaManager; if (am) am.refreshIntervalMs = 1500;
}
await wait(300);
const drive = async () => {
  const ticks = [];
  for (const p of peers) { const am = p.peer._axonaManager; if (am) { try { const r = am.refreshTick(); if (r?.catch) ticks.push(r.catch(() => {})); } catch { /* */ } } }
  await Promise.all(ticks); await wait(250);
};
for (let r = 0; r < 10; r++) await drive();

// Publish once, let anti-entropy carry it across the (converged) root set.
const publisher = peers[SUBS] || peers[0];
publisher.author = await createAuthorIdentity();
await publisher.peer.pub(topic, 'probe', { signWith: publisher.author });
for (let r = 0; r < 3; r++) await drive();

// ── classify every role-bearing node ──
let roots = 0, inTrue = 0, spuriousRootsWithChildren = 0, subAxons = 0, maxFan = 0;
for (const p of peers) {
  const role = p.peer._axonaManager?.axonRoles?.get(topicBig);
  if (!role) continue;
  if (role.parentId != null && !role.isRoot) subAxons++;
  if (role.isRoot || role.isInRootSet) {
    roots++;
    maxFan = Math.max(maxFan, role.children.size);
    if (trueRoots.has(p.big.toString(16))) inTrue++;
    else if (role.children.size > 0) spuriousRootsWithChildren++;
  }
}
const delivered = [...recv.values()].filter(v => v > 0).length;
console.log(`  roots=${roots} (in-true-closest=${inTrue}) spuriousRootsWithChildren=${spuriousRootsWithChildren} subAxons=${subAxons} maxFan=${maxFan} delivered=${delivered}/${SUBS}`);

// Roots stay canonical: the root set is the ~R closest, not a balloon of minted roots.
check('root set stays ~rootSetSize (≤ 2×R), not dozens', roots <= 2 * R);
check('every root is in the true K-closest set', inTrue === roots);
check('zero spurious roots holding children', spuriousRootsWithChildren === 0);
// Overload triggers recruitment (SUBS ≫ maxDirectSubs ⇒ sub-axons must exist).
check('recruitment fired — sub-axons exist under the roots', subAxons > 0);
check('per-axon fan-out respected the cap (≤ maxDirectSubs)', maxFan <= domain.MAX_DIRECT_SUBS || maxFan <= 20);
check('delivery ≥ 95% over the converged tree', delivered >= Math.ceil(SUBS * 0.95));

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
