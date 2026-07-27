// =====================================================================
// beacon_radius_sweep.mjs — find the beacon fan-out radius where the system
// is robust without flooding the full mesh.
//
// Model: a random bounded-degree GAPPY mesh (each node ~DEGREE random neighbors).
// routeMessage greedy-walks each node's neighbor set and stops at a local XOR
// minimum — exactly DHT greedy routing on a sparse graph, where publisher and
// subscribers strand at DIFFERENT near-miss nodes and form competing roots.
// The root beacon is the cure: each (possibly-spurious) root announces itself
// to its F XOR-closest neighbors, recursive L layers; when those announcements
// reach a competing root, the farther one demotes and the islands collapse onto
// the globally-closest root → delivery converges.
//
// We sweep beaconFanout (F) at fixed layers, plus a layers sweep at fixed F,
// over the SAME topology + trials, and report mean delivery% AND mean beacon
// packets/topic (the cost) so the knee is visible.
//
// Run: node test/beacon_radius_sweep.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(s){ s>>>=0; return () => { s = (s*1664525 + 1013904223)>>>0; return s/4294967296; }; }

const N        = Number(process.env.N        ?? 60);   // nodes
const DEGREE   = Number(process.env.DEGREE   ?? 5);    // ~neighbors per node (sparse → strands)
const TOPICS   = Number(process.env.TOPICS   ?? 30);   // trials per config
const SUBS     = Number(process.env.SUBS     ?? 6);    // subscribers per topic
const TICKS    = Number(process.env.TICKS    ?? 4);    // beacon-propagation rounds before publish

// A gappy mesh whose routeMessage greedy-walks a fixed random adjacency, and
// whose AxonaManagers are built with a given beacon (fanout, layers). Counts
// ROOTBEACON packets so we can price the reach.
class Sweep {
  constructor(fanout, layers) {
    this.nodes = new Map(); this.adj = new Map(); this.queue = [];
    this.clock = Date.now(); this.fanout = fanout; this.layers = layers; this.beaconPkts = 0;
    // Per-trial packet budget = the real DHT's MAX_HOPS exhaustion, modeled in
    // aggregate. A via-routed correction toward a beacon-named root that greedy
    // routing can't actually reach would re-forward forever in an idealized
    // fabric; on a real mesh those packets die after MAX_HOPS. Once the budget is
    // spent we drop further sends (and flag a "storm") — which is itself the
    // flooding signal we're hunting.
    this.sent = 0; this.BUDGET = 150000; this.stormed = false;
  }
  addNode(idBig) {
    const handlers = new Map(); const self = this; const me = idBig;
    const dht = {
      getSelfId: () => me,
      neighbors: () => [...(self.adj.get(me) || [])],
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload, meta = {}) => {
        if (self.sent++ > self.BUDGET) { self.stormed = true; return; }   // MAX_HOPS-exhaustion analogue
        if (type === 'pubsub:rootbeacon') self.beaconPkts++;
        const dest = self._greedyTerminus(me, target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: meta.fromId ?? idHex(me) } });
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000,
                                  beaconFanout: self.fanout, beaconLayers: self.layers });
    const rec = { id: me, am, handlers, alive: true, got: new Set() };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.add(msgId));
    this.nodes.set(me, rec); return rec;
  }
  link(a, b) {
    (this.adj.get(a) ?? this.adj.set(a, new Set()).get(a)).add(b);
    (this.adj.get(b) ?? this.adj.set(b, new Set()).get(b)).add(a);
  }
  _greedyTerminus(start, target) {
    let cur = start, guard = 0;
    while (guard++ < 128) {
      let next = cur, bestD = cur ^ target;
      for (const nb of (this.adj.get(cur) || [])) {
        const n = this.nodes.get(nb); if (!n || !n.alive) continue;
        const d = nb ^ target; if (d < bestD) { bestD = d; next = nb; }
      }
      if (next === cur) return cur;
      cur = next;
    }
    return cur;
  }
  async settle(cap = 4000000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle cap');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest); if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type); if (h) await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

// Build ONE fixed pool of identities + ONE fixed random graph, reused across all
// configs so the only variable is beacon reach.
async function buildWorld() {
  const ids = [];
  for (let i = 0; i < N; i++) {
    const id = await createNodeIdentity({ lat: (i*13+7)%80 - 40, lng: (i*29+11)%300 - 150 });
    ids.push(BigInt('0x' + id.id));
  }
  const rand = lcg(12345);
  const TOPO = process.env.TOPO || 'random';   // 'random' | 'knn'
  const LONG = Number(process.env.LONG ?? 2);  // long-range random links per node (knn mode)
  const edges = [];
  if (TOPO === 'knn') {
    // Each node links to its KNEAR XOR-nearest peers (dense near-stratum → last-mile
    // cliques) PLUS LONG random long-range links (so far targets are reachable).
    const KNEAR = DEGREE - LONG;
    for (let i = 0; i < N; i++) {
      const near = ids.map((id, j) => [j, ids[i] ^ id]).filter(([j]) => j !== i)
        .sort((a, b) => (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0)).slice(0, KNEAR);
      for (const [j] of near) edges.push([i, j]);
      for (let k = 0; k < LONG; k++) { const j = Math.floor(rand() * N); if (j !== i) edges.push([i, j]); }
    }
  } else {
    // random: a connecting ring + random extra edges to ~DEGREE (no near-structure).
    for (let i = 0; i < N; i++) edges.push([i, (i + 1) % N]);
    for (let i = 0; i < N; i++) {
      for (let k = 0; k < DEGREE - 2; k++) { const j = Math.floor(rand() * N); if (j !== i) edges.push([i, j]); }
    }
  }
  return { ids, edges };
}

async function runConfig(world, fanout, layers, author) {
  const { ids, edges } = world;
  let totFrac = 0, totPkts = 0, converged = 0, storms = 0;
  for (let t = 0; t < TOPICS; t++) {
    const fab = new Sweep(fanout, layers);
    for (const id of ids) fab.addNode(id);
    for (const [a, b] of edges) fab.link(ids[a], ids[b]);

    const desc = { region: 'useast', owner: null, name: `sweep-${fanout}-${layers}-${t}`, write: 'open' };
    const topicId = await deriveTopicIdBig(desc);
    const rand = lcg(1000 + t);
    // pick SUBS distinct subscribers + 1 publisher
    const order = ids.map((_, i) => i).sort(() => rand() - 0.5);
    const subIdx = order.slice(0, SUBS);
    const pubIdx = order[SUBS % N];
    const subs = subIdx.map(i => fab.nodes.get(ids[i]));
    for (const s of subs) s.am.pubsubSubscribe(topicId);
    await fab.settle();
    // beacon-propagation rounds (roots announce; competing roots discover + collapse)
    for (let r = 0; r < TICKS; r++) { fab.clock += 21000; await fab.tickAll(); }
    // publish
    const e = await buildEnvelope({ topic: desc, message: { t }, seq: 1, identity: author, ts: fab.clock });
    fab.nodes.get(ids[pubIdx]).am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle();
    // one more renewal in case a late beacon correction needs a re-fan
    fab.clock += 6000; await fab.tickAll();

    const got = subs.filter(s => s.got.has(e.msgId)).length;
    const frac = got / SUBS;
    totFrac += frac; totPkts += fab.beaconPkts;
    if (frac === 1) converged++;
    if (fab.stormed) storms++;
  }
  return { fanout, layers, meanDeliv: 100 * totFrac / TOPICS, fullPct: 100 * converged / TOPICS,
           pktsPerTopic: totPkts / TOPICS, stormPct: 100 * storms / TOPICS };
}

async function main() {
  console.log(`Beacon radius sweep — N=${N} degree~${DEGREE} topics=${TOPICS} subs=${SUBS} ticks=${TICKS}`);
  const author = await createAuthorIdentity();
  const world = await buildWorld();

  console.log('\n=== fan-out sweep (layers=2) ===');
  console.log('fanout  meanDeliv%  full100%  beaconPkts/topic  storm%');
  for (const f of [0, 2, 3, 4, 6, 8, 12]) {
    const r = await runConfig(world, f, 2, author);
    console.log(`${String(f).padStart(5)}   ${r.meanDeliv.toFixed(1).padStart(8)}   ${r.fullPct.toFixed(0).padStart(6)}    ${r.pktsPerTopic.toFixed(1).padStart(8)}   ${r.stormPct.toFixed(0).padStart(4)}`);
  }

  console.log('\n=== layers sweep (fanout=4) ===');
  console.log('layers  meanDeliv%  full100%  beaconPkts/topic  storm%');
  for (const L of [1, 2, 3]) {
    const r = await runConfig(world, 4, L, author);
    console.log(`${String(L).padStart(5)}   ${r.meanDeliv.toFixed(1).padStart(8)}   ${r.fullPct.toFixed(0).padStart(6)}    ${r.pktsPerTopic.toFixed(1).padStart(8)}   ${r.stormPct.toFixed(0).padStart(4)}`);
  }
}
main().catch(e => { console.error('sweep threw:', e?.stack || e); process.exit(1); });
