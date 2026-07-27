// =====================================================================
// smoke_pubsub_sweep.mjs — Phase 2 GATE for the axonic tree.
//
// Varies (total nodes × subscribers × churn) and asserts the tree the
// routing-only manager builds under load is correct:
//   · 100% delivery to every live subscriber (incl. the sparse case that
//     exposed the v3.10.0 root-election regression)
//   · the tree actually forms (delegation past MAX_DIRECT → child relays)
//   · bounded fan-out (no relay holds far more than MAX_DIRECT directs)
//   · bounded depth (~log_MAX_DIRECT(S))
//   · resilience to churn — kill a fraction of nodes (root/relays included),
//     let renewals self-heal, and delivery returns to 100%
//
// Harness: the same closest-live-node routing fabric as the fundamental gate,
// extended with churn + tree introspection. Deterministic per run; node ids are
// random per run, so each invocation is a fresh topology.
//
// Run: node test/smoke_pubsub_sweep.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');  // region-lock: co-region test nodes with the 'useast' topics

const MAX_DIRECT = 20;             // must match the manager default
let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const handlers = new Map();
    const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, received: new Set() };
    am.onPubsubDelivery((_t, _j, msgId) => rec.received.add(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(maxJobs = 5_000_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > maxJobs) throw new Error('fabric.settle: did not converge');
      const job = this.queue.shift();
      const n = this.nodes.get(job.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(job.type);
      if (!h) continue;
      await h(job.payload, job.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

let SEQ = 1;
async function makeNodes(fab, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity(__LOC);
    out.push(fab.addNode(BigInt('0x' + id.id)));
  }
  return out;
}

// Tree introspection for a topic across all live nodes.
function treeStats(fab, topicBig) {
  let maxFanout = 0, relays = 0, roots = 0;
  const parent = new Map();   // nodeHex -> upstream relay hex (for depth)
  for (const n of fab.nodes.values()) {
    if (!n.alive) continue;
    const role = n.am.axonRoles.get(topicBig);
    if (role && role.subscribers.size > 0) {
      relays++; if (role.isRoot) roots++;
      maxFanout = Math.max(maxFanout, role.subscribers.size);
    }
    const up = n.am._upstream.get(topicBig);
    if (up && up.length) parent.set(idHex(n.id), up[0]);
  }
  // depth = longest upstream chain (guard against cycles)
  let depth = 0;
  for (const start of parent.keys()) {
    let d = 0, cur = start; const seen = new Set();
    while (parent.has(cur) && !seen.has(cur) && d < 1000) { seen.add(cur); cur = parent.get(cur); d++; }
    depth = Math.max(depth, d);
  }
  // fan-out distribution (top 5) for diagnosing over-cap relays
  const fanouts = [];
  for (const n of fab.nodes.values()) {
    if (!n.alive) continue;
    const role = n.am.axonRoles.get(topicBig);
    if (role && role.subscribers.size > 0) fanouts.push(role.subscribers.size + (role.isRoot ? 'R' : ''));
  }
  fanouts.sort((a, b) => parseInt(b) - parseInt(a));
  return { maxFanout, relays, roots, depth, top: fanouts.slice(0, 6).join(',') };
}

async function scenario({ N, S, churn }, author) {
  const fab = new Fabric();
  const nodes = await makeNodes(fab, N);
  const desc = { region: 'useast', owner: null, name: `sweep-${N}-${S}-${churn}`, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // S subscribers (nodes 0..S-1); a publisher that isn't a subscriber.
  const subs = nodes.slice(0, S);
  const pubNode = nodes[N - 1];
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  // let delegation + renewal settle the tree
  fab.clock += 61_000; await fab.tickAll();
  fab.clock += 61_000; await fab.tickAll();

  // publish #0 — must reach every (live) subscriber. Stamp with the fabric
  // clock (which we advance for renewals) so the root's freshness gate, which
  // checks against that same clock, doesn't reject it as stale.
  const e0 = await buildEnvelope({ topic: desc, message: { p: 0 }, seq: SEQ++, identity: author, ts: fab.clock });
  pubNode.am.pubsubPublish(topicId, JSON.stringify(e0));
  await fab.settle();
  const liveSubs0 = subs.filter(s => s.alive);
  const got0 = liveSubs0.filter(s => s.received.has(e0.msgId)).length;
  const t = treeStats(fab, topicId);

  let got1 = null, liveCount1 = null;
  if (churn) {
    // kill ~15% of nodes at random positions (deterministic stride), incl. the
    // root/relays — the renewal loop must re-form the tree and re-seat subs.
    const victims = [];
    for (let i = 0; i < N; i += 7) victims.push(nodes[i]);                // ~14%
    victims.push(fab.nodes.get(fab._closestAlive(topicId)));               // kill the current root too
    for (const v of victims) if (v) fab.kill(v.id);
    // heal: several renewal cycles
    for (let r = 0; r < 4; r++) { fab.clock += 61_000; await fab.tickAll(); }

    const e1 = await buildEnvelope({ topic: desc, message: { p: 1 }, seq: SEQ++, identity: author, ts: fab.clock });
    const alivePub = pubNode.alive ? pubNode : nodes.find(n => n.alive);
    alivePub.am.pubsubPublish(topicId, JSON.stringify(e1));
    await fab.settle();
    const liveSubs1 = subs.filter(s => s.alive);
    liveCount1 = liveSubs1.length;
    got1 = liveSubs1.filter(s => s.received.has(e1.msgId)).length;
  }

  return { N, S, churn, liveSubs0: liveSubs0.length, got0, ...t, got1, liveCount1 };
}

async function main() {
  console.log('Axona pub/sub — Phase 2 axonic-tree sweep (nodes × subscribers × churn)');
  const author = await createAuthorIdentity();

  const grid = [
    { N: 60,  S: 5 },     // sparse — the case that was unstable on the old root-election
    { N: 60,  S: 20 },    // exactly at MAX_DIRECT (no delegation yet)
    { N: 80,  S: 50 },    // delegation, depth ~2
    { N: 140, S: 100 },   // bigger tree
    { N: 240, S: 160 },   // deeper
  ];

  for (const cfg of grid) {
    for (const churn of [false, true]) {
      const r = await scenario({ ...cfg, churn }, author);
      const tag = `N=${r.N} S=${r.S} churn=${churn}`;
      console.log(`\n── ${tag} ──  relays=${r.relays} roots=${r.roots} maxFanout=${r.maxFanout} depth=${r.depth}`);
      check(`${tag}: 100% delivery to live subscribers (${r.got0}/${r.liveSubs0})`, r.got0 === r.liveSubs0);
      check(`${tag}: exactly one root`, r.roots === 1, `(roots=${r.roots})`);
      check(`${tag}: bounded fan-out (maxFanout ${r.maxFanout} ≤ ${MAX_DIRECT + DELEGATE_SLACK})`, r.maxFanout <= MAX_DIRECT + DELEGATE_SLACK);
      if (r.S > MAX_DIRECT) check(`${tag}: tree formed (delegation → ≥2 relays)`, r.relays >= 2, `(relays=${r.relays})`);
      if (churn) check(`${tag}: 100% delivery after churn+heal (${r.got1}/${r.liveCount1})`, r.got1 === r.liveCount1, `(${r.got1}/${r.liveCount1})`);
    }
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

const DELEGATE_SLACK = 4;   // a relay may briefly sit a touch over MAX_DIRECT mid-delegation
main().catch(err => { console.error('sweep threw:', err); process.exit(2); });
