// =====================================================================
// churn_refill.mjs — does a complete near-stratum make churn-repair cheap?
//
// Two claims under test:
//  (A) EFFICIENCY: when a node's nearest peer dies, is the GLOBAL next-nearest
//      live node already visible within the node's 2-hop neighborhood (its
//      neighbors + neighbors-of-neighbors)? If yes, refill is a local query,
//      not a global lookup. (Chord's "successor's successor" property.)
//  (B) DELIVERY: build a KNN mesh (near + long → ~100% baseline), churn out a
//      fraction of nodes, then measure pub/sub delivery on the kernel with NO
//      repair vs LOCAL 2-hop refill of the near-stratum.
//
// Run: node test/churn_refill.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(s){ s>>>=0; return () => { s = (s*1664525 + 1013904223)>>>0; return s/4294967296; }; }
const xcmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

const N      = Number(process.env.N      ?? 60);
const KNEAR  = Number(process.env.KNEAR  ?? 5);
const LONG   = Number(process.env.LONG   ?? 8);
const CHURN  = Number(process.env.CHURN  ?? 0.3);   // fraction of nodes that leave
const TOPICS = Number(process.env.TOPICS ?? 25);
const SUBS   = Number(process.env.SUBS   ?? 6);

// nearest KNEAR live nodes to `i` (by XOR), excluding a dead set and i itself.
function nearestLive(ids, i, k, dead) {
  return ids.map((id, j) => [j, ids[i] ^ id])
    .filter(([j]) => j !== i && !dead.has(j))
    .sort((a, b) => xcmp(a[1], b[1])).slice(0, k).map(([j]) => j);
}
function buildKNN(ids, rand, dead = new Set()) {
  const adj = new Map(ids.map((_, i) => [i, new Set()]));
  const link = (a, b) => { if (a !== b && !dead.has(a) && !dead.has(b)) { adj.get(a).add(b); adj.get(b).add(a); } };
  for (let i = 0; i < ids.length; i++) {
    if (dead.has(i)) continue;
    for (const j of nearestLive(ids, i, KNEAR, dead)) link(i, j);
    for (let k = 0; k < LONG; k++) { const j = Math.floor(rand() * ids.length); link(i, j); }
  }
  return adj;
}

// ── Experiment A: is the next-best near peer visible within 2 hops? ──────
function experimentA(ids, adj) {
  let oneHop = 0, twoHop = 0, miss = 0, poolSum = 0, n = 0;
  for (let i = 0; i < ids.length; i++) {
    const nbrs = [...adj.get(i)];
    if (!nbrs.length) continue;
    // the node's current nearest neighbor (what it would lose)
    const u = nbrs.slice().sort((a, b) => xcmp(ids[i] ^ ids[a], ids[i] ^ ids[b]))[0];
    // the TRUE next-nearest live node overall (excluding i and the dying u)
    const trueNext = nearestLive(ids, i, 1, new Set([u]))[0];
    if (trueNext == null) continue;
    n++;
    const oneHopSet = new Set(nbrs.filter(x => x !== u));
    const twoHopSet = new Set(oneHopSet);
    for (const nb of nbrs) if (nb !== u) for (const nn of adj.get(nb)) if (nn !== i && nn !== u) twoHopSet.add(nn);
    poolSum += twoHopSet.size;
    if (oneHopSet.has(trueNext)) oneHop++;
    else if (twoHopSet.has(trueNext)) twoHop++;
    else miss++;
  }
  return { n, oneHopPct: 100*oneHop/n, twoHopCumPct: 100*(oneHop+twoHop)/n, missPct: 100*miss/n, avgPool: poolSum/n };
}

// ── Experiment B: delivery on a given (alive) graph via the kernel ───────
class Fab {
  constructor(){ this.nodes=new Map(); this.adj=new Map(); this.queue=[]; this.clock=Date.now(); this.sent=0; }
  add(idBig){ const h=new Map(); const self=this; const me=idBig;
    const dht={ getSelfId:()=>me, neighbors:()=>[...(self.adj.get(me)||[])],
      onRoutedMessage:(t,fn)=>h.set(t,fn),
      routeMessage:(tg,t,p,m={})=>{ if(self.sent++>2_000_000) return; const d=self._term(me,tg); if(d===null) return;
        self.queue.push({dest:d,type:t,payload:p,meta:{targetId:tg,isTerminal:true,hopCount:1,fromId:m.fromId??idHex(me)}}); } };
    const am=new AxonaManager({dht,now:()=>self.clock,renewMs:60000,renewFastMs:5000,dropMs:180000,beaconFanout:0,beaconLayers:1});
    const rec={id:me,am,h,got:new Set()}; am.onPubsubDelivery((_t,_j,mid)=>rec.got.add(mid)); this.nodes.set(me,rec); return rec; }
  link(a,b){ (this.adj.get(a)??this.adj.set(a,new Set()).get(a)).add(b); (this.adj.get(b)??this.adj.set(b,new Set()).get(b)).add(a); }
  _term(s,t){ let c=s,g=0; while(g++<128){ let n=c,bd=c^t; for(const nb of(this.adj.get(c)||[])){ if(!this.nodes.has(nb))continue; const d=nb^t; if(d<bd){bd=d;n=nb;} } if(n===c)return c; c=n; } return c; }
  async settle(cap=2_000_000){ let i=0; while(this.queue.length){ if(++i>cap)return; const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n)continue; const fn=n.h.get(j.type); if(fn)await fn(j.payload,j.meta); } }
  async tickAll(){ for(const n of this.nodes.values()) await n.am.refreshTick(); await this.settle(); }
}
async function deliveryOnGraph(ids, adj, alive, author, salt) {
  let totFrac = 0;
  for (let t = 0; t < TOPICS; t++) {
    const fab = new Fab();
    const liveIdx = ids.map((_, i) => i).filter(i => alive.has(i));
    for (const i of liveIdx) fab.add(ids[i]);
    for (const i of liveIdx) for (const j of adj.get(i)) if (alive.has(j) && i < j) fab.link(ids[i], ids[j]);
    const desc = { region:'useast', owner:null, name:`churn-${salt}-${t}`, write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    const rand = lcg(7000 + t);
    const pool = liveIdx.slice().sort(() => rand() - 0.5);
    const subs = pool.slice(0, SUBS).map(i => fab.nodes.get(ids[i]));
    const pub = fab.nodes.get(ids[pool[SUBS % pool.length]]);
    for (const s of subs) s.am.pubsubSubscribe(topicId);
    await fab.settle();
    for (let r = 0; r < 3; r++) { fab.clock += 21000; await fab.tickAll(); }
    const e = await buildEnvelope({ topic: desc, message:{t}, seq:1, identity:author, ts:fab.clock });
    pub.am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    totFrac += subs.filter(s => s.got.has(e.msgId)).length / SUBS;
  }
  return 100 * totFrac / TOPICS;
}

// LOCAL 2-hop refill: each alive node below its near quota links to the nearest
// alive node found within its 2-hop neighborhood. Counts candidates examined.
function localRefill(ids, adj, alive) {
  let refilled = 0, candExamined = 0, refillMiss = 0;
  for (let i = 0; i < ids.length; i++) {
    if (!alive.has(i)) continue;
    const have = [...adj.get(i)].filter(x => alive.has(x));
    const nearHave = have.filter(x => nearestLive(ids, i, KNEAR, new Set()).includes(x)).length;
    let need = KNEAR - nearHave;
    while (need-- > 0) {
      const twoHop = new Set();
      for (const nb of adj.get(i)) if (alive.has(nb)) for (const nn of adj.get(nb)) if (nn !== i && alive.has(nn) && !adj.get(i).has(nn)) twoHop.add(nn);
      candExamined += twoHop.size;
      if (!twoHop.size) { refillMiss++; break; }
      const best = [...twoHop].sort((a, b) => xcmp(ids[i] ^ ids[a], ids[i] ^ ids[b]))[0];
      adj.get(i).add(best); adj.get(best).add(i); refilled++;
    }
  }
  return { refilled, avgCand: refilled ? candExamined/refilled : 0, refillMiss };
}

async function main() {
  console.log(`churn+refill — N=${N} near=${KNEAR} long=${LONG} churn=${CHURN} topics=${TOPICS} subs=${SUBS}`);
  const author = await createAuthorIdentity();
  const ids = [];
  for (let i = 0; i < N; i++) { const id = await createNodeIdentity({ lat:(i*13+7)%80-40, lng:(i*29+11)%300-150 }); ids.push(BigInt('0x'+id.id)); }
  const rand = lcg(12345);
  const adj = buildKNN(ids, rand);

  console.log('\n=== (A) is the next-best near peer visible within 2 hops? ===');
  const a = experimentA(ids, adj);
  console.log(`nodes=${a.n}  1-hop hit=${a.oneHopPct.toFixed(0)}%  ≤2-hop hit=${a.twoHopCumPct.toFixed(0)}%  miss=${a.missPct.toFixed(0)}%  avg 2-hop pool=${a.avgPool.toFixed(0)} nodes`);

  console.log('\n=== (B) delivery: pre-churn → churn(no repair) → churn(local refill) ===');
  const allAlive = new Set(ids.map((_, i) => i));
  const pre = await deliveryOnGraph(ids, adj, allAlive, author, 'pre');
  // churn: kill CHURN fraction
  const crand = lcg(999);
  const order = ids.map((_, i) => i).sort(() => crand() - 0.5);
  const dead = new Set(order.slice(0, Math.floor(N * CHURN)));
  const alive = new Set(order.slice(Math.floor(N * CHURN)));
  // post-churn adjacency WITHOUT repair (just drop dead nodes' edges)
  const adjNoRepair = new Map([...adj].map(([i, s]) => [i, new Set([...s].filter(x => alive.has(x)))]));
  const postNo = await deliveryOnGraph(ids, adjNoRepair, alive, author, 'postno');
  // post-churn WITH local refill (clone, then refill)
  const adjRefill = new Map([...adjNoRepair].map(([i, s]) => [i, new Set(s)]));
  const rr = localRefill(ids, adjRefill, alive);
  const postRf = await deliveryOnGraph(ids, adjRefill, alive, author, 'postrf');
  console.log(`pre-churn delivery:            ${pre.toFixed(1)}%`);
  console.log(`post-churn, NO repair:         ${postNo.toFixed(1)}%`);
  console.log(`post-churn, LOCAL 2-hop refill:${postRf.toFixed(1)}%   (refilled ${rr.refilled} links, avg ${rr.avgCand.toFixed(0)} candidates/refill, ${rr.refillMiss} not-found)`);
}
main().catch(e => { console.error('threw:', e?.stack || e); process.exit(1); });
