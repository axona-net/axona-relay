// =====================================================================
// churn_sustained.mjs — sustained multi-round churn: does the near-stratum
// erode WITHOUT active maintenance, and does cheap local refill HOLD it?
//
// This is the parameter-locking regression for Synaptome-Maintenance-v0.1.
// Each round: churn out a fraction of live nodes, admit the same number of
// fresh nodes (which build full near+long links at join), then either do
// NOTHING (no-repair) or run LOCAL 2-hop refill on every survivor. We track,
// per round, for each policy:
//   · near-quota occupancy — of each node's K_NEAR globally-nearest live peers,
//     what fraction is it actually connected to? (the structural health metric)
//   · delivery%             — kernel pub/sub over the current graph
//   · refill cost           — candidates examined + 2-hop-hit rate (no global lookup)
//
// Expectation (locks the design): no-repair occupancy + delivery DRIFT DOWN over
// rounds; refill HOLDS both near 100% at a cost of a handful of local candidates.
//
// Run: node test/churn_sustained.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(s){ s>>>=0; return () => { s = (s*1664525 + 1013904223)>>>0; return s/4294967296; }; }
const xcmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

const N0     = Number(process.env.N0     ?? 70);    // steady-state live size
const ROUNDS = Number(process.env.ROUNDS ?? 6);
const CHURN  = Number(process.env.CHURN  ?? 0.2);   // fraction replaced each round
const KNEAR  = Number(process.env.KNEAR  ?? 5);
const LONG   = Number(process.env.LONG   ?? 6);
const TOPICS = Number(process.env.TOPICS ?? 20);
const SUBS   = Number(process.env.SUBS   ?? 6);

// nearest k live indices to node i by XOR (excludes i + dead)
function nearestLive(ids, i, k, alive) {
  const out = [];
  for (let j = 0; j < ids.length; j++) if (j !== i && alive.has(j)) out.push([j, ids[i] ^ ids[j]]);
  out.sort((a, b) => xcmp(a[1], b[1]));
  return out.slice(0, k).map(([j]) => j);
}
function linkFull(ids, adj, alive, i, rand) {            // fresh node i builds near + long links
  for (const j of nearestLive(ids, i, KNEAR, alive)) { adj.get(i).add(j); adj.get(j).add(i); }
  const liveArr = [...alive];
  for (let k = 0; k < LONG; k++) { const j = liveArr[Math.floor(rand() * liveArr.length)]; if (j !== i) { adj.get(i).add(j); adj.get(j).add(i); } }
}

// near-quota occupancy: avg over live nodes of (connected ∩ K_NEAR-nearest)/K_NEAR
function nearOccupancy(ids, adj, alive) {
  let sum = 0, n = 0;
  for (const i of alive) {
    const want = nearestLive(ids, i, KNEAR, alive);
    if (!want.length) continue;
    const have = want.filter(j => adj.get(i).has(j)).length;
    sum += have / want.length; n++;
  }
  return 100 * sum / n;
}

// LOCAL refill of BOTH invariants on every live node: (1) near quota (K_NEAR
// XOR-nearest) and (2) long-range coverage (≥ LONG_TARGET links that are NOT in
// the near set — the "fingers"). Both sourced 2-hop-first. Returns cost stats.
const LONG_TARGET = Number(process.env.LONG_TARGET ?? 6);
function refillAll(ids, adj, alive) {
  let candExamined = 0, refills = 0, twoHopHit = 0, fellBack = 0;
  for (const i of alive) {
    const twoHop = () => { const s = new Set(); for (const nb of adj.get(i)) if (alive.has(nb)) for (const nn of adj.get(nb)) if (nn !== i && alive.has(nn) && !adj.get(i).has(nn)) s.add(nn); return s; };
    // (1) near quota
    const want = nearestLive(ids, i, KNEAR, alive);
    for (const tgt of want) {
      if (adj.get(i).has(tgt)) continue;
      const th = twoHop(); candExamined += th.size; refills++;
      if (th.has(tgt)) twoHopHit++; else fellBack++;     // either way we link (fallback = findKClosest)
      adj.get(i).add(tgt); adj.get(tgt).add(i);
    }
    // (2) long-range coverage: count links NOT among the near set; top up to LONG_TARGET
    const nearSet = new Set(nearestLive(ids, i, KNEAR, alive));
    let longHave = [...adj.get(i)].filter(j => alive.has(j) && !nearSet.has(j)).length;
    while (longHave < LONG_TARGET) {
      const th = [...twoHop()].filter(j => !nearSet.has(j));   // prefer distant 2-hop peers as fingers
      if (!th.length) break;
      // pick a spread-out finger: the farthest-from-self 2-hop candidate (adds reach)
      const pick = th.sort((a, b) => xcmp(ids[i] ^ ids[b], ids[i] ^ ids[a]))[0];
      candExamined += th.length; refills++; twoHopHit++;
      adj.get(i).add(pick); adj.get(pick).add(i); longHave++;
    }
  }
  return { avgCand: refills ? candExamined / refills : 0, twoHopPct: refills ? 100*twoHopHit/refills : 100, fellBack };
}

// ── kernel pub/sub delivery over the current live graph ──────────────────
class Fab {
  constructor(){ this.nodes=new Map(); this.adj=new Map(); this.queue=[]; this.clock=Date.now(); this.sent=0; }
  add(idBig){ const h=new Map(); const self=this; const me=idBig;
    const dht={ getSelfId:()=>me, neighbors:()=>[...(self.adj.get(me)||[])], onRoutedMessage:(t,fn)=>h.set(t,fn),
      routeMessage:(tg,t,p,m={})=>{ if(self.sent++>3_000_000) return; const d=self._term(me,tg); if(d===null)return;
        self.queue.push({dest:d,type:t,payload:p,meta:{targetId:tg,isTerminal:true,hopCount:1,fromId:m.fromId??idHex(me)}}); } };
    const am=new AxonaManager({dht,now:()=>self.clock,renewMs:60000,renewFastMs:5000,dropMs:180000,beaconFanout:0,beaconLayers:1});
    const rec={id:me,am,h,got:new Set()}; am.onPubsubDelivery((_t,_j,mid)=>rec.got.add(mid)); this.nodes.set(me,rec); return rec; }
  link(a,b){ (this.adj.get(a)??this.adj.set(a,new Set()).get(a)).add(b); (this.adj.get(b)??this.adj.set(b,new Set()).get(b)).add(a); }
  _term(s,t){ let c=s,g=0; while(g++<128){ let n=c,bd=c^t; for(const nb of(this.adj.get(c)||[])){ if(!this.nodes.has(nb))continue; const d=nb^t; if(d<bd){bd=d;n=nb;} } if(n===c)return c; c=n; } return c; }
  async settle(cap=3_000_000){ let i=0; while(this.queue.length){ if(++i>cap)return; const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n)continue; const fn=n.h.get(j.type); if(fn)await fn(j.payload,j.meta); } }
  async tickAll(){ for(const n of this.nodes.values()) await n.am.refreshTick(); await this.settle(); }
}
async function delivery(ids, adj, alive, author, salt) {
  let tot = 0;
  for (let t = 0; t < TOPICS; t++) {
    const fab = new Fab();
    const live = [...alive];
    for (const i of live) fab.add(ids[i]);
    for (const i of live) for (const j of adj.get(i)) if (alive.has(j) && i < j) fab.link(ids[i], ids[j]);
    const desc = { region:'useast', owner:null, name:`sust-${salt}-${t}`, write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    const rand = lcg(5000 + t);
    const pool = live.slice().sort(() => rand() - 0.5);
    const subs = pool.slice(0, SUBS).map(i => fab.nodes.get(ids[i]));
    const pub = fab.nodes.get(ids[pool[SUBS % pool.length]]);
    for (const s of subs) s.am.pubsubSubscribe(topicId);
    await fab.settle();
    for (let r = 0; r < 3; r++) { fab.clock += 21000; await fab.tickAll(); }
    const e = await buildEnvelope({ topic: desc, message:{t}, seq:1, identity:author, ts:fab.clock });
    pub.am.pubsubPublish(topicId, JSON.stringify(e));
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    tot += subs.filter(s => s.got.has(e.msgId)).length / SUBS;
  }
  return 100 * tot / TOPICS;
}

// one churn schedule, replayed identically for both policies
function makeSchedule(total, rand) {
  const sched = [];
  for (let r = 0; r < ROUNDS; r++) {
    const nKill = Math.floor(N0 * CHURN);
    sched.push(nKill);
  }
  return sched;
}

async function runPolicy(ids, repair, author, schedule) {
  const adj = new Map(ids.map((_, i) => [i, new Set()]));
  const alive = new Set();
  const rand = lcg(4242);
  let nextFresh = 0;
  // seed N0 live nodes with full near+long structure
  for (let i = 0; i < N0; i++) alive.add(i); nextFresh = N0;
  for (const i of alive) { /* build over the seeded set */ }
  for (let i = 0; i < N0; i++) linkFull(ids, adj, alive, i, rand);

  const rows = [];
  rows.push({ round: 0, occ: nearOccupancy(ids, adj, alive), deliv: await delivery(ids, adj, alive, author, `${repair?'rf':'no'}0`), cost: null });

  const crand = lcg(909);
  for (let r = 0; r < schedule.length; r++) {
    const nKill = schedule[r];
    // churn out nKill random live nodes
    const liveArr = [...alive].sort(() => crand() - 0.5);
    for (let k = 0; k < nKill; k++) { const dead = liveArr[k]; alive.delete(dead); for (const nb of adj.get(dead)) adj.get(nb).delete(dead); adj.get(dead).clear(); }
    // admit nKill fresh nodes with full structure
    for (let k = 0; k < nKill; k++) { const f = nextFresh++; if (f >= ids.length) break; alive.add(f); linkFull(ids, adj, alive, f, rand); }
    // policy
    let cost = null;
    if (repair) cost = refillAll(ids, adj, alive);
    rows.push({ round: r + 1, occ: nearOccupancy(ids, adj, alive), deliv: await delivery(ids, adj, alive, author, `${repair?'rf':'no'}${r+1}`), cost });
  }
  return rows;
}

async function main() {
  console.log(`sustained churn — N0=${N0} rounds=${ROUNDS} churn/round=${CHURN} near=${KNEAR} long=${LONG} topics=${TOPICS}`);
  const author = await createAuthorIdentity();
  const total = N0 + ROUNDS * Math.floor(N0 * CHURN) + 5;
  const ids = [];
  for (let i = 0; i < total; i++) { const id = await createNodeIdentity({ lat:(i*13+7)%80-40, lng:(i*29+11)%300-150 }); ids.push(BigInt('0x'+id.id)); }
  const schedule = makeSchedule(total, lcg(1));

  const no = await runPolicy(ids, false, author, schedule);
  const rf = await runPolicy(ids, true,  author, schedule);

  console.log('\nround |  NO-REPAIR (occ% / deliv%)  |  REFILL (occ% / deliv%)  | refill cost');
  for (let i = 0; i < no.length; i++) {
    const a = no[i], b = rf[i];
    const cost = b.cost ? `${b.cost.avgCand.toFixed(0)} cand, ${b.cost.twoHopPct.toFixed(0)}% 2-hop, ${b.cost.fellBack} fallback` : '—';
    console.log(`  ${String(i).padStart(2)}  |     ${a.occ.toFixed(0).padStart(3)}  /  ${a.deliv.toFixed(0).padStart(3)}          |   ${b.occ.toFixed(0).padStart(3)}  /  ${b.deliv.toFixed(0).padStart(3)}        | ${cost}`);
  }
}
main().catch(e => { console.error('threw:', e?.stack || e); process.exit(1); });
