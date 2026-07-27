// repro_lossy_restart.mjs — reproduce the WebRTC "strand without retry" residual
// in-process by injecting MESSAGE LOSS into the pub/sub fabric. A stable root R
// holds [A,B]; N late subscribers join since:'all' under a drop rate p; we then
// advance MANY renewal ticks. If the system is eventually-consistent, every late
// subscriber recovers [A,B] given enough retries. A subscriber that NEVER
// recovers despite many ticks = a strand-without-retry (the residual).
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(seed){ let s = seed >>> 0; return () => { s = (s*1664525 + 1013904223) >>> 0; return s/4294967296; }; }

class LossyFabric {
  constructor({ drop = 0, seed = 1 }) {
    this.nodes = new Map(); this.queue = []; this.clock = Date.now();
    this.drop = drop; this.rand = lcg(seed); this.sent = 0; this.dropped = 0;
  }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      // single-hop to the global-closest-alive, but DROP with prob `drop`.
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.sent++;
        if (self.rand() < self.drop) { self.dropped++; return; }   // packet lost
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      // the iterative lookup is robust (multi-round) — model it as reliable so we
      // isolate DATA-plane loss (SUB/DELIVER/PUB), not lookup loss.
      findKClosest: async (target, k = 3) =>
        [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
          .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: new Set() };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.add(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig){ const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target){ let best=null,bd=null; for (const [id,n] of this.nodes){ if(!n.alive) continue; const d=id^target; if(bd===null||d<bd){bd=d;best=id;} } return best; }
  async settle(cap = 500000){ let i=0; while(this.queue.length){ if(++i>cap) throw new Error('settle cap'); const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n||!n.alive) continue; const h=n.handlers.get(j.type); if(!h) continue; await h(j.payload, j.meta);} }
  async tickAll(){ for(const n of this.nodes.values()) if(n.alive) await n.am.refreshTick(); await this.settle(); }
}

async function run({ drop, seed, M = 2, lateSubs = 4, ticks = 30 }) {
  const fab = new LossyFabric({ drop: 0, seed });   // setup/publish LOSS-FREE
  const author = await createAuthorIdentity();
  // 8 stable nodes; the global-closest is the root and will hold the cache.
  const nodes = [];
  for (let i = 0; i < 8; i++) { const id = await createNodeIdentity({ lat:(i*11)%80-40, lng:(i*17)%300-150 }); nodes.push(fab.addNode(BigInt('0x'+id.id))); }
  const desc = { region:'useast', owner:null, name:`lossy-${seed}`, write:'open' };
  const topicId = await deriveTopicIdBig(desc);
  // one early subscriber + the publisher seed the root with [A,B]
  const early = nodes[0]; early.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6000; await fab.tickAll();
  const ids = [];
  for (let k=0;k<M;k++){ const e = await buildEnvelope({ topic: desc, message:{k}, seq:k+1, identity:author, ts: fab.clock }); ids.push(e.msgId); nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle(); }
  const root0 = fab.nodes.get(fab._closestAlive(topicId));
  const rootCache = (root0.am.axonRoles.get(topicId)?.cache || []).length;
  fab.drop = drop;   // ── inject loss ONLY on the late-subscriber replay path ──
  const late = [];
  for (let i=0;i<lateSubs;i++){ const id = await createNodeIdentity({ lat:1+i, lng:2+i }); const r = fab.addNode(BigInt('0x'+id.id)); r.am._lastSeenTsByTopic.set(topicId, 0); r.am.pubsubSubscribe(topicId); late.push(r); }
  // advance MANY renewal ticks — eventual consistency should heal all under retry
  for (let t=0;t<ticks;t++){ fab.clock += 5000; await fab.tickAll(); }
  const recovered = late.filter(r => ids.every(id => r.got.has(id))).length;
  return { recovered, lateSubs, rootCache, M };
}

const drops = [0, 0.05, 0.1, 0.2, 0.3];
console.log('Lossy late-subscriber REPLAY only (publish loss-free; M=2, 4 late subs, 30 ticks)\n');
let badRoot = 0;
for (const drop of drops) {
  let totRec = 0, totSub = 0;
  for (let seed = 1; seed <= 8; seed++) { const r = await run({ drop, seed }); totRec += r.recovered; totSub += r.lateSubs; if (r.rootCache !== r.M) badRoot++; }
  const pct = (100*totRec/totSub).toFixed(0);
  console.log(`  drop=${(drop*100).toFixed(0).padStart(2)}%  →  ${totRec}/${totSub} late subs recovered full history (${pct}%)`);
}
if (badRoot) console.log(`\n  ⚠ ${badRoot} runs had an incomplete root cache (publish-side, not replay) — should be 0 now`);
