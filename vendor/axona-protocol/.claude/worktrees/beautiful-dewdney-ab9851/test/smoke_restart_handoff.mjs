// =====================================================================
// smoke_restart_handoff.mjs — deterministic RESTART / graceful-handoff repro.
//
// Reproduces the soak's "restart" scenario entirely in-process on the sim
// Fabric, with NO bridge, NO WebRTC and NO wall-clock timing — so the outcome
// is a pure function of topology and kernel logic, immune to machine load.
// (The black-box A/B on this mac was load-bound: local 18-relay fleet pins
// loadPerCore ~1.8 even with testnet quiet, so the 96%-vs-79% full-timeline
// gap could not be cleanly attributed to the kernel.)
//
// Scenario per trial (mirrors soak scenarioRestart):
//   1. 12 co-region subscribers + 1 publisher; publish PRE messages.
//   2. GRACEFUL churn: topic root hands off + departs; publisher departs;
//      4 subscribers gracefully leave. Renewal windows elapse (dead nodes
//      age out of neighbour tables past dropMs).
//   3. 4 fresh newcomers subscribe; publisher REJOINS as a new node id and
//      publishes POST messages.
//   4. Measure (a) POST delivery to every live subscriber, and
//      (b) full-timeline recovery for a fresh since:'all' late joiner
//      (PRE + POST) — the metric that separated 4.29.0 from 4.30.0.
//
// Runs UNLESS overridden, at PRODUCTION-DEFAULT rootReplicas (2), so the
// Phase 8 PUB_DURABLE / cohort-replication + heir-reresolution path is under
// test — NOT rootReplicas:0 (that isolates handoff-only and hides replication).
//
// Node ids are fabricated deterministically (region byte 0x89 = useast,
// followed by sha256(counter)) so a given TRIALS/base produces byte-identical
// topologies across kernel versions → a clean PAIRED differential when the
// same file is run against 4.29.0 src and 4.30.0 src.
//
// Run:  node test/smoke_restart_handoff.mjs           # report rates
//       TRIALS=60 node test/smoke_restart_handoff.mjs
//       MIN_TIMELINE=0.9 node test/smoke_restart_handoff.mjs   # gate (exit 1 if below)
// =====================================================================
import { createHash } from 'node:crypto';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

const TRIALS       = Number(process.env.TRIALS || 40);
const SUBS         = 12;
const PRE          = 6;
const POST         = 6;
const DROP         = 4;
const FRESH        = 4;
const CLOCK_BASE   = 1_600_000_000_000;   // fixed → bit-reproducible, no Date.now()
const MIN_TIMELINE = process.env.MIN_TIMELINE ? Number(process.env.MIN_TIMELINE) : null;
const MIN_POST     = process.env.MIN_POST ? Number(process.env.MIN_POST) : null;

const idHex = (big) => big.toString(16).padStart(66, '0');
// Fabricated co-region node id: region byte 0x89 (useast) ++ 256-bit sha256(tag).
const fabId = (tag) => BigInt('0x89' + createHash('sha256').update(String(tag)).digest('hex'));

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = CLOCK_BASE; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, _k = 3) => {
        return [...self.nodes.entries()].filter(([, n]) => n.alive)
          .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
          .slice(0, _k);
      },
    };
    // Production default rootReplicas (2) — the Phase 8 cohort path IS under test here.
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(cap = 500_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle: did not converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type);
      if (!h) continue;
      await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}

async function trial(t, author, SEQ) {
  const fab = new Fabric();
  const desc = { region: 'useast', owner: null, name: `restart-${t}`, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // 12 subscribers + 1 publisher, all co-region, deterministic ids.
  const subs = [];
  for (let i = 0; i < SUBS; i++) subs.push(fab.addNode(fabId(`t${t}-sub${i}`)));
  const pub = fab.addNode(fabId(`t${t}-pub`));
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  fab.clock += 6_000; await fab.tickAll();

  // PRE publishes.
  const pre = [];
  for (let k = 0; k < PRE; k++) {
    const e = await buildEnvelope({ topic: desc, message: { phase: 'pre', k }, seq: SEQ.n++, identity: author, ts: fab.clock });
    pre.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  fab.clock += 6_000; await fab.tickAll(); await fab.settle();

  // ── GRACEFUL churn: root hands off + departs, publisher departs, 4 subs leave ──
  const root = fab.nodes.get(fab._closestAlive(topicId));
  await root.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(root.id);
  fab.kill(pub.id);   // publisher "goes down" for the restart
  // Drop 4 subscribers that are NOT the root, gracefully.
  const droppable = subs.filter(s => s.id !== root.id).slice(0, DROP);
  for (const s of droppable) { await s.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(s.id); }

  // Renewal windows — live nodes renew each tick; dead nodes age past dropMs (180s).
  for (let r = 0; r < 4; r++) { fab.clock += 60_000; await fab.tickAll(); }

  // 4 fresh newcomers join and subscribe.
  const fresh = [];
  for (let i = 0; i < FRESH; i++) { const n = fab.addNode(fabId(`t${t}-fresh${i}`)); n.am.pubsubSubscribe(topicId); fresh.push(n); }
  await fab.settle(); fab.clock += 6_000; await fab.tickAll(); await fab.settle();

  // Publisher REJOINS as a new node id and publishes POST.
  const pub2 = fab.addNode(fabId(`t${t}-pub2`));
  const post = [];
  for (let k = 0; k < POST; k++) {
    const e = await buildEnvelope({ topic: desc, message: { phase: 'post', k }, seq: SEQ.n++, identity: author, ts: fab.clock });
    post.push(e.msgId); pub2.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  // POST_TICKS: renewal rounds granted before measuring live POST delivery
  // (1 = strictest immediacy; more rounds separate "lost" from "converging").
  const postTicks = Math.max(1, Number(process.env.POST_TICKS || 1));
  for (let r = 0; r < postTicks; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.settle(); }

  // (a) live subscribers (survivors + newcomers) receive every POST.
  const survivors = subs.filter(s => s.alive);
  const live = [...survivors, ...fresh];
  let postHits = 0;
  for (const s of live) postHits += post.filter(id => s.got.includes(id)).length;
  const postFrac = postHits / (live.length * POST);

  // (b) full-timeline recovery: fresh since:'all' late joiner recovers PRE + POST.
  const late = fab.addNode(fabId(`t${t}-late`));
  late.am._lastSeenTsByTopic.set(topicId, 0);
  late.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll(); await fab.settle();
  const all = [...pre, ...post];
  const tlHits = all.filter(id => late.got.includes(id)).length;
  const tlFrac = tlHits / all.length;

  return { postFrac, tlFrac, postOk: postFrac === 1, tlOk: tlFrac === 1 };
}

async function main() {
  const kv = process.env.KERNEL_LABEL || 'current-src';
  console.log(`Axona pub/sub — deterministic RESTART handoff repro  [${kv}]  rootReplicas=2  trials=${TRIALS}`);
  const author = await createAuthorIdentity();
  const SEQ = { n: 1 };
  const rows = [];
  for (let t = 0; t < TRIALS; t++) rows.push(await trial(t, author, SEQ));

  const mean = (f) => rows.reduce((a, r) => a + f(r), 0) / rows.length;
  const rate = (f) => rows.filter(f).length / rows.length;
  const postFracMean = mean(r => r.postFrac);
  const tlFracMean   = mean(r => r.tlFrac);
  const postOkRate   = rate(r => r.postOk);
  const tlOkRate     = rate(r => r.tlOk);

  const pct = (x) => (100 * x).toFixed(1) + '%';
  console.log(`\n  POST delivery to live subs   mean=${pct(postFracMean)}   all-delivered trials=${pct(postOkRate)}`);
  console.log(`  Full-timeline (since:'all')  mean=${pct(tlFracMean)}   full-recovery trials=${pct(tlOkRate)}`);

  let fail = false;
  if (MIN_POST != null)     { const ok = postOkRate >= MIN_POST;   console.log(`  gate POST all-delivered >= ${pct(MIN_POST)}: ${ok ? 'PASS' : 'FAIL'}`);   fail ||= !ok; }
  if (MIN_TIMELINE != null) { const ok = tlOkRate  >= MIN_TIMELINE; console.log(`  gate full-timeline      >= ${pct(MIN_TIMELINE)}: ${ok ? 'PASS' : 'FAIL'}`); fail ||= !ok; }
  process.exit(fail ? 1 : 0);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
