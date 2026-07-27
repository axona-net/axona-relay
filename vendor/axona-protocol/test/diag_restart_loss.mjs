// diag_restart_loss.mjs — localize WHERE the ~10% of restart/handoff pubs die.
//
// Same golden-path scenario as smoke_restart_handoff.mjs (production rootReplicas),
// but instrumented: for every PRE message we record whether it is durably stored
// (a) in the root's cache and (b) anywhere in the network, BEFORE any churn; then
// again in the heir + network AFTER the graceful root handoff; then whether a
// fresh since:'all' late joiner recovers it. Each LOST message is bucketed:
//
//   PUBLISH_GAP   — never landed anywhere in the network even before churn
//                   (the publish/replication path lost it at ingest time)
//   REPLICATION_GAP— was on the root but NOT on any cohort replica before churn
//                   (single copy; churn then takes the only holder)
//   HANDOFF_GAP   — was on ≥1 surviving holder before churn but vanished from the
//                   network after churn (handoff/ingest dropped it, no re-source)
//   RECOVER_GAP   — still present in the network after churn but the late joiner's
//                   since:'all' replay never delivered it (read/replay path)
//
// Run: node test/diag_restart_loss.mjs   (TRIALS=40 default)
import { createHash } from 'node:crypto';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

const TRIALS = Number(process.env.TRIALS || 40);
const SUBS = 12, PRE = 8, DROP = 4, FRESH = 4;
const CLOCK_BASE = 1_600_000_000_000;
const idHex = (b) => b.toString(16).padStart(66, '0');
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
      findKClosest: async (target, _k = 3) =>
        [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
          .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, _k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) { let best = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  async settle(cap = 500_000) { let i = 0; while (this.queue.length) { if (++i > cap) throw new Error('no converge'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (!h) continue; await h(j.payload, j.meta); } }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const holds = (rec, t, id) => !!rec.am.axonRoles.get(t)?.cacheIds?.has(id);
const holdersOf = (fab, t, id) => [...fab.nodes.values()].filter(n => n.alive && holds(n, t, id));

async function trial(tn, author, SEQ) {
  const fab = new Fabric();
  const desc = { region: 'useast', owner: null, name: `diag-${tn}`, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const subs = []; for (let i = 0; i < SUBS; i++) subs.push(fab.addNode(fabId(`t${tn}-s${i}`)));
  const pub = fab.addNode(fabId(`t${tn}-pub`));
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();

  const pre = [];
  for (let k = 0; k < PRE; k++) { const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ.n++, identity: author, ts: fab.clock }); pre.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle(); }
  // steady state: let replication sweeps run (several ticks past ROOT_REPLICATE_FULL_MS delta triggers)
  for (let r = 0; r < 3; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.settle(); }

  // ── snapshot BEFORE churn ──
  const root0 = fab.nodes.get(fab._closestAlive(topicId));
  const before = new Map();  // msgId -> {inRoot, holders}
  for (const id of pre) { const hs = holdersOf(fab, topicId, id); before.set(id, { inRoot: holds(root0, topicId, id), holders: hs.length, nonRootHolders: hs.filter(n => n !== root0).length }); }

  // ── graceful golden-path churn: root hands off + departs; publisher + 4 subs leave ──
  await root0.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(root0.id);
  fab.kill(pub.id);
  for (const s of subs.filter(s => s.id !== root0.id).slice(0, DROP)) { await s.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(s.id); }
  for (let r = 0; r < 4; r++) { fab.clock += 60_000; await fab.tickAll(); }
  const fresh = []; for (let i = 0; i < FRESH; i++) { const n = fab.addNode(fabId(`t${tn}-f${i}`)); n.am.pubsubSubscribe(topicId); fresh.push(n); }
  await fab.settle(); fab.clock += 6_000; await fab.tickAll(); await fab.settle();

  // ── snapshot AFTER churn ──
  const heir = fab.nodes.get(fab._closestAlive(topicId));
  const after = new Map();
  for (const id of pre) after.set(id, { inHeir: holds(heir, topicId, id), holders: holdersOf(fab, topicId, id).length });

  // ── late joiner since:'all' ──
  const late = fab.addNode(fabId(`t${tn}-late`));
  late.am._lastSeenTsByTopic.set(topicId, 0); late.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll(); await fab.settle();

  // ── classify each pre ──
  const buckets = { OK: 0, PUBLISH_GAP: 0, REPLICATION_GAP: 0, HANDOFF_GAP: 0, RECOVER_GAP: 0 };
  for (const id of pre) {
    if (late.got.includes(id)) { buckets.OK++; continue; }
    const b = before.get(id), a = after.get(id);
    if (b.holders === 0) buckets.PUBLISH_GAP++;               // never durably stored anywhere
    else if (a.holders === 0) {                               // was stored, gone after churn
      if (b.nonRootHolders === 0) buckets.REPLICATION_GAP++;  // only the (departed) root had it
      else buckets.HANDOFF_GAP++;                             // a survivor had it, yet it vanished
    } else buckets.RECOVER_GAP++;                             // still in network, replay missed it
  }
  return buckets;
}

async function main() {
  console.log(`diag restart-loss  rootReplicas=2  trials=${TRIALS}  PRE=${PRE}/trial`);
  const author = await createAuthorIdentity(); const SEQ = { n: 1 };
  const tot = { OK: 0, PUBLISH_GAP: 0, REPLICATION_GAP: 0, HANDOFF_GAP: 0, RECOVER_GAP: 0 };
  for (let t = 0; t < TRIALS; t++) { const b = await trial(t, author, SEQ); for (const k in tot) tot[k] += b[k]; }
  const N = TRIALS * PRE;
  const pc = (x) => (100 * x / N).toFixed(1).padStart(5) + '%';
  console.log(`\n  total PRE messages: ${N}`);
  console.log(`  OK (recovered)      ${pc(tot.OK)}   ${tot.OK}`);
  console.log(`  LOST total          ${pc(N - tot.OK)}   ${N - tot.OK}`);
  console.log(`   ├ PUBLISH_GAP      ${pc(tot.PUBLISH_GAP)}   ${tot.PUBLISH_GAP}   (never stored anywhere pre-churn)`);
  console.log(`   ├ REPLICATION_GAP  ${pc(tot.REPLICATION_GAP)}   ${tot.REPLICATION_GAP}   (only the departed root held it)`);
  console.log(`   ├ HANDOFF_GAP      ${pc(tot.HANDOFF_GAP)}   ${tot.HANDOFF_GAP}   (a survivor had it, vanished post-churn)`);
  console.log(`   └ RECOVER_GAP      ${pc(tot.RECOVER_GAP)}   ${tot.RECOVER_GAP}   (still in net, replay missed it)`);
}
main().catch(e => { console.error(e?.stack || e); process.exit(2); });
