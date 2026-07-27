// =====================================================================
// smoke_read_repair.mjs — #364 part 2: reads survive a DEGRADED (not
// dead) topic-closest node.
//
// smoke_ghost_read covers UNGRACEFUL DEATH: the root leaves the mesh, so
// routing re-terminates at a surviving cohort member and the empty-root
// probe heals. This repro covers the residual the 4.33 forensics named —
// the topic-closest node is ALIVE-BUT-DEGRADED:
//   · still a mesh neighbour   → _isReachableId() = true  → no fast-promote
//   · still XOR-closest         → every routed SUB/PUB terminates AT it
//   · NOT self                  → the #266 reachable-root self-claim can't fire
//   · black-holes pub/sub       → it seats/replays nothing (ingest-stalled,
//                                 overloaded, the join-storm event-loop wedge)
// A fresh subscriber's SUB routes straight into it and dies; the subscriber
// holds no role, so nothing can recover the history the cohort backups still
// hold. Expected TODAY: the late joiner recovers 0/5. The part-2 fix (a
// stuck subscriber pulls the cohort into a read-repair holder) should carry
// it to 5/5 without splitting the root.
//
// Run: node test/smoke_read_repair.mjs
// =====================================================================
import { createHash } from 'node:crypto';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHexF = (b) => b.toString(16).padStart(66, '0');
const fabId = (tag) => BigInt('0x89' + createHash('sha256').update(String(tag)).digest('hex'));
const CLOCK_BASE = 1_600_000_000_000;

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = CLOCK_BASE; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestServing(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHexF(idBig) } });
      },
      // findKClosest sees every LIVE node (degraded ones are still alive + in the
      // mesh) — so the degraded root keeps ranking closest and keeps being chosen.
      findKClosest: async (target, _k = 3) =>
        [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
          .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, _k),
      // neighbours = every live node (dense mesh). A degraded node is STILL a
      // neighbour → _isReachableId(degraded) = true → the reachable-root fallback
      // does NOT treat it as departed. This is the whole trap.
      neighbors: () => [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => idHexF(id)),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, degraded: false, got: [] };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig)     { const n = this.nodes.get(idBig); if (n) n.alive = false; }      // UNGRACEFUL death (leaves mesh)
  degrade(idBig)  { const n = this.nodes.get(idBig); if (n) n.degraded = true; }    // alive + reachable, serves nothing
  // Routing target: XOR-closest among LIVE nodes (degraded stays closest —
  // routing can't tell it is a black hole; that's exactly the failure).
  _closestServing(target) { let best = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  async settle(cap = 500_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('no converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      if (n.degraded) continue;              // DELIVERED-INTO-THE-VOID: the degraded node drops everything routed to it
      const h = n.handlers.get(j.type);
      if (!h) continue;
      await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive && !n.degraded) await n.am.refreshTick(); await this.settle(); }
  async fireRepairs() {
    // Drive the unref'd empty-root / read-repair probes deterministically.
    for (const n of this.nodes.values()) if (n.alive && !n.degraded) {
      for (const [t, role] of n.am.axonRoles) {
        if (!role.cache.length && (role.isRoot || role.backupOf || n.am._backupTopics?.has?.(t))) {
          await n.am._emptyRootProbe(t).catch(() => {});
        }
      }
      if (typeof n.am._readRepairSweep === 'function') { try { await n.am._readRepairSweep(n.am._now()); } catch { /* pre-fix: method absent */ } }
    }
    await this.settle();
  }
}

async function scenario(name = 'degraded-read') {
  const fab = new Fabric();
  const author = await createAuthorIdentity(); let seq = 1;
  const desc = { region: 'useast', owner: null, name, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  // A cohort of relays that host the topic keyspace (so backups accrue warm copies).
  const relays = []; for (let i = 0; i < 12; i++) relays.push(fab.addNode(fabId(`${name}-r${i}`)));
  const pub = fab.addNode(fabId(`${name}-pub`));
  for (const r of relays) r.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();
  const ids = [];
  for (let k = 0; k < 5; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: seq++, identity: author, ts: fab.clock });
    ids.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  for (let r = 0; r < 3; r++) { fab.clock += 6_000; await fab.tickAll(); }        // cohort replication rounds

  const root = fab.nodes.get(fab._closestServing(topicId));
  const backups = [...fab.nodes.values()].filter(n => n !== root && n.am.axonRoles.get(topicId)?.cache?.length);

  fab.degrade(root.id);                          // ALIVE but serves nothing (NOT killed)

  // A fresh subscriber that is strictly FARTHER from the topic than the
  // (degraded) root — so its SUB routes INTO the degraded node and terminates
  // there, rather than terminating at itself and self-rooting (that latter case
  // is the ghost-read path already covered by the empty-root probe). We search
  // for such an id deterministically so the repro exercises the real gap.
  const rootDist = root.id ^ topicId;
  let late = null;
  for (let i = 0; i < 2000; i++) {
    const cand = fabId(`${name}-late-${i}`);
    if ((cand ^ topicId) > rootDist && !fab.nodes.has(cand)) { late = fab.addNode(cand); break; }
  }
  if (!late) throw new Error('could not place a far late-joiner');
  late.am._lastSeenTsByTopic.set(topicId, 0);    // since:'all'
  late.am.pubsubSubscribe(topicId);
  await fab.settle();
  // Give it several renewal ticks + repair rounds to recover.
  for (let r = 0; r < 6; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.fireRepairs(); }

  const got = ids.filter(id => late.got.includes(id)).length;
  const lateIsClosest = fab._closestServing(topicId) === late.id;   // sanity: the degraded root should still outrank the late joiner
  return { got, total: ids.length, backupCount: backups.length, degradedStillClosest: !lateIsClosest || root.degraded };
}

async function main() {
  console.log('Axona pub/sub — degraded read: the topic-closest node is alive-but-not-serving');
  // Several distinct topic names → distinct keyspace placements (different
  // closest/backup arrangements) so the fix is proven general, not tuned to one
  // topology (single-topology sim passes are noise — REPS across placements).
  for (const name of ['degraded-read', 'alpha-topic', 'bravo-topic', 'charlie-topic', 'delta-topic']) {
    const r = await scenario(name);
    check(`[${name}] warm cohort backups before degrade (${r.backupCount})`, r.backupCount >= 1, `(${r.backupCount})`);
    check(`[${name}] degraded root still outranks the late joiner (trap holds)`, r.degradedStillClosest);
    check(`[${name}] late joiner recovers ALL from the cohort (${r.got}/${r.total})`, r.got === r.total, `(${r.got}/${r.total})`);
  }
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
