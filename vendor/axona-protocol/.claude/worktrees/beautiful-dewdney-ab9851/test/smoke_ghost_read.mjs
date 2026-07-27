// =====================================================================
// smoke_ghost_read.mjs — #364-A1 diagnosis: after a root dies UNGRACEFULLY
// (no handoff — process kill / power loss), can a late joiner recover the
// history from the surviving cohort backups via the empty-root probe?
//
// The live signature (runs 6-8, 2026-07-21): every read miss mapped to a
// dead node's keyspace slice and 0/38 healed on re-read. The pieces exist —
// empty sub-terminal root fires EMPTY_ROOT_PROBE (PULLUP), a cache-holding
// backup answers REPLAYUP — this smoke asks whether the CHAIN works.
//
// 1. root killed ungracefully, backups alive → late joiner recovers ALL
// 2. root AND publisher killed → still recovers (backups are the source)
//
// Run: node test/smoke_ghost_read.mjs
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
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHexF(idBig) } });
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
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }   // UNGRACEFUL — no handoff
  _closestAlive(target) { let best = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  async settle(cap = 500_000) { let i = 0; while (this.queue.length) { if (++i > cap) throw new Error('no converge'); const j = this.queue.shift(); const n = this.nodes.get(j.dest); if (!n || !n.alive) continue; const h = n.handlers.get(j.type); if (!h) continue; await h(j.payload, j.meta); } }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
  async fireProbes() {
    // empty-root probes arm via unref'd real timers; drive them deterministically
    for (const n of this.nodes.values()) if (n.alive) {
      for (const [t, role] of n.am.axonRoles) {
        if (role.isRoot && !role.cache.length) { await n.am._emptyRootProbe(t).catch(() => {}); }
      }
    }
    await this.settle();
  }
}

async function scenario(killPublisherToo) {
  const fab = new Fabric();
  const author = await createAuthorIdentity(); let seq = 1;
  const desc = { region: 'useast', owner: null, name: `ghost-${killPublisherToo}`, write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const subs = []; for (let i = 0; i < 10; i++) subs.push(fab.addNode(fabId(`g${killPublisherToo}-s${i}`)));
  const pub = fab.addNode(fabId(`g${killPublisherToo}-p`));
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();
  const ids = [];
  for (let k = 0; k < 5; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: seq++, identity: author, ts: fab.clock });
    ids.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  for (let r = 0; r < 3; r++) { fab.clock += 6_000; await fab.tickAll(); }   // cohort replication rounds
  const root = fab.nodes.get(fab._closestAlive(topicId));
  const backups = [...fab.nodes.values()].filter(n => n !== root && n.am.axonRoles.get(topicId)?.cache?.length);
  fab.kill(root.id);                                     // UNGRACEFUL — history dies with it unless backups serve
  if (killPublisherToo) fab.kill(pub.id);
  const late = fab.addNode(fabId(`g${killPublisherToo}-late`));
  late.am._lastSeenTsByTopic.set(topicId, 0);
  late.am.pubsubSubscribe(topicId);
  await fab.settle();
  await fab.fireProbes();                                 // drive the empty-root probe deterministically
  fab.clock += 6_000; await fab.tickAll(); await fab.fireProbes();
  const got = ids.filter(id => late.got.includes(id)).length;
  return { got, total: ids.length, backupCount: backups.length };
}

async function main() {
  console.log('Axona pub/sub — ghost read: ungraceful root death, cohort serves the late joiner');
  {
    const r = await scenario(false);
    check(`backups existed before the kill (${r.backupCount})`, r.backupCount >= 1, `(${r.backupCount})`);
    check(`root killed ungracefully → late joiner recovers ALL (${r.got}/${r.total})`, r.got === r.total, `(${r.got}/${r.total})`);
  }
  {
    const r = await scenario(true);
    check(`root AND publisher killed → late joiner recovers ALL (${r.got}/${r.total})`, r.got === r.total, `(${r.got}/${r.total})`);
  }
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
