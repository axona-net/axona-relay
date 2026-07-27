// =====================================================================
// experiment_packet_loss_ab.mjs — does read-repair (4.36) shrink the
// delivery-loss window under packet loss / saturation? (Howard's Q, 2026-07-22)
//
// NOT a regression test (not wired into `npm test`). A repeatable RESEARCH
// harness — kept so we can re-run the packet-loss / saturation study later.
//
//   Run:  node test/experiment_packet_loss_ab.mjs
//
// ── WHAT IT DOES ─────────────────────────────────────────────────────
// Deterministic in-process sim. Uniform per-message packet loss on EVERY
// routed hop (writes, replication, SUBs, replays, pulls). One publisher posts
// to M topics across a cohort of H hosts, runs retry rounds while alive (so
// writes land on the cohort under loss), then leaves gracefully (4.32 handoff).
// R fresh readers then subscribe since:'all' within a BOUNDED read window
// (READ_ROUNDS — the alert-bot's subTimeoutS analogue), and we measure how many
// (reader,topic) pairs received the FULL history.
//
// Two arms differ in EXACTLY ONE thing — the 4.35→4.36 delta:
//   · repair ON  (4.36): _readRepair active (stuck-subscriber cohort pull)
//   · repair OFF (4.35): _readRepair no-op'd; everything else identical
//     (empty-root probe, publish/kill retries, leave-handoff all still on)
// We also record "cohort has a full copy" per topic (cohortHad) to separate
// WRITE loss (data never replicated) from READ loss (data present, reader
// couldn't get it — read-repair's nominal domain).
//
// ── KNOBS (in main() / top of file) ─────────────────────────────────
//   H, R, M, REPS      — hosts, readers, topics, reps per cell
//   lossRates          — the swept per-message drop probabilities
//   READ_ROUNDS        — reader's bounded recovery window (const, below)
//   pub-alive rounds   — the `for r<5` loop before pub.leave() (write retries)
//
// ── FINDINGS (2026-07-22, kernel 4.36.0) ────────────────────────────
// Uniform loss ≤35% (bounded 3-round read window): 100.0±0.0% delivery in BOTH
// arms — read-repair makes ZERO measurable difference. It is ORTHOGONAL to
// packet-loss resilience: uniform transient loss is healed by the publish/kill
// retry loop + empty-root probe + anti-entropy union, NOT by read-repair.
// Harness validated sensitive: retries-disabled → 0/24; 90% loss → 0/24;
// baseline → 24/24. Bonus: at 60% loss WITH recovery, delivery stayed 100%
// even with only 2/6 topics fully-copied on any SINGLE host — partial fragments
// union to complete across the cohort (anti-entropy). Loss causes PERMANENT
// miss only when recovery is prevented from running.
// ⇒ Read-repair's positive case is a PERSISTENTLY-WEDGED closest holder (a
//   blocked event loop), which uniform loss does not create. That case is
//   proven separately in test/smoke_read_repair.mjs (0/5 → 5/5).
//
// ── TO EXTEND (the wedged-holder-under-loss variant) ─────────────────
// To make read-repair's benefit visible, add a persistently-unresponsive
// closest holder (see the Fabric.degrade() pattern in smoke_read_repair.mjs:
// keep it alive + a neighbour + XOR-closest, but drop everything routed to it)
// ON TOP OF uniform loss, and place readers FARTHER than it. That is the only
// regime where the ON arm should beat the OFF arm.
// =====================================================================
import { createHash } from 'node:crypto';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

const idHexF = (b) => b.toString(16).padStart(66, '0');
const fabId = (tag) => BigInt('0x89' + createHash('sha256').update(String(tag)).digest('hex'));
const CLOCK_BASE = 1_600_000_000_000;

class Fabric {
  constructor(lossRate = 0) { this.nodes = new Map(); this.queue = []; this.clock = CLOCK_BASE; this.lossRate = lossRate; this.repairOn = true; }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHexF(idBig) } });
      },
      findKClosest: async (target, k = 3) =>
        [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => id)
          .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
      neighbors: () => [...self.nodes.entries()].filter(([, n]) => n.alive).map(([id]) => idHexF(id)),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 600_000 });
    // Arm switch: disable ONLY read-repair to simulate 4.35 (keep empty-root
    // probe + publish/kill retries + leave-handoff — everything else 4.36).
    if (!self.repairOn) am._readRepair = async () => {};
    const rec = { id: idBig, am, handlers, alive: true, got: new Set() };
    am.onPubsubDelivery((_t, _j, msgId) => rec.got.add(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closest(target) { let best = null, bd = null; for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bd === null || d < bd) { bd = d; best = id; } } return best; }
  async settle(cap = 800_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('no converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      if (this.lossRate && Math.random() < this.lossRate) continue;   // PACKET DROP
      const h = n.handlers.get(j.type);
      if (!h) continue;
      try { await h(j.payload, j.meta); } catch { /* keep going */ }
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
  async fireRepairs() {
    for (const n of this.nodes.values()) if (n.alive) {
      for (const [t, role] of n.am.axonRoles) {
        if (!role.cache.length && (role.isRoot || role.backupOf)) await n.am._emptyRootProbe(t).catch(() => {});
      }
      if (this.repairOn && typeof n.am._readRepairSweep === 'function') { try { await n.am._readRepairSweep(n.am._now()); } catch { /* */ } }
    }
    await this.settle();
  }
}

const READ_ROUNDS = 3;   // bounded read window (subTimeoutS analogue)

// One rep: returns { delivered, total, cohortHad, readMissable }
async function rep({ lossRate, repairOn, H, R, M, seedTag }) {
  const fab = new Fabric(lossRate); fab.repairOn = repairOn;
  const author = await createAuthorIdentity(); let seq = 1;

  // topics
  const topics = [];
  for (let m = 0; m < M; m++) {
    const desc = { region: 'useast', owner: null, name: `pl-${seedTag}-t${m}`, write: 'open' };
    topics.push({ desc, id: await deriveTopicIdBig(desc), ids: [] });
  }
  // cohort of hosts (subscribe → hold keyspace) + publisher
  const hosts = []; for (let i = 0; i < H; i++) hosts.push(fab.addNode(fabId(`pl-${seedTag}-h${i}`)));
  const pub = fab.addNode(fabId(`pl-${seedTag}-pub`));
  for (const h of hosts) for (const t of topics) h.am.pubsubSubscribe(t.id);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();

  // publish 2 msgs/topic
  for (const t of topics) {
    for (let k = 0; k < 2; k++) {
      const e = await buildEnvelope({ topic: t.desc, message: { k }, seq: seq++, identity: author, ts: fab.clock });
      t.ids.push(e.msgId); pub.am.pubsubPublish(t.id, JSON.stringify(e)); await fab.settle();
    }
  }
  // retry rounds WHILE PUBLISHER ALIVE → writes land on the cohort under loss
  for (let r = 0; r < 5; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.fireRepairs(); }
  // publisher departs gracefully (4.32 handoff hands its self-rooted topics to the cohort)
  pub.am.leave ? await pub.am.leave().catch(() => {}) : null;
  fab.kill(pub.id);
  for (let r = 0; r < 3; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.fireRepairs(); }

  // Is a FULL copy of each topic present on some LIVE host? (write-loss check)
  const cohortHad = topics.map(t => hosts.some(h => {
    const role = h.am.axonRoles.get(t.id); if (!role) return false;
    return t.ids.every(id => role.cacheIds.has(id));
  }));

  // fresh readers, since:'all'. BOUNDED read window (mimics the alert-bot's
  // subTimeoutS): only readRounds recovery ticks, with loss persisting through
  // them — the regime where read-repair's extra cohort pulls can matter.
  const readers = []; for (let i = 0; i < R; i++) { const rd = fab.addNode(fabId(`pl-${seedTag}-rd${i}`)); readers.push(rd); }
  for (const rd of readers) for (const t of topics) { rd.am._lastSeenTsByTopic.set(t.id, 0); rd.am.pubsubSubscribe(t.id); }
  await fab.settle();
  for (let r = 0; r < READ_ROUNDS; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.fireRepairs(); }

  // measure delivery per (reader, topic)
  let delivered = 0, total = 0, readMissable = 0;
  for (let ti = 0; ti < topics.length; ti++) {
    const t = topics[ti];
    for (const rd of readers) {
      total++;
      const ok = t.ids.every(id => rd.got.has(id));
      if (ok) delivered++;
      else if (cohortHad[ti]) readMissable++;   // data WAS recoverable → a read-side miss
    }
  }
  return { delivered, total, readMissable, cohortFull: cohortHad.filter(Boolean).length, M };
}

function stats(xs) {
  const n = xs.length, mean = xs.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(xs.reduce((a, b) => a + (b - mean) ** 2, 0) / n);
  return { mean, sd };
}

async function main() {
  const H = 6, R = 4, M = 6, REPS = 6;
  const lossRates = [0, 0.10, 0.20, 0.35];
  console.log(`Packet-loss A/B — read-repair vs none | hosts=${H} readers=${R} topics=${M} reps=${REPS}`);
  console.log(`(delivery% = (reader,topic) pairs receiving FULL history; readMiss% = misses where the data WAS on a live host)\n`);
  console.log('loss%  | 4.36 repair ON        | 4.35 repair OFF       | Δ deliver | readMiss ON/OFF');
  console.log('-------|-----------------------|-----------------------|-----------|----------------');
  for (const loss of lossRates) {
    const onD = [], offD = [], onRM = [], offRM = [];
    for (let r = 0; r < REPS; r++) {
      const on = await rep({ lossRate: loss, repairOn: true, H, R, M, seedTag: `on-${loss}-${r}` });
      const off = await rep({ lossRate: loss, repairOn: false, H, R, M, seedTag: `off-${loss}-${r}` });
      onD.push(100 * on.delivered / on.total); offD.push(100 * off.delivered / off.total);
      onRM.push(100 * on.readMissable / on.total); offRM.push(100 * off.readMissable / off.total);
    }
    const a = stats(onD), b = stats(offD), ra = stats(onRM), rb = stats(offRM);
    const p2 = (m, s) => `${m.toFixed(1)}±${s.toFixed(1)}%`.padEnd(21);
    const dLabel = `${a.mean - b.mean >= 0 ? '+' : ''}${(a.mean - b.mean).toFixed(1)}%`.padEnd(9);
    const rmLabel = `${ra.mean.toFixed(1)}/${rb.mean.toFixed(1)}%`;
    console.log(`${String(Math.round(loss * 100)).padStart(4)}%  | ${p2(a.mean, a.sd)} | ${p2(b.mean, b.sd)} | ${dLabel} | ${rmLabel}`);
  }
}
main().catch(err => { console.error('experiment threw:', err?.stack || err); process.exit(2); });
