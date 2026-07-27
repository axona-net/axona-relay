// =====================================================================
// smoke_backup_handoff.mjs — departing NON-ROOT holders hand off their cache,
// gated on root liveness (the alert-bot "9-13% of pubs never preserved" fix).
//
// The bug: pubsubLeaveHandoff only handed off ROOTED topics. A node leaving
// while holding the LAST copy of a message as a BACKUP replica dropped it
// silently — under churn (root departs → copy cascades to a backup → backup
// departs) the message died with no survivor to replay from. Diagnosis
// (test/diag_restart_loss.mjs): 100% of restart-scenario loss was exactly
// this HANDOFF_GAP.
//
// The gate: a departing non-root holder SKIPS the handoff only on POSITIVE
// confirmation the topic's root is alive right now (candidate root is a
// current direct neighbour). Passive signals (beacons/keepalives) stay fresh
// for tens of seconds after a root departs, so on a mass teardown they lie —
// hence unconfirmable ⇒ hand off. False-"dead" costs one redundant handoff
// (heir reconciliation converges it); false-"alive" loses data forever.
//
// 1. END-TO-END (sim fabric, no neighbour introspection → unconfirmable):
//    root gracefully leaves, then the backups leave; a fresh since:'all'
//    late joiner recovers EVERY message. (Pre-fix: ~85%.)
// 2. UNIT: with neighbors() introspection —
//    a. rooted topic          → HANDOFF sent (gate never applies to roots)
//    b. backup, root ALIVE    → NO handoff (root is a live neighbour)
//    c. backup, root DEAD     → HANDOFF sent (not a neighbour)
//
// Run: node test/smoke_backup_handoff.mjs
// =====================================================================
import { createHash } from 'node:crypto';
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { makeRole } from '../src/pubsub/rootClaim.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { T } from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (b) => b.toString(16).padStart(66, '0');
const fabId = (tag) => BigInt('0x89' + createHash('sha256').update(String(tag)).digest('hex'));
const CLOCK_BASE = 1_600_000_000_000;

// ── the shared sim fabric (no dht.neighbors → liveness unconfirmable) ──
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

async function main() {
  console.log('Axona pub/sub — backup handoff, root-liveness gated');
  const author = await createAuthorIdentity(); const SEQ = { n: 1 };

  // ── 1. end-to-end: mass graceful teardown loses NOTHING ─────────────
  {
    const TRIALS = 8, PRE = 4;
    let recovered = 0, total = 0;
    for (let t = 0; t < TRIALS; t++) {
      const fab = new Fabric();
      const desc = { region: 'useast', owner: null, name: `bh-${t}`, write: 'open' };
      const topicId = await deriveTopicIdBig(desc);
      const subs = []; for (let i = 0; i < 12; i++) subs.push(fab.addNode(fabId(`b${t}-s${i}`)));
      const pub = fab.addNode(fabId(`b${t}-p`));
      for (const s of subs) s.am.pubsubSubscribe(topicId);
      await fab.settle(); fab.clock += 6_000; await fab.tickAll();
      const ids = [];
      for (let k = 0; k < PRE; k++) { const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ.n++, identity: author, ts: fab.clock }); ids.push(e.msgId); pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle(); }
      for (let r = 0; r < 3; r++) { fab.clock += 6_000; await fab.tickAll(); await fab.settle(); }
      // root leaves first (its beacon is still "fresh" when the backups leave —
      // the exact window where passive liveness signals lie)
      const root = fab.nodes.get(fab._closestAlive(topicId));
      await root.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(root.id);
      fab.kill(pub.id);
      for (const s of subs.filter(s => s.id !== root.id).slice(0, 4)) { await s.am.pubsubLeaveHandoff(); await fab.settle(); fab.kill(s.id); }
      for (let r = 0; r < 4; r++) { fab.clock += 60_000; await fab.tickAll(); }
      const late = fab.addNode(fabId(`b${t}-late`));
      late.am._lastSeenTsByTopic.set(topicId, 0); late.am.pubsubSubscribe(topicId);
      await fab.settle(); fab.clock += 6_000; await fab.tickAll(); await fab.settle();
      total += ids.length; recovered += ids.filter(id => late.got.includes(id)).length;
    }
    check(`mass graceful teardown: late joiner recovers ALL history (${recovered}/${total})`, recovered === total && total > 0, `(${recovered}/${total})`);
  }

  // ── 2. unit: the liveness gate itself ───────────────────────────────
  {
    const self = fabId('unit-self'), heir = fabId('unit-heir');
    // An OUT-OF-REGION candidate (0x54 prefix) that findKClosest returns FIRST
    // (i.e. XOR-closer) — the region preference must still pick the in-region heir.
    const outHeir = BigInt('0x54' + createHash('sha256').update('unit-outheir').digest('hex'));
    const rootAlive = fabId('unit-root-alive'), rootDead = fabId('unit-root-dead');
    const tRooted = fabId('unit-topic-rooted'), tBackAlive = fabId('unit-topic-backalive'), tBackDead = fabId('unit-topic-backdead');
    const sent = [];   // {target, type, payloadStr}
    let clock = CLOCK_BASE;
    const dht = {
      getSelfId: () => self,
      onRoutedMessage: () => {},
      routeMessage: (target, type, payload) => { sent.push({ target, type, payloadStr: JSON.stringify(payload ?? {}) }); },
      findKClosest: async () => [outHeir, heir],   // out-of-region candidate listed CLOSER
      neighbors: () => [idHex(rootAlive), idHex(heir)],   // rootAlive IS a live neighbour; rootDead is NOT
    };
    const am = new AxonaManager({ dht, now: () => clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const seed = (tBig, isRoot, backupOfBig) => {
      const role = makeRole(tBig, isRoot);
      if (backupOfBig) role.backupOf = idHex(backupOfBig).toLowerCase();
      am._cachePush(role, { msgId: `m-${idHex(tBig).slice(0, 8)}`, json: '{"v":1}', publishTs: clock, ts: clock, seq: 1 });
      am.axonRoles.set(tBig, role);
      return role;
    };
    seed(tRooted, true, null);
    seed(tBackAlive, false, rootAlive);
    // fresh beacon for the DEAD root — proves a fresh beacon alone must NOT count as alive
    seed(tBackDead, false, rootDead);
    am._rootBeacons.set(tBackDead, { root: idHex(rootDead).toLowerCase(), at: clock, exp: clock + 50_000 });

    await am.pubsubLeaveHandoff();
    const sentFor = (type, tBig) => sent.some(s => s.type === type && s.payloadStr.includes(idHex(tBig).toLowerCase().slice(2)));
    check('rooted topic sends confirmed HANDOFF (gate never applies to roots)', sentFor(T.HANDOFF, tRooted));
    check('backup with root ALIVE (live neighbour) sends NOTHING', !sentFor(T.HANDOFF, tBackAlive) && !sentFor(T.REPLICATE, tBackAlive));
    check('backup with root DEAD pushes REPLICATE — even with a still-fresh beacon', sentFor(T.REPLICATE, tBackDead));
    check('backup push never mints a root at the receiver (no HANDOFF from a backup)', !sentFor(T.HANDOFF, tBackDead));
    // Region preference (#362): the PRIMARY heir must be the in-region
    // candidate even when an out-of-region one is XOR-closer (listed first by
    // findKClosest) — an out-of-region holder is durable but unfindable by
    // routed reads. The redundant runner-up push MAY go out-of-region (a
    // wrong-place second copy beats no second copy).
    const handoffTargets = sent.filter(s => s.type === T.HANDOFF).map(s => s.target);
    check('confirmed HANDOFF targets the IN-REGION heir (out-of-region closer candidate skipped)',
      handoffTargets.length > 0 && handoffTargets.every(t => t === heir),
      `(targets=${handoffTargets.map(t => idHex(t).slice(0, 4))})`);
    const backupPrimary = sent.find(s => s.type === T.REPLICATE);
    check('backup push PRIMARY target is the in-region heir',
      backupPrimary != null && backupPrimary.target === heir,
      `(first=${backupPrimary ? idHex(backupPrimary.target).slice(0, 4) : 'none'})`);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
