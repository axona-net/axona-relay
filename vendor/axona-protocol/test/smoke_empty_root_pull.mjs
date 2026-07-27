// smoke_empty_root_pull.mjs — empty-self-root cohort pull (v4.24.0).
//
// Field-captured mechanism (alert-bot, 2026-07-15; DIAGNOSIS-4.22.1-alertbot-
// misses-repro.md): a cold subscriber whose SUB terminates at itself becomes
// the topic's root with an EMPTY cache while a live holder (ex-root / backup)
// still has the full history — and nothing tells the holder about the new
// closer root, so the empty state is STICKY (82% of the field misses; still
// empty at 600s). The oracle proved holderExists=40/40: the data was there,
// only the reader's empty root claim blocked it.
//
// The fix: a root born with no history PULLS — PULLUP(sinceHw:0) at the
// K-closest cohort; holders reply via the existing REPLAYUP → verified
// union-ingest. This smoke proves:
//   1. an empty self-root converges to the holder's full cache via the probe
//   2. the probe QUENCHES once the cache is non-empty (no re-fire)
//   3. a genuinely-fresh topic (no holder anywhere) probes at most
//      EMPTY_ROOT_PROBE_MAX times via refreshTick, then goes quiet
//   4. the birth-scheduled probe skips when the cache fills within the delay
//      (a pub-terminal root's own publish → no probe chatter)
//
// Run: node test/smoke_empty_root_pull.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { EMPTY_ROOT_PROBE_MAX } from '../src/pubsub/constants.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
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
      findKClosest: async (target, k = 3) => {
        return [...self.nodes.entries()].filter(([, n]) => n.alive)
          .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
          .slice(0, k).map(b => idHex(b));
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000, rootReplicas: 0 });
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
    for (let round = 0; ; round++) {
      if (round > 1000) throw new Error('settle: ingest/wire did not converge');
      while (this.queue.length) {
        if (++i > cap) throw new Error('settle: did not converge');
        const j = this.queue.shift();
        const n = this.nodes.get(j.dest);
        if (!n || !n.alive) continue;
        const h = n.handlers.get(j.type);
        if (!h) continue;
        await h(j.payload, j.meta);
      }
      // REPLICATE/REPLAYUP are queued since #332 — flush each node's ingest
      // pump too; ingest may emit new wire traffic, so loop until both quiet.
      let flushed = false;
      for (const n of this.nodes.values()) {
        if (n.alive && ((n.am._ingestQueue && n.am._ingestQueue.length) || n.am._ingestPumping)) {
          await n.am._ingestIdle();
          flushed = true;
        }
      }
      if (!this.queue.length && !flushed) return;
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const cacheSize = (rec, t) => rec.am.axonRoles.get(t)?.cache.length ?? 0;

async function main() {
  console.log('empty-self-root cohort pull (v4.24.0) — the alert-bot read-miss fix\n');
  const author = await createAuthorIdentity();
  const M = 4; const SEQ = { n: 1 };

  const fab = new Fabric();
  const nodes = [];
  for (let i = 0; i < 12; i++) { const id = await createNodeIdentity(__LOC); nodes.push(fab.addNode(BigInt('0x' + id.id))); }
  const desc = { region: 'useast', owner: null, name: 'empty-root-pull', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // Publish M messages: the natural root H (closest live node) stamps + caches.
  for (const s of nodes.slice(0, 3)) s.am.pubsubSubscribe(topicId);
  await fab.settle(); fab.clock += 6_000; await fab.tickAll();
  const ids = [];
  for (let k = 0; k < M; k++) {
    const e = await buildEnvelope({ topic: desc, message: { k }, seq: SEQ.n++, identity: author, ts: fab.clock });
    ids.push(e.msgId); nodes[nodes.length - 1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
  }
  const H = fab.nodes.get(fab._closestAlive(topicId));
  check('setup: holder H (natural root) caches the full history', cacheSize(H, topicId) === M, `(${cacheSize(H, topicId)}/${M})`);

  // A NEW node R strictly closer to the topic than H joins — the field
  // topology: the fresh reader is now the routing terminus while H holds
  // everything. R self-roots EMPTY (the captured bug state).
  let R = null;
  for (let draw = 0; draw < 400 && !R; draw++) {
    const id = await createNodeIdentity(__LOC);
    const b = BigInt('0x' + id.id);
    if ((b ^ topicId) < (H.id ^ topicId)) R = fab.addNode(b);
  }
  check('setup: drew a joiner R strictly closer to the topic than H', !!R);
  R.am._becomeRoot(topicId, 'sub-terminal');
  check('R is an EMPTY self-root (the sticky field state)', R.am.axonRoles.get(topicId)?.isRoot === true && cacheSize(R, topicId) === 0);

  // ── 1. the cohort pull converges the empty root to the holder's history ──
  await R.am._emptyRootProbe(topicId);
  await fab.settle();
  check('probe → PULLUP → REPLAYUP: R ingested the FULL history', cacheSize(R, topicId) === M, `(${cacheSize(R, topicId)}/${M})`);
  const role = R.am.axonRoles.get(topicId);
  check('every holder msgId present at R (verified union-ingest)', ids.every(id => role.cacheIds.has(id)));
  check('probe accounted (probeTries=1)', role.sync.probeTries === 1, `(${role.sync.probeTries})`);

  // ── 2. quench: a filled root never probes again ──────────────────────────
  const triesBefore = role.sync.probeTries;
  await R.am._emptyRootProbe(topicId);
  await fab.settle();
  check('non-empty root skips further probes (quench)', role.sync.probeTries === triesBefore, `(${role.sync.probeTries})`);

  // ── 3. no-holder topic: bounded probes via refreshTick, then quiet ───────
  const desc2 = { region: 'useast', owner: null, name: 'empty-root-fresh', write: 'open' };
  const topic2 = await deriveTopicIdBig(desc2);
  const R2 = fab.nodes.get(fab._closestAlive(topic2));   // the natural terminus for topic2
  R2.am.pubsubSubscribe(topic2);                          // field shape: the reader HOLDS a subscription
  await fab.settle();                                     // (keeps the empty role from the idle-role sweep)
  R2.am._becomeRoot(topic2, 'sub-terminal');
  for (let r = 0; r < EMPTY_ROOT_PROBE_MAX + 3; r++) {
    fab.clock += 6_000;                       // past EMPTY_ROOT_PROBE_INTERVAL_MS
    await fab.tickAll();
    await new Promise(res => setImmediate(res));   // let the fire-and-forget probe run
    await fab.settle();
  }
  const role2 = R2.am.axonRoles.get(topic2);
  check(`fresh no-holder topic probes at most ${EMPTY_ROOT_PROBE_MAX} times then quiets`,
    role2.sync.probeTries === EMPTY_ROOT_PROBE_MAX, `(${role2.sync.probeTries})`);
  check('fresh topic stays an empty root (nothing to pull — correct)', cacheSize(R2, topic2) === 0);

  // ── 4. birth-scheduled probe skips when the cache fills within the delay ──
  const desc3 = { region: 'useast', owner: null, name: 'empty-root-pubterm', write: 'open' };
  const topic3 = await deriveTopicIdBig(desc3);
  const R3 = nodes[5];
  R3.am._becomeRoot(topic3, 'pub-terminal');
  // simulate the root's own publish landing immediately (pub-terminal shape)
  const e3 = await buildEnvelope({ topic: desc3, message: { own: 1 }, seq: SEQ.n++, identity: author, ts: fab.clock });
  const role3 = R3.am.axonRoles.get(topic3);
  role3.cache.push({ msgId: e3.msgId, publishTs: fab.clock, json: JSON.stringify(e3), seq: 1 });
  role3.cacheIds.add(e3.msgId);
  await sleep(1000);                          // real timer: past EMPTY_ROOT_PROBE_DELAY_MS
  await fab.settle();
  check('pub-terminal root (cache filled in-delay) never probed', role3.sync.probeTries === 0, `(${role3.sync.probeTries})`);

  // …and the scheduled path DOES fire for a root still empty after the delay:
  check('scheduled birth probe fired for the still-empty root R (wall-clock)', role.sync.probeTries >= 1);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
