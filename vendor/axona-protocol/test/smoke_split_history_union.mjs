// smoke_split_history_union.mjs — post-transition split history converges to the
// union; a fresh since:'all' subscriber replays the FULL timeline (the cold-attach
// residual, soak restart-scenario "exactly 6/12" signature).
//
// The disease (captured on the 4.21.0 overnight soak, ~1-in-15 restart runs):
// after a root transition the topic's history splits — the NEW root R holds the
// post-transition half (it stamps new publishes), the demoted ex-root H holds the
// pre-transition half. The designed heal was H's re-subscribe advertising `hw`
// (its NEWEST stamp) with R issuing PULLUP when hw > its own — but H's half is
// OLDER, its hw sits BELOW R's, and the pull never fires. The cohort channel was
// closed too: _onReplicate dropped pushes at a root, so co-hosting roots never
// unioned (contradicting _replicateRole's own contract). A fresh subscriber
// attaching to R replayed exactly the post half — 6/12, every time.
//
// The fix (two halves of one signal, no new mechanism):
//   F1  a ROOT union-ingests an incoming REPLICATE (dels first, stamped-verified
//       bodies via _ingestStamped → dedup + tombstone-suppress + fanout/deliver)
//   F2  the SUB advertises `lw` (oldest cached stamp) beside `hw`; the root
//       PULLUPs the child's FULL range (sinceHw:0) when the child holds strictly-
//       older history the hw rule cannot see. Optional field, wire-compatible;
//       self-quenching once the union completes.
//
// This smoke drives the real AxonaManager over the divergent-view fabric and
// proves, end to end:
//   1. the split forms (R roots the post half while H holds the pre half)
//   2. H demotes on R's beacon and the union converges at R (lw→PULLUP path)
//   3. a FRESH since:'all' subscriber attaching to R replays ALL 12 messages
//   4. a message KILLED at R before the union does not resurrect through it
//   5. an already-attached subscriber is healed by the arriving history (fanout
//      on ingest), not just future joiners
//
// Run: node test/smoke_split_history_union.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

// Divergent-view fabric (same as smoke_root_reconcile): explicit per-node
// neighbour lists, greedy hop-by-hop walk, local-only findKClosest — the real
// routing's failure mode.
class DivergentFabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); this.pullups = 0; }
  addNode(idBig) {
    const self = this; const handlers = new Map();
    const rec = { id: idBig, handlers, alive: true, got: [], kills: [], neighbors: new Set() };
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => self._walk(idBig, target, type, payload),
      neighbors: () => [...rec.neighbors].filter((n) => self.nodes.get(n)?.alive).map((n) => idHex(n)),
      bridgeId: () => null,
      findKClosest: async (target, k = 3) => {
        const cands = [idBig, ...[...rec.neighbors].filter((n) => self.nodes.get(n)?.alive)];
        return cands.sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k);
      },
      lookup: async (target) => {
        let best = null, bestD = null;
        for (const [id, n] of self.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
        return best === null ? { path: [] } : { path: [idHex(best)] };
      },
    };
    rec.am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    rec.am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    if (typeof rec.am.onPubsubKill === 'function') rec.am.onPubsubKill((_t, msgId) => rec.kills.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  link(a, b) { this.nodes.get(a).neighbors.add(b); this.nodes.get(b).neighbors.add(a); }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _walk(from, target, type, payload) {
    if (type === 'pubsub:pullup') this.pullups++;
    const via0 = (Array.isArray(payload?.via) && payload.via.length) ? BigInt('0x' + payload.via[0]) : null;
    let cur = from;
    for (let hop = 0; hop < 32; hop++) {
      const rec = this.nodes.get(cur);
      if (!rec || !rec.alive) return;
      if (via0 !== null && cur === via0 && cur !== from) {
        this.queue.push({ dest: cur, type, payload, meta: { targetId: target, isTerminal: true, hopCount: hop, fromId: idHex(from) } });
        return;
      }
      let next = null, bestD = cur ^ target;
      for (const nb of rec.neighbors) {
        const n = this.nodes.get(nb);
        if (!n || !n.alive) continue;
        const d = nb ^ target;
        if (d < bestD) { bestD = d; next = nb; }
      }
      if (next === null) {
        this.queue.push({ dest: cur, type, payload, meta: { targetId: target, isTerminal: true, hopCount: hop, fromId: idHex(from) } });
        return;
      }
      cur = next;
    }
  }
  async settle(cap = 100_000) {
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
        try { await h(j.payload, j.meta); } catch (e) { console.error('handler threw:', e.message); }
      }
      // REPLICATE/REPLAYUP are queued since #332 (time-sliced ingest pump) —
      // flush every node's queue too; ingest can emit new wire messages
      // (fanout, PULLUP), so loop until BOTH planes are quiet.
      let flushed = false;
      for (const [, n] of this.nodes) {
        if (n.alive && ((n.am._ingestQueue && n.am._ingestQueue.length) || n.am._ingestPumping)) {
          await n.am._ingestIdle();
          flushed = true;
        }
      }
      if (!this.queue.length && !flushed) return;
    }
  }
  async tickAll() { for (const [, n] of this.nodes) if (n.alive) n.am.refreshTick(); await this.settle(); }
  advance(ms) { this.clock += ms; }
  roots(topicBig) {
    const out = [];
    for (const [id, n] of this.nodes) {
      if (!n.alive) continue;
      const r = n.am.axonRoles.get(topicBig);
      if (r && r.isRoot) out.push(idHex(id).slice(-4));
    }
    return out;
  }
}

// ── topology ──────────────────────────────────────────────────────────
// R = true closest (offline for the pre phase) ; H = near-miss (roots the pre
// half) ; P = publisher (knows both) ; S_old = attached early via H ; S_new =
// the fresh since:'all' joiner, attaching to R after the transition.
const author = await createAuthorIdentity();
const topicDesc = { region: 'useast', name: 'split-history-union-smoke', write: 'open' };
const T = await deriveTopicIdBig(topicDesc);
const near = (lo) => T ^ BigInt(lo);
const envFor = async (text) => JSON.stringify(await buildEnvelope({ topic: topicDesc, message: text, identity: author }));

const fab = new DivergentFabric();
const R  = fab.addNode(near(0x01));    // true closest — the post-transition root
const H  = fab.addNode(near(0x40));    // near-miss — roots the pre half
const P  = fab.addNode(near(0x80));    // publisher
const So = fab.addNode(near(0x44));    // early subscriber (attached under H)
const Sn = fab.addNode(near(0x08));    // fresh joiner (will attach to R)
// NOTE: H↔R deliberately NOT linked until phase 4 — while unlinked, H never
// hears R's beacon, so the split persists observably (disease precondition +
// kill-BEFORE-union ordering are testable). Linking them models the mesh
// forming; the union must then converge in one beacon→demote→pull round.
fab.link(P.id, H.id); fab.link(P.id, R.id);
fab.link(So.id, H.id);
fab.link(Sn.id, R.id);

console.log('— phase 1: R offline; H roots and stamps the PRE half (6 msgs) —');
fab.kill(R.id);
So.am.pubsubSubscribe(T); So.am.mySubscriptions.get(T).since = 0;
await fab.settle();
const pre = [];
for (let i = 1; i <= 6; i++) {
  fab.advance(20);
  const json = await envFor(`pre-${i}`);
  pre.push(JSON.parse(json).msgId);
  P.am._send('pubsub:pub', { topicId: idHex(T), via: [], json });
  await fab.settle();
}
check('H is sole root of the pre half', fab.roots(T).length === 1 && fab.roots(T)[0] === idHex(H.id).slice(-4), JSON.stringify(fab.roots(T)));
check('H caches 6 pre messages', H.am.axonRoles.get(T)?.cache.length === 6, `${H.am.axonRoles.get(T)?.cache.length}`);
check('early subscriber received the pre half', So.got.length === 6, `got ${So.got.length}`);

console.log('— phase 2: R comes alive; the POST half (6 msgs) roots on R → split —');
fab.nodes.get(R.id).alive = true;
const post = [];
for (let i = 1; i <= 6; i++) {
  fab.advance(20);
  const json = await envFor(`post-${i}`);
  post.push(JSON.parse(json).msgId);
  P.am._send('pubsub:pub', { topicId: idHex(T), via: [], json });
  await fab.settle();
}
const rRole = R.am.axonRoles.get(T);
check('R rooted the post half', !!rRole?.isRoot, JSON.stringify(fab.roots(T)));
check('disease precondition: R holds ONLY the post half', rRole?.cache.length === 6, `${rRole?.cache.length}`);

console.log('— phase 3: kill one PRE message at R before the union (tombstone-first) —');
// The kill routes to the CURRENT root R, which has never seen the target body —
// a provisional tombstone. The union arriving later must not resurrect it.
const killedId = pre[0];   // the OLDEST — its holder's lw stays below the root's forever
const kill = await buildKill({ topicId: idHex(T), msgId: killedId, identity: author });
P.am._send('pubsub:kill', { topicId: idHex(T), via: [], kill });
await fab.settle();
check('R holds a provisional tombstone for the killed pre message', rRole?.tombstones.has(killedId));

console.log('— phase 4: H hears R\'s beacon, demotes, and the halves UNION at R —');
fab.link(H.id, R.id);                  // the mesh forms between the two holders
R.am._emitRootBeacons();
await fab.settle();
await fab.tickAll();          // H's demote re-subscribe (hw+lw) → R PULLUPs → H replays up
await fab.settle();
await fab.tickAll();          // one more round for the replay-up + cohort push to land
await fab.settle();
const rootsAfter = fab.roots(T);
check('exactly one root after the transition (R)', rootsAfter.length === 1 && rootsAfter[0] === idHex(R.id).slice(-4), JSON.stringify(rootsAfter));
check('THE FIX: R converged to the union (11 = 12 minus the killed one)', rRole?.cache.length === 11, `R cache ${rRole?.cache.length}`);
check('killed pre message did NOT resurrect through the union', !rRole?.cache.some((c) => c.msgId === killedId));

console.log('— phase 5: a FRESH since:\'all\' subscriber replays the FULL timeline —');
Sn.am._lastSeenTsByTopic.set(T, 0); Sn.am.pubsubSubscribe(T);
await fab.settle();
await fab.tickAll();
await fab.settle();
const wantIds = new Set([...pre, ...post].filter((m) => m !== killedId));
const gotIds = new Set(Sn.got);
const missing = [...wantIds].filter((m) => !gotIds.has(m));
check(`fresh subscriber replayed the full timeline (${wantIds.size}/${wantIds.size})`, missing.length === 0, `missing ${missing.length}: got ${Sn.got.length}`);
check('fresh subscriber never saw the killed body', !gotIds.has(killedId));

console.log('— phase 6: an ALREADY-ATTACHED subscriber is healed by a late union —');
// New topic, same shape, but the fresh subscriber attaches to R BEFORE the
// union lands — the arriving pre half must be fanned to it (heal-in-place),
// not only served to future joiners.
const topic2 = { region: 'useast', name: 'split-history-union-smoke-2', write: 'open' };
const T2 = await deriveTopicIdBig(topic2);
const near2 = (lo) => T2 ^ BigInt(lo);
const env2 = async (text) => JSON.stringify(await buildEnvelope({ topic: topic2, message: text, identity: author }));
const R2 = fab.addNode(near2(0x01));
const H2 = fab.addNode(near2(0x40));
const P2 = fab.addNode(near2(0x80));
const S2 = fab.addNode(near2(0x08));
fab.link(H2.id, R2.id); fab.link(P2.id, H2.id); fab.link(P2.id, R2.id); fab.link(S2.id, R2.id);
fab.kill(R2.id);
const pre2 = [];
for (let i = 1; i <= 3; i++) {
  fab.advance(20);
  const json = await env2(`p-${i}`);
  pre2.push(JSON.parse(json).msgId);
  P2.am._send('pubsub:pub', { topicId: idHex(T2), via: [], json });
  await fab.settle();
}
fab.nodes.get(R2.id).alive = true;
fab.advance(20);
P2.am._send('pubsub:pub', { topicId: idHex(T2), via: [], json: await env2('q-1') });
await fab.settle();
S2.am._lastSeenTsByTopic.set(T2, 0); S2.am.pubsubSubscribe(T2);   // attaches to R2 pre-union
await fab.settle();
check('attached subscriber starts with only the post half', S2.got.length === 1, `got ${S2.got.length}`);
R2.am._emitRootBeacons();
await fab.settle();
await fab.tickAll(); await fab.settle();
await fab.tickAll(); await fab.settle();
const missing2 = pre2.filter((m) => !S2.got.includes(m));
check('late union HEALED the attached subscriber (pre half fanned to it)', missing2.length === 0, `missing ${missing2.length}, got ${S2.got.length}`);

console.log('— phase 7 (THE 4.22.1 FIX): a refused pull must QUENCH, not loop —');
// After the union, H still holds the killed pre-1 as its OLDEST message — R holds
// its tombstone and will never accept the body, so H's lw sits below R's forever.
// On 4.22.0 every renewal re-fired PULLUP(sinceHw:0) → H replayed its FULL cache
// up each tick, forever (the testnet relay storm: flapping roots, 0% backlog runs,
// leave() drains pinned at timeout). The pull must fire at most once per
// (subscriber, lw) — re-arming only if the child's lw DECREASES further.
const before = fab.pullups;
for (let i = 0; i < 6; i++) { fab.advance(6_000); await fab.tickAll(); await fab.settle(); }
const rePulls = fab.pullups - before;
check(`six renewal rounds re-fire at most one pull (got ${rePulls})`, rePulls <= 1, `${rePulls} PULLUPs across 6 rounds`);
check('killed body still suppressed at R after the rounds', !R.am.axonRoles.get(T)?.cache.some((c) => c.msgId === killedId));

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
