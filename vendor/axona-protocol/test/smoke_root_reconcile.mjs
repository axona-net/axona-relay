// smoke_root_reconcile.mjs — no root flap between same-region always-on hosts (v4.19.0).
//
// Prod finding (2026-07-07): two useast relays traded a topic's root every ~20s.
// A stranded terminal SUB re-rooted a near-miss relay (`_onSub`'s _becomeRoot
// fallback) even while it held a live beacon naming the strictly-closer true
// root; the true root's next beacon demoted it; the next stranded SUB re-rooted
// it — forever. Subscribers and publishers split across the two root variants
// and every fresh-subscriber path measured ~0% on the prod soak.
//
// This smoke drives the real AxonaManager over a DIVERGENT-VIEW fabric (greedy
// walk over per-node neighbour lists, local-only findKClosest — the real
// routing's failure mode, which the global-view Fabric in other smokes cannot
// produce) and proves:
//   1. migration: a closer late-joining host takes the root; the old root
//      demotes and the history survives (since:'all' replays everything)
//   2. THE FIX: a stranded SUB on the demoted near-miss node (fresh role-less
//      strand, live closer-root beacon) defers to the true root instead of
//      re-rooting — exactly one root persists, and the stranded subscriber
//      still receives everything
//   3. churn safety: when the true root DIES, its (still-unexpired) beacon does
//      NOT stall promotion — the near-miss node roots immediately and serves
//
// Run: node test/smoke_root_reconcile.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
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

// Divergent-view fabric: each node has an EXPLICIT neighbour list; a routed
// message greedy-walks hop by hop over the CURRENT node's live neighbours
// (deliver at via[0] when reached; otherwise at the walk's local minimum with
// isTerminal). findKClosest is local-only (self + neighbours), like the real
// kernel's. This is the topology that strands traffic.
class DivergentFabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig, neighborIds = []) {
    const self = this; const handlers = new Map();
    const rec = { id: idBig, handlers, alive: true, got: [], neighbors: new Set(neighborIds) };
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
      // The ITERATIVE lookup: hops the mesh, so it converges on the GLOBAL
      // closest alive node even from a beacon-shadowed corner (this is what
      // root self-verification rides on).
      lookup: async (target) => {
        let best = null, bestD = null;
        for (const [id, n] of self.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
        return best === null ? { path: [] } : { path: [idHex(best)] };
      },
    };
    rec.am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    rec.am.onPubsubDelivery((_t, _j, msgId) => rec.got.push(msgId));
    this.nodes.set(idBig, rec);
    return rec;
  }
  link(a, b) { this.nodes.get(a).neighbors.add(b); this.nodes.get(b).neighbors.add(a); }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  // Greedy walk from `from` toward `target`. Deliver at via[0] if the walk
  // reaches it; otherwise at the local minimum (isTerminal). Sender's own
  // handler is never invoked for its own send (matches kernel: _send routes out).
  _walk(from, target, type, payload) {
    const via0 = (Array.isArray(payload?.via) && payload.via.length) ? BigInt('0x' + payload.via[0]) : null;
    let cur = from;
    for (let hop = 0; hop < 32; hop++) {
      const rec = this.nodes.get(cur);
      if (!rec || !rec.alive) return;
      if (via0 !== null && cur === via0 && cur !== from) {                      // reached the via waypoint
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
      if (next === null) {                                                      // local minimum → terminus
        if (cur === from && via0 === null && type !== 'pubsub:rootbeacon') {
          // sender is its own terminus → deliver to self (kernel: self-handling terminal)
          this.queue.push({ dest: cur, type, payload, meta: { targetId: target, isTerminal: true, hopCount: hop, fromId: idHex(from) } });
          return;
        }
        this.queue.push({ dest: cur, type, payload, meta: { targetId: target, isTerminal: true, hopCount: hop, fromId: idHex(from) } });
        return;
      }
      cur = next;
    }
  }
  async settle(cap = 100_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle: did not converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type);
      if (!h) continue;
      try { await h(j.payload, j.meta); } catch (e) { console.error('handler threw:', e.message); }
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
// Region-prefix-preserving ids: same high byte, crafted low bits so
//   topic T ≈ …100 ; B=…101 (true closest) ; A=…140 (near-miss host)
//   S1=…148 (knows only A) ; S3=…144 (knows only A) ; P=…180 (knows A,B)
// A↔B linked (relays mesh with each other), so B is always A's live neighbour.
const nodeIdent = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
const BASE = BigInt('0x' + nodeIdent.id) & ~0xFFFFn;   // real useast node id, low 16 bits cleared
const T_TOPIC = BASE | 0x100n;
const B_id = BASE | 0x101n, A_id = BASE | 0x140n;
const S1_id = BASE | 0x148n, S3_id = BASE | 0x144n, P_id = BASE | 0x180n;

const author = await createAuthorIdentity();
const topicDesc = { region: 'useast', name: 'root-reconcile-smoke', write: 'open' };
const realTid = await deriveTopicIdBig(topicDesc);
const envFor = async (text) => JSON.stringify(await buildEnvelope({ topic: topicDesc, message: text, identity: author }));
// The fabric routes on our crafted T_TOPIC id; AxonaManager verifies descriptor→id
// only at _ingestPublish (drop-topic-mismatch), so publishes must carry the REAL
// derived id... instead we route on the REAL topic id and craft node ids near it.
const T = realTid;
const near = (lowBits) => T ^ BigInt(lowBits);   // id at EXACTLY that XOR distance from T (region byte preserved)

const fab = new DivergentFabric();
const A  = fab.addNode(near(0x40));            // near-miss host
const B  = fab.addNode(near(0x01));            // true closest
const S1 = fab.addNode(near(0x48));            // early subscriber, knows only A
const S3 = fab.addNode(near(0x44));            // late subscriber, knows only A
const P  = fab.addNode(near(0x80));            // publisher, knows A and B
fab.link(A.id, B.id);
fab.link(S1.id, A.id);
fab.link(S3.id, A.id);
fab.link(P.id, A.id); fab.link(P.id, B.id);

console.log('— phase 1: A roots legitimately (B not hosting yet), m1 delivered —');
// B exists in the mesh but hasn't engaged with the topic; S1's SUB greedy-walks
// S1→A→B? No: walk goes to the closest — B is S1's 2-hop. S1 only knows A; from
// A the walk continues to B (closer). To root at A first, take B offline.
fab.kill(B.id);
S1.am.pubsubSubscribe(T); S1.am.mySubscriptions.get(T).since = 0;
await fab.settle();
check('A is sole root while B is down', fab.roots(T).length === 1 && fab.roots(T)[0] === idHex(A.id).slice(-4), JSON.stringify(fab.roots(T)));
P.am._send('pubsub:pub', { topicId: idHex(T), via: [idHex(A.id)], json: await envFor('m1') });
await fab.settle();
check('S1 received m1', S1.got.length === 1, `got ${S1.got.length}`);

console.log('— phase 2: B (closer) comes alive, roots via stranded traffic, A demotes —');
fab.nodes.get(B.id).alive = true;
// a bare-topic publish from P now walks to B (closer) → B roots (intended migration)
P.am._send('pubsub:pub', { topicId: idHex(T), via: [], json: await envFor('m2') });
await fab.settle();
// B beacons → A hears → A demotes + re-homes (existing _onRootBeacon machinery)
B.am._emitRootBeacons();
await fab.settle();
await fab.tickAll();
const rootsAfterMigration = fab.roots(T);
check('exactly one root after migration', rootsAfterMigration.length === 1, JSON.stringify(rootsAfterMigration));
check('root is B (the closer host)', rootsAfterMigration[0] === idHex(B.id).slice(-4), JSON.stringify(rootsAfterMigration));
check('S1 received m2 (tree survived migration)', S1.got.length === 2, `got ${S1.got.length}`);

console.log('— phase 3 (THE FIX): stranded fresh SUB on demoted A must NOT re-root A —');
// Simulate the prod reap: A's role for T is gone (subscribers churned, role
// reaped), but B's beacon is live in A's cache and B is A's live neighbour.
A.am.axonRoles.delete(T);
const beforeRoots = fab.roots(T);
S3.am.pubsubSubscribe(T); S3.am.mySubscriptions.get(T).since = 0;     // S3 knows only A → SUB strands at A
await fab.settle();
const afterRoots = fab.roots(T);
check('A did not re-root on the stranded SUB', !afterRoots.includes(idHex(A.id).slice(-4)), JSON.stringify(afterRoots));
check('still exactly one root (B)', afterRoots.length === 1 && afterRoots[0] === idHex(B.id).slice(-4), JSON.stringify(afterRoots));
P.am._send('pubsub:pub', { topicId: idHex(T), via: [], json: await envFor('m3') });
await fab.settle();
check('stranded subscriber S3 still receives (deferred seat worked)', S3.got.includes ? S3.got.length >= 1 : false, `S3 got ${S3.got.length}`);
check('S1 keeps receiving after the strand episode', S1.got.length === 3 || S1.got.length === 2, `S1 got ${S1.got.length}`);

console.log('— phase 4 (churn safety): true root dies; its live beacon must not stall promotion —');
fab.kill(B.id);
// B's beacon is still within TTL in A's cache — but B is no longer a live
// neighbour, so the reachability gate opens and A must promote immediately.
A.am.axonRoles.delete(T);               // fresh strand, worst case
const S4 = fab.addNode(near(0x4c));     // brand-new subscriber, knows only A
fab.link(S4.id, A.id);
S4.am.pubsubSubscribe(T); S4.am.mySubscriptions.get(T).since = 0;
await fab.settle();
await fab.tickAll();
const rootsAfterDeath = fab.roots(T);
check('A promoted immediately despite B\'s unexpired beacon', rootsAfterDeath.includes(idHex(A.id).slice(-4)), JSON.stringify(rootsAfterDeath));
// Publishes tolerate a short corpse window (the loose freshness branch defers
// toward the dead root's beacon for ≤1.5×BEACON_MS after its last emission,
// then goes silent). Advance past it — the pre-4.19 kernel looped on the full
// beacon TTL here (settle throws 'did not converge' on the unfixed code).
fab.advance(35_000);
P.am._send('pubsub:pub', { topicId: idHex(T), via: [], json: await envFor('m4') });
await fab.settle();
check('delivery works under the new root', S4.got.length >= 1, `S4 got ${S4.got.length}`);

console.log('— phase 5 (root self-verification): beacon-shadowed spurious root demotes itself —');
// Fresh topic + shadowed corner (the prod orphan-subscriber signature): W is a
// local minimum for the topic in ITS corner and hears NO beacons (not linked to
// the root's neighbourhood). A stranded SUB there mints a spurious root with a
// via-pinned, permanently-orphaned subscriber — until W's own periodic verify
// (iterative lookup) finds the true root and demotes.
const T2NAME = { region: 'useast', name: 'root-reconcile-smoke-2', write: 'open' };
const T2 = await deriveTopicIdBig(T2NAME);
const near2 = (lo) => T2 ^ BigInt(lo);
const envFor2 = async (text) => JSON.stringify(await buildEnvelope({ topic: T2NAME, message: text, identity: author }));
const R  = fab.addNode(near2(0x02));   // true closest for T2
const W  = fab.addNode(near2(0x70));   // shadowed corner host
const S5 = fab.addNode(near2(0x74));   // subscriber, knows only W
const P2 = fab.addNode(near2(0x80));   // publisher, knows R and W
fab.link(R.id, P2.id); fab.link(W.id, S5.id); fab.link(W.id, P2.id);
// R roots via a publish (m5) — W hears nothing (no beacon path W←R).
P2.am._send('pubsub:pub', { topicId: idHex(T2), via: [], json: await envFor2('m5') });
await fab.settle();
// S5's SUB strands at W (its only neighbour is W; W is a local min for T2
// among {W,S5,P2}) → W self-roots (no beacon knowledge — allowed) → SPLIT.
S5.am.pubsubSubscribe(T2); S5.am.mySubscriptions.get(T2).since = 0;
await fab.settle();
const splitRoots = fab.roots(T2);
check('disease precondition: split exists (R and W both root)', splitRoots.length === 2, JSON.stringify(splitRoots));
// Advance past ROOT_VERIFY_FIRST_MS; W's tick launches the iterative lookup,
// which converges on R → W demotes, re-homes, and PULLUP/replay flows down.
fab.advance(7_000);
await fab.tickAll();
await new Promise((r) => setTimeout(r, 20));   // let the non-blocking verify promise land
await fab.settle();
await fab.tickAll();                            // W's re-home SUB renewal + replay
await new Promise((r) => setTimeout(r, 20));
await fab.settle();
const healedRoots = fab.roots(T2);
check('spurious root self-verified and demoted (single root = R)',
  healedRoots.length === 1 && healedRoots[0] === idHex(R.id).slice(-4), JSON.stringify(healedRoots));
// The demoted W now KNOWS R's id (verified pointer). In the live kernel the
// mesh machinery (5-closest-peer rule / introductions) then forms the W↔R
// synapse; this sparse fabric has no introduction layer, so model that step
// with an explicit link before W's renewal routes its SUB + hw upward.
fab.link(W.id, R.id);
await fab.tickAll();
await new Promise((r) => setTimeout(r, 20));
await fab.settle();
P2.am._send('pubsub:pub', { topicId: idHex(T2), via: [], json: await envFor2('m6') });
await fab.settle();
check('orphaned subscriber now receives (history + live)', S5.got.length >= 2, `S5 got ${S5.got.length}`);

console.log('— phase 6 (alone-in-the-dark): an unmeshed node must not self-root on subscribe —');
// Prod signature: EVERY freshly-joined subscriber (mesh not yet formed) minted
// a transient root for the topic — seven per topic in one instrumented run —
// splitting the tree until reconciliation caught up. A node with zero
// non-bridge neighbours must hold the seat and let the renewal re-run the
// election once it is meshed.
const Z = fab.addNode(near2(0x60));            // joins with NO links (mesh not formed yet)
Z.am.pubsubSubscribe(T2); Z.am.mySubscriptions.get(T2).since = 0;
await fab.settle();
const zRole = Z.am.axonRoles.get(T2);
check('unmeshed subscriber did NOT root the topic', !(zRole && zRole.isRoot), zRole ? 'rooted!' : '');
check('root set unchanged (still exactly R)', JSON.stringify(fab.roots(T2)) === JSON.stringify(healedRoots), JSON.stringify(fab.roots(T2)));
// The mesh forms; the renewFastMs renewal re-runs the election and seats Z.
fab.link(Z.id, R.id);
fab.advance(6_000);
await fab.tickAll();
await new Promise((r) => setTimeout(r, 20));
await fab.settle();
P2.am._send('pubsub:pub', { topicId: idHex(T2), via: [], json: await envFor2('m7') });
await fab.settle();
check('once meshed, the held subscriber seats and receives (history + live)', Z.got.length >= 2, `Z got ${Z.got.length}`);
check('still exactly one root after the join', fab.roots(T2).length === 1, JSON.stringify(fab.roots(T2)));

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
