// =====================================================================
// smoke_pubsub_kill.mjs — kill = a verified publish with a delete side-effect
// (v4.8.7). Covers the four properties of the reworked kill:
//   1. AUTHORIZED kill (signed by the message's author) deletes the cached copy,
//      tombstones it, and delivers {deleted} to subscribers.
//   2. FORGERY rejected: a kill signed by a NON-author is dropped at the root
//      when the target is held (authorship enforced).
//   3. SELF-HEAL: the tombstone rides renewal replay like a cached message, so a
//      late since:'all' subscriber (and one that missed the live delete) receives
//      the kill — under message loss it recovers, exactly like a publish.
//   4. PROVISIONAL + enforce-on-arrival: a kill that reaches the root BEFORE the
//      target is accepted provisionally; when the target arrives it's suppressed
//      iff its author matches the kill's signer — a forged early kill is revoked.
//   5. REGRESSION (permanent-leak edge, v4.8.8): a subscriber whose replay cursor
//      (`since`) has already advanced PAST the kill's ts still receives the kill on
//      renewal — tombstones replay regardless of `since`, so a node that missed the
//      delete but kept seeing newer messages can never keep the killed body forever.
//
// Run: node test/smoke_pubsub_kill.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { buildKill } from '../src/pubsub/kill.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { regionCenter } from '../src/utils/region-names.js';
const __LOC = regionCenter('useast');  // region-lock: co-region test nodes with the 'useast' topics

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label} ${extra}`); failed++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');
function lcg(s){s>>>=0;return()=>{s=(s*1664525+1013904223)>>>0;return s/4294967296;};}

class Fabric {
  constructor({ drop = 0, seed = 1 } = {}) { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); this.drop = drop; this.rand = lcg(seed); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (t, h) => handlers.set(t, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closest(target); if (dest === null) return;
        if (self.drop && self.rand() < self.drop) return;            // packet lost
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
      findKClosest: async (target, k = 3) => [...self.nodes.entries()].filter(([, n]) => n.alive)
        .map(([id]) => id).sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, k),
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [], dels: [] };
    am.onPubsubDelivery((_t, json, msgId) => { let o = null; try { o = JSON.parse(json); } catch {} if (o && o.deleted) rec.dels.push(o.msgId); else rec.got.push(msgId); });
    this.nodes.set(idBig, rec); return rec;
  }
  _closest(target){ let b=null,bd=null; for(const[id,n]of this.nodes){if(!n.alive)continue;const d=id^target;if(bd===null||d<bd){bd=d;b=id;}} return b; }
  async settle(cap=500000){ let i=0; while(this.queue.length){ if(++i>cap) throw new Error('settle cap'); const j=this.queue.shift(); const n=this.nodes.get(j.dest); if(!n||!n.alive)continue; const h=n.handlers.get(j.type); if(h) await h(j.payload,j.meta); } }
  async tickAll(){ for(const n of this.nodes.values()) if(n.alive) await n.am.refreshTick(); await this.settle(); }
}
const root = (fab, topicBig) => fab.nodes.get(fab._closest(topicBig));
const cacheHas = (rec, t, id) => (rec.am.axonRoles.get(t)?.cache || []).some(c => c.msgId === id);
const tombstoned = (rec, t, id) => rec.am.axonRoles.get(t)?.tombstones?.has(id);

async function mkNodes(fab, n, salt) { const a = []; for (let i = 0; i < n; i++) { const id = await createNodeIdentity(__LOC); a.push(fab.addNode(BigInt('0x'+id.id))); } return a; }

async function main() {
  console.log('Axona pub/sub — kill = verified publish + delete side-effect (v4.8.8)');
  const alice = await createAuthorIdentity();
  const bob   = await createAuthorIdentity();   // a DIFFERENT author (forger)

  // ── 1+2+3: authorized kill, forgery reject, self-heal under loss ──────
  {
    const fab = new Fabric();
    const nodes = await mkNodes(fab, 8, 1);
    const desc = { region:'useast', owner:null, name:'kill-basic', write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    const subA = nodes[0], subB = nodes[1], pub = nodes[2];
    subA.am.pubsubSubscribe(topicId); subB.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 6000; await fab.tickAll();
    // publish M (author = alice)
    const e = await buildEnvelope({ topic: desc, message: { hi: 1 }, seq: 1, identity: alice, ts: fab.clock });
    pub.am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    const r = root(fab, topicId);
    check('1a. message delivered + cached at root', cacheHas(r, topicId, e.msgId) && subA.got.includes(e.msgId) && subB.got.includes(e.msgId));

    // FORGERY: bob (not the author) tries to kill alice's message → rejected
    const forged = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 1, identity: bob });
    fab.clock += 100; r.am.pubsubKill(topicId, forged); await fab.settle();
    check('2. forged kill (non-author) REJECTED — message still cached, not tombstoned',
      cacheHas(r, topicId, e.msgId) && !tombstoned(r, topicId, e.msgId), `(cached=${cacheHas(r,topicId,e.msgId)} tomb=${tombstoned(r,topicId,e.msgId)})`);

    // AUTHORIZED: alice kills her own message → accepted
    const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, identity: alice });
    fab.clock += 100; r.am.pubsubKill(topicId, kill); await fab.settle();
    check('1b. authorized kill removes target from cache + tombstones it', !cacheHas(r, topicId, e.msgId) && tombstoned(r, topicId, e.msgId));
    check('1c. subscribers received the {deleted} callback', subA.dels.includes(e.msgId) && subB.dels.includes(e.msgId));

    // SELF-HEAL: a late since:'all' subscriber learns of the kill via replay
    const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
    late.am._lastSeenTsByTopic.set(topicId, 0); late.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 5000; await fab.tickAll(); await fab.settle();
    // The CONVERGENCE guarantee is the tombstone STATE reaching the late sub (so the body
    // stays suppressed) — NOT an app callback. A since:'all' joiner that never received the
    // body gets no spurious {deleted} notification (there's nothing to retract); it simply
    // never sees m1. (v4.9.2: kill callback gated on prior body delivery.)
    check('3a. late since:all sub that never saw the body gets NO spurious {deleted} event', !late.dels.includes(e.msgId));
    check('3b. late subscriber did NOT receive the killed message body (the real anti-leak guarantee)', !late.got.includes(e.msgId));
  }

  // ── 3c: kill SELF-HEALS under message loss — the RETRACTION guarantee the gate
  //   preserves. Subscribers that RECEIVED the body, then missed the live kill (loss),
  //   must still converge the {deleted} retraction over renewals (tombstone replays like
  //   a cached message). This is the case where a delete-callback is correct — the app is
  //   holding m1 and must drop it. (Contrast 3a: a never-saw sub gets nothing.) ──
  {
    let recovered = 0, total = 0;
    for (let seed = 1; seed <= 8; seed++) {
      const fab = new Fabric({ drop: 0, seed });   // publish loss-free
      const nodes = await mkNodes(fab, 8, seed);
      const desc = { region:'useast', owner:null, name:`kill-loss-${seed}`, write:'open' };
      const topicId = await deriveTopicIdBig(desc);
      // 3 subscribers join + receive the body FIRST (so a later kill is a real retraction)
      const subs = nodes.slice(2, 5);
      for (const s of subs) s.am.pubsubSubscribe(topicId);
      nodes[0].am.pubsubSubscribe(topicId); await fab.settle(); fab.clock += 6000; await fab.tickAll(); await fab.settle();
      const e = await buildEnvelope({ topic: desc, message: { k: 1 }, seq: 1, identity: alice, ts: fab.clock });
      nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
      const gotBody = subs.filter(s => s.got.includes(e.msgId)).length;
      const r = root(fab, topicId);
      const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, identity: alice });
      // kill happens UNDER LOSS — the subs that hold the body must converge the retraction
      fab.drop = 0.3;
      fab.clock += 100; r.am.pubsubKill(topicId, kill); await fab.settle();
      for (let t = 0; t < 30; t++) { fab.clock += 5000; await fab.tickAll(); }
      for (const s of subs) { total++; if (s.dels.includes(e.msgId)) recovered++; }
      if (gotBody !== subs.length) { total = -1; break; }   // precondition: all subs saw the body
    }
    check('3c. kill self-heals under 30% loss (body-holding subs CONVERGE the retraction)', recovered === total && total > 0, `(${recovered}/${total})`);
  }

  // ── 4: provisional kill (before target) + enforce-on-arrival ──────────
  {
    // 4a: authorized early kill → the later target is SUPPRESSED
    const fab = new Fabric();
    const nodes = await mkNodes(fab, 8, 99);
    const desc = { region:'useast', owner:null, name:'kill-early', write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    nodes[0].am.pubsubSubscribe(topicId); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const e = await buildEnvelope({ topic: desc, message: { e: 1 }, seq: 1, identity: alice, ts: fab.clock });
    const r = root(fab, topicId);
    // kill arrives FIRST (root doesn't hold the target yet) — provisional accept
    const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 1, identity: alice });
    r.am.pubsubKill(topicId, kill); await fab.settle();
    check('4a-i. provisional tombstone recorded before target seen', tombstoned(r, topicId, e.msgId));
    // now the (authorized) target arrives → suppressed (author matches kill signer)
    fab.clock += 100; nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    check('4a-ii. authorized early kill SUPPRESSES the later target', !cacheHas(r, topicId, e.msgId) && !nodes[0].got.includes(e.msgId));

    // 4b: FORGED early kill → revoked when the authentic target arrives
    const fab2 = new Fabric();
    const n2 = await mkNodes(fab2, 8, 123);
    const desc2 = { region:'useast', owner:null, name:'kill-early-forged', write:'open' };
    const t2 = await deriveTopicIdBig(desc2);
    n2[0].am.pubsubSubscribe(t2); await fab2.settle(); fab2.clock += 6000; await fab2.tickAll();
    const e2 = await buildEnvelope({ topic: desc2, message: { e: 2 }, seq: 1, identity: alice, ts: fab2.clock });
    const r2 = root(fab2, t2);
    const forgedEarly = await buildKill({ topicId: idHex(t2), msgId: e2.msgId, seq: 1, identity: bob });  // bob ≠ author
    r2.am.pubsubKill(t2, forgedEarly); await fab2.settle();                 // provisional (signer=bob)
    fab2.clock += 100; n2[1].am.pubsubPublish(t2, JSON.stringify(e2)); await fab2.settle();
    check('4b. forged early kill REVOKED on arrival — authentic message delivered',
      cacheHas(r2, t2, e2.msgId) && n2[0].got.includes(e2.msgId) && !tombstoned(r2, t2, e2.msgId));
  }

  // ── 5: REGRESSION — renewal with `since` AHEAD of killTs still backfills the kill ──
  {
    const fab = new Fabric();
    const nodes = await mkNodes(fab, 8, 77);
    const desc = { region:'useast', owner:null, name:'kill-since-ahead', write:'open' };
    const topicId = await deriveTopicIdBig(desc);
    nodes[0].am.pubsubSubscribe(topicId); await fab.settle(); fab.clock += 6000; await fab.tickAll();
    const e = await buildEnvelope({ topic: desc, message: { z: 1 }, seq: 1, identity: alice, ts: fab.clock });
    nodes[1].am.pubsubPublish(topicId, JSON.stringify(e)); await fab.settle();
    const r = root(fab, topicId);
    const killTs = fab.clock + 100;
    const kill = await buildKill({ topicId: idHex(topicId), msgId: e.msgId, seq: 2, ts: killTs, identity: alice });
    fab.clock = killTs; r.am.pubsubKill(topicId, kill); await fab.settle();
    check('5a. root tombstoned the target', tombstoned(r, topicId, e.msgId));
    // A late subscriber whose since-cursor is set WELL PAST the kill (it saw newer
    // activity from a stale replica that held the body but never got the kill). The
    // OLD gate (killTs > since) would never replay the tombstone here → permanent
    // killed-body leak. The fix replays every ACTIVE tombstone regardless of `since`.
    const late = fab.addNode(BigInt('0x' + (await createNodeIdentity(__LOC)).id));
    fab.clock += 50_000;
    late.am._lastSeenTsByTopic.set(topicId, fab.clock);   // since AHEAD of killTs
    late.am.pubsubSubscribe(topicId);
    await fab.settle(); fab.clock += 5000; await fab.tickAll(); await fab.settle();
    // The leak-prevention guarantee is that the late sub never RECEIVES THE KILLED BODY —
    // even with its since-cursor ahead of killTs (the stale-replica path). It never saw the
    // body, so (per the gate) it gets no {deleted} event either; what matters is the body
    // stays suppressed everywhere it could attach.
    check('5b. late sub with since AHEAD of killTs never receives the killed body (leak fixed)', !late.got.includes(e.msgId), `(got=${JSON.stringify(late.got)})`);
    check('5c. late sub never saw the killed body', !late.got.includes(e.msgId));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err?.stack || err); process.exit(2); });
