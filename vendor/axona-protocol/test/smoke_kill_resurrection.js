// =====================================================================
// smoke_kill_resurrection.js — regression guard for the user-reported
// "kill, then reload, and the message comes back" bug, and its fix.
//
// Root cause (replica divergence): a topic is replicated across R root
// axons, but a kill is best-effort-pushed to the K-closest set. If even
// ONE root misses the kill (a dropped delivery, churn between publish- and
// kill-time K-closest, or a root that joined the set late) that root keeps
// the message AND has no tombstone. On a fresh reload the subscriber's
// in-memory tombstone set is empty, so the stale root's replay resurrects it.
//
// Fix (Phase A #2 kill convergence): after a root applies a creator-authorized
// kill it RE-GOSSIPS the signed kill to the current K-closest root set on every
// refreshTick (bounded to KILL_REGOSSIP_MS). A replica that missed the original
// kill re-runs the full verifier against the message it holds and removes +
// tombstones it. Plus a send-side tombstone backstop in _maybeSendReplay.
//
// This test reproduces the divergence, then proves reconciliation heals the
// stale root and a later reload no longer resurrects the message.
//
// Run: node test/smoke_kill_resurrection.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { buildKill }      from '../src/pubsub/kill.js';
import { toHex }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON    = { lat: 51.5074, lng: -0.1278 };
const TOPIC_HEX = '89' + 'ab'.repeat(32);          // 66-char hex topic id
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T         = 1_700_000_000_000;               // fixed test clock
const tick      = () => new Promise(r => setTimeout(r, 0));

// ── In-memory mesh of real AxonaManager instances, wired by sendDirect /
//    routeMessage / findKClosest over XOR distance (mirrors smoke_metrics_
//    kfanout). `dropKillTo`: a root that never receives `pubsub:kill-k`. ──
class MockNet {
  constructor() { this.mgrs = new Map(); this.dropKillTo = null; this.logs = []; }

  kclosest(topicId, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ topicId, db = b ^ topicId; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }

  makeDht(selfId) {
    const net = this;
    const routed = new Map(), direct = new Map();
    const dht = {
      getSelfId:       () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent:         () => () => {},
      findKClosest:    async (topicId, K) => net.kclosest(topicId, K),
      routeMessage:    async (topicId, type, payload) => {
        const target = net.kclosest(topicId, 1)[0];
        const h = net.mgrs.get(target)?._dht._routed.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
      },
      sendDirect:      async (target, type, payload) => {
        if (type === 'pubsub:kill-k' && net.dropKillTo != null && target === net.dropKillTo) return false; // missed delivery
        const m = net.mgrs.get(target);
        if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _routed: routed, _direct: direct,
    };
    return dht;
  }

  spawn(selfId, tag) {
    const dht = this.makeDht(selfId);
    const net = this;
    const mgr = new AxonaManager({
      dht, now: () => T,
      emitLog: (level, code, data) => net.logs.push({ node: tag, code, ...data }),
    });
    mgr._dht = dht;
    this.mgrs.set(selfId, mgr);
    return mgr;
  }
}

const hasMsg = (r) => r.axonRoles.get(TOPIC_BIG).replayCache.length === 1;

async function reloadSubscriberAndCount(net, roots, label) {
  // A brand-new subscriber with an empty tombstone set re-subscribes to every
  // root; each replays what it still holds (the reload path).
  const B = net.spawn(TOPIC_BIG ^ (1n << 201n), label);
  const got = [];
  B.onPubsubDelivery((_t, j) => { try { got.push(JSON.parse(j)); } catch {} });
  for (const r of roots) {
    await r._maybeSendReplay(TOPIC_BIG, r.axonRoles.get(TOPIC_BIG), B.nodeId, 0);
  }
  await tick();
  net.mgrs.delete(B.nodeId);
  return got;
}

async function main() {
  console.log('Axona kill-resurrection regression (replica divergence + convergence fix)');

  const alice = await createAuthorIdentity();
  const env   = await buildEnvelope({ topic: { region: 'useast', name: 'cats' }, message: 'hi', identity: alice, ts: T, seq: T });
  const json  = JSON.stringify(env);

  const net     = new MockNet();
  const rootIds = [1n, 3n, 5n, 9n, 17n].map(x => TOPIC_BIG ^ x);    // close to topic ⇒ the K-closest
  const roots   = rootIds.map((id, i) => net.spawn(id, `root${i}`));
  const A       = net.spawn(TOPIC_BIG ^ (1n << 200n), 'A');         // killer, far ⇒ never a root

  // ── Seed: a fully-replicated publish — every root holds the message. ──
  for (const r of roots) {
    r.axonRoles.set(TOPIC_BIG, {
      isRoot: true, isInRootSet: true, children: new Map(),
      replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
    });
  }
  check('1. all 5 roots initially hold the message', roots.every(hasMsg));

  // ── Kill from A, but DROP the kill to root index 2 (models a missed delivery). ──
  const STALE = 2;
  net.dropKillTo = rootIds[STALE];
  const kill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice });
  await A._asyncKill(TOPIC_BIG, kill);
  await tick(); await tick();

  const others    = roots.filter((_, i) => i !== STALE);
  const staleRoot = roots[STALE];
  check('2a. roots that got the kill removed + tombstoned the message',
    others.every(r => !hasMsg(r) && r._isTombstoned(env.msgId)));
  check('2b. the dropped root is STALE: still holds it, no tombstone',
    hasMsg(staleRoot) && staleRoot._isTombstoned(env.msgId) === false);

  // ── Without reconciliation, a reloaded subscriber resurrects it (the bug). ──
  net.logs.length = 0;
  const before = await reloadSubscriberAndCount(net, roots, 'B-before');
  const serveLog = net.logs.find(l => l.node === 'B-before' && l.code === 'replay-serve' && l.msgId === env.msgId);
  check('3. divergence is real: a reloaded subscriber resurrects the message',
    before.filter(d => !d.deleted && d.msgId === env.msgId).length === 1);
  check('3b. instrumentation names the stale root as the source',
    !!serveLog && serveLog.from === toHex(rootIds[STALE]));

  // ── FIX: a root that applied the kill re-gossips it (the refreshTick path). ──
  net.dropKillTo = null;                                   // the transient drop has cleared
  await roots[0]._syncKillsForTopic(TOPIC_BIG, roots[0].axonRoles.get(TOPIC_BIG));
  await tick(); await tick();
  check('4. reconciliation healed the stale root (message removed + tombstoned)',
    !hasMsg(staleRoot) && staleRoot._isTombstoned(env.msgId) === true);

  // ── Now a fresh reload sees NO resurrection. ──
  const after = await reloadSubscriberAndCount(net, roots, 'B-after');
  check('5. FIX VERIFIED: a reloaded subscriber no longer resurrects the message',
    after.filter(d => !d.deleted && d.msgId === env.msgId).length === 0);

  // ── Send-side backstop: even if a tombstoned root kept a stale cache entry,
  //    it must not replay it (defense in depth). ──
  const guinea = roots[0];                                 // tombstoned + we re-inject a stale copy
  guinea.axonRoles.get(TOPIC_BIG).replayCache.push({ json, publishId: 'zz', publishTs: T, postHash: env.msgId, publisher: null });
  const leak = await reloadSubscriberAndCount(net, [guinea], 'B-backstop');
  check('6. send-side backstop: a tombstoned root never replays the killed msg',
    leak.filter(d => !d.deleted && d.msgId === env.msgId).length === 0);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
