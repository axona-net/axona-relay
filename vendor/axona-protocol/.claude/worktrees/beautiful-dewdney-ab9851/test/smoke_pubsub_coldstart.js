// =====================================================================
// smoke_pubsub_coldstart.js — cold-start anti-entropy drain.
//
// A freshly (re)started or newly-recruited keyspace host holds an EMPTY replay
// cache for EVERY topic it roots. Steady-state anti-entropy reconciles only
// MSGSYNC_TOPICS_PER_TICK (8) topics per refresh tick, so a host with many
// topics would serve empty replays for minutes after a restart (the
// "reload → 0 / partial" window). The cold-start drain reconciles never-yet-
// synced ("cold") roles with a large per-tick budget, ahead of the steady
// round-robin, so the host converges in a tick or two.
//
//   1. ONE refreshTick backfills ALL cold roles (here 20 > the 8/tick steady cap).
//   2. reconciled roles are marked synced (not re-drained as cold next tick).
//   3. a siblingless cold role stays cold (nothing to pull from) and retries.
//
// Run: node test/smoke_pubsub_coldstart.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { toHex }          from '../src/utils/hexid.js';

// v0.3: an envelope's topic is the structured DESCRIPTOR object. The root binds
// the SIGNED descriptor to the routed topic id during anti-entropy, so each
// topic's routed id must be the one its descriptor resolves to.
const TOPIC_DESC = (i) => ({ region: 0x89, owner: null, name: `chan-${i}`, write: 'open' });

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const BASE   = BigInt('0x' + '89' + 'cd'.repeat(32));   // topic-id base; per-topic = BASE ^ i
const T      = 1_700_000_000_000;
const flush  = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setTimeout(r, 0)); };

class MockNet {
  constructor() { this.mgrs = new Map(); }
  kclosest(topicId, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ topicId, db = b ^ topicId; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }
  makeDht(selfId) {
    const net = this, routed = new Map(), direct = new Map();
    const dht = {
      getSelfId: () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent: () => () => {},
      findKClosest: async (topicId, K) => net.kclosest(topicId, K),
      routeMessage: async () => {},
      sendDirect: async (target, type, payload) => {
        const m = net.mgrs.get(target); if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _routed: routed, _direct: direct,
    };
    return dht;
  }
  spawn(selfId) {
    const dht = this.makeDht(selfId);
    const mgr = new AxonaManager({ dht, now: () => T });
    mgr._dht = dht; this.mgrs.set(selfId, mgr); return mgr;
  }
}

const role = (over = {}) => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set(), emptiedAt: 0, ...over });
const cacheHas = (mgr, topicBig, postHash) => (mgr.axonRoles.get(topicBig)?.replayCache || []).some((e) => e.postHash === postHash);

async function main() {
  console.log('Axona pub/sub cold-start anti-entropy drain');
  const alice = await createAuthorIdentity();
  const N = 20;   // > MSGSYNC_TOPICS_PER_TICK (8): steady round-robin alone could not do these in one tick

  // ── 1+2. holder r1 has N topics cached; cold host r2 drains them all in ONE tick ──
  {
    const net = new MockNet();
    const r1 = net.spawn(BASE ^ 1n);   // holder (warm)
    const r2 = net.spawn(BASE ^ 3n);   // freshly restarted host — N cold empty roles
    const msgs = [];
    for (let i = 0; i < N; i++) {
      // v0.3: the routed topic id is the one the SIGNED descriptor resolves to.
      const topicBig = BigInt('0x' + await deriveTopicId(TOPIC_DESC(i)));
      const env  = await buildEnvelope({ topic: TOPIC_DESC(i), message: `m${i}`, identity: alice, ts: T, seq: T + i });
      const json = JSON.stringify(env);
      msgs.push({ topicBig, postHash: env.msgId });
      r1.axonRoles.set(topicBig, role({ replayCache: [{ json, publishId: `p${i}`, publishTs: T, postHash: env.msgId, publisher: null }], synced: true }));
      r2.axonRoles.set(topicBig, role());   // cold: empty cache, synced undefined
    }
    const coldBefore = [...r2.axonRoles.values()].filter((r) => !r.synced).length;
    const haveBefore = msgs.filter((m) => cacheHas(r2, m.topicBig, m.postHash)).length;
    check(`1a. precondition: ${N} cold roles on r2, 0 cached`, coldBefore === N && haveBefore === 0);

    await r2.refreshTick();   // single tick
    await flush();

    const haveAfter = msgs.filter((m) => cacheHas(r2, m.topicBig, m.postHash)).length;
    check(`1b. ONE refreshTick backfilled ALL ${N} cold roles (> 8/tick steady cap)`, haveAfter === N);
    const synced = [...r2.axonRoles.values()].filter((r) => r.synced).length;
    check(`2. reconciled roles marked synced (${synced}/${N})`, synced === N);
  }

  // ── 3. siblingless cold role stays cold (nothing to pull) and retries next tick ──
  {
    const net = new MockNet();
    const solo = net.spawn(BASE ^ 7n);          // the ONLY node → no siblings for any topic
    const topicBig = BASE ^ 0x99n;
    solo.axonRoles.set(topicBig, role());        // cold
    await solo.refreshTick();
    await flush();
    check('3. siblingless cold role stays cold (no sibling to reconcile against)',
      solo.axonRoles.get(topicBig)?.synced !== true);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
