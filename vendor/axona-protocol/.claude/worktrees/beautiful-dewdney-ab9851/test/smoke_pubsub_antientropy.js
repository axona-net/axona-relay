// =====================================================================
// smoke_pubsub_antientropy.js — root-to-root message anti-entropy (Fix 2).
//
// A publish lands only on the PUBLISHER's K-closest root set, which need not be
// a subscriber's — so a subscriber attached to a different root never sees it.
// Anti-entropy converges the root set: roots exchange digests of held postHashes
// and pull what they're missing (re-verified — a sibling root isn't trusted).
//
//   1. divergence heals: a root missing a message pulls it from a sibling.
//   2. steady-state: when both hold it, nothing is transferred.
//   3. SECURITY: a forged-signature message offered by a sibling is rejected.
//   4. SECURITY: a poisoned postHash (content≠id) is rejected.
//   5. a tombstoned message is not resurrected via sync.
//
// Run: node test/smoke_pubsub_antientropy.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { deriveTopicId }  from '../src/pubsub/post.js';
import { buildKill }      from '../src/pubsub/kill.js';
import { toHex }          from '../src/utils/hexid.js';

// v0.3: an envelope's topic is the structured DESCRIPTOR object. The root binds
// the SIGNED descriptor to the routed topic id, so the topic id under test must
// be the one this descriptor resolves to.
const TOPIC_DESC = { region: 0x89, owner: null, name: 'cats', write: 'open' };

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON    = { lat: 51.5074, lng: -0.1278 };
// Derived from TOPIC_DESC at the top of main() (resolveTopic is async).
let TOPIC_HEX;
let TOPIC_BIG;
const T         = 1_700_000_000_000;
const tick      = () => new Promise((r) => setTimeout(r, 0));
const flush     = async () => { for (let i = 0; i < 6; i++) await tick(); };

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
const emptyRole = () => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set() });
const cacheHas = (mgr, postHash) => (mgr.axonRoles.get(TOPIC_BIG)?.replayCache || []).some((e) => e.postHash === postHash);

async function main() {
  console.log('Axona pub/sub anti-entropy (Fix 2)');
  TOPIC_HEX = await deriveTopicId(TOPIC_DESC);
  TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
  const alice = await createAuthorIdentity();
  const env   = await buildEnvelope({ topic: TOPIC_DESC, message: 'hi-anti-entropy', identity: alice, ts: T, seq: T });
  const json  = JSON.stringify(env);
  const msg   = { json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null };

  // ── 1. divergence heals ──
  {
    const net = new MockNet();
    const r1 = net.spawn(TOPIC_BIG ^ 1n);   // holds the message
    const r2 = net.spawn(TOPIC_BIG ^ 3n);   // missing it
    r1.axonRoles.set(TOPIC_BIG, { ...emptyRole(), replayCache: [{ ...msg }] });
    r2.axonRoles.set(TOPIC_BIG, emptyRole());
    check('1a. precondition: r1 has it, r2 does not', cacheHas(r1, env.msgId) && !cacheHas(r2, env.msgId));
    await r2._antiEntropyTopic(TOPIC_BIG, r2.axonRoles.get(TOPIC_BIG));
    await flush();
    check('1b. HEALED: r2 pulled + verified + cached the message', cacheHas(r2, env.msgId));
  }

  // ── 2. steady-state: nothing transferred ──
  {
    const net = new MockNet();
    const r1 = net.spawn(TOPIC_BIG ^ 1n);
    const r2 = net.spawn(TOPIC_BIG ^ 3n);
    r1.axonRoles.set(TOPIC_BIG, { ...emptyRole(), replayCache: [{ ...msg }] });
    r2.axonRoles.set(TOPIC_BIG, { ...emptyRole(), replayCache: [{ ...msg }] });
    const before = r2.axonRoles.get(TOPIC_BIG).replayCache.length;
    await r2._antiEntropyTopic(TOPIC_BIG, r2.axonRoles.get(TOPIC_BIG));
    await flush();
    check('2. up-to-date root pulls nothing (no duplicate)',
      r2.axonRoles.get(TOPIC_BIG).replayCache.length === before);
  }

  // ── 3. SECURITY: forged-signature message from a sibling is rejected ──
  {
    const net = new MockNet();
    const r2 = net.spawn(TOPIC_BIG ^ 3n);
    r2.axonRoles.set(TOPIC_BIG, emptyRole());
    const forged = JSON.parse(json); forged.message = 'TAMPERED';        // breaks the signature
    const bad = { json: JSON.stringify(forged), publishId: 'pX', publishTs: T, postHash: env.msgId, publisher: null };
    const ok = await r2._ingestSyncedMessage(TOPIC_BIG, r2.axonRoles.get(TOPIC_BIG), bad);
    check('3. forged-signature message rejected (not cached)', ok === false && !cacheHas(r2, env.msgId));
  }

  // ── 4. SECURITY: poisoned postHash (content ≠ claimed id) is rejected ──
  {
    const net = new MockNet();
    const r2 = net.spawn(TOPIC_BIG ^ 3n);
    r2.axonRoles.set(TOPIC_BIG, emptyRole());
    const poisoned = { json, publishId: 'pY', publishTs: T, postHash: 'f'.repeat(64), publisher: null };  // wrong id
    const ok = await r2._ingestSyncedMessage(TOPIC_BIG, r2.axonRoles.get(TOPIC_BIG), poisoned);
    check('4. poisoned postHash rejected', ok === false && !cacheHas(r2, 'f'.repeat(64)));
  }

  // ── 5. a tombstoned (killed) message isn't resurrected via sync ──
  {
    const net = new MockNet();
    const r1 = net.spawn(TOPIC_BIG ^ 1n);
    const r2 = net.spawn(TOPIC_BIG ^ 3n);
    r1.axonRoles.set(TOPIC_BIG, { ...emptyRole(), replayCache: [{ ...msg }] });
    r2.axonRoles.set(TOPIC_BIG, { ...emptyRole(), replayCache: [{ ...msg }] });   // r2 held it…
    // …then killed it: _handleKill removes it from r2's cache + records the tombstone.
    await r2._handleKill(TOPIC_BIG, await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice }));
    check('5a. precondition: r2 tombstoned it (cache empty)', !cacheHas(r2, env.msgId) && r2._isTombstoned(env.msgId));
    await r2._antiEntropyTopic(TOPIC_BIG, r2.axonRoles.get(TOPIC_BIG));
    await flush();
    check('5. tombstoned message not resurrected by anti-entropy', !cacheHas(r2, env.msgId));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
