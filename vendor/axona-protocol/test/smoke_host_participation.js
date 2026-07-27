// =====================================================================
// smoke_host_participation.js — the decoupled host() primitive.
//
// A relay/infrastructure node should be able to STORE + SERVE a topic for
// other peers WITHOUT subscribing as a consumer. `pubsubHost` announces the
// node into a topic's K-closest set on the same `pubsub:subscribe-k` heartbeat
// a subscriber uses — so it gets recruited as a root and can serve replays —
// but it is NOT added to mySubscriptions and registers no app delivery.
//
//   1. host announces: pubsubHost(T) emits subscribe-k (subscriberId = self).
//   2. DECOUPLED: hosting does NOT add T to mySubscriptions, sets no callback.
//   3. introspection: inspectHosting() reflects the hosted topic.
//   4. refresh heartbeat: refreshTick re-announces hosted topics.
//   5. unhost: pubsubUnhost(T) emits unsubscribe-k, stops the heartbeat.
//   6. keyspace host: pubsubHostKeyspace(true) announces toward self's
//      neighborhood (topicId = self); off → silent.
//   7. store + serve: once recruited (has a role), a host caches an arriving
//      publish (so it can serve replays) — proving participation works.
//   8. wire-compat: a host's subscribe-k is indistinguishable from a real
//      subscriber's, so an unmodified root recruits it with no flag day.
//
// Run: node test/smoke_host_participation.js
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { toHex }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON    = { lat: 51.5074, lng: -0.1278 };
const TOPIC_HEX = '89' + 'ab'.repeat(32);
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T         = 1_700_000_000_000;
const tick      = () => new Promise((r) => setTimeout(r, 0));
const flush     = async () => { for (let i = 0; i < 8; i++) await tick(); };

// MockNet with K-closest over the live mgr set + a shared send log.
class MockNet {
  constructor() { this.mgrs = new Map(); this.sends = []; }
  kclosest(target, K) {
    return [...this.mgrs.keys()]
      .sort((a, b) => { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; })
      .slice(0, K);
  }
  makeDht(selfId) {
    const net = this, routed = new Map(), direct = new Map();
    const dht = {
      getSelfId: () => selfId,
      onRoutedMessage: (t, h) => routed.set(t, h),
      onDirectMessage: (t, h) => direct.set(t, h),
      onEvent: () => () => {},
      findKClosest: async (target, K) => net.kclosest(target, K),
      routeMessage: async (target, type, payload) => { net.sends.push({ from: toHex(selfId), target: 'routed', type, payload }); },
      sendDirect: async (target, type, payload) => {
        net.sends.push({ from: toHex(selfId), target: toHex(target), type, payload });
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
  sendsOf(type, from) { return this.sends.filter(s => s.type === type && (!from || s.from === from)); }
  clear() { this.sends = []; }
}

const emptyRole = () => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set(), roleCreatedAt: T, emptiedAt: 0 });
const cacheHas  = (mgr, postHash) => (mgr.axonRoles.get(TOPIC_BIG)?.replayCache || []).some((e) => e.postHash === postHash);

async function main() {
  console.log('Axona decoupled host() primitive');
  const alice = await createAuthorIdentity();
  const env   = await buildEnvelope({ topic: { region: 'useast', name: 'cats' }, message: 'served-by-a-host', identity: alice, ts: T, seq: T });
  const json  = JSON.stringify(env);

  // host H sits near the topic so it's a plausible K-closest root; a couple of
  // neighbors give findKClosest(self) something to return for keyspace mode.
  const net  = new MockNet();
  const H     = net.spawn(TOPIC_BIG ^ 0x10n);
  const selfHex = toHex(TOPIC_BIG ^ 0x10n);
  net.spawn(TOPIC_BIG ^ 0x11n);          // neighbor
  net.spawn(TOPIC_BIG ^ 0x14n);          // neighbor

  // ── 1. host announces via subscribe-k ──
  net.clear();
  H.pubsubHost(TOPIC_BIG);
  await flush();
  const ann = net.sendsOf('pubsub:subscribe-k', selfHex);
  check('1. pubsubHost emits subscribe-k', ann.length > 0);
  check('1b. announce carries subscriberId = self, topicId = T',
    ann.every(s => s.payload.subscriberId === selfHex) && ann.some(s => s.payload.topicId === TOPIC_HEX));

  // ── 2. DECOUPLED from subscribe ──
  check('2a. host did NOT add T to mySubscriptions', H.mySubscriptions.has(TOPIC_BIG) === false);
  check('2b. host registered no app delivery callback', H._deliveryCallback === null);

  // ── 3. introspection ──
  const hi = H.inspectHosting();
  check('3. inspectHosting() lists the hosted topic', hi.topics.includes(TOPIC_HEX) && hi.keyspace === false);

  // ── 4. refresh re-announces the hosted topic ──
  net.clear();
  await H.refreshTick();
  await flush();
  check('4. refreshTick re-announces hosted topic',
    net.sendsOf('pubsub:subscribe-k', selfHex).some(s => s.payload.topicId === TOPIC_HEX));

  // ── 5. unhost stops it ──
  net.clear();
  H.pubsubUnhost(TOPIC_BIG);
  await flush();
  check('5a. pubsubUnhost emits unsubscribe-k', net.sendsOf('pubsub:unsubscribe-k', selfHex).length > 0);
  check('5b. hosted topic dropped from inspectHosting', !H.inspectHosting().topics.includes(TOPIC_HEX));
  net.clear();
  await H.refreshTick();
  await flush();
  check('5c. refreshTick no longer announces the unhosted topic',
    !net.sendsOf('pubsub:subscribe-k', selfHex).some(s => s.payload.topicId === TOPIC_HEX));

  // ── 6. keyspace host announces toward self's neighborhood ──
  net.clear();
  H.pubsubHostKeyspace(true);
  await flush();
  const ks = net.sendsOf('pubsub:subscribe-k', selfHex);
  check('6a. keyspace host announces (topicId = self id) to neighbors',
    ks.length > 0 && ks.every(s => s.payload.topicId === selfHex && s.payload.subscriberId === selfHex));
  check('6b. inspectHosting().keyspace is true', H.inspectHosting().keyspace === true);
  H.pubsubHostKeyspace(false);
  net.clear();
  await H.refreshTick();
  await flush();
  check('6c. keyspace off → refreshTick makes no self-announce',
    net.sendsOf('pubsub:subscribe-k', selfHex).every(s => s.payload.topicId !== selfHex));

  // ── 7. store + serve once recruited: a host with a role caches a publish ──
  H.pubsubHost(TOPIC_BIG);
  H.axonRoles.set(TOPIC_BIG, emptyRole());          // recruited (announce → K-closest → role)
  await H._onDeliver(
    { topicId: TOPIC_HEX, json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null },
    { fromId: toHex(TOPIC_BIG ^ 1n) });
  await flush();
  check('7a. recruited host caches the publish (can serve replays)', cacheHas(H, env.msgId));
  check('7b. still no consumer: mySubscriptions empty, no callback',
    H.mySubscriptions.size === 0 && H._deliveryCallback === null);

  // ── 8. wire-compat: the host's announce IS a normal subscribe-k ──
  check('8. announce type is the standard pubsub:subscribe-k (no new wire msg)',
    net.sends.concat(ann).every(s => s.type !== 'pubsub:host' && s.type !== 'pubsub:host-advert'));

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
