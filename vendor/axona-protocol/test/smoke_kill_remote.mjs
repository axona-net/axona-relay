// =====================================================================
// smoke_kill_remote.mjs — kill() delivers deleted:true to a REMOTE subscriber.
//
// Regression for the "kill silently removes the message but never invokes the
// subscriber's handler with deleted:true" bug. The earlier smoke_kill only
// exercised a LOCAL subscriber child (self), whose delete is delivered directly
// (no postHash → dedup on the kill id). A REMOTE child receives the delete via
// pubsub:deliver carrying postHash = msgId; _deliverToApp then deduped it against
// the ORIGINAL message's delivery (same `${topicId}:${msgId}` key) and dropped
// it. So a normal subscriber (on a different node than the root) saw the message
// vanish from replays but never got the deleted:true callback.
//
//   1. a remote subscriber that already received the message gets deleted:true
//      when the root applies a creator-authorized kill
//   2. the root removes + tombstones the content
//   3. the delete is NOT deduped against the original delivery
//
// Run: node test/smoke_kill_remote.mjs
// =====================================================================

import { AxonaManager }   from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope }  from '../src/pubsub/envelope.js';
import { buildKill }      from '../src/pubsub/kill.js';
import { toHex }          from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

const TOPIC_HEX = '89' + 'ab'.repeat(32);
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const T = 1_700_000_000_000;
const flush = async () => { for (let i = 0; i < 8; i++) await new Promise(r => setTimeout(r, 0)); };

// MockNet: sendDirect(target, type, payload) → target manager's registered
// direct handler, with the sender's id as meta.fromId.
class MockNet {
  constructor() { this.mgrs = new Map(); }
  makeDht(selfId) {
    const net = this, direct = new Map();
    return {
      getSelfId: () => selfId,
      onRoutedMessage: () => {}, onDirectMessage: (t, h) => direct.set(t, h),
      onEvent: () => () => {}, routeMessage: async () => {}, findKClosest: async () => [],
      sendDirect: async (target, type, payload) => {
        const m = net.mgrs.get(target); if (!m) return false;
        const h = m._dht._direct.get(type);
        if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _direct: direct,
    };
  }
  spawn(selfId) {
    const dht = this.makeDht(selfId);
    const mgr = new AxonaManager({ dht, now: () => T });
    mgr._dht = dht; mgr.nodeId = selfId; this.mgrs.set(selfId, mgr);
    return mgr;
  }
}

async function main() {
  console.log('Axona kill() → remote subscriber deleted:true smoke');
  const net = new MockNet();
  const R = net.spawn(0x01n);          // root hosting the topic
  const S = net.spawn(0x02n);          // remote subscriber, different node than the root

  const alice = await createAuthorIdentity();
  const env   = await buildEnvelope({ topic: { region: 'useast', name: 'cats' }, message: 'hi', identity: alice, ts: T, seq: T });
  const json  = JSON.stringify(env);

  // S records every app delivery (message envelopes and delete markers).
  const sGot = [];
  S.onPubsubDelivery((_topicId, j) => { try { sGot.push(JSON.parse(j)); } catch {} });

  // 1. S already RECEIVED + delivered the original message (sets its dedup entry
  //    keyed on `${topicId}:${msgId}` — the collision that hid the tombstone).
  await S._onDeliver({ topicId: TOPIC_HEX, json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null },
                     { fromId: toHex(0x01n) });
  await flush();
  check('precondition: S received the original message',
    sGot.some(d => d.msgId === env.msgId && d.deleted !== true));

  // 2. R hosts the topic with S as a REMOTE subscriber child + the message cached.
  R.axonRoles.set(TOPIC_BIG, {
    isRoot: true,
    children: new Map([[S.nodeId, {}]]),
    replayCache: [{ json, publishId: 'p1', publishTs: T, postHash: env.msgId, publisher: null }],
  });

  // 3. Creator applies the kill at the root → fans the delete to S.
  const kill = await buildKill({ topicId: TOPIC_HEX, msgId: env.msgId, ts: T, seq: T, identity: alice });
  const verdict = await R._handleKill(TOPIC_BIG, kill);
  await flush();

  check('kill consumed at root', verdict === 'consumed');
  check('root removed the message from its cache', R.axonRoles.get(TOPIC_BIG).replayCache.length === 0);
  check('root tombstoned the msgId', R._isTombstoned(env.msgId) === true);

  // THE FIX: the remote subscriber's handler is invoked with deleted:true.
  const del = sGot.find(d => d.deleted === true && d.msgId === env.msgId);
  check('remote subscriber received deleted:true (not silently dropped)', !!del);
  check('S tombstoned/anti-resurrects (delete not deduped vs original)',
    sGot.filter(d => d.deleted === true).length === 1);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
