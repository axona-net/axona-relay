// =====================================================================
// smoke_resubscribe.js — re-subscribe with since:'all' re-delivers.
//
// Regression for the "unsubscribe a hashtag, re-subscribe, it never
// reappears" bug (and the related "missed alert until reload"). Three
// per-topic structures survived an unsub and each silently suppressed the
// redelivery a since:'all' resubscribe is supposed to produce:
//   - _haveByTopic       (gap-safe digest → roots replay NOTHING)
//   - _lastSeenTsByTopic (legacy replay floor)
//   - _appDelivered      (exactly-once app gate → replay dropped pre-handler)
// The fix: pubsubUnsubscribe() (and since:'all' via _applySince) call
// pubsubResetTopicConsumption(topicId), which clears all three for the topic
// without touching the node's root/host role.
//
// Run: node test/smoke_resubscribe.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);
const big = (h) => BigInt('0x' + h);
const OTHER = hex(0x02);
const TOPIC = hex(0x05);
const TOPIC2 = hex(0x06);

function makeManager() {
  const dht = {
    getSelfId:       () => big(hex(0x01)),
    onRoutedMessage: () => {},
    onDirectMessage: () => {},
    routeMessage:    () => {},
    sendDirect:      async () => true,
    findKClosest:    undefined,
  };
  const am = new AxonaManager({ dht });
  const delivered = [];
  am._deliveryCallback = (_t, _j, publishId) => delivered.push(publishId);
  return { am, delivered };
}

// Simulate a root replaying a message to us on (re)subscribe.
async function replay(am, topicHex, publishId, ts) {
  await am._onReplayBatch({
    topicId: topicHex,
    messages: [{ json: JSON.stringify({ m: publishId }), publishId, publishTs: ts,
                 postHash: publishId, publisher: OTHER }],
  }, { fromId: OTHER });
}

async function testResubscribeRedelivers() {
  console.log('\n── re-subscribe (since:all) after unsub re-delivers ──');
  const { am, delivered } = makeManager();
  const T = big(TOPIC);

  await replay(am, TOPIC, 'p1', 101);
  await replay(am, TOPIC, 'p2', 102);
  check('first subscribe delivers both', delivered.length === 2);

  // Within a live subscription, a duplicate replay is deduped (exactly-once).
  await replay(am, TOPIC, 'p1', 101);
  check('duplicate replay without unsub is deduped', delivered.length === 2);

  // Consumption state is populated.
  check('have digest populated', (am._haveByTopic.get(T)?.size ?? 0) >= 1);
  check('lastSeenTs populated',  am._lastSeenTsByTopic.has(T));
  check('app-dedup populated',   [...am._appDelivered.keys()].some(k => k.startsWith(`${T}:`)));

  // Unsubscribe → must forget consumption for this topic.
  am.pubsubUnsubscribe(T);
  check('have digest cleared on unsub',   !am._haveByTopic.has(T));
  check('lastSeenTs cleared on unsub',    !am._lastSeenTsByTopic.has(T));
  check('app-dedup cleared on unsub',     ![...am._appDelivered.keys()].some(k => k.startsWith(`${T}:`)));

  // Re-subscribe (since:'all') → the SAME messages must re-deliver.
  await replay(am, TOPIC, 'p1', 101);
  await replay(am, TOPIC, 'p2', 102);
  check('re-subscribe re-delivers both (BUG FIX)', delivered.length === 4);
  check('re-delivered the original p1,p2',
    delivered.slice(2).sort().join(',') === 'p1,p2');
}

async function testTopicIsolation() {
  console.log('\n── reset is per-topic (does not disturb other topics) ──');
  const { am, delivered } = makeManager();
  const T1 = big(TOPIC), T2 = big(TOPIC2);
  await replay(am, TOPIC,  'a1', 201);
  await replay(am, TOPIC2, 'b1', 202);
  check('two topics delivered', delivered.length === 2);

  am.pubsubUnsubscribe(T1);                  // reset topic 1 only
  check('topic1 consumption cleared', !am._haveByTopic.has(T1));
  check('topic2 consumption intact',   am._haveByTopic.has(T2));

  // topic2 still deduped (unchanged); topic1 re-delivers.
  await replay(am, TOPIC2, 'b1', 202);
  check('topic2 still deduped',  delivered.length === 2);
  await replay(am, TOPIC,  'a1', 201);
  check('topic1 re-delivers',    delivered.length === 3 && delivered[2] === 'a1');
}

async function testResetMethodGuards() {
  console.log('\n── pubsubResetTopicConsumption guards ──');
  const { am } = makeManager();
  let threw = false;
  try { am.pubsubResetTopicConsumption('not-a-bigint'); } catch { threw = true; }
  check('non-bigint is a safe no-op (no throw)', threw === false);
  check('method exists', typeof am.pubsubResetTopicConsumption === 'function');
}

async function main() {
  console.log('Axona re-subscribe (since:all) redelivery smoke');
  await testResubscribeRedelivers();
  await testTopicIsolation();
  await testResetMethodGuards();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
