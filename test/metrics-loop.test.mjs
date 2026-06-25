// metrics-loop.test.mjs — the relay metric-publish POLICY (v4.3.0).
//
// Drives startMetricsLoop() against a mock peer whose rootedTopics() returns one
// of each shape, and asserts the loop publishes BOTH open and owned data topics
// to their metricTopic(), signing with the given author:
//
//   • open topic            → published to metricTopic(id), signed
//   • owned topic           → published too (v4.3.0: owned-topic metrics are
//                             PUBLIC — anyone can subscribe to an owned topicID)
//   • metric topic          → SKIPPED (recursion guard)
//   • descriptor: null      → SKIPPED (nothing to report / can't guard)
//   • snapshot payload carries the counts + provenance
//   • a failing pub doesn't abort the cycle (other topics still publish)
//
//   node test/metrics-loop.test.mjs
import { startMetricsLoop } from '../src/metrics-loop.js';
import { deriveTopicId, metricTopic, isMetricTopicName }
  from '../vendor/axona-protocol/src/index.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  console.log('relay metrics-loop policy');

  // Resolve real topic ids so metricTopic() derivation is exercised end to end.
  const openId   = await deriveTopicId({ region: 'useast', name: 'lobby' });
  const ownedId  = await deriveTopicId({ region: 'useast', owner: 'cd'.repeat(32), name: 'feed', write: 'owner' });
  const metricId = await deriveTopicId(metricTopic(openId));   // a metric topic this relay also roots
  const emptyId  = await deriveTopicId({ region: 'useast', name: 'ghost' });

  const rooted = [
    { topicId: openId,   descriptor: { region: 'useast', name: 'lobby' },                          current_count: 3, subscribers: 5, bytes: 900 },
    { topicId: metricId, descriptor: metricTopic(openId),                                          current_count: 1, subscribers: 0, bytes: 120 },
    { topicId: ownedId,  descriptor: { region: 'useast', owner: 'cd'.repeat(32), name: 'feed', write: 'owner' }, current_count: 2, subscribers: 9, bytes: 400 },
    { topicId: emptyId,  descriptor: null,                                                          current_count: 0, subscribers: 1, bytes: 0 },
  ];

  const pubs = [];
  let failNext = false;
  const peer = {
    rootedTopics: () => rooted,
    pub: async (topic, message, opts) => {
      if (failNext) { failNext = false; throw new Error('simulated pub failure'); }
      pubs.push({ topic, message, opts });
      return 'msgid-' + pubs.length;
    },
  };
  const author = { authorId: 'aa'.repeat(32) };

  const stop = startMetricsLoop({
    peer, author, nodeId: 'relay-node-1',
    intervalMs: 1_000_000,      // don't let the interval fire during the test
    firstRunDelayMs: 0,         // run the first cycle immediately
    now: () => 1700000000000,
  });
  await sleep(30);              // let the first cycle complete
  stop();

  // Two publishes — the open topic AND the owned topic (metric + null skipped).
  check('1. exactly two topics published (open + owned)', pubs.length === 2);
  const p = pubs.find(x => x.topic.name === metricTopic(openId).name);
  check('2. published to metricTopic(openId)',
    p && p.topic.name === metricTopic(openId).name && isMetricTopicName(p.topic.name));
  check('3. signed with the given author (signWith)', p?.opts?.signWith === author);

  const snap = p ? JSON.parse(p.message) : {};
  check('4a. snapshot.topic = the DATA topic id', snap.topic === openId);
  check('4b. snapshot carries counts', snap.current_count === 3 && snap.subscribers === 5 && snap.bytes === 900);
  check('4c. snapshot carries provenance (by + ts)', snap.by === 'relay-node-1' && snap.ts === 1700000000000);

  check('5. metric topic was NOT republished (recursion guard)',
    !pubs.some(x => x.topic.name === metricTopic(metricId).name));
  check('6. owned topic WAS published (v4.3.0: owned metrics are public)',
    pubs.some(x => x.topic.name === metricTopic(ownedId).name));

  // A failing pub must not abort the cycle: fail the FIRST pub (open topic);
  // the owned topic still publishes, so exactly one publish survives and the
  // cycle does not throw.
  failNext = true;
  pubs.length = 0;
  const stop2 = startMetricsLoop({
    peer, author, nodeId: 'relay-node-1', intervalMs: 1_000_000, firstRunDelayMs: 0,
    now: () => 1700000000000,
  });
  await sleep(30);
  stop2();
  check('7. a failing pub is swallowed (cycle did not throw, other topic still published)', pubs.length === 1);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('test threw:', e?.stack || e); process.exit(2); });
