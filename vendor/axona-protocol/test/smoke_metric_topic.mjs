// =====================================================================
// smoke_metric_topic.mjs — core derived metric-topic convention
// (src/pubsub/metrics.js, exported from @axona/protocol).
//
//   1. metricTopic(id) shape: { region = data region byte, name = prefix+id }
//   2. the metric descriptor resolves via the real deriveTopicId (valid 66-hex)
//   3. derivation is deterministic — same data id → identical metric topic
//      (so any subscriber computes the same one for discovery)
//   4. distinct data ids → distinct metric topics (no collisions)
//   5. recursion guard: a metric topic's OWN descriptor is flagged (a root skips it)
//   6. isMetricTopic / isMetricTopicName recognise name or descriptor; reject normal
//   7. dataTopicIdOf round-trips: metricTopic(id) → id
//   8. metricTopic rejects non-66-hex input loudly
//
//   node test/smoke_metric_topic.mjs
// =====================================================================
import { metricTopic, isMetricTopic, isMetricTopicName, dataTopicIdOf,
         METRIC_NAMESPACE, deriveTopicId } from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

// Resolve a couple of real data-topic ids in different regions.
const lobbyId   = await deriveTopicId({ region: 'useast', name: 'lobby' });            // open topic
const ownedId   = await deriveTopicId({ region: 'alaska', owner: 'ab'.repeat(32), name: 'feed', write: 'owner' });
check('deriveTopicId yields 66-hex data ids',
  /^[0-9a-f]{66}$/.test(lobbyId) && /^[0-9a-f]{66}$/.test(ownedId));

// 1. shape
const mt = metricTopic(lobbyId);
check('metricTopic.name = prefix + data id', mt.name === METRIC_NAMESPACE + lobbyId);
check('metricTopic.region = data region byte', mt.region === parseInt(lobbyId.slice(0, 2), 16));
check('metric topic is open (no owner field)', mt.owner === undefined);

// 2. resolves through the real kernel deriveTopicId
const mtId = await deriveTopicId(mt);
check('metric descriptor resolves to a 66-hex id', /^[0-9a-f]{66}$/.test(mtId));
check('metric topic id differs from its data topic id', mtId !== lobbyId);

// 3. deterministic — discovery depends on this
const mtId2 = await deriveTopicId(metricTopic(lobbyId));
check('derivation is deterministic (same data id → same metric id)', mtId === mtId2);

// 4. no collisions across data topics
const ownedMtId = await deriveTopicId(metricTopic(ownedId));
check('distinct data topics → distinct metric topics', ownedMtId !== mtId);
check('metric topic inherits the owned topic region byte',
  metricTopic(ownedId).region === parseInt(ownedId.slice(0, 2), 16));

// 5. recursion guard — a metric topic must be recognised as one
check('isMetricTopic(metric descriptor) is true (root skips it)', isMetricTopic(mt) === true);
check('isMetricTopicName(metric name) is true', isMetricTopicName(mt.name) === true);

// 6. negative cases
check('isMetricTopic(normal descriptor) is false', isMetricTopic({ region: 'useast', name: 'lobby' }) === false);
check('isMetricTopic(null) is false', isMetricTopic(null) === false);
check('isMetricTopic(undefined) is false', isMetricTopic(undefined) === false);
check('isMetricTopicName(non-string) is false', isMetricTopicName(123) === false);

// 7. inverse round-trip
check('dataTopicIdOf(metricTopic(id)) === id', dataTopicIdOf(mt) === lobbyId);
check('dataTopicIdOf(normal name) === null', dataTopicIdOf('lobby') === null);
check('dataTopicIdOf accepts a descriptor', dataTopicIdOf({ name: mt.name }) === lobbyId);

// 8. bad input rejected loudly (uppercase, short, garbage)
const throws = (fn) => { try { fn(); return false; } catch { return true; } };
check('metricTopic rejects a too-short id', throws(() => metricTopic('deadbeef')));
check('metricTopic rejects a non-hex id', throws(() => metricTopic('z'.repeat(66))));
check('metricTopic accepts/normalises an uppercased id',
  metricTopic(lobbyId.toUpperCase()).name === METRIC_NAMESPACE + lobbyId);

console.log(`\n${failed ? '✗' : '✓'} smoke_metric_topic: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
