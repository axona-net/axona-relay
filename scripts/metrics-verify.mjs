// metrics-verify.mjs — LIVE end-to-end check of the derived-metric publish loop
// against a real Axona bridge (testnet by default).
//
// One peer that both ROOTS and SUBSCRIBES the relevant topics, so the result is
// deterministic (no dependence on which other node happens to be K-closest),
// while still exercising the real kernel pub/sub + auth over a real bridge:
//
//   1. host an OPEN data topic T and metricTopic(T); host an OWNED topic and
//      metricTopic(owned). Subscribe to both metric topics.
//   2. publish to T (open) and to the owned topic, so both roles cache a post.
//   3. run startMetricsLoop (short cadence).
//   4. assert: a snapshot lands on metricTopic(T) with .topic === deriveTopicId(T)
//      and counts matching rootedTopics(); NO snapshot on metricTopic(owned)
//      (privacy skip); no metric-of-metric (recursion guard — the loop skips the
//      metric topic it now also roots).
//
//   node scripts/metrics-verify.mjs            # testnet
//   node scripts/metrics-verify.mjs --network prod
//   node scripts/metrics-verify.mjs --bridge wss://testnet.axona.net

import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { createEphemeralIdentity, createEphemeralAuthor } from '../src/identity.js';
import { createRelay, startRelay, stopRelay, regionDescriptor } from '../src/relay.js';
import { resolveBridgeUrl } from '../src/network.js';
import { startMetricsLoop } from '../src/metrics-loop.js';
import { deriveTopicId, metricTopic } from '../vendor/axona-protocol/src/index.js';

const argv = process.argv.slice(2);
const opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };
const elog = (...a) => process.stderr.write(a.join(' ') + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

const REGION = opt('region', 'useast');
const bridge = resolveBridgeUrl({ override: opt('bridge'), network: opt('network', 'testnet') });
const rd = regionDescriptor(REGION);
if (!rd) { elog('unknown region', REGION); process.exit(2); }
const region = rd.name, center = rd.center;
const stamp = Date.now();
const dataDesc  = { region, name: `metrics-verify/open/${stamp}` };
const owner     = await createEphemeralAuthor();
const ownedDesc = { region, owner: owner.authorId, name: `metrics-verify/owned/${stamp}`, write: 'owner' };

async function main() {
  console.log(`metrics-verify → ${bridge} (region ${REGION})`);
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const metricsAuthor = await createEphemeralAuthor();
  const { peer, transport } = createRelay({
    bridgeUrl: bridge, identity, region: center,
    onLog: (l, e) => { if (l === 'error') elog('ERR', e); },
  });
  await startRelay({ peer, transport });

  // mesh readiness
  const readyBy = Date.now() + 30000;
  while (Date.now() < readyBy) {
    let h; try { h = peer.health(); } catch { h = null; }
    if (h && (h.synaptomeSize >= 1 || (h.peers?.length >= 1))) break;
    await sleep(500);
  }
  await sleep(1500);

  const dataId    = await deriveTopicId(dataDesc);
  const ownedId   = await deriveTopicId(ownedDesc);
  const mtData    = metricTopic(dataId);
  const mtOwned   = metricTopic(ownedId);

  // root both data topics AND their metric topics on THIS peer (deterministic)
  await peer.host(dataDesc);
  await peer.host(mtData);
  await peer.host(ownedDesc);
  await peer.host(mtOwned);

  const openSnaps = [], ownedSnaps = [];
  await peer.sub(mtData,  (env) => { try { openSnaps.push(JSON.parse(env.message)); } catch { /* */ } }, { since: 'all' });
  await peer.sub(mtOwned, (env) => { try { ownedSnaps.push(JSON.parse(env.message)); } catch { /* */ } }, { since: 'all' });
  await sleep(3000);   // let host/sub announcements anchor in the K-closest sets

  // publish to the data topics so each role caches a post (→ descriptor + count)
  await peer.pub(dataDesc,  'mv-open-1',  { signWith: owner });
  await peer.pub(dataDesc,  'mv-open-2',  { signWith: owner });
  await peer.pub(ownedDesc, 'mv-owned-1', { signWith: owner });   // owner signs (write:owner)
  await sleep(2500);

  // sanity: the open topic shows up in rootedTopics() with a descriptor + count
  const rooted = peer.rootedTopics();
  const rOpen  = rooted.find(r => r.topicId === dataId);
  const rOwned = rooted.find(r => r.topicId === ownedId);
  check('rootedTopics() includes the open topic with a descriptor', !!rOpen?.descriptor, JSON.stringify(rOpen));
  check('rootedTopics() open topic current_count ≥ 1', (rOpen?.current_count ?? 0) >= 1, `count=${rOpen?.current_count}`);
  check('rootedTopics() open topic descriptor is open (not owner)', rOpen?.descriptor && rOpen.descriptor.write !== 'owner');
  check('rootedTopics() owned topic descriptor is write:owner', rOwned?.descriptor?.write === 'owner', JSON.stringify(rOwned?.descriptor));

  // run the real publish loop with a short cadence
  let cycles = 0;
  const stop = startMetricsLoop({
    peer, author: metricsAuthor, nodeId: identity.id,
    intervalMs: 8000, firstRunDelayMs: 1500,
    log: (l, e, c) => { if (e === 'metrics_cycle') { cycles++; elog('cycle', JSON.stringify(c)); } else if (l === 'warn') elog('WARN', e, JSON.stringify(c || {})); },
  });

  await sleep(22000);   // ≥ 2 cadences
  stop();

  // ── assertions ──
  check('a snapshot arrived on metricTopic(open)', openSnaps.length >= 1, `got ${openSnaps.length}`);
  const s = openSnaps[openSnaps.length - 1] || {};
  check('snapshot.topic === deriveTopicId(open)', s.topic === dataId, `topic=${s.topic}`);
  check('snapshot has current_count ≥ 1 + subscribers field', (s.current_count ?? 0) >= 1 && typeof s.subscribers === 'number', JSON.stringify(s));
  check('snapshot carries provenance (by = relay node id, ts)', s.by === identity.id && typeof s.ts === 'number');
  check('every open snapshot is for the DATA topic (no metric-of-metric)', openSnaps.every(x => x.topic === dataId));
  check('NO snapshot on metricTopic(owned) — privacy skip', ownedSnaps.length === 0, `got ${ownedSnaps.length}`);
  check('loop ran at least one publishing cycle', cycles >= 1, `cycles=${cycles}`);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  try { await stopRelay({ peer, transport }); } catch { /* */ }
  cleanupWebRTC();
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { elog('verify threw:', e?.stack || e); cleanupWebRTC(); process.exit(2); });
