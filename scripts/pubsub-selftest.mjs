#!/usr/bin/env node
// =====================================================================
// pubsub-selftest.mjs — "Is my relay actually being used for pub/sub?"
//
// A relay is USED for a topic only if it's in that topic's K-closest root set
// (publishes route there) AND in the same keyspace as the topic's anchor region.
// This probe answers it directly, from the network, for the topics you care about.
//
//   RELAY_REGION=uswest \
//   RELAY_TOPICS=axona-share/public-images,pow-bench/results \
//   [RELAY_NODEID=80ab…]  [RELAY_TOPIC_REGION=uswest]  [BRIDGE_URL=wss://…] \
//   node scripts/pubsub-selftest.mjs
//
// RELAY_NODEID defaults to the id in ./identity.<region>.json (what `npm start`
// uses), so if you run this next to your relay you usually don't need to set it.
//
// For each topic it reports:
//   • whether your relay is in the K-closest root set (rank k of R) — the test
//   • a live publish→subscribe round-trip, and which node served the replay
// =====================================================================
import '../src/polyfill.js';
import { readFileSync } from 'node:fs';
import { connectPeer, regionToDescriptor } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';
import { resolveRegion, regionName } from '../vendor/axona-protocol/src/utils/region-names.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(...a);
const ROOT_SET = Number(process.env.RELAY_ROOT_SET ?? 5);   // R (kernel default)

function relayNodeId(regionTok) {
  if (process.env.RELAY_NODEID) return process.env.RELAY_NODEID.toLowerCase();
  // Default identity file the relay persists per region: identity.<name>.json
  const name = regionName(resolveRegion(regionTok)) ?? regionTok;
  for (const f of [process.env.RELAY_IDENTITY_PATH, `./identity.${name}.json`, `./identity.${regionTok}.json`]) {
    if (!f) continue;
    try { const j = JSON.parse(readFileSync(f, 'utf8')); if (j.id) return String(j.id).toLowerCase(); } catch { /* */ }
  }
  return null;
}

async function main() {
  const region  = (process.env.RELAY_REGION || 'uswest').trim();
  const anchor  = (process.env.RELAY_TOPIC_REGION || region).trim();
  const topics  = (process.env.RELAY_TOPICS || '').split(',').map((s) => s.trim()).filter(Boolean);
  const bridge  = process.env.BRIDGE_URL || 'wss://bridge.axona.net';
  if (!topics.length) { console.error('Set RELAY_TOPICS=topic1,topic2 (the topics your relay should serve).'); process.exit(2); }
  const relayId = relayNodeId(region);
  const { name: anchorRegion } = regionToDescriptor(anchor);   // structured-topic region name

  log(`Probe: region ${region} · anchor ${anchorRegion} · bridge ${bridge}`);
  log(relayId ? `Testing your relay: ${relayId.slice(0,16)}… (prefix 0x${relayId.slice(0,2)})`
              : `(no RELAY_NODEID / identity file found — will only show the K-closest set, not a YES/NO)`);

  const probe = await connectPeer({ region, bridge, readyTimeoutSec: 45 });
  log(`probe ${String(probe.nodeId).slice(0,12)}… connected; meshing 20s…`);
  await sleep(20000);

  const peers = (probe.peer.health().peers || []).map((p) => p.toLowerCase());
  const known = relayId ? peers.includes(relayId) : null;
  log(`\nsynaptome: ${peers.length} peers · your relay is ${relayId ? (known ? 'PRESENT ✓' : 'NOT present ✗ (different region, or relay down/unmeshed)') : 'n/a'}`);

  // Candidate root pool = everything the probe can see + the probe itself + the relay.
  const pool = new Set([...peers, String(probe.nodeId).toLowerCase()]);
  if (relayId) pool.add(relayId);

  for (const topic of topics) {
    const tid = await deriveTopicIdBig({ region: anchorRegion, name: topic });
    const ranked = [...pool].map((h) => ({ h, d: BigInt('0x' + h) ^ tid }))
      .sort((a, b) => (a.d < b.d ? -1 : a.d > b.d ? 1 : 0));
    const roots = ranked.slice(0, ROOT_SET);
    const rank  = relayId ? ranked.findIndex((r) => r.h === relayId) : -1;
    const isRoot = rank >= 0 && rank < ROOT_SET;

    log(`\n── ${topic} ──  (topic-id 0x${tid.toString(16).slice(0,4)}…)`);
    log(`  K-closest roots (top ${ROOT_SET}): ${roots.map((r) => '0x' + r.h.slice(0,2) + ':' + r.h.slice(2,8)).join('  ')}`);
    if (relayId) {
      // "Used" requires BOTH meshed (online + reachable so publishers route to it)
      // AND close enough (in the top-R). The XOR rank alone is only potential.
      const verdict = !known
        ? (isRoot ? `WELL-POSITIONED (rank ${rank+1}) but NOT currently meshed ✗ — start the relay / check it's online & same-region`
                  : `NOT participating ✗ — not meshed AND wrong region (rank ${rank+1} of ${pool.size})`)
        : (isRoot ? `ROOTS this topic ✓ — meshed AND in the top ${ROOT_SET} (rank ${rank+1} of ${pool.size})`
                  : `meshed but NOT a root ✗ — rank ${rank+1} of ${pool.size}, outside the top ${ROOT_SET} (wrong region for this topic)`);
      log(`  YOUR RELAY: ${verdict}`);
    }

    // Live round-trip: publish a unique heartbeat, subscribe from a 2nd probe.
    const beacon = `selftest-${topic}-${probe.nodeId.slice(0,6)}`;
    let got = false, servedBy = null;
    const sub = await connectPeer({ region, bridge, readyTimeoutSec: 45 });
    sub.peer.onLog?.('debug', (m, ctx) => { if (String(m) === 'replay-serve' && ctx?.from) servedBy = ctx.from; });
    await sub.peer.sub({ region: anchorRegion, name: topic }, (env) => { if (env && !env.deleted && String(env.message).includes(beacon)) got = true; }, { since: 'all' });
    await sleep(2000);
    await probe.peer.pub({ region: anchorRegion, name: topic }, JSON.stringify({ beacon }), { signWith: probe.author });
    await sleep(8000);
    log(`  live round-trip: ${got ? 'OK ✓ (a subscriber received the publish)' : 'FAILED ✗ (publish not delivered)'}` +
        (servedBy ? ` · replay served by 0x${String(servedBy).slice(0,2)}:${String(servedBy).slice(2,8)}` +
                    (relayId && String(servedBy).toLowerCase() === relayId ? '  ← YOUR RELAY served it ✓✓' : '') : ''));
    await sub.close();
  }

  await probe.close();
  log('\nVerdict: a relay is "used" for a topic when it shows ROOTS this topic ✓. If it');
  log('says NOT a root, it\'s the wrong region/keyspace for that topic — run the relay in');
  log('the SAME region the apps anchor (RELAY_REGION) and ensure RELAY_TOPICS lists it.');
  process.exit(0);
}
main().catch((e) => { console.error('selftest threw:', e?.stack || e); process.exit(2); });
