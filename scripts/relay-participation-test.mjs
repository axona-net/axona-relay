#!/usr/bin/env node
// =====================================================================
// relay-participation-test.mjs — "DO relays ever root a pub/sub topic?"
//
// A controlled experiment, not an inference. We stand up N relay-peers in a
// region, have each SUBSCRIBE to a test topic (exactly what RELAY_TOPICS does),
// publish to that topic, let the mesh converge, then read back EACH relay-peer's
// own health().axonRoles — the same field the TUI "Pub/sub roles" panel shows.
//
// We run two cohorts so the result is unambiguous:
//   • MATCH    — relay-peers in the SAME region the topic anchors  → should ROOT
//   • MISMATCH — relay-peers in a DIFFERENT region than the topic   → should NOT
//
// If MATCH relays show isRoot and MISMATCH relays don't, that is the whole story:
// relays participate when (and only when) they share the topic's keyspace.
//
//   N=4 TOPIC_REGION=useast MISMATCH_REGION=uswest \
//   node scripts/relay-participation-test.mjs
// =====================================================================
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer, regionToDescriptor } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(...a);

const N             = Number(process.env.N || 3);
const TOPIC_REGION  = (process.env.TOPIC_REGION || 'useast').trim();
const MISMATCH      = (process.env.MISMATCH_REGION || 'uswest').trim();
const BRIDGE        = process.env.BRIDGE_URL || 'wss://bridge.axona.net';
const TOPIC         = `relaytest/${TOPIC_REGION}-${N}`;
const CONVERGE_MS   = Number(process.env.CONVERGE_MS || 35000);

const { name: ANCHOR_REGION } = regionToDescriptor(TOPIC_REGION);   // structured-topic region (topic anchor)

// normalize a topic-id to a bare lowercase hex string for loose matching
const hexOf = (v) => String(v == null ? '' : v).toLowerCase().replace(/^0x/, '');

async function rolesFor(peerObj, topicIdHex) {
  let h; try { h = peerObj.peer.health(); } catch { return null; }
  const roles = h?.axonRoles || [];
  const mine = roles.find((r) => hexOf(r.topic).startsWith(topicIdHex.slice(0, 12)) ||
                                 topicIdHex.startsWith(hexOf(r.topic).slice(0, 12)));
  return { total: roles.length, mine, all: roles };
}

async function runCohort(label, region) {
  log(`\n=========================  COHORT: ${label}  (relays in ${region})  =========================`);
  const tidBig = await deriveTopicIdBig({ region: ANCHOR_REGION, name: TOPIC });
  const tidHex = tidBig.toString(16).padStart(66, '0');
  log(`topic "${TOPIC}"  anchor ${ANCHOR_REGION} (${TOPIC_REGION})  topic-id 0x${tidHex.slice(0,12)}…`);

  const relays = [];
  const opened = [];
  try {
    // 1) stand up N relay-peers in `region` and SUBSCRIBE each to the topic
    for (let i = 0; i < N; i++) {
      const r = await connectPeer({ region, bridge: BRIDGE, readyTimeoutSec: 45 });
      opened.push(r);
      await r.peer.sub({ region: ANCHOR_REGION, name: TOPIC }, () => {}, { since: 'all' });   // RELAY_TOPICS-style active sub
      relays.push(r);
      log(`  relay#${i + 1} up: ${String(r.nodeId).slice(0,12)}… (0x${String(r.nodeId).slice(0,2)}) subscribed`);
    }

    // 2) a publisher in the TOPIC's region publishes a few messages
    const pub = await connectPeer({ region: TOPIC_REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(pub);
    for (let i = 0; i < 3; i++) {
      await pub.peer.pub({ region: ANCHOR_REGION, name: TOPIC }, JSON.stringify({ n: i, label }), { signWith: pub.author });
      await sleep(400);
    }
    log(`  published 3 messages from ${String(pub.nodeId).slice(0,12)}… (0x${String(pub.nodeId).slice(0,2)})`);

    // 3) let refresh ticks + anti-entropy converge
    log(`  converging ${CONVERGE_MS / 1000}s…`);
    await sleep(CONVERGE_MS);

    // 4) read each relay-peer's OWN axonRoles for the topic
    log(`\n  ── per-relay axonRoles (their own health(), == TUI "Pub/sub roles") ──`);
    let rooted = 0, holding = 0;
    for (let i = 0; i < relays.length; i++) {
      const rep = await rolesFor(relays[i], tidHex);
      const m = rep?.mine;
      if (m?.isRoot) rooted++;
      if (m && m.cacheSize > 0) holding++;
      log(`    relay#${i + 1} (0x${String(relays[i].nodeId).slice(0,2)}): roles=${rep?.total ?? '?'}  ` +
          (m ? `THIS topic → isRoot=${m.isRoot} children=${m.children} cached=${m.cacheSize}`
             : `THIS topic → not present (not rooting it)`));
    }

    // 5) independent confirmation: a fresh subscriber pulls it back
    const sub = await connectPeer({ region: TOPIC_REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(sub);
    let got = 0, servedBy = null;
    sub.peer.onLog?.('debug', (m, ctx) => { if (String(m) === 'replay-serve' && ctx?.from) servedBy = ctx.from; });
    await sub.peer.sub({ region: ANCHOR_REGION, name: TOPIC }, (env) => { if (env && !env.deleted) got++; }, { since: 'all' });
    await sleep(8000);
    log(`\n  fresh subscriber received ${got}/3 messages` +
        (servedBy ? ` · served by 0x${String(servedBy).slice(0,2)}:${String(servedBy).slice(2,8)}` : ''));

    log(`\n  >>> COHORT ${label}: ${rooted}/${N} relay-peers ROOT the topic, ${holding}/${N} hold cached copies.`);
    return { label, region, rooted, holding, got };
  } finally {
    for (const o of opened) { try { await o.close(); } catch { /* */ } }
    cleanupWebRTC();
    await sleep(1500);
  }
}

async function main() {
  log(`Relay pub/sub participation test · N=${N} per cohort · bridge ${BRIDGE}`);
  const a = await runCohort('MATCH', TOPIC_REGION);
  const b = await runCohort('MISMATCH', MISMATCH);

  log(`\n========================================  VERDICT  ========================================`);
  log(`  MATCH   (relays ${a.region} == topic ${TOPIC_REGION}): ${a.rooted}/${N} rooted, ${a.holding}/${N} holding`);
  log(`  MISMATCH(relays ${b.region} != topic ${TOPIC_REGION}): ${b.rooted}/${N} rooted, ${b.holding}/${N} holding`);
  if (a.rooted > 0 && b.rooted === 0)
    log(`\n  ✓ Relays DO participate as pub/sub roots — but ONLY when they share the topic's region.`);
  else if (a.rooted > 0)
    log(`\n  ✓ Relays DO participate (MATCH cohort rooted). MISMATCH also rooted — investigate region gating.`);
  else
    log(`\n  ✗ Even the region-MATCHED relays did NOT root the topic — this is a real bug, not a config issue.`);
  process.exit(0);
}
main().catch((e) => { console.error('test threw:', e?.stack || e); try { cleanupWebRTC(); } catch {}; process.exit(2); });
