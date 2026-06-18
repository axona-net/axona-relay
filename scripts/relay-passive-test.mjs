#!/usr/bin/env node
// relay-passive-test.mjs — does a relay that MESHES but never SUBSCRIBES
// accumulate any axonRoles? This reproduces the default relay (no RELAY_TOPICS)
// and compares it to a relay that subscribes to one topic.
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer, regionToDescriptor } from '../src/ops.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(...a);
const REGION = (process.env.REGION || 'useast').trim();
const BRIDGE = process.env.BRIDGE_URL || 'wss://bridge.axona.net';
const SETTLE = Number(process.env.SETTLE_MS || 40000);
const { name: TOPIC_REGION } = regionToDescriptor(REGION);   // structured-topic region name

async function main() {
  log(`Passive-vs-subscribed relay test · region ${REGION} · bridge ${BRIDGE}`);
  const opened = [];
  try {
    // PASSIVE: mesh only, subscribe to nothing (== default relay, no RELAY_TOPICS)
    const passive = await connectPeer({ region: REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(passive);
    log(`passive  relay up: ${String(passive.nodeId).slice(0,12)}… (no .sub call)`);

    // SUBSCRIBED: mesh + subscribe to one live app topic (== RELAY_TOPICS set)
    const active = await connectPeer({ region: REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(active);
    await active.peer.sub({ region: TOPIC_REGION, name: 'pow-bench/results' }, () => {}, { since: 'all' });
    log(`active   relay up: ${String(active.nodeId).slice(0,12)}… (subscribed pow-bench/results)`);

    log(`settling ${SETTLE/1000}s (refresh ticks + recruitment)…`);
    for (let t = 10000; t <= SETTLE; t += 10000) {
      await sleep(10000);
      const p = (() => { try { return passive.peer.health(); } catch { return null; } })();
      const a = (() => { try { return active.peer.health();  } catch { return null; } })();
      log(`  +${t/1000}s   passive: synaptome=${p?.synaptomeSize ?? '?'} roles=${p?.axonRoles?.length ?? '?'}   |   active: synaptome=${a?.synaptomeSize ?? '?'} roles=${a?.axonRoles?.length ?? '?'}`);
    }
    const pf = passive.peer.health(), af = active.peer.health();
    log(`\nFINAL  passive roles=${pf.axonRoles.length} ${roles_summary(pf)}   active roles=${af.axonRoles.length} ${roles_summary(af)}`);
    log(`\nReading: if passive≈0 and active>0, a relay needs to SUBSCRIBE (RELAY_TOPICS) to show roles.`);
  } finally {
    for (const o of opened) { try { await o.close(); } catch {} }
    cleanupWebRTC();
  }
  process.exit(0);
}
function roles_summary(h){ const roots = (h.axonRoles||[]).filter(r=>r.isRoot).length; return `  (${roots} as ROOT, ${(h.axonRoles||[]).length-roots} as replica)`; }
main().catch((e) => { console.error('threw:', e?.stack || e); try { cleanupWebRTC(); } catch {}; process.exit(2); });
