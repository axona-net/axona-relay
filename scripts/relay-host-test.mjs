#!/usr/bin/env node
// =====================================================================
// relay-host-test.mjs — does the NEW host() primitive make a node
// participate as a pub/sub root WITHOUT subscribing? Three live peers:
//   • passive   — connects, neither subs nor hosts          (control: ~0 roles)
//   • keyspace  — peer.host()            (host my keyspace)  (expect roles ↑)
//   • topic     — peer.host('pow-bench/results')            (expect roles ↑)
// None register a delivery handler. We poll health().axonRoles over ~40s.
//
//   REGION=useast node scripts/relay-host-test.mjs
// =====================================================================
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer, regionToPublisher } from '../src/ops.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(...a);
const REGION = (process.env.REGION || 'useast').trim();
const BRIDGE = process.env.BRIDGE_URL || 'wss://bridge.axona.net';
const SETTLE = Number(process.env.SETTLE_MS || 45000);
const { publisher } = regionToPublisher(REGION);
const rolesOf = (p) => { try { return p.peer.health().axonRoles?.length ?? -1; } catch { return -1; } };
const hostingOf = (p) => { try { const h = p.peer.health().hosting; return h ? `keyspace=${h.keyspace} topics=${h.topics.length}` : 'n/a'; } catch { return '?'; } };

async function main() {
  log(`host() participation test · region ${REGION} · bridge ${BRIDGE}`);
  const opened = [];
  try {
    const passive  = await connectPeer({ region: REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(passive);
    log(`passive  up: ${String(passive.nodeId).slice(0,12)}…  (no host, no sub)`);

    const keyspace = await connectPeer({ region: REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(keyspace);
    await keyspace.peer.host();                                  // host my keyspace
    log(`keyspace up: ${String(keyspace.nodeId).slice(0,12)}…  peer.host()  → ${hostingOf(keyspace)}`);

    const topic = await connectPeer({ region: REGION, bridge: BRIDGE, readyTimeoutSec: 45 });
    opened.push(topic);
    const hr = await topic.peer.host('pow-bench/results', { publisher });
    log(`topic    up: ${String(topic.nodeId).slice(0,12)}…  peer.host('pow-bench/results') → ${JSON.stringify(hr)}`);

    log(`\nsettling ${SETTLE/1000}s (refresh ticks + recruitment)…`);
    for (let t = 10000; t <= SETTLE; t += 10000) {
      await sleep(10000);
      log(`  +${String(t/1000).padStart(2)}s   passive roles=${rolesOf(passive)}   |   keyspace roles=${rolesOf(keyspace)}   |   topic roles=${rolesOf(topic)}`);
    }
    const pr = rolesOf(passive), kr = rolesOf(keyspace), tr = rolesOf(topic);
    log(`\nFINAL  passive=${pr}  keyspace=${kr}  topic=${tr}`);
    log(`\nVerdict:`);
    if (kr > pr && tr > pr && (kr > 0 || tr > 0))
      log(`  ✓ host() PARTICIPATES without subscribing — host peers accrue roles, passive stays low (${pr}).`);
    else if (kr > 0 || tr > 0)
      log(`  ~ host peers accrued roles (${kr}/${tr}) but passive=${pr} too — recruitment is broad; inspect.`);
    else
      log(`  ✗ host peers did NOT accrue roles in ${SETTLE/1000}s — keyspace announce may need longer / per-topic.`);
  } finally {
    for (const o of opened) { try { await o.close(); } catch {} }
    cleanupWebRTC();
  }
  process.exit(0);
}
main().catch((e) => { console.error('threw:', e?.stack || e); try { cleanupWebRTC(); } catch {}; process.exit(2); });
