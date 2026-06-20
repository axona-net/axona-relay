#!/usr/bin/env node
// index.js — axona-relay entrypoint.
//
// Boots a headless Axona mesh peer that acts as a relay/supernode and
// renders its live state to the console. Configuration is via env:
//
//   RELAY_NETWORK        prod|testnet                   (default prod)
//   BRIDGE_URL           explicit bridge URL (overrides RELAY_NETWORK)
//   RELAY_IDENTITY_PATH  persisted keypair file        (default ./identity.relay.json)
//   RELAY_LAT/RELAY_LNG  geo prefix for the nodeId      (default SF 37.77,-122.42)
//   RELAY_TUI            1=force dashboard, 0=plain log (default: auto by isTTY)
//   RELAY_HOST_KEYSPACE  1=host this relay's keyspace neighborhood so it gets
//                        recruited as a root for whatever topics land near its
//                        id — "host whatever lands near me" (default 1). This
//                        is what makes a relay participate with ZERO topic
//                        config; set 0 to opt out.
//   RELAY_TOPICS         comma-separated topics to HOST (store + serve for
//                        others, without consuming them), so the relay joins
//                        those specific axons. e.g.
//                        "pow-bench/results,pow-bench/leaderboard"
//                        v0.3: each is hosted as { region, name } where region
//                        is RELAY_TOPIC_REGION (the structured-topic anchor).
//   RELAY_TOPIC_REGION   region for RELAY_TOPICS (default = the relay's own
//                        region — where the demo/bench/share apps anchor)
//   RELAY_METRICS        1=publish rooted-topic metrics to their derived metric
//                        topics on a timer (the publish side of the
//                        derived-metric-topic convention), 0=off. Default 1.
//                        Only OPEN topics are published; metric topics (recursion
//                        guard) and owned topics (owner-only privacy) are skipped.
//   RELAY_METRICS_INTERVAL_MS  cadence for the above (default ~5 min).
//
// Quit with q or Ctrl-C.

import './polyfill.js';                 // MUST be first — installs RTCPeerConnection/WebSocket
import { cleanupWebRTC } from './polyfill.js';
import { createEphemeralIdentity, createEphemeralAuthor } from './identity.js';
import { createRelay, startRelay, stopRelay, KERNEL_VERSION, regionName, resolveRegion, regionDescriptor } from './relay.js';
import { startMetricsLoop, DEFAULT_METRICS_INTERVAL_MS } from './metrics-loop.js';
import { powCalibrate, powDifficulty } from '../vendor/axona-protocol/src/pow/pow.js';
import { makeDashboard, makePlainLog } from './tui.js';
import { geoCellId, geoCellCenter } from '../vendor/axona-protocol/src/utils/s2.js';
import { autoDetectRegion } from './geolocate.js';
import { resolveBridgeUrl } from './network.js';
import { readFile } from 'node:fs/promises';

const RELAY_VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

// Resilience: a relay is long-lived infrastructure. A stray async error must NOT
// take the process down — most commonly a WebSocket `error` event on a bridge
// blip (502 while the bridge restarts, ECONNREFUSED when it's briefly down) or a
// malformed inbound frame. Log loudly and keep running; the transport's
// reconnect:true re-establishes the bridge link, and the kernel's
// dispatch-boundary guard already drops malformed frames. Without these handlers
// such errors reach Node's top level and exit the process (the recurring relay
// crashes). Same posture as pow-collector.js.
//
// Route through the active TUI's log panel when one exists — a raw console.error
// writes over the blessed/dashboard frame and shreds the display (looks like a
// crash). `activePresent` is set once main() builds the presenter; before that
// (early boot) we fall back to console so nothing is swallowed.
let activePresent = null;
const reportCaught = (msg) => {
  if (activePresent) { try { activePresent.logLine(`{red-fg}⚠{/} ${msg}`); return; } catch { /* fall through */ } }
  console.error(`⚠ ${msg}`);
};
process.on('uncaughtException',  (e) => reportCaught(`uncaughtException (continuing): ${e?.stack || e?.message || e}`));
process.on('unhandledRejection', (e) => reportCaught(`unhandledRejection (continuing): ${e?.message || e}`));

const BRIDGE_URL = resolveBridgeUrl();   // BRIDGE_URL env › RELAY_NETWORK env › prod
const USE_TUI = process.env.RELAY_TUI != null
  ? process.env.RELAY_TUI !== '0'
  : Boolean(process.stdout.isTTY);

// Transport 'debug' events worth surfacing (skips ping/pong chatter).
const INTERESTING = /bridge|welcome|mesh|peer|relay|reconnect|close|degraded|error|signal/i;

const DEFAULT_REGION = { lat: 37.77, lng: -122.42 };  // SF (us-west / uswest)

// v0.3: a region token (e.g. "useast" / "0x89") → the structured-topic region
// NAME used in { region, name } descriptors. Replaces the old synthetic-
// publisher anchor; the region→keyspace mapping is unchanged. null if unknown.
function topicRegionFor(regionTok) {
  return regionDescriptor(regionTok)?.name ?? null;
}

/**
 * Resolve the desired region from env, precedence:
 *   RELAY_REGION = "auto"          → IP-geo, then timezone, then default
 *   RELAY_REGION = name | code     → that region (e.g. "useast" / "0x89")
 *   RELAY_LAT / RELAY_LNG          → by coordinate
 *   (none)                         → default SF
 * Returns { lat, lng, code, label, source, fileKey, notes[] }.
 */
async function resolveRegionConfig() {
  const notes = [];
  const at = (lat, lng, source, fileKey) => {
    const code = geoCellId(lat, lng, 8);
    return { lat, lng, code, label: regionName(code), source, fileKey, notes };
  };
  const tok = (process.env.RELAY_REGION ?? '').trim();

  if (tok.toLowerCase() === 'auto') {
    const got = await autoDetectRegion({ log: (m) => notes.push(m) });
    if (got) {
      const r = at(got.lat, got.lng, got.source, 'auto');
      notes.push(`detected ${r.label} (0x${r.code.toString(16)}) via ${got.source}` +
        (got.detail ? ` — ${got.detail}` : ''));
      return r;
    }
    notes.push('auto-detect unavailable; using default region');
    return at(DEFAULT_REGION.lat, DEFAULT_REGION.lng, 'default(auto-failed)', 'auto');
  }
  if (tok !== '') {
    const code = resolveRegion(tok);
    if (code == null) {
      console.error(`axona-relay: unknown RELAY_REGION "${tok}". ` +
        `Use "auto", a region name (e.g. useast), or a code (e.g. 0x89).`);
      process.exit(1);
    }
    const c = geoCellCenter(code);
    return { lat: c.lat, lng: c.lng, code, label: regionName(code), source: 'RELAY_REGION', fileKey: regionName(code), notes };
  }
  if (process.env.RELAY_LAT != null || process.env.RELAY_LNG != null) {
    const lat = Number(process.env.RELAY_LAT ?? DEFAULT_REGION.lat);
    const lng = Number(process.env.RELAY_LNG ?? DEFAULT_REGION.lng);
    const r = at(lat, lng, 'latlng', null);
    return { ...r, fileKey: r.label };
  }
  const r = at(DEFAULT_REGION.lat, DEFAULT_REGION.lng, 'default', null);
  return { ...r, fileKey: r.label };
}

async function main() {
  const cfg = await resolveRegionConfig();
  // Phase 2: the transport id is EPHEMERAL — never persisted. Every relay mints
  // a fresh in-memory identity on each start (no cross-restart linkage; a
  // restarted relay re-joins as a new node and the cold-start anti-entropy drain
  // re-warms its keyspace). Multiple relays in one region just get distinct
  // random ids — no shared file, no lock.
  const releaseLock = null;
  const identity = await createEphemeralIdentity({ lat: cfg.lat, lng: cfg.lng });
  const mode = 'ephemeral';
  const region      = identity.region ?? cfg;
  const regionCodeN = geoCellId(region.lat, region.lng, 8);
  const regionCode  = regionCodeN.toString(16).padStart(2, '0');
  const regionLabel = regionName(regionCodeN) ?? '?';

  const present = (USE_TUI ? makeDashboard : makePlainLog)({
    version: RELAY_VERSION, kernelVersion: KERNEL_VERSION,
    bridgeUrl: BRIDGE_URL, nodeId: identity.id, region,
    regionLabel, regionName, mode,
  });
  activePresent = present;   // process-level error handlers now log to the panel, not over the frame
  for (const n of cfg.notes) present.logLine(`{gray-fg}geo:{/} ${n}`);
  present.logLine(`{cyan-fg}ephemeral node{/} — fresh id in ${regionLabel} (0x${regionCode}); ` +
    `transport ids are never persisted (re-minted every start)`);

  // Stage 2 (E-1): log this device's PoW solve-rate. Difficulty is 0 (inert), so
  // this is pure CALIBRATION DATA for choosing a Stage-4 difficulty — `estMintMs`
  // is the expected one-time mint cost at N bits on THIS host. Runs a ~400ms
  // benchmark once, off the critical path.
  powCalibrate().then(c => present.logLine(
    `{gray-fg}pow-calibrate{/} ${c.hashesPerSec.toLocaleString()} H/s · ` +
    `mint@16b≈${c.estMintMs[16]}ms 20b≈${c.estMintMs[20]}ms 24b≈${c.estMintMs[24]}ms · ` +
    `active diff transport=${powDifficulty('transport')} publish=${powDifficulty('publish')}`)).catch(() => {});

  // Sticky-region warning: the region is baked into the persisted identity, so
  // an explicit region request is ignored once the file exists.
  if (mode === 'primary' && !created && cfg.source !== 'default' && regionCodeN !== cfg.code) {
    present.logLine(`{yellow-fg}WRN{/} identity file pins region ${regionLabel} (0x${regionCode}); ` +
      `requested ${cfg.label} (0x${cfg.code.toString(16)}) ignored — delete ${IDENTITY_PATH} ` +
      `or set RELAY_IDENTITY_PATH to re-mint`);
  }

  const onLog = (level, event, ctx) => {
    if (level === 'debug' && !INTERESTING.test(event)) return;
    const tag = level === 'error' ? '{red-fg}ERR{/}'
              : level === 'warn'  ? '{yellow-fg}WRN{/}' : '';
    const detail = ctx ? ' ' + JSON.stringify(ctx).slice(0, 120) : '';
    present.logLine(`${tag ? tag + ' ' : ''}${event}${detail}`);
  };

  // Stack is (re)built per connect attempt below, so these are mutable and the
  // shutdown closure always sees the live instances.
  let peer, transport, node, domain;

  // Shutdown wiring goes up BEFORE the connect so Ctrl-C / q work even while
  // we're still retrying an unreachable bridge.
  let shuttingDown = false;
  let tick = null;
  let stopMetrics = null;
  const shutdown = async (why) => {
    if (shuttingDown) return; shuttingDown = true;
    if (tick) clearInterval(tick);
    if (stopMetrics) { try { stopMetrics(); } catch { /* */ } }
    activePresent = null;        // stop routing late errors into a torn-down panel
    present.destroy();
    console.log(`\naxona-relay shutting down (${why})…`);
    try { await stopRelay({ peer, transport }); } catch { /* */ }
    cleanupWebRTC();
    try { await releaseLock?.(); } catch { /* */ }
    process.exit(0);
  };
  process.on('relay:quit', () => shutdown('quit'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // Resilient startup: a bridge that's down or restarting (ECONNREFUSED / 502 /
  // handshake timeout) must not kill the relay. webTransport's reconnect:true
  // only self-heals AFTER a first successful bind — a failed FIRST connect does
  // NOT auto-retry — so we drive the retry here, rebuilding the stack each
  // attempt (re-calling start() on a half-dead transport is unsafe) and bounding
  // each attempt with a timeout so a hung connect can't stall us. The timeout
  // timer also keeps the event loop alive so the process can't silently drain
  // out while the bridge is unreachable. Once connected, reconnect:true takes
  // over for later drops.
  const CONNECT_TIMEOUT_MS = 20000;
  for (let attempt = 1; !shuttingDown; attempt++) {
    if (transport) {                       // tear down the prior failed attempt
      try { await stopRelay({ peer, transport }); } catch { /* */ }
      try { cleanupWebRTC(); } catch { /* */ }
    }
    ({ peer, transport, node, domain } = createRelay({ bridgeUrl: BRIDGE_URL, identity, region, onLog }));
    peer.onPeerJoin?.((id)  => present.logLine(`{green-fg}+ peer{/} ${id.slice(0, 12)}…`));
    peer.onPeerLeave?.((id) => present.logLine(`{gray-fg}- peer{/} ${id.slice(0, 12)}…`));
    ['info', 'warn', 'error'].forEach((lvl) => peer.onLog?.(lvl, (msg, ctx) => onLog(lvl, msg, ctx)));
    peer.onError?.((err) => onLog('error', err?.code || 'error', { message: err?.message }));
    try {
      await Promise.race([
        startRelay({ peer, transport }),
        new Promise((_, rej) => setTimeout(() => rej(new Error('connect/handshake timeout')), CONNECT_TIMEOUT_MS)),
      ]);
      break;
    } catch (e) {
      if (shuttingDown) return;
      const wait = Math.min(30000, 2000 * attempt);
      present.logLine(`{yellow-fg}WRN{/} bridge connect failed (attempt ${attempt}): ` +
        `${e?.message || e} — retry in ${Math.round(wait / 1000)}s`);
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  if (shuttingDown) return;
  present.logLine('started — meshing…');

  const startedAt = Date.now();
  tick = setInterval(() => {
    let health;
    try { health = peer.health(); }
    catch (e) { present.logLine(`{red-fg}health() threw{/} ${e.message}`); return; }
    present.update({ health, uptimeMs: Date.now() - startedAt, regionCode });
  }, 1000);

  // HOST mode: a relay's job is to store + serve topics for OTHERS, which is
  // decoupled from subscribing (consuming). peer.host() volunteers the relay
  // for its keyspace neighborhood so it's recruited as a root for whatever
  // lands near its id — participation with zero topic config. RELAY_TOPICS
  // additionally hosts named topics. Neither registers a consumer. Fire-and-
  // forget so it never blocks shutdown; waits briefly for the mesh to converge
  // so the announce anchors on the right K-closest set.
  void (async () => {
    const hostKeyspace = (process.env.RELAY_HOST_KEYSPACE ?? '1') !== '0';
    const topics = (process.env.RELAY_TOPICS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
    if (!hostKeyspace && !topics.length) return;
    // Anchor named topics at the relay's OWN region by default, so a regional
    // relay hosts in its own keyspace — matching same-region apps — instead of
    // being pinned to us-east. Override with RELAY_TOPIC_REGION.
    const anchorRegion = (process.env.RELAY_TOPIC_REGION ?? ('0x' + regionCode)).trim();
    const topicRegion = topicRegionFor(anchorRegion);
    if (topics.length && !topicRegion) { present.logLine(`{red-fg}ERR{/} RELAY_TOPICS: unknown RELAY_TOPIC_REGION "${anchorRegion}"`); return; }
    const readyBy = Date.now() + 25000;
    while (!shuttingDown && Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
      await new Promise((r) => setTimeout(r, 500));
    }
    if (shuttingDown) return;
    if (hostKeyspace) {
      try {
        await peer.host();   // host my keyspace — recruit for nearby topics, no consume
        present.logLine(`{cyan-fg}hosting{/} keyspace 0x${regionCode} — recruiting as a root for nearby topics`);
      } catch (e) {
        present.logLine(`{red-fg}ERR{/} host keyspace: ${e?.message || e}`);
      }
    }
    let n = 0;
    for (const topic of topics) {
      if (shuttingDown) return;
      try {
        await peer.host({ region: topicRegion, name: topic });   // store + serve, don't consume
        n++;
        present.logLine(`{cyan-fg}hosting{/} ${topic} @ ${topicRegion} — serving its axon`);
      } catch (e) {
        present.logLine(`{red-fg}ERR{/} host ${topic}: ${e?.message || e}`);
      }
    }
    if (topics.length) present.logLine(`{gray-fg}RELAY_TOPICS: ${n}/${topics.length} hosted — watch the "root axon" panel for promotion to root{/}`);
  })();

  // METRICS PUBLISH loop (the publish side of the derived-metric-topic
  // convention). For each OPEN topic this relay roots, recompute its local
  // metrics every ~5 min and publish a signed snapshot to metricTopic(T), so
  // clients sub() that instead of scatter-gathering. Metric topics (recursion
  // guard) and owned topics (owner-only privacy) are skipped by the loop. An
  // ephemeral author signs the snapshots — advisory provenance, not authority.
  if ((process.env.RELAY_METRICS ?? '1') !== '0') {
    try {
      const metricsAuthor = await createEphemeralAuthor();
      const intervalMs = Number(process.env.RELAY_METRICS_INTERVAL_MS) || DEFAULT_METRICS_INTERVAL_MS;
      stopMetrics = startMetricsLoop({
        peer, author: metricsAuthor, nodeId: identity.id, intervalMs, log: onLog,
      });
      present.logLine(`{cyan-fg}metrics{/} republishing rooted open-topic snapshots every ` +
        `${Math.round(intervalMs / 1000)}s (signer ${metricsAuthor.authorId.slice(0, 12)}…) — RELAY_METRICS=0 to disable`);
    } catch (e) {
      present.logLine(`{red-fg}ERR{/} metrics loop: ${e?.message || e}`);
    }
  }
}

main().catch((err) => {
  console.error('axona-relay failed to start:', err);
  process.exit(1);
});
