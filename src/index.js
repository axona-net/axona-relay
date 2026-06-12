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
//   RELAY_TOPICS         comma-separated topics to SUBSCRIBE to, so the relay
//                        actively joins those axons (and, in-region, becomes a
//                        stable root) instead of idly meshing. e.g.
//                        "pow-bench/results,pow-bench/leaderboard"
//   RELAY_TOPIC_REGION   anchor region for RELAY_TOPICS (default useast — where
//                        the demo/bench/share apps anchor their topics)
//
// Quit with q or Ctrl-C.

import './polyfill.js';                 // MUST be first — installs RTCPeerConnection/WebSocket
import { cleanupWebRTC } from './polyfill.js';
import { loadOrCreateIdentity, acquireIdentityLock, createEphemeralIdentity } from './identity.js';
import { createRelay, startRelay, stopRelay, KERNEL_VERSION, regionName, resolveRegion } from './relay.js';
import { powCalibrate, powDifficulty } from '../vendor/axona-protocol/src/pow/pow.js';
import { makeDashboard, makePlainLog } from './tui.js';
import { geoCellId, geoCellCenter } from '../vendor/axona-protocol/src/utils/s2.js';
import { autoDetectRegion } from './geolocate.js';
import { resolveBridgeUrl } from './network.js';
import { readFile } from 'node:fs/promises';

const RELAY_VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

const BRIDGE_URL = resolveBridgeUrl();   // BRIDGE_URL env › RELAY_NETWORK env › prod
const USE_TUI = process.env.RELAY_TUI != null
  ? process.env.RELAY_TUI !== '0'
  : Boolean(process.stdout.isTTY);

// Transport 'debug' events worth surfacing (skips ping/pong chatter).
const INTERESTING = /bridge|welcome|mesh|peer|relay|reconnect|close|degraded|error|signal/i;

const DEFAULT_REGION = { lat: 37.77, lng: -122.42 };  // SF (us-west / uswest)

// Synthetic-publisher anchor for a region token (e.g. "useast" / "0x89"), matching
// exactly how the apps anchor their topics (geoCellId prefix + 64 zeros). Reuses
// helpers already imported above; returns null for an unknown region.
function topicPublisherFor(regionTok) {
  const code = resolveRegion(regionTok);
  if (code == null) return null;
  const c = geoCellCenter(code);
  const prefix = geoCellId(c.lat, c.lng, 8).toString(16).padStart(2, '0');
  return prefix + '0'.repeat(64);
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
  // Region-keyed default identity file, so two different regions never collide
  // on one file (and the same region intentionally shares — guarded by a lock).
  // 'auto' uses a fixed key so re-detection variance can't orphan the identity.
  const IDENTITY_PATH = process.env.RELAY_IDENTITY_PATH || `./identity.${cfg.fileKey}.json`;

  // The first instance claims the KNOWN persistent identity (stable nodeId
  // across restarts). If it's already in use, we don't refuse — we run as an
  // ADDITIONAL node: a fresh ephemeral identity in the same region (unique
  // nodeId, not persisted). So `npm start` again just adds a node.
  let releaseLock, identity, created = false, mode;
  try {
    releaseLock = await acquireIdentityLock(IDENTITY_PATH);
    ({ identity, created } = await loadOrCreateIdentity(IDENTITY_PATH, { lat: cfg.lat, lng: cfg.lng }));
    mode = 'primary';
  } catch (e) {
    if (e.code !== 'IDENTITY_LOCKED') throw e;
    identity = await createEphemeralIdentity({ lat: cfg.lat, lng: cfg.lng });
    mode = 'additional';
  }
  const region      = identity.region ?? cfg;
  const regionCodeN = geoCellId(region.lat, region.lng, 8);
  const regionCode  = regionCodeN.toString(16).padStart(2, '0');
  const regionLabel = regionName(regionCodeN) ?? '?';

  const present = (USE_TUI ? makeDashboard : makePlainLog)({
    version: RELAY_VERSION, kernelVersion: KERNEL_VERSION,
    bridgeUrl: BRIDGE_URL, nodeId: identity.id, region,
    regionLabel, regionName, mode,
  });
  for (const n of cfg.notes) present.logLine(`{gray-fg}geo:{/} ${n}`);
  present.logLine(
    mode === 'additional'
      ? `{cyan-fg}ADDITIONAL node{/} — known identity in use; minted an ephemeral ` +
        `id in ${regionLabel} (0x${regionCode}), not persisted`
      : created
        ? `{green-fg}PRIMARY node{/} — minted a new known identity in ${regionLabel} (0x${regionCode})`
        : `{green-fg}PRIMARY node{/} — loaded known identity (${regionLabel} 0x${regionCode})`);

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

  const { peer, transport, node, domain } = createRelay({
    bridgeUrl: BRIDGE_URL, identity, region, onLog,
  });

  peer.onPeerJoin?.((id) => present.logLine(`{green-fg}+ peer{/} ${id.slice(0, 12)}…`));
  peer.onPeerLeave?.((id) => present.logLine(`{gray-fg}- peer{/} ${id.slice(0, 12)}…`));
  ['info', 'warn', 'error'].forEach(lvl =>
    peer.onLog?.(lvl, (msg, ctx) => onLog(lvl, msg, ctx)));
  peer.onError?.((err) => onLog('error', err?.code || 'error', { message: err?.message }));

  await startRelay({ peer, transport });
  present.logLine('started — meshing…');

  const startedAt = Date.now();
  const tick = setInterval(() => {
    let health;
    try { health = peer.health(); }
    catch (e) { present.logLine(`{red-fg}health() threw{/} ${e.message}`); return; }
    present.update({ health, uptimeMs: Date.now() - startedAt, regionCode });
  }, 1000);

  let shuttingDown = false;
  const shutdown = async (why) => {
    if (shuttingDown) return; shuttingDown = true;
    clearInterval(tick);
    present.destroy();
    console.log(`\naxona-relay shutting down (${why})…`);
    await stopRelay({ peer, transport });
    cleanupWebRTC();
    try { await releaseLock?.(); } catch { /* */ }
    process.exit(0);
  };
  process.on('relay:quit', () => shutdown('quit'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  // RELAY_TOPICS: actively SUBSCRIBE so the relay joins those axons (and, when
  // it's in-region for the anchor, gets promoted to a stable root) instead of
  // idly meshing and never appearing in any Axon structure. Fire-and-forget so
  // it never blocks shutdown wiring; waits briefly for the mesh to converge so
  // the subscribe anchors on the right K-closest set.
  void (async () => {
    const topics = (process.env.RELAY_TOPICS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
    if (!topics.length) return;
    const anchorRegion = (process.env.RELAY_TOPIC_REGION ?? 'useast').trim();
    const publisher = topicPublisherFor(anchorRegion);
    if (!publisher) { present.logLine(`{red-fg}ERR{/} RELAY_TOPICS: unknown RELAY_TOPIC_REGION "${anchorRegion}"`); return; }
    const readyBy = Date.now() + 25000;
    while (!shuttingDown && Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
      await new Promise((r) => setTimeout(r, 500));
    }
    let n = 0;
    for (const topic of topics) {
      if (shuttingDown) return;
      try {
        await peer.sub(topic, () => {}, { publisher, since: 'all' });   // no-op cb: we relay/cache, don't consume
        n++;
        present.logLine(`{cyan-fg}subscribed{/} ${topic} @ ${anchorRegion} (0x${publisher.slice(0, 2)}) — joined its axon`);
      } catch (e) {
        present.logLine(`{red-fg}ERR{/} subscribe ${topic}: ${e?.message || e}`);
      }
    }
    present.logLine(`{gray-fg}RELAY_TOPICS: ${n}/${topics.length} active — watch the "root axon" panel for promotion to root{/}`);
  })();
}

main().catch((err) => {
  console.error('axona-relay failed to start:', err);
  process.exit(1);
});
