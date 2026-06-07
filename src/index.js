#!/usr/bin/env node
// index.js — axona-relay entrypoint.
//
// Boots a headless Axona mesh peer that acts as a relay/supernode and
// renders its live state to the console. Configuration is via env:
//
//   BRIDGE_URL           bridge for bootstrap+signaling (default testnet)
//   RELAY_IDENTITY_PATH  persisted keypair file        (default ./identity.relay.json)
//   RELAY_LAT/RELAY_LNG  geo prefix for the nodeId      (default SF 37.77,-122.42)
//   RELAY_TUI            1=force dashboard, 0=plain log (default: auto by isTTY)
//
// Quit with q or Ctrl-C.

import './polyfill.js';                 // MUST be first — installs RTCPeerConnection/WebSocket
import { cleanupWebRTC } from './polyfill.js';
import { loadOrCreateIdentity, acquireIdentityLock } from './identity.js';
import { createRelay, startRelay, stopRelay, KERNEL_VERSION, regionName, resolveRegion } from './relay.js';
import { makeDashboard, makePlainLog } from './tui.js';
import { geoCellId, geoCellCenter } from '../vendor/axona-protocol/src/utils/s2.js';
import { readFile } from 'node:fs/promises';

const RELAY_VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

const BRIDGE_URL = process.env.BRIDGE_URL || 'wss://testnet.axona.net';
const USE_TUI = process.env.RELAY_TUI != null
  ? process.env.RELAY_TUI !== '0'
  : Boolean(process.stdout.isTTY);

// Transport 'debug' events worth surfacing (skips ping/pong chatter).
const INTERESTING = /bridge|welcome|mesh|peer|relay|reconnect|close|degraded|error|signal/i;

const DEFAULT_REGION = { lat: 37.77, lng: -122.42 };  // SF (us-west / uswest)

/**
 * Resolve the desired region from env, precedence:
 *   RELAY_REGION (name like "useast" or code like "0x89")  >
 *   RELAY_LAT / RELAY_LNG  >  default SF.
 * Returns { lat, lng, code, label, source }.
 */
function resolveRegionConfig() {
  const tok = process.env.RELAY_REGION;
  if (tok != null && tok.trim() !== '') {
    const code = resolveRegion(tok.trim());
    if (code == null) {
      console.error(`axona-relay: unknown RELAY_REGION "${tok}". ` +
        `Use a region name (e.g. useast) or code (e.g. 0x89).`);
      process.exit(1);
    }
    const c = geoCellCenter(code);
    return { lat: c.lat, lng: c.lng, code, label: regionName(code), source: 'RELAY_REGION' };
  }
  if (process.env.RELAY_LAT != null || process.env.RELAY_LNG != null) {
    const lat = Number(process.env.RELAY_LAT ?? DEFAULT_REGION.lat);
    const lng = Number(process.env.RELAY_LNG ?? DEFAULT_REGION.lng);
    const code = geoCellId(lat, lng, 8);
    return { lat, lng, code, label: regionName(code), source: 'latlng' };
  }
  const code = geoCellId(DEFAULT_REGION.lat, DEFAULT_REGION.lng, 8);
  return { ...DEFAULT_REGION, code, label: regionName(code), source: 'default' };
}

async function main() {
  const cfg = resolveRegionConfig();
  // Region-keyed default identity file, so two different regions never collide
  // on one file (and the same region intentionally shares — guarded by a lock).
  const IDENTITY_PATH = process.env.RELAY_IDENTITY_PATH || `./identity.${cfg.label}.json`;

  // Exclusive lock: refuse to start a second relay on the same identity.
  let releaseLock;
  try {
    releaseLock = await acquireIdentityLock(IDENTITY_PATH);
  } catch (e) {
    if (e.code === 'IDENTITY_LOCKED') { console.error('axona-relay:', e.message); process.exit(1); }
    throw e;
  }

  const { identity, created } = await loadOrCreateIdentity(IDENTITY_PATH, { lat: cfg.lat, lng: cfg.lng });
  const region      = identity.region ?? cfg;
  const regionCodeN = geoCellId(region.lat, region.lng, 8);
  const regionCode  = regionCodeN.toString(16).padStart(2, '0');
  const regionLabel = regionName(regionCodeN) ?? '?';

  const present = (USE_TUI ? makeDashboard : makePlainLog)({
    version: RELAY_VERSION, kernelVersion: KERNEL_VERSION,
    bridgeUrl: BRIDGE_URL, nodeId: identity.id, region,
    regionLabel, regionName,
  });
  present.logLine(created
    ? `minted a new relay identity in ${regionLabel} (0x${regionCode})`
    : `loaded existing relay identity (${regionLabel} 0x${regionCode})`);

  // Sticky-region warning: the region is baked into the persisted identity, so
  // an explicit region request is ignored once the file exists.
  if (!created && cfg.source !== 'default' && regionCodeN !== cfg.code) {
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
}

main().catch((err) => {
  console.error('axona-relay failed to start:', err);
  process.exit(1);
});
