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
import { loadOrCreateIdentity } from './identity.js';
import { createRelay, startRelay, stopRelay, KERNEL_VERSION, regionName } from './relay.js';
import { makeDashboard, makePlainLog } from './tui.js';
import { geoCellId } from '../vendor/axona-protocol/src/utils/s2.js';
import { readFile } from 'node:fs/promises';

const RELAY_VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

const BRIDGE_URL    = process.env.BRIDGE_URL || 'wss://testnet.axona.net';
const IDENTITY_PATH = process.env.RELAY_IDENTITY_PATH || './identity.relay.json';
const REGION = {
  lat: Number(process.env.RELAY_LAT ?? 37.77),
  lng: Number(process.env.RELAY_LNG ?? -122.42),
};
const USE_TUI = process.env.RELAY_TUI != null
  ? process.env.RELAY_TUI !== '0'
  : Boolean(process.stdout.isTTY);

// Transport 'debug' events worth surfacing (skips ping/pong chatter).
const INTERESTING = /bridge|welcome|mesh|peer|relay|reconnect|close|degraded|error|signal/i;

async function main() {
  const { identity, created } = await loadOrCreateIdentity(IDENTITY_PATH, REGION);
  const region      = identity.region ?? REGION;
  const regionCodeN = geoCellId(region.lat, region.lng, 8);
  const regionCode  = regionCodeN.toString(16).padStart(2, '0');
  const regionLabel = regionName(regionCodeN) ?? '?';

  const present = (USE_TUI ? makeDashboard : makePlainLog)({
    version: RELAY_VERSION, kernelVersion: KERNEL_VERSION,
    bridgeUrl: BRIDGE_URL, nodeId: identity.id, region,
    regionLabel, regionName,
  });
  present.logLine(created ? 'minted a new relay identity' : 'loaded existing relay identity');

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
