// ops.js — reusable connect + pub/sub/pull against a live Axona network.
//
// The shared core behind both the CLI (src/cli.js) and the MCP server
// (src/mcp.js): connect a fresh EPHEMERAL peer through the testnet bridge,
// wait for mesh readiness, run one operation, tear everything down. Topics are
// anchored at a synthetic region publisher (`<s2-prefix>‖0^256`), exactly as
// axona-peer / the kernel demo do, so these interoperate with the live apps.

import './polyfill.js';                     // MUST be first — RTCPeerConnection/WebSocket globals
import { cleanupWebRTC } from './polyfill.js';
import { createEphemeralIdentity } from './identity.js';
import { createRelay, startRelay, stopRelay, resolveRegion } from './relay.js';
import { geoCellId, geoCellCenter } from '../vendor/axona-protocol/src/utils/s2.js';

export const DEFAULT_BRIDGE = process.env.BRIDGE_URL || 'wss://testnet.axona.net';

/** region name|code → { code, center:{lat,lng}, prefixHex, publisher (66-hex) }. */
export function regionToPublisher(region = 'useast') {
  const code = resolveRegion(region);
  if (code == null) throw new Error(`unknown region "${region}" (use a name like "useast" or a code like "0x89")`);
  const center    = geoCellCenter(code);
  const prefixHex = geoCellId(center.lat, center.lng, 8).toString(16).padStart(2, '0');
  return { code, center, prefixHex, publisher: prefixHex + '0'.repeat(64) };
}

/**
 * Connect an ephemeral peer, wait until the mesh is usable, run `fn(peer, ctx)`,
 * then always tear down. `ctx = { publisher, prefixHex, center, nodeId }`.
 */
export async function withConnectedPeer({ region = 'useast', bridge = DEFAULT_BRIDGE, readyTimeoutSec = 30, onError } = {}, fn) {
  const { center, prefixHex, publisher } = regionToPublisher(region);
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const { peer, transport } = createRelay({ bridgeUrl: bridge, identity, region: center, onLog: () => {} });
  if (onError) peer.onError?.((e) => onError(e));
  await startRelay({ peer, transport });
  try {
    const readyBy = Date.now() + readyTimeoutSec * 1000;
    let ready = false;
    while (Date.now() < readyBy) {
      let h; try { h = peer.health(); } catch { h = null; }
      if (h && (h.synaptomeSize >= 1 || (h.peers && h.peers.length >= 1))) { ready = true; break; }
      await new Promise(r => setTimeout(r, 500));
    }
    if (!ready) throw new Error(`timed out waiting for mesh readiness (bridge ${bridge})`);
    await new Promise(r => setTimeout(r, 1500));        // brief settle so roots are reachable
    return await fn(peer, { publisher, prefixHex, center, nodeId: identity.id });
  } finally {
    try { await stopRelay({ peer, transport }); } catch { /* */ }
    cleanupWebRTC();
  }
}

export async function publish({ topic, message, region = 'useast', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const msgId = await peer.pub(topic, message, { publisher: ctx.publisher });
    await new Promise(r => setTimeout(r, 1500));        // let it propagate to roots
    return { ok: true, topic, region, prefix: '0x' + ctx.prefixHex, msgId, nodeId: ctx.nodeId };
  });
}

export async function pull({ topic, region = 'useast', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const env = await peer.pull(null, { topic, publisher: ctx.publisher });   // null msgId → latest
    return { ok: true, topic, region, found: !!env, message: env ? env.message : null, msgId: env?.msgId ?? null };
  });
}

export async function subscribe({ topic, region = 'useast', bridge = DEFAULT_BRIDGE, seconds = 20, since = 'all' } = {}) {
  const secs = Math.max(1, Math.min(120, Number(seconds) || 20));
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const messages = [];
    await peer.sub(topic, (env) => {
      messages.push({ message: env.message, signer: env.signerPubkey ?? null, seq: env.seq ?? null, ts: env.ts ?? null, msgId: env.msgId ?? null });
    }, { publisher: ctx.publisher, since });
    await new Promise(r => setTimeout(r, secs * 1000));
    return { ok: true, topic, region, listenedSec: secs, since, received: messages.length, messages };
  });
}
