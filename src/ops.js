// ops.js — reusable connect + pub/sub/pull against a live Axona network.
//
// The shared core behind both the CLI (src/cli.js) and the MCP server
// (src/mcp.js): connect a fresh EPHEMERAL peer to the live Axona network
// (production by default — see network.js), wait for mesh readiness, run one
// operation, tear everything down.
//
// v0.3 topic addressing: topics are STRUCTURED descriptors { region, name }.
// The region NAME (e.g. "useast") anchors the topic in a keyspace — replacing
// the old "synthetic publisher = <s2-prefix>‖0^256" anchor. An app and this
// relay both open { region: 'useast', name: 'foo' } and meet on the same
// topic id, so these still interoperate with the live apps. Publishes are
// signed by an ephemeral AUTHOR identity (key separation: the node key never
// signs); pass { signWith } on each pub.

import './polyfill.js';                     // MUST be first — RTCPeerConnection/WebSocket globals
import { cleanupWebRTC } from './polyfill.js';
import { createEphemeralIdentity, createEphemeralAuthor } from './identity.js';
import { createRelay, startRelay, stopRelay, regionDescriptor } from './relay.js';
import { resolveBridgeUrl } from './network.js';

export const DEFAULT_BRIDGE = resolveBridgeUrl();   // BRIDGE_URL env › RELAY_NETWORK env › prod

/**
 * region name|code → { code, name, center:{lat,lng} }.
 * `name` is the structured-topic region (use it as `{ region: name, name: topic }`).
 */
export function regionToDescriptor(region = 'useast') {
  const d = regionDescriptor(region);
  if (!d) throw new Error(`unknown region "${region}" (use a name like "useast" or a code like "0x89")`);
  return d;
}

/**
 * Connect an ephemeral peer and wait until the mesh is usable. Returns the LIVE
 * peer plus `close()` — which stops the relay but does NOT tear down WebRTC,
 * since `cleanupWebRTC()` is process-global (node-datachannel cleanup destroys
 * ALL connections). A multi-peer caller must therefore close each peer, then
 * call `cleanupWebRTC()` exactly once after the last one. `withConnectedPeer`
 * below is the single-peer convenience wrapper that does both.
 *
 * The returned `ctx` carries `regionName` (the structured-topic region) and a
 * fresh ephemeral `author` to sign publishes with.
 */
export async function connectPeer({ region = 'useast', bridge = DEFAULT_BRIDGE, readyTimeoutSec = 30, onError } = {}) {
  const { name: regionName, center } = regionToDescriptor(region);
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const author   = await createEphemeralAuthor();
  const { peer, transport } = createRelay({ bridgeUrl: bridge, identity, region: center, onLog: () => {} });
  if (onError) peer.onError?.((e) => onError(e));
  await startRelay({ peer, transport });
  const readyBy = Date.now() + readyTimeoutSec * 1000;
  let ready = false;
  while (Date.now() < readyBy) {
    let h; try { h = peer.health(); } catch { h = null; }
    if (h && (h.synaptomeSize >= 1 || (h.peers && h.peers.length >= 1))) { ready = true; break; }
    await new Promise(r => setTimeout(r, 500));
  }
  if (!ready) {
    try { await stopRelay({ peer, transport }); } catch { /* */ }
    throw new Error(`timed out waiting for mesh readiness (bridge ${bridge})`);
  }
  await new Promise(r => setTimeout(r, 1500));          // brief settle so roots are reachable
  return {
    peer, regionName, center, author, nodeId: identity.id,
    async close() { try { await stopRelay({ peer, transport }); } catch { /* */ } },
  };
}

export async function withConnectedPeer(opts, fn) {
  const h = await connectPeer(opts);
  try {
    return await fn(h.peer, { regionName: h.regionName, center: h.center, author: h.author, nodeId: h.nodeId });
  } finally {
    await h.close();
    cleanupWebRTC();
  }
}

export async function publish({ topic, message, region = 'useast', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const msgId = await peer.pub({ region: ctx.regionName, name: topic }, message, { signWith: ctx.author });
    await new Promise(r => setTimeout(r, 1500));        // let it propagate to roots
    return { ok: true, topic, region, msgId, signer: ctx.author.authorId, nodeId: ctx.nodeId };
  });
}

export async function pull({ topic, region = 'useast', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const env = await peer.pull(null, { topic: { region: ctx.regionName, name: topic } });   // null msgId → latest
    return { ok: true, topic, region, found: !!env, message: env ? env.message : null, msgId: env?.msgId ?? null };
  });
}

export async function subscribe({ topic, region = 'useast', bridge = DEFAULT_BRIDGE, seconds = 20, since = 'all' } = {}) {
  const secs = Math.max(1, Math.min(120, Number(seconds) || 20));
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const messages = [];
    await peer.sub({ region: ctx.regionName, name: topic }, (env) => {
      messages.push({ message: env.message, signer: env.signerPubkey ?? null, seq: env.seq ?? null, ts: env.ts ?? null, msgId: env.msgId ?? null });
    }, { since });
    await new Promise(r => setTimeout(r, secs * 1000));
    return { ok: true, topic, region, listenedSec: secs, since, received: messages.length, messages };
  });
}
