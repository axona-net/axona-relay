// ops.js — reusable connect + pub/sub/pull against a live Axona network.
//
// The shared core behind both the CLI (src/cli.js) and the MCP server
// (src/mcp.js): connect a fresh EPHEMERAL peer to the live Axona network
// (production by default — see network.js), wait for mesh readiness, run one
// operation, tear everything down.
//
// v0.3 topic addressing: topics are STRUCTURED descriptors { region, name }.
// The region NAME (e.g. "eagle") anchors the topic in a keyspace — replacing
// the old "synthetic publisher = <s2-prefix>‖0^256" anchor. An app and this
// relay both open { region: 'eagle', name: 'foo' } and meet on the same
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
export function regionToDescriptor(region = 'eagle') {
  const d = regionDescriptor(region);
  if (!d) throw new Error(`unknown region "${region}" (use a name like "eagle" or a code like "0x89")`);
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
export async function connectPeer({ region = 'eagle', bridge = DEFAULT_BRIDGE, readyTimeoutSec = 30, onError, author: providedAuthor, onLog } = {}) {
  const { name: regionName, center } = regionToDescriptor(region);
  // A caller may supply a DURABLE author (stable Author ID); it defaults to a
  // throwaway. There is deliberately NO way to supply a transport identity —
  // it is always minted fresh here (INVARIANT I-ID). The parameter used to
  // exist, and the persistent MCP session used it to pin one long-lived nodeId,
  // which made the peer correlatable across every restart. Removed 2026-07-25.
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const author   = providedAuthor   || await createEphemeralAuthor();
  const { peer, transport } = createRelay({ bridgeUrl: bridge, identity, region: center, onLog: onLog ?? (() => {}) });
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

export async function publish({ topic, message, region = 'eagle', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    const msgId = await peer.pub({ region: ctx.regionName, name: topic }, message, { signWith: ctx.author });
    await new Promise(r => setTimeout(r, 1500));        // let it propagate to roots
    return { ok: true, topic, region, msgId, signer: ctx.author.authorId, nodeId: ctx.nodeId };
  });
}

export async function pull({ topic, region = 'eagle', bridge = DEFAULT_BRIDGE } = {}) {
  return withConnectedPeer({ region, bridge }, async (peer, ctx) => {
    // Same defect the MCP layer had (2a9c611): a read that did not COMPLETE is
    // not an empty topic. 8s budget, and absence is reported only when observed.
    const startedAt = Date.now();
    let env = null, failure = null;
    try { env = await peer.pull(null, { topic: { region: ctx.regionName, name: topic }, timeoutMs: 8000 }); }
    catch (e) { failure = String((e && e.message) || e); }
    const elapsedMs = Date.now() - startedAt;
    const inconclusive = !!failure || (!env && elapsedMs >= 7200);
    return { ok: !inconclusive, topic, region,
             found: inconclusive ? null : !!env,          // null = UNKNOWN, never false
             message: env ? env.message : null, msgId: env?.msgId ?? null, elapsedMs,
             ...(inconclusive ? { reason: failure ? `pull failed: ${failure} — absence NOT established`
                                                  : 'no answer within 8000ms — absence NOT established' } : {}) };
  });
}

export async function subscribe({ topic, region = 'eagle', bridge = DEFAULT_BRIDGE, seconds = 20, since = 'all' } = {}) {
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
