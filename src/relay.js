// relay.js — assemble a full Axona mesh peer that behaves as a relay/supernode.
//
// This is the SAME stack a browser peer runs (webTransport + NeuronNode +
// AxonaDomain + AxonaPeer); the only difference is it runs in Node with the
// node-datachannel RTCPeerConnection polyfill. As a relay it:
//   • forms authenticated WebRTC DataChannels with every other peer,
//   • participates in DHT routing (lookups forward through it),
//   • acts as a pub/sub ROOT AXON for topics it is K-closest to
//     (caches + fans out + replays — no app subscription required), and
//   • relays WebRTC signaling for other peers (meshRelay capability),
//     which is what lets two peers connect with no bridge in the path.
//
// It does NOT run a public WS server, mint TURN, or gate admission — those
// are bridge-only roles.

import { AxonaPeer, AxonaDomain, NeuronNode,
         regionName, resolveRegion, regionCenter, POPULATED_REGIONS }
  from '../vendor/axona-protocol/src/index.js';
import { webTransport }
  from '../vendor/axona-protocol/src/transport/web/index.js';
import { KERNEL_VERSION }
  from '../vendor/axona-protocol/src/transport/handshake.js';
import { WebSocketImpl } from './polyfill.js';

export { KERNEL_VERSION, regionName, resolveRegion, regionCenter, POPULATED_REGIONS };

/**
 * Resolve a region token (name like "eagle" or code like "0x89") to the
 * structured-topic region descriptor used by the v0.3 pub/sub API.
 *
 * v0.3 replaces the old "synthetic publisher = <s2-prefix>‖0^256" anchor with
 * a region NAME carried in the topic descriptor ({ region, name }). The topic
 * id's region byte is derived from this name, so a relay/CLI that opens
 * { region: 'eagle', name: 'foo' } lands in the SAME keyspace as any app that
 * does the same — preserving the old region→keyspace mapping with no synthetic
 * publisher.
 *
 * @param {string} token  region name or code
 * @returns {{ code:number, name:string, center:{lat,lng} } | null}  null if unknown
 */
export function regionDescriptor(token = 'eagle') {
  const code = resolveRegion(token);
  if (code == null) return null;
  return { code, name: regionName(code), center: regionCenter(code) };
}

/**
 * Build (but do not start) the relay peer.
 *
 * @param {object}   opts
 * @param {string}   opts.bridgeUrl   wss:// bridge for bootstrap + signaling
 * @param {object}   opts.identity    ephemeral transport Identity, minted fresh
 *                                    every process start and never persisted (I-ID)
 * @param {{lat:number,lng:number}} opts.region
 * @param {(level:string, event:string, ctx?:object)=>void} [opts.onLog]
 */
export function createRelay({ bridgeUrl, identity, region, onLog = () => {} }) {
  const transport = webTransport({
    bridgeUrl,
    identity:    { ...identity, id: identity.id },  // kernel id is already 66-char hex
    // NOTE: peerVersion is left unset on purpose ⇒ webTransport sends
    // KERNEL_VERSION in the client-hello (now 3.0.0 with the v0.3 kernel).
    // The bridge classifies the hello `version` by major: ≥3 → peer-app floor,
    // else kernel floor. Sending the relay's own 0.x version here would be
    // classified kernel-namespace and REJECTED, so we never override it.
    // TODO(deploy): KERNEL_VERSION crossed 2.x→3.0.0 — confirm the live bridge's
    // peer-app floor admits 3.0.0 (a 3.x hello is now classified peer-app, not
    // kernel). Verify against bridge.axona.net / testnet before rollout.
    meshRelay:     true,           // relay signaling for others (bridgeless help)
    reconnect:     true,           // a relay should self-heal the bridge link
    WebSocketImpl,
    log: (event, ctx) => onLog('debug', event, ctx),
  });

  const node = new NeuronNode({
    id:  BigInt('0x' + identity.id),
    lat: region.lat,
    lng: region.lng,
  });
  node.transport = transport;

  const domain = new AxonaDomain({ k: 20 });
  // v0.3: the AxonaPeer takes the NODE identity as `nodeIdentity:` (the
  // transport/connection key). There is NO `publishIdentity:` — publishes name
  // their author per-call via pub(..., { signWith }). The transport factory
  // above keeps `identity:` (it's the same node key, used for the auth hello).
  // Synaptome maintenance: REVERTED to off (2026-06-29) — enabling it on the
  // backbone regressed Howard's suite (0/9/6 failures across 3 runs; connection-count
  // storm + convergence wedge). Re-enable only behind a fix + Howard gate. Opt back in
  // via env once fixed: synaptomeMaintain: process.env.RELAY_SYNAPTOME_MAINTAIN==='1' ? {...} : null
  const peer   = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });

  return { peer, transport, node, domain };
}

/**
 * Bring the relay up: wire, then peer, then WAIT FOR THE MESH, then integrate.
 *
 * This mirrors kernel `connect()`'s lifecycle EXACTLY and on purpose — it is the
 * one bootstrap site for this repo (src/index.js, src/cli.js and src/ops.js all
 * come through here), so getting the order right here fixes every relay, every
 * CLI call, and every mcp-post/mcp-bot-post publisher at once.
 *
 * THE ORDER IS THE WHOLE POINT (fixed 2026-07-25). This function used to do:
 *
 *     await transport.start(); await peer.start();
 *     peer.integrate().catch(() => {});        // ← fire-and-forget
 *
 * with no `peer.ready()` in between. `integrate()` is `findKClosest(self)` plus
 * authenticated channel opens, so running it before the bridge welcome has
 * seeded ANY peers means it queries a near-empty routing table and does
 * essentially nothing — and because it was never awaited, a pub() could be
 * issued while the node was still unknown to its neighbours. Kernel connect.js
 * states the consequence outright: without effective self-integration a node
 * "sits at the passive-adoption churn floor and self-roots its topics as
 * SINGLETON roots in a sparse region — the cross-region pub/sub loss (fresh
 * subscribers read 0, publishers strand on leave)". That matches the live
 * #axona.bot symptom: K-closest populated, publishes vanish, reads empty.
 *
 * So: ready() FIRST (the welcome seeds peers to query), integrate() SECOND, and
 * AWAIT it, so a caller that got a resolved startRelay() is genuinely woven in.
 *
 * `peer.ready()` never throws — on timeout it returns { ready, peers, ms,
 * reason:'timeout' } — so awaiting it cannot wedge start-up. Pass `ready:false`
 * to skip the warm-up (integration then heals in the background, connect()'s
 * own semantics); pass an object to tune { minPeers, timeoutMs, stableMs }.
 *
 * Returns the readiness status plus what integration did, so a caller can log or
 * refuse to publish instead of guessing. Integration failure is non-fatal but no
 * longer INVISIBLE — the old `.catch(() => {})` swallowed it completely.
 */
export async function startRelay({ peer, transport, ready = {} }) {
  await transport.start();
  await peer.start();
  const status = (ready === false) ? null : await peer.ready(ready);

  let integrated = null;
  let integrateError = null;
  if (typeof peer.integrate === 'function') {
    const integrating = Promise.resolve(peer.integrate())
      .then((r) => { integrated = r ?? true; })
      .catch((e) => { integrateError = e; });
    if (ready !== false) await integrating;
  }
  return { ...(status ?? {}), integrated, integrateError };
}

/** Best-effort graceful shutdown. */
export async function stopRelay({ peer, transport }) {
  try { await peer.leave?.(); } catch { /* ignore */ }
  try { await peer.stop?.(); }  catch { /* ignore */ }
  try { await transport.stop?.(); } catch { /* ignore */ }
}
