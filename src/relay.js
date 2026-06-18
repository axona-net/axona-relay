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
 * Resolve a region token (name like "useast" or code like "0x89") to the
 * structured-topic region descriptor used by the v0.3 pub/sub API.
 *
 * v0.3 replaces the old "synthetic publisher = <s2-prefix>‖0^256" anchor with
 * a region NAME carried in the topic descriptor ({ region, name }). The topic
 * id's region byte is derived from this name, so a relay/CLI that opens
 * { region: 'useast', name: 'foo' } lands in the SAME keyspace as any app that
 * does the same — preserving the old region→keyspace mapping with no synthetic
 * publisher.
 *
 * @param {string} token  region name or code
 * @returns {{ code:number, name:string, center:{lat,lng} } | null}  null if unknown
 */
export function regionDescriptor(token = 'useast') {
  const code = resolveRegion(token);
  if (code == null) return null;
  return { code, name: regionName(code), center: regionCenter(code) };
}

/**
 * Build (but do not start) the relay peer.
 *
 * @param {object}   opts
 * @param {string}   opts.bridgeUrl   wss:// bridge for bootstrap + signaling
 * @param {object}   opts.identity    loaded Identity (stable nodeId)
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
  const peer   = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });

  return { peer, transport, node, domain };
}

/** Start the transport (bridge handshake) then the peer (mesh + routing). */
export async function startRelay({ peer, transport }) {
  await transport.start();
  await peer.start();
}

/** Best-effort graceful shutdown. */
export async function stopRelay({ peer, transport }) {
  try { await peer.leave?.(); } catch { /* ignore */ }
  try { await peer.stop?.(); }  catch { /* ignore */ }
  try { await transport.stop?.(); } catch { /* ignore */ }
}
