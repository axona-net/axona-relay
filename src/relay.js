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

import { AxonaPeer, AxonaDomain, NeuronNode, regionName, resolveRegion }
  from '../vendor/axona-protocol/src/index.js';
import { webTransport }
  from '../vendor/axona-protocol/src/transport/web/index.js';
import { KERNEL_VERSION }
  from '../vendor/axona-protocol/src/transport/handshake.js';
import { WebSocketImpl } from './polyfill.js';

export { KERNEL_VERSION, regionName, resolveRegion };

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
    // NOTE: peerVersion is left unset on purpose. The bridge classifies the
    // client-hello `version` by major: ≥3 → peer-app floor, else kernel floor.
    // Unset ⇒ webTransport sends KERNEL_VERSION (2.x) ⇒ checked against the
    // kernel floor (2.28.0), which 2.29.0 clears. Sending the relay's own
    // 0.x version here would be classified kernel-namespace and REJECTED.
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
  const peer   = new AxonaPeer({ domain, node, identity, transport });

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
