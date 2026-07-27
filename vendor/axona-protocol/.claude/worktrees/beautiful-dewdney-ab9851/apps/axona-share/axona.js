// axona.js — Axona connection for Axona-share. One peer; each channel is a topic
// string. The peer's OWN identity AND the topic anchor both come from the
// resolved region (?region=, default useast), so a regional deployment is a
// self-contained keyspace — local nodes root local channels and the bridge is
// only the rendezvous. (Was: both hardcoded us-east, which pinned every peer +
// topic to one region and locked out anyone elsewhere.)
import { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity, createAuthorIdentity, KERNEL_VERSION } from '/src/index.js?v=0.20.0';
import { webTransport } from '/src/transport/web/index.js?v=0.20.0';
import { resolveAnchor } from '../lib/region.js?v=0.20.0';

export { KERNEL_VERSION };          // surfaced in the app header (kernel-version visibility)

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net' : 'wss://bridge.axona.net');
const ANCHOR = resolveAnchor();                          // { token, name, code, center:{lat,lng} }
const REGION_TOKEN = ANCHOR.token;                       // the `region` field of every { region, name } topic

export const REGION = { token: ANCHOR.token, name: ANCHOR.name, code: ANCHOR.code };   // for the UI

export async function connectAxona(onStatus = () => {}) {
  onStatus(`connecting ${BRIDGE_URL} · region ${ANCHOR.name}…`);
  const nodeIdentity = await createNodeIdentity({ lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });   // ephemeral connection
  // Durable AUTHOR key — signs every publish (v0.3 key separation). Keyed per region
  // so each keyspace has its own stable author. persistAs load-or-creates in localStorage.
  const author       = await createAuthorIdentity({ persistAs: `axonashare:author:${REGION_TOKEN}` });
  const transport = webTransport({ bridgeUrl: BRIDGE_URL, identity: nodeIdentity });   // transport factory keeps `identity:`
  const node      = new NeuronNode({ id: BigInt('0x' + nodeIdentity.id), lat: ANCHOR.center.lat, lng: ANCHOR.center.lng });
  node.transport  = transport;
  const domain    = new AxonaDomain({ k: 20 });
  const peer      = new AxonaPeer({ domain, node, nodeIdentity, transport });

  await transport.start(nodeIdentity.id);
  await peer.start();
  const readyBy = Date.now() + 30000;
  while (Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
    onStatus(`forming mesh… (${node.synaptome?.size ?? 0})`);
    await new Promise((r) => setTimeout(r, 600));
  }
  await new Promise((r) => setTimeout(r, 1500));                       // settle so roots are reachable
  onStatus('connected');

  // A channel id (e.g. "axona-share/public-images") is the topic NAME; region is the
  // resolved anchor → a { region, name } descriptor every participant computes alike.
  const topicOf = (channelId) => ({ region: REGION_TOKEN, name: channelId });

  return {
    nodeId: nodeIdentity.id,
    // axona-share is a FILE/STREAM app: its canonical message convention is
    // @axona/protocol/std/chunk — the binary sibling of std/message (text). See
    // programmer-guide/Message-Convention. The app drives the chunk helpers
    // directly with the raw peer + author + topic descriptor:
    //   publishChunkedBytes(peer, bytes, { topic: topicOf(id), signWith: author })
    //   createReassembler(...) fed from peer.sub(topicOf(id), env => …, { since })
    // Chunk messages are OBJECTS on a { region, name } topic — there is NO
    // JSON-string wrapper (the old sub/pub helpers double-encoded and conflicted
    // with the object-message model std/chunk + the kernel use; removed).
    peer, author, topicOf,
    async unsub(topic) { try { return await peer.unsub?.(topicOf(topic)); } catch { /* */ } },
    async close() { try { await peer.leave?.(); } catch { /* */ } try { await transport.stop?.(); } catch { /* */ } },
  };
}
