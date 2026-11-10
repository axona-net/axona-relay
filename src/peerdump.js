// =============================================================================
// peerdump.js — which nodes this relay holds a channel to, by node id.
//
// THE QUESTION (David 2026-11-09). "When you update the database for the
// known-network, do you also list the connected nodes to each node you query?"
// Until 0.198.0 the answer for a relay was no. `health-dump` printed a peer
// COUNT: src/healthdump.js reduced health().peers to its length and dropped
// health().nodeId, so a relay's view named neither itself nor one neighbour,
// and the known-network viewer could draw no edge from 48 of the fleet's nodes.
// The kernel held all of it. This module prints it.
//
// WHAT IT IS NOT. It is not an endpoint: no listener, no port, no query from
// the network. It is one more log line on the trigger `health-dump` already has
// (SIGUSR1; a request file on Windows, src/dumprequest.js). It is not
// `health-dump` with a longer body either: that line has readers and a size
// history (journald cuts a line at 49152 bytes), so the peer list rides in a
// line of its own, `peer-dump`, and `health-dump` is unchanged.
//
// WHAT A PEER IS HERE. A peer is a node this relay's transport has BOUND: the
// handshake completed and the channel carries an authenticated node id. That
// is transport.boundPeers(), the union of the bridge socket (at most one) and
// the WebRTC mesh. It is NOT the synaptome: the synaptome is routing-table
// membership and can name a node whose channel has closed. Synaptome entries
// with no bound channel are COUNTED apart, in `unbound`, so the two are never
// counted as one.
//
// `unbound` IS A NUMBER (0.200.0, David 2026-11-10 UTC: "build the 0.200.0 with
// the unbound count"). 0.198.0 and 0.199.0 printed the ids themselves. No
// reader used them: the collector kept their count and the viewer draws live
// channels only (Orion, council e5591be8). An id in a relay's log is a
// correlator on disk, and one that names a node this relay holds no channel
// to is the stalest kind. The line now names bound peers and nobody else.
// `unbound` is null when either list could not be read: the synaptome, or the
// bound channels it is subtracted from. Without the bound list every synaptome
// member would count as unbound, and that number is the synaptome size under
// a wrong name. A count that could not be made is never zero.
//
// LINK KIND. `ws` = the bridge socket and no mesh channel. `turn` = a mesh
// channel whose nominated candidate pair has a `relay` end (mesh.js getPeers:
// "Either end being 'relay' means this specific connection is going through
// TURN"). `webrtc` = a mesh channel whose candidate types are known and neither
// is a relay. `mesh` = a mesh channel whose candidate types are not yet read:
// unknown is printed as unknown and is never called direct.
//
// CONTRACT. Pure apart from the reads it is handed. Every read is guarded:
// this runs inside a signal handler on a production relay, absence prints
// null, and nothing here throws. `complete` is true only when boundPeers()
// returned an array, so a reader can tell "no peers" from "could not read".
// =============================================================================

const HEX66 = /^[0-9a-f]{66}$/;

/** A node id as 66 hex, or null. Accepts a bigint or an already-hex string. */
export function idHex(id) {
  if (typeof id === 'bigint') {
    if (id < 0n) return null;
    const h = id.toString(16).padStart(66, '0');
    return HEX66.test(h) ? h : null;
  }
  if (typeof id === 'string' && HEX66.test(id)) return id;
  return null;
}

const call = (fn, fallback = null) => { try { return fn(); } catch { return fallback; } };
const rtt = (v) => (typeof v === 'number' && Number.isFinite(v) && v >= 0 ? Math.round(v * 10) / 10 : null);
const str = (v) => (typeof v === 'string' && v.length > 0 && v.length <= 32 ? v : null);

/**
 * Build the `peer-dump` context.
 * @param {object} h          peer.health() (or null)
 * @param {object} transport  the relay's composite transport (or null)
 * @param {object} [meta]     { relay, kernel, now }
 */
export function buildPeerDump(h, transport, meta = {}) {
  const now = typeof meta.now === 'number' ? meta.now : Date.now();
  const self = idHex(h?.nodeId ?? null);

  const boundRaw = call(() => (typeof transport?.boundPeers === 'function' ? transport.boundPeers() : null));
  const complete = Array.isArray(boundRaw);

  // One row per mesh channel, keyed by mesh id.
  const meshRows = new Map();
  const mp = call(() => (typeof transport?.mesh?.getPeers === 'function' ? transport.mesh.getPeers() : null));
  if (Array.isArray(mp)) {
    for (const r of mp) { if (r && typeof r.peerId === 'string') meshRows.set(r.peerId, r); }
  }
  const webrtc = transport?.webrtc ?? null;
  const bridgeHex = idHex(call(() => transport?.bridgeNodeIdBig ?? null));

  const peers = [];
  const seen = new Set();
  for (const id of (complete ? boundRaw : [])) {
    const hex = idHex(id);
    if (!hex || hex === self || seen.has(hex)) continue;
    seen.add(hex);
    const meshId = call(() => (typeof webrtc?.meshIdFor === 'function' ? webrtc.meshIdFor(id) : null));
    const row = (typeof meshId === 'string' && meshRows.get(meshId)) || null;
    let link, rttMs, state = null, cls = null, openMs = null;
    if (typeof meshId === 'string') {
      const lc = str(row?.localCand), rc = str(row?.remoteCand);
      link = (lc === 'relay' || rc === 'relay') ? 'turn' : (lc && rc ? 'webrtc' : 'mesh');
      rttMs = rtt(row?.rttLast);
      state = str(row?.state);
      cls = str(call(() => (typeof webrtc?.classOf === 'function' ? webrtc.classOf(id) : null)));
      openMs = typeof row?.openedAt === 'number' && row.openedAt > 0 && row.openedAt <= now ? now - row.openedAt : null;
    } else {
      link = hex === bridgeHex ? 'ws' : null;
      rttMs = rtt(call(() => (typeof transport?.getLatency === 'function' ? transport.getLatency(id) : null)));
    }
    peers.push({ id: hex, link, rttMs, state, cls, openMs, bridge: hex === bridgeHex ? true : undefined });
  }

  // Synaptome members with no bound channel: routing-table entries, not links.
  // Counted, never listed. The set lives for this call and is not returned.
  // No count without the bound list: there is nothing to subtract.
  let unbound = null;
  if (complete && Array.isArray(h?.peers)) {
    const loose = new Set();
    for (const p of h.peers) {
      const hex = idHex(p);
      if (hex && hex !== self && !seen.has(hex)) loose.add(hex);
    }
    unbound = loose.size;
  }

  return {
    nodeId:   self,
    relay:    str(meta.relay),
    kernel:   str(meta.kernel),
    complete,
    count:    complete ? peers.length : null,
    synaptome: h?.synaptomeSize ?? null,
    peers,
    unbound,
  };
}
