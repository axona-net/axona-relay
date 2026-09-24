// mesh_degree.js — pure selection for a bounded WebRTC mesh degree.
//
// WHY THIS EXISTS. The mesh manager's own header says it maintains a channel
// "to every other peer in the mesh", and that is literally true: onPeerList
// initiates to every unknown peer and onPeerJoined accepts every arrival, with
// no bound anywhere. For a browser on a small mesh that is correct. For a
// BRIDGE it reproduces the problem the WebSocket degree cap was built to solve,
// one layer down and invisible to it: measured 2026-09-24, the east bridge held
// 17 inbound WebSockets against a cap of 15+2 while the west bridge held ONE
// inbound socket and 7 WebRTC peer connections, and its synaptome was exactly
// those 7. The cap governed one side of the node and not the other.
//
// WHAT THIS MODULE IS. The same two-axis choice the bridge's WebSocket
// graduation makes (graduation_select.js), adapted to what the mesh layer can
// actually see:
//
//   1. KEYSPACE BALANCE (primary) — release from the most over-represented
//      nodeId region, and NEVER a region's last representative, so the set we
//      keep always spans the address space.
//   2. AGE (secondary) — within that region, release the LONGEST-HELD open
//      channel: it has had the most time for routing to find another path to
//      that peer, and the newest channel is the one most likely to have been
//      opened because something needed it.
//
// WHAT IT CANNOT SEE, stated because the WebSocket side DOES see it. That path
// releases the BEST-MESHED peer, using the `meshBound` each client reports on
// its heartbeat — "the node whose departure the mesh best absorbs". No such
// report exists on the mesh path, so this is the same UPTIME FALLBACK the
// WebSocket side uses for peers that do not report. It is a weaker signal and
// the code should not pretend otherwise.
//
// THE OTHER THING IT CANNOT SEE is which peers carry obligations — a topic's
// root, an upstream, a cohort principal. The mesh layer holds channels, not
// roles. So the caller passes `protected`, and anything in it is never chosen.
// An empty `protected` set means the caller has told us nothing, NOT that
// nothing matters.
//
// Pure: no I/O, no timers, no manager state. The caller resolves each peer into
// a descriptor first, exactly as the bridge does for graduation_select.

/**
 * @typedef {Object} MeshCandidate
 * @property {string}  id         peerId (the opaque handle the caller retires)
 * @property {string|null} region nodeId keyspace region (top byte hex), null if unauthenticated
 * @property {number}  openedAt   epoch ms the data channel opened, 0 if never
 * @property {number|null} rttMs  smoothed round-trip, or null if unknown
 * @property {boolean} inCooldown retired too recently to be chosen again
 * @property {boolean} isProtected caller says this channel carries an obligation
 */

/**
 * Choose the single mesh peer to retire, or null if none is eligible.
 *
 * @param {MeshCandidate[]} candidates  every peer the mesh currently holds
 * @param {Object} opts
 * @param {number} opts.now
 * @param {number} opts.minUptimeMs  an open channel must be at least this old
 * @returns {{ id: string, region: string, ageMs: number, rttMs: number|null, basis: 'age' } | null}
 */
export function selectMeshRetire(candidates, { now, minUptimeMs }) {
  // Region occupancy over ALL peers with a known region — including the ones
  // that are not eligible — so a region holding one eligible and one protected
  // peer still counts as 2 and its eligible peer can be released.
  const regionCount = new Map();
  for (const c of candidates) {
    if (c.region != null) regionCount.set(c.region, (regionCount.get(c.region) || 0) + 1);
  }

  const eligible = candidates.filter((c) =>
    c.openedAt > 0                       // never retire a channel still negotiating
    && c.region != null                  // never retire an unauthenticated peer: no region, no balance
    && !c.inCooldown
    && !c.isProtected
    && (now - c.openedAt) >= minUptimeMs);
  if (eligible.length === 0) return null;

  // Primary: most over-represented region. Secondary: oldest open channel.
  // Tiebreak: worst RTT (unknown sorts as best, so a measured-bad path is
  // preferred over a guess — the same "known beats unknown" rule the WebSocket
  // side applies to meshBound).
  const rttKey = (c) => (c.rttMs == null ? -1 : c.rttMs);
  const sorted = [...eligible].sort((a, b) => {
    const rd = (regionCount.get(b.region) || 0) - (regionCount.get(a.region) || 0);
    if (rd !== 0) return rd;
    const ad = a.openedAt - b.openedAt;            // older first
    if (ad !== 0) return ad;
    return rttKey(b) - rttKey(a);
  });

  for (const c of sorted) {
    if ((regionCount.get(c.region) || 0) > 1) {
      return { id: c.id, region: c.region, ageMs: now - c.openedAt, rttMs: c.rttMs, basis: 'age' };
    }
  }
  return null;
}
