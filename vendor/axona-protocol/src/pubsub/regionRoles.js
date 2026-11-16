// regionRoles.js — A ROLE GOES ONLY TO A NODE IN THE TOPIC'S REGION.
//
// David, 2026-11-13: "A topic role can only be assigned to a node that shares
// the region of that topic." Message sending is NOT affected: routing uses the
// best route whatever the region. A region with too few nodes may fail, and its
// history may be lost; the app is told no nodes were found in the region and
// may switch regions. The bridge region is not excluded outright: the rule is
// region EQUALITY and nothing more, so a bridge holds no role for an ordinary
// topic and may hold one for a topic minted in the bridge region.
//
// Design and the three council reads: axona-docs design/same-region-role-rule.md
// (c07a204). This REVERSES invariant I-16 for roles and keeps it for routing.
//
// DEFAULT OFF (option regionRoles; null = env REGION_ROLES). Unarmed, every
// method here answers as 4.137.0 did and nothing in the kernel compares regions.
//
// THE SHAPE (Orion ef78e8c7, Aster cd1da075):
//   · Routing is untouched. Greedy and the two-hop lookahead use every peer.
//   · The test is on SELF wherever a node takes a role itself: root birth,
//     promotion, the reachable-root claim, the self retry of a write, and the
//     receiver's commit of ADOPT, REPLICATE, HANDOFF and a staged handover.
//   · The test is on the LIST wherever a node picks another: backup cohort,
//     heir, write-retry target, verify nominee, child relay.
//   · A node outside the region that is the walk's end STOPS the walk. It does
//     not root, and it does not climb by the incapable-leaf fallback (that
//     path, reused on relays, walks uphill to MAX_HOPS). The handler returns
//     NOT consumed, so the sender's retry or renewal carries the message.
//   · A foreign root's beacon is still EVIDENCE (epoch, the merge trigger). It
//     is only never deferred to and never yielded to.
//   · It is NOT in the "am I alone?" guard, which has no topic and measures
//     reach: foreign peers are the route into the region.
//   · Reads are never filtered: an old holder outside the region stays
//     pullable until its history has been transferred.

import { extractS2Prefix } from '../utils/hexid.js';
import { idHex, idBig } from './ids.js';

const REGION_STOP_LOG_MS = 60_000;

/** Region code of an id, or null when the id is unreadable. */
export function regionOf(id) {
  try { return extractS2Prefix(typeof id === 'bigint' ? id : idBig(id)); } catch { return null; }
}

/** Pure: do the topic and the id share a region? Unreadable ids share nothing. */
export function sameRegion(topicBig, id) {
  const t = regionOf(topicBig), r = regionOf(id);
  return t !== null && r !== null && t === r;
}

export const regionRolesMethods = {
  /**
   * May `id` (default: this node) hold a role for the topic?
   * Unarmed: always. Armed: only in the topic's region.
   */
  _roleRegionOk(topicBig, id = this.nodeId) {
    if (!this._regionRoles) return true;
    return sameRegion(topicBig, id);
  },

  /** Armed and this node is outside the topic's region. */
  _regionIneligible(topicBig) {
    return !!this._regionRoles && !sameRegion(topicBig, this.nodeId);
  },

  /**
   * The walk ended at this node and this node may not root the topic. Count
   * it, log it at most once a minute per topic, and return UNDEFINED: the
   * routed handler's "not consumed", which the origin reads as not delivered.
   */
  _regionStop(type, topicBig) {
    const st = (this._regionStats ??= { stops: 0, byType: {}, refusals: {} });
    st.stops++;
    st.byType[type] = (st.byType[type] || 0) + 1;
    const now = this._now();
    const seen = (this._regionStopLogged ??= new Map());
    const last = seen.get(topicBig);
    if (last === undefined || now - last >= REGION_STOP_LOG_MS) {
      if (seen.size >= 512) seen.delete(seen.keys().next().value);   // bounded
      seen.set(topicBig, now);
      this._log('info', 'region-no-terminal', {
        topic: idHex(topicBig).slice(0, 12), type,
        topicRegion: regionOf(topicBig), selfRegion: regionOf(this.nodeId),
      });
    }
    return undefined;
  },

  /** A role offered to this node by another (ADOPT, REPLICATE, HANDOFF, staging) is refused. */
  _regionRefuse(kind, topicBig) {
    const st = (this._regionStats ??= { stops: 0, byType: {}, refusals: {} });
    st.refusals[kind] = (st.refusals[kind] || 0) + 1;
    // RATE-LIMITED (4.139.0). Every refusal is counted; the log line is at most
    // one a minute per (kind, topic). On testnet, 2026-11-13 20:21Z, an unarmed
    // bridge pushed REPLICATE at an armed one for 56 roots and the armed bridge
    // wrote 2,922 of these lines in 22 s. In a mixed fleet an unarmed root goes
    // on choosing an armed foreign backup, so the refusal recurs for as long
    // as the fleet is mixed.
    const now = this._now();
    const seen = (this._regionRefuseLogged ??= new Map());
    const key = kind + ':' + topicBig.toString(16);
    const last = seen.get(key);
    if (last === undefined || now - last >= REGION_STOP_LOG_MS) {
      if (seen.size >= 1024) seen.delete(seen.keys().next().value);   // bounded
      seen.set(key, now);
      this._log('info', 'role-refused', { topic: idHex(topicBig).slice(0, 12), why: 'region', kind, total: st.refusals[kind] });
    }
    return false;
  },

  /** Keep the ids that may hold a role for the topic. Order is preserved. */
  _regionFilter(topicBig, ids) {
    if (!this._regionRoles) return ids;
    return ids.filter((id) => sameRegion(topicBig, id));
  },

  inspectRegionRoles() {
    const st = this._regionStats ?? { stops: 0, byType: {}, refusals: {} };
    return {
      armed: !!this._regionRoles,
      selfRegion: regionOf(this.nodeId),
      stops: st.stops, byType: { ...st.byType }, refusals: { ...st.refusals },
    };
  },
};

export default regionRolesMethods;
