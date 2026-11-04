// standingLease.js — Closer-Born Relay Inherits the Root v0.6 Part 6c, "a
// lease for standing topics" (David 2026-11-03 "go ahead with 6c"). Manager
// option standingLease / env STANDING_LEASE, DEFAULT OFF.
//
// Jokes healed because an open app's metrics lease kept its EMPTY root alive
// long enough to be filled; council had no lease, and an empty root with no
// demand is dismantled DROP_MS after its last demand (Topic Lifetime 4.5),
// which ends its readers' subscriptions. A standing topic now carries a lease
// that keeps its root alive across the gap between a vanished holder and the
// next fresh read, held exactly where the metrics lease is held:
//  · a subscription made with { standing: true } declares it on its SUB
//    (`standing`: the lease it asks for, in ms); a child relay renews upstream
//    with what it still holds, so the lease reaches the root through the tree;
//  · the root keeps standingUntil = the latest declared expiry, bounded by
//    STANDING_LEASE_MAX_MS; while it is in the future the root is exempt from
//    the termination sweep and the empty-role reapers;
//  · a FULL REPLICATE carries standingRemainingMs, and a backup promoted to
//    root inherits it, so the lease survives the holder that held it.
// The cost is named in the packet: an idle root per standing topic, for at
// most STANDING_LEASE_MS after the last standing subscriber renewed.

import { idHex } from './ids.js';
import { STANDING_LEASE_MS, STANDING_LEASE_MAX_MS } from './constants.js';

export const standingLeaseMethods = {
  // What a SUB from this node declares: its own standing subscription, or the
  // remainder a relay role still holds for the seats below it.
  _standingForSub(role, sub) {
    if (!this._standingLease) return 0;
    const now = this._now();
    const own = sub && sub.standing ? STANDING_LEASE_MS : 0;
    const held = role && role.standingUntil > now ? role.standingUntil - now : 0;
    return Math.min(Math.max(own, held), STANDING_LEASE_MAX_MS);
  },

  // A holder receiving a SUB that declares a standing lease.
  _noteStandingSub(role, payload) {
    if (!this._standingLease || !role) return;
    const ms = Number(payload?.standing);
    if (!Number.isFinite(ms) || ms <= 0) return;
    const until = this._now() + Math.min(ms, STANDING_LEASE_MAX_MS);
    if (!(role.standingUntil > until)) {
      if (!(role.standingUntil > this._now())) this._log('info', 'standing-leased', { topic: idHex(role.topicId).slice(0, 12), ms: Math.min(ms, STANDING_LEASE_MAX_MS), root: !!role.isRoot });
      role.standingUntil = until;
    }
  },

  _standingLeased(role, now) {
    return !!(this._standingLease && role && role.standingUntil > now);
  },
};
