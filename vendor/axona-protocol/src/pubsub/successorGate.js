// successorGate.js — Closer-Born Relay Inherits the Root v0.6 Part 6b,
// "hold the root only with a current successor" (David 2026-11-03 "go ahead
// with 6b"). Manager option successorGate / env SUCCESSOR_GATE, DEFAULT OFF.
//
// Every node may hold a root (Part 5). One whose process is not a service (a
// browser tab, a seat whose host app quits) cannot promise that its cache
// survives until a pull is acknowledged, so it states what it has: a root
// held by such a node (processKind 'app') counts its successors, and reports
// `degraded: no-successor` when none is CURRENT rather than promising a
// durability it lacks. It keeps the root either way.
//
// A successor is a replica (role.replicas), a standby seat, or the loser of a
// staged handover this node won (it served the pull this node completed).
// It is CURRENT when its last SUB to this root arrived within RENEW_MS and
// advertised a high-water at or above this root's high-water as it stood
// RENEW_MS ago — one renewal of lag — and its failure domain is not this
// node's: a successor whose SUB names the same host tag does not count. A
// successor that names no host counts; the rule can only see what is named.
// No ack frame is added: the SUB's hw is the possession evidence the root
// already receives.
//
// With no current successor the root re-seats: the replicate push on that
// tick is FULL (role.sync.sig cleared), once on entering and then at most
// once per RENEW_MS while it stays degraded. A service (processKind
// 'service', the default) is never gated: 6b states availability while a
// successor is current, not durability, and a service can crash too.

import { idHex, lc, isHexId } from './ids.js';
import { RENEW_MS } from './constants.js';

const SUCCESSORS_MAX = 32;

export const successorGateMethods = {
  // The failure-domain block this node's SUBs carry when the gate is armed.
  _fdBlock() {
    if (!this._successorGate) return null;
    return { kind: this._processKind, ...(this._hostTag ? { host: this._hostTag } : {}) };
  },

  // Root side, on every SUB: remember what a would-be successor advertised.
  _noteSuccessorSub(role, subHex, payload, seat) {
    if (!this._successorGate || !role?.isRoot || !isHexId(subHex)) return;
    const s = role.successors ??= new Map();
    const fd = (payload && typeof payload.fd === 'object' && payload.fd) ? payload.fd : null;
    const prior = s.get(subHex);
    const eligible = seat?.kind === 'standby' || role.replicas?.has(subHex) || prior?.via === 'loser';
    if (!eligible) return;
    s.set(subHex, {
      hw: Number.isFinite(payload?.hw) ? payload.hw : 0,
      at: this._now(),
      host: (fd && typeof fd.host === 'string') ? fd.host.slice(0, 32) : null,
      kind: (fd && typeof fd.kind === 'string') ? fd.kind.slice(0, 16) : null,
      via: prior?.via === 'loser' ? 'loser' : (seat?.kind === 'standby' ? 'standby' : 'replica'),
    });
    while (s.size > SUCCESSORS_MAX) s.delete(s.keys().next().value);
  },

  // A staged handover this node won: the loser served the whole pull.
  _noteLoserSuccessor(role, loserHex) {
    if (!this._successorGate || !isHexId(lc(loserHex))) return;
    (role.successors ??= new Map()).set(lc(loserHex), { hw: this._highWater(role), at: this._now(), host: null, kind: null, via: 'loser' });
  },

  // The root's high-water as it stood `ago` ms before now.
  _highWaterAgo(role, ago) {
    const cutoff = this._now() - ago;
    let hw = 0;
    for (const e of role.cache) { if (e.publishTs <= cutoff && e.publishTs > hw) hw = e.publishTs; }
    return hw;
  },

  // The current successors of a root, by the rule in the header.
  _currentSuccessors(role) {
    return this._currentSuccessorsWhere(role, { excludeHex: null, excludeHost: this._hostTag });
  },
  _currentSuccessorsWhere(role, { excludeHex = null, excludeHost = null } = {}) {
    const now = this._now();
    const need = this._highWaterAgo(role, RENEW_MS);
    const out = [];
    for (const [hex, s] of (role.successors ?? [])) {
      if (excludeHex && hex === excludeHex) continue;
      if (now - s.at > RENEW_MS) continue;
      if (s.hw < need) continue;
      if (excludeHost && s.host && s.host === excludeHost) continue;
      out.push(hex);
    }
    return out;
  },

  // 6b GATE (the loser's side): a SECOND holder for an ephemeral winner — a
  // current successor of this root that is not the winner and not on the
  // winner's host.
  // 6BG-1 (Orion 12a4f349): the second holder must hold what this root holds
  // NOW, not "one renewal of lag": with a cache younger than RENEW_MS the lag
  // rule's floor is 0, and an empty standby (hw 0) passed.
  _secondHolderCurrent(role, m) {
    const now = this._now(), need = this._highWater(role);
    const winner = lc(String(m.winner)), host = m.winnerHost ?? null;
    for (const [hex, s] of (role.successors ?? [])) {
      if (hex === winner || now - s.at > RENEW_MS || s.hw < need) continue;
      if (host && s.host && s.host === host) continue;
      return true;
    }
    return false;
  },

  // Per refreshTick: a step 5 deferred for want of a second holder runs once
  // one is current; past the lease, _mergeStep5 aborts and the root stays.
  _step5Tick() {
    if (!this._successorGate) return;
    for (const [t, role] of this.axonRoles) {
      for (const [corr, m] of (role.sync?.openMerges ?? [])) {
        if (!m.deferred) continue;
        const { leaseOk } = m.deferred;
        // 6BG-2 (Orion 12a4f349): the round must still be the one deferred —
        // this node root at the loser epoch, the round bound, its gate open.
        if (!role.isRoot || (role.epoch | 0) !== (m.loserEpoch | 0) || m.state !== 'bound' || role.merge?.gate?.corr !== corr) {
          m.deferred = null;
          this._log('warn', 'merge-step5-stale', { topic: idHex(t).slice(0, 12), corr: corr.slice(0, 8), isRoot: !!role.isRoot });
          // 6BG-3 (Orion a7e17f28): if this round's gate is still open, abort it
          // (unpin, close the corr, clear the candidate and the gate) so the next
          // round is not refused 'busy' until the sweep times the gate out.
          if (role.merge?.gate?.corr === corr) { this._mergeAbort(role, 'step5-stale'); continue; }
          this._mergeUnpinDeferred(role, m);
          this._mergeCloseCorr(role, corr);
          continue;
        }
        if (leaseOk() && !this._secondHolderCurrent(role, m)) continue;
        m.deferred = null;
        if (this._secondHolderCurrent(role, m)) this._log('info', 'merge-step5-released', { topic: idHex(t).slice(0, 12), corr: corr.slice(0, 8) });
        this._mergeStep5(role, t, corr, m, leaseOk);
      }
    }
  },

  // Per refreshTick, each root this node holds. Only an 'app' node is gated.
  _successorTick() {
    if (!this._successorGate || this._processKind === 'service') return;
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot) { if (role.degraded) role.degraded = null; continue; }
      const cur = this._currentSuccessors(role);
      if (cur.length === 0) {
        // Re-seat: the replicate push that follows this tick is FULL. Once on
        // entering, then at most once per RENEW_MS: a full push every tick was
        // the bandwidth fuel of the #332 role-bloat collapse.
        const now = this._now();
        if (role.sync && (role.reseatAt == null || now - role.reseatAt >= RENEW_MS)) { role.sync.sig = null; role.reseatAt = now; }
        if (role.degraded !== 'no-successor') {
          role.degraded = 'no-successor';
          this._log('warn', 'degraded', { topic: idHex(t).slice(0, 12), why: 'no-successor', known: role.successors?.size ?? 0 });
        }
      } else if (role.degraded === 'no-successor') {
        role.degraded = null; role.reseatAt = null;
        this._log('info', 'successor-current', { topic: idHex(t).slice(0, 12), successors: cur.map((h) => h.slice(0, 10)) });
      }
    }
  },

  // The role row's 6b state, for a topic this node holds.
  successorStatus(topicBig) {
    const role = this.axonRoles.get(topicBig);
    if (!role) return null;
    return { isRoot: !!role.isRoot, processKind: this._processKind, degraded: role.degraded ?? null, current: role.isRoot ? this._currentSuccessors(role).length : 0, known: role.successors?.size ?? 0 };
  },
};
