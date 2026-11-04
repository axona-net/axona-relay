// holderProbe.js — Closer-Born Relay Inherits the Root v0.6 Part 3a, "the
// newborn pulls from a holder, not from a neighbour" (David 2026-11-03 "go
// ahead with 3a"), with the Part 7 probe budget it needs. Manager option
// holderProbe / env HOLDER_PROBE, DEFAULT OFF.
//
// The empty-root probe ranked direct neighbours first, and a role-less
// neighbour answered its PULLUP consumed, so the three tries were spent on
// nodes that hold nothing. Armed:
//  · a probe PULLUP carries a `probe` nonce; a node with NO role for the
//    topic answers REPLAYACK { corr: nonce, disposition: 'no-role', from }.
//    A node with an old kernel, or unarmed, answers as before (silence).
//  · a try whose every target answered no-role is REFUNDED: it does not
//    count against EMPTY_ROOT_PROBE_MAX, and those nodes are never asked
//    again for this role.
//  · candidates are ordered by evidence of holding: nodes a SUB to this
//    empty root named (a subscriber that advertised history, and the
//    upstream its `via` named), then the lookup path, then neighbours.
//  · refunds make the try count unbounded, so a separate budget bounds the
//    probe: HOLDER_PROBE_FRAMES pulls and HOLDER_PROBE_WINDOW_MS from the
//    first try. When it is spent the probe stops and the root reports
//    degraded: no-holder-found (role.holderDegraded; a 'degraded' row).
// The REPLAYUP answer already returns over the route the PULLUP came by
// (return routes, a9ee5d5 / 64dfc9e).

import { idHex, idBig, lc, isHexId } from './ids.js';
import { T, HOLDER_PROBE_FRAMES, HOLDER_PROBE_WINDOW_MS } from './constants.js';

const EVIDENCE_MAX = 16, ROLELESS_MAX = 64;
const nonce = () => { const b = new Uint8Array(8); globalThis.crypto.getRandomValues(b); return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join(''); };

export const holderProbeMethods = {
  // _onSub at an empty root: what the SUB says about who holds history.
  _noteProbeEvidence(role, subHex, payload) {
    if (!this._holderProbe || !role?.isRoot || role.cache.length) return;
    const ev = (role.sync.probeEvidence ??= new Set());
    const selfHex = lc(idHex(this.nodeId));
    const add = (h) => { const x = lc(String(h)); if (isHexId(x) && x !== selfHex && ev.size < EVIDENCE_MAX) ev.add(x); };
    if (Number.isFinite(payload?.hw) && payload.hw > 0) add(subHex);
    for (const v of (Array.isArray(payload?.via) ? payload.via : [])) add(v);
  },

  // _onPullUp at a node with no role: a probe gets a NACK, nothing else does.
  _answerRolelessPull(payload) {
    if (!this._holderProbe || typeof payload?.probe !== 'string' || !isHexId(lc(String(payload.parentId)))) return false;
    let t; try { t = idBig(payload.topicId); } catch { return false; }
    if (this.axonRoles.has(t)) return false;
    this._route(idBig(payload.parentId), T.REPLAYACK, { topicId: idHex(t), corr: payload.probe.slice(0, 32), disposition: 'no-role', from: idHex(this.nodeId) });
    return true;
  },

  // _onReplayAck: a no-role NACK for one of this node's probe tries.
  _onProbeNack(payload) {
    let t; try { t = idBig(payload.topicId); } catch { return; }
    const role = this.axonRoles.get(t);
    const out = role?.sync?.probeOut;
    const from = lc(String(payload.from ?? ''));
    if (!out || out.nonce !== payload.corr || !out.targets.has(from)) return;
    (role.sync.roleless ??= new Set()).add(from);
    while (role.sync.roleless.size > ROLELESS_MAX) role.sync.roleless.delete(role.sync.roleless.values().next().value);
    out.nacked.add(from);
    if (out.nacked.size === out.targets.size && out.counted) {
      out.counted = false;
      role.sync.probeTries = Math.max(0, (role.sync.probeTries | 0) - 1);
      this._log('info', 'empty-root-probe-refunded', { topic: idHex(t).slice(0, 12), nacked: out.nacked.size, tries: role.sync.probeTries });
    }
  },

  // Before a try: is the budget spent? Marks the role degraded once.
  _probeBudgetSpent(role, t) {
    if (!this._holderProbe) return false;
    const now = this._now();
    const spent = (role.sync.probeFrames | 0) >= HOLDER_PROBE_FRAMES
      || (role.sync.probeStart != null && now - role.sync.probeStart >= HOLDER_PROBE_WINDOW_MS);
    if (spent && role.holderDegraded !== 'no-holder-found') {
      role.holderDegraded = 'no-holder-found';
      this._log('warn', 'degraded', { topic: idHex(t).slice(0, 12), why: 'no-holder-found', frames: role.sync.probeFrames | 0, roleless: role.sync.roleless?.size ?? 0 });
    }
    return spent;
  },

  // Candidate order for a try: evidence, then the lookup path, then the rest;
  // reachable first within each; never a node that answered no-role.
  _orderProbeCandidates(role, cand, pathSet, reach) {
    const ev = role.sync.probeEvidence ?? new Set();
    const rl = role.sync.roleless ?? new Set();
    const rank = (b) => { const h = lc(idHex(b)); return ev.has(h) ? 0 : pathSet.has(b) ? 1 : 2; };
    return [...cand].filter((b) => !rl.has(lc(idHex(b))))
      .sort((a, b) => (rank(a) - rank(b)) || ((reach(b) ? 1 : 0) - (reach(a) ? 1 : 0)));
  },

  _openProbeTry(role, targets) {
    const out = { nonce: nonce(), targets: new Set(targets.map((b) => lc(idHex(b)))), nacked: new Set(), counted: false };
    role.sync.probeOut = out;
    if (role.sync.probeStart == null) role.sync.probeStart = this._now();
    role.sync.probeFrames = (role.sync.probeFrames | 0) + targets.length;
    return out;
  },

  /** 3a state for a topic this node holds. */
  probeStatus(topicBig) {
    const role = this.axonRoles.get(topicBig);
    if (!role) return null;
    return { tries: role.sync.probeTries | 0, frames: role.sync.probeFrames | 0, roleless: role.sync.roleless?.size ?? 0, evidence: role.sync.probeEvidence?.size ?? 0, degraded: role.holderDegraded ?? null };
  },
};
