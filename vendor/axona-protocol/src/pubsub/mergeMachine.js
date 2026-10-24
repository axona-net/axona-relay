// =====================================================================
// mergeMachine.js — AxonaManager mixin: DUAL-ROOT RECONCILIATION, Part 4(d)
// of the design packet (axona-docs/architecture/Axona-Dual-Root-
// Reconciliation-v0.1.md, header v0.15, SHA 14a6bd8; David "build it"
// 2026-10-17). Everything here is behind `mergeCapable` (OFF by default);
// an unarmed node never sets a gate, never cuts, never offers, never pulls.
//
// THE CUT AND THE GATE (stage d2). When the loser L binds a merge round it
// CUTS and GATES in one synchronous run: `role.gen` increments and
// `role.merge.gate = {winner, winnerEpoch, corr, at}` is set. While the gate
// is set L stamps nothing. The cut is defined AT THE COMMIT, not at the
// handler's entry: every commit path captures {role0, gen0} once before its
// first await (`_cutCapture`) and each mutation point (`_cachePush`,
// `_applyKill`) re-checks `role === role0 && role.gen === gen0`
// synchronously, immediately before the write (`_cutHolds`). The gate alone
// is not the test: a cut followed by a demotion clears the gate, and a check
// against the gate, or against a `gen` re-read after the await, would pass a
// write admitted before the cut. A cut increments `gen`; nothing decrements
// it.
//
// THE GATE'S FORWARD is its own contract (packet 4(d), "Placement" to
// "Unreachable or refusing W"):
//   · `_mergeGateIntercept` runs in _onPub / _onKill directly after
//     _holdIntercept, for bare writes only (a via-pinned write is somebody's
//     forward, possibly ours to W). `_reroute` is never called for a gated
//     write: in a split its topic-id walk can return to L.
//   · The write goes to W BY NAME through `_forwardToWinner`, the sibling of
//     `_forwardToRoot`: same flight, same ack routing, two differences. It
//     demotes on NO verdict (only step 5 moves L's claim), and it opens the
//     flight with an EXPLICIT epoch, the bound `winnerEpoch`, never the beacon
//     record and never 0, so W's ack at any other epoch discharges nothing.
//   · Re-entry: the forward carries a `gateFwd` nonce in a bounded ledger of
//     the hold's shape; a bare write carrying L's own nonce that returns is
//     undeliverable `merge-gate-return`, never forwarded again, never stamped.
//   · A `failed` verdict, or a flight that passes INGEST_ACK_MS without W's
//     ack, ends the round: the gate clears, L keeps root under its epoch.
//   · W re-minted (an ack from W at a different epoch) aborts the round and
//     records the new epoch on the candidate for the next OFFER.
//   · MERGE_TIMEOUT_MS bounds one gate interval from the bind (`_mergeSweep`).
//
// THE EXCHANGE (stage d3). Two roots for one topic, W the one that WINS
// under the Part 3 relation (rootClaim.claimWins), L the other. One merge
// per (topic, L, W) at a time, bound by `corr`, `winnerEpoch` and
// `loserEpoch` in every frame. Four messages, two of them new:
//   1. OFFER, L → W (`MERGE_OFFER`): from the candidate `_mergeCandidate`
//      recorded. L stays root, keeps stamping; `openMerges[corr]` = offered.
//   2. PULL, W → L (`PULLUP` with corr, winnerEpoch, cursor:null, timing
//      echoed): W evaluates the relation first (if the offer wins over W's
//      claim, W is the loser and offers instead), then the timing limits.
//   3. FRAMES, L → W: on the first PULLUP under corr L BINDS — timing must
//      equal what was offered — then CUTS and GATES (`_mergeCut`), fixes the
//      export (cache sorted by (publishTs, msgId) then the active dels),
//      measures it against EXPORT_BYTES_MAX, records tB on its monotonic
//      clock, and answers the HEADER frame (no data; bodies, dels, bytes,
//      hash). W admits or refuses on the header against its baseline plus
//      the manifest plus MERGE_WRITE_RESERVE, pins its baseline, and pulls
//      with the cursor. Each data frame is at most REPLAY_CHUNK_BYTES, never
//      ends inside an equal-stamp group unless the group alone exceeds a
//      frame, carries its manifest slice, and the terminal frame carries the
//      manifest's hash and counts. L validates every cursor against
//      lastCursorSent. W pins each frame's ids BEFORE ingesting it.
//   4. ACK, W → L (`REPLAYACK`), terminal only: retained and delsActive
//      computed against the manifest at ack time, the hash, tS recorded,
//      pins held; the record moves to `acked`.
//   5. STEP-DOWN at L: all six fields match, the lease holds (monotonic
//      now ≤ tB + A, wall clock not jumped past E), the ack is full; then,
//      synchronously, the lease is re-checked, `completing` is set, the
//      release token is minted and `demote(W)` runs with it — the one call
//      `_set` accepts on a role with custody. The gate clears with the flip;
//      the export stays pinned at L until MERGE_CLOSED.
//   Completion is correlated: every SUB L emits while `completing` carries
//   the merge block; W completes only on a SUB whose block matches the acked
//   record on all five fields, answers `MERGE_CLOSED`, converts pins to
//   ordinary custody. W's retention runs from tS for MERGE_RETENTION_MS
//   (= A + 2E + P); at its end the pins release and the record fails. L on
//   a lost completion keeps the export pinned as W's child until its own
//   TTL — the custodian rule, unconditional.
// =====================================================================
import {
  T, REPLAY_CHUNK_BYTES, FUTURE_TOLERANCE_MS,
  MERGE_TIMEOUT_MS, MERGE_GATE_FWD_MAX, MERGE_GATE_REENTRY_MS,
  MERGE_TIMING_VERSION, EXPORT_BYTES_MAX, MERGE_A_MIN_MS, MERGE_A_MAX_MS, MERGE_E_MAX_MS,
  MERGE_RETENTION_MS, MERGE_OFFER_TIMEOUT_MS, MERGE_WRITE_RESERVE_ENTRIES, MERGE_WRITE_RESERVE_BYTES,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { makeMergeState, RootClaim } from './rootClaim.js';
import { dispatchVerdict } from './dispatch.js';
import { sha256Hex } from './post.js';

const randomNonce = () => {
  const b = new Uint8Array(8); globalThis.crypto.getRandomValues(b);
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
};
const cursorOf = (e) => `${e.publishTs}:${e.msgId}`;
const sameTiming = (a, b) => !!a && !!b && a.version === b.version && a.A === b.A && a.E === b.E;

export const mergeMachineMethods = {

  // ── the cut ────────────────────────────────────────────────────────────
  /** Entry-time capture for a commit path: taken ONCE, synchronously, before the first await. */
  _cutCapture(role) {
    return role ? { role0: role, gen0: role.gen | 0 } : null;
  },
  /**
   * The check at the mutation point. True when the write may commit: the
   * role is the one captured, its generation has not moved, and no gate is
   * set (the cheap third refusal). A null `cut` (a caller with no capture)
   * is checked against the gate alone.
   */
  _cutHolds(role, cut) {
    if (!role) return false;
    if (role.merge?.gate) return false;
    // DISMANTLING (Topic Lifetime 4.5): a root whose termination began takes no
    // write; a stamped write arriving after the install is refused here and the
    // publisher's own re-send forms the next generation at a clean role.
    if (role.dismantling) return false;
    // RETIRED (Aster edfdd869, 4.116.0): a role the reaper detached carries the
    // mark `retired` (set by `_retireRole`, the one detach site) and no commit
    // resuming on it holds, replacement or not — a tombstone or body written
    // into a detached object has no custodian and the write would be acked
    // for nothing. A role absent from the map WITHOUT the mark is one a
    // construction path pushes into before registering (fixtures build roles
    // that way) and is still allowed: never registered is not retired.
    if (role.retired) return false;
    // MAP MEMBERSHIP (Aster c6719863 C3): a commit resuming after its role was
    // REPLACED in axonRoles — retired and re-admitted under a new object — is
    // refused exactly as a moved generation is: it would write into a detached
    // object while a live role for the topic exists. (The mark above now
    // refuses the detached object whether or not it was replaced; this check
    // stays for a replacement made without the reaper, e.g. a fixture swap.)
    const live = this.axonRoles.get(role.topicId);
    if (live !== undefined && live !== role) return false;
    if (!cut) return true;
    return role === cut.role0 && (role.gen | 0) === cut.gen0;
  },
  /**
   * CUT + GATE, one synchronous run, no await between. Called at the bind
   * and by the fences. A second cut on a gated role is refused.
   */
  _mergeCut(topicBig, { winner, winnerEpoch, corr }) {
    const role = this.axonRoles.get(topicBig);
    if (!role || !role.isRoot) return null;
    if (!role.merge) role.merge = makeMergeState();
    if (role.merge.gate) return null;
    role.gen = (role.gen | 0) + 1;
    role.merge.gate = { winner: lc(winner), winnerEpoch: winnerEpoch | 0, corr: String(corr), at: this._now() };
    role.merge.corr = String(corr);
    this._log('info', 'merge-cut', { topic: idHex(topicBig).slice(0, 12), winner: lc(winner).slice(0, 10), winnerEpoch: winnerEpoch | 0, corr: String(corr).slice(0, 8), gen: role.gen, cache: role.cache.length });
    return role.merge.gate;
  },
  /** Clear the gate (the round ended, by completion or abort). `gen` is never decremented. */
  _mergeGateClear(role, why, extra = {}) {
    const g = role?.merge?.gate;
    if (!g) return false;
    role.merge.gate = null;
    role.merge.corr = null;
    this._log('info', 'merge-gate-clear', { topic: idHex(role.topicId).slice(0, 12), why, corr: g.corr.slice(0, 8), heldMs: this._now() - g.at, ...extra });
    return true;
  },
  /**
   * ABORT a round at L: the gate clears, the open record is dropped, L keeps
   * root under its own epoch, forwarded writes the winner never acked are the
   * publisher's to re-send. The next trigger issues a new OFFER with a new corr.
   */
  _mergeAbort(role, why, extra = {}) {
    if (!role?.merge?.gate) return false;
    const corr = role.merge.gate.corr;
    this._log('warn', 'merge-abort', { topic: idHex(role.topicId).slice(0, 12), why, corr: corr.slice(0, 8), ...extra });
    this._mergeCloseCorr(role, corr);
    return this._mergeGateClear(role, why, extra);
  },
  /** A corr this node ended (abort, refusal, completion) stays closed: a late PULLUP under it is stale, never a fresh open. Bounded. */
  _mergeCloseCorr(role, corr) {
    role.sync.openMerges?.delete(corr);
    const closed = (role.merge.closedCorrs ??= new Set());
    closed.add(corr);
    while (closed.size > 64) closed.delete(closed.values().next().value);
  },
  /**
   * The sweep, from refreshTick: MERGE_TIMEOUT_MS bounds ONE gate interval
   * from the bind; an OFFER unanswered for MERGE_OFFER_TIMEOUT_MS runs the
   * LEGACY FALLBACK when the offer was reached (W drops MERGE_OFFER as an
   * unknown frame and never pulls under corr): L logs merge-legacy-fallback
   * and takes today's path, demote() with the SUB carrying hw and lw, so the
   * legacy W pulls what the shipped SPLIT_UNION rule pulls — the delta-only
   * weakness the packet names. An offer whose route FAILED is dropped and
   * retried on the next trigger; W's retention ends at tS + R.
   */
  _mergeSweep(now) {
    for (const role of this.axonRoles.values()) {
      const g = role.merge?.gate;
      if (g && now - g.at >= MERGE_TIMEOUT_MS) this._mergeAbort(role, 'timeout', { boundMs: MERGE_TIMEOUT_MS });
      for (const [corr, m] of role.sync.openMerges || []) {
        if (m.state === 'offered' && now - m.offeredAt >= MERGE_OFFER_TIMEOUT_MS) {
          this._mergeCloseCorr(role, corr);
          if (m.reached === false || !role.isRoot || role.merge?.gate) {
            this._log('warn', 'merge-offer-timeout', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8), winner: m.winner.slice(0, 10), reached: m.reached ?? null });
          } else {
            this._log('warn', 'merge-legacy-fallback', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8), winner: m.winner.slice(0, 10), cache: role.cache.length });
            role.merge.releaseToken = randomNonce() + randomNonce();
            this._rootClaim.demote(role.topicId, m.winner, 'merge-legacy-fallback', { releaseToken: role.merge.releaseToken });
          }
        }
      }
      for (const [corr, p] of role.sync.openPulls || []) {
        if (p.state === 'acked' && now - p.tS >= MERGE_RETENTION_MS) {
          this._mergeReleasePins(role, p, 'retention-expired');
          role.sync.openPulls.delete(corr);
          this._log('warn', 'merge-retention-expired', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8), loser: p.loser.slice(0, 10), heldMs: now - p.tS });
        } else if (p.state === 'pulling' && now - p.startedAt >= MERGE_TIMEOUT_MS) {
          this._mergeReleasePins(role, p, 'pull-timeout');
          role.sync.openPulls.delete(corr);
          this._log('warn', 'merge-pull-timeout', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8) });
        }
      }
    }
  },

  /**
   * CUSTODY-ALIVE (packet v0.14): a role holds custody of something a reaper
   * must not discard — an open merge or pull, a completion in flight, an
   * accepted-winner record, pinned entries, or ANY active tombstone (a
   * tombstone is custody of a deletion). Both role reapers, the role delete
   * and backup retirement read this and skip the role while it holds.
   * Discharge: correlated closure, or local expiry of every pin and tombstone.
   */
  _custodyAlive(role, now = this._now()) {
    if (!role) return false;
    const mg = role.merge;
    if (mg) {
      if (mg.completing || mg.gate) return true;
      if (mg.acceptedWinner && mg.acceptedWinner.exp > now) return true;
      if (mg.pins && mg.pins.size) return true;
    }
    if (role.sync?.openMerges?.size || role.sync?.openPulls?.size) return true;
    for (const t of role.tombstones.values()) if ((t?.exp ?? 0) > now) return true;
    return false;
  },

  // ── the gate's forward ─────────────────────────────────────────────────
  /**
   * In _onPub / _onKill directly after _holdIntercept. Returns true when the
   * write was handled here (forwarded, or marked undeliverable) and the
   * handler must return 'consumed' without stamping.
   */
  _mergeGateIntercept(topicBig, type, payload) {
    if (Array.isArray(payload.via) && payload.via.length) return false;   // a forward, never intercepted
    const role = this.axonRoles.get(topicBig);
    const gate = role?.merge?.gate || null;
    const fwd = role?.merge?.gateFwd || null;
    // RE-ENTRY: a bare write carrying a gateFwd token. Ours → consumed from the
    // ledger, undeliverable, never forwarded again, never stamped, whatever the
    // state of the gate (the publisher's flight recovers it). Unknown while
    // gated → fail closed. Unknown and no gate → nobody's forward that we can
    // tell; it is an ordinary bare write here.
    if (typeof payload.gateFwd === 'string') {
      const ours = !!(fwd && fwd.has(payload.gateFwd) && fwd.get(payload.gateFwd).type === type);
      if (ours) fwd.delete(payload.gateFwd);
      if (ours || gate) {
        this._log('info', 'merge-gate-reentry', { topic: idHex(topicBig).slice(0, 12), type, ours, gated: !!gate });
        this._undeliverable(type, topicBig, ours ? 'merge-gate-return' : 'merge-gate-return-unknown');
        return true;
      }
      return false;
    }
    if (!gate) return false;
    return this._mergeGateForward(topicBig, type, payload, role, gate);
  },
  _mergeGateForward(topicBig, type, payload, role, gate) {
    const now = this._now();
    const fwd = (role.merge.gateFwd ??= new Map());
    for (const [k, e] of fwd) { if (now - e.at >= MERGE_GATE_REENTRY_MS) fwd.delete(k); }
    if (fwd.size >= MERGE_GATE_FWD_MAX) { this._undeliverable(type, topicBig, 'merge-gate-saturated'); return true; }
    const nonce = randomNonce();
    fwd.set(nonce, { at: now, type, corr: gate.corr });
    this._log('info', 'merge-gate-forward', { topic: idHex(topicBig).slice(0, 12), type, to: gate.winner.slice(0, 10), corr: gate.corr.slice(0, 8) });
    this._forwardToWinner(topicBig, type, { ...payload, gateFwd: nonce }, gate);
    return true;
  },
  /**
   * The sibling of _forwardToRoot (AxonaManager.js): same dispatch, same
   * flight and ack routing, with (1) NO demote on any verdict and (2) the
   * flight opened under the EXPLICIT bound winnerEpoch. A `failed` verdict
   * ends the round. A `consumed` verdict is hop-local evidence and moves
   * nothing. The write is discharged at L only by W's ingest ack arriving at
   * _onIngestAck for this flight, bound to W and winnerEpoch.
   */
  _forwardToWinner(topicBig, type, payload, gate) {
    const declares = this.dht?.verdictsSupported;
    const w = gate.winner;
    let ackRoute = null;
    if (type === T.PUB || type === T.KILL) {
      ackRoute = this._flightOpen(topicBig, w, type, payload, payload?.attemptId ?? null, gate.winnerEpoch);
      const f = this._writeFlights?.get(`${topicBig.toString(16)}|${w}|${gate.winnerEpoch}`);
      if (f) f.merge = { corr: gate.corr };
    }
    const out = ackRoute
      ? { ...payload, via: [w], ackTo: ackRoute.ackTo, flightNonce: ackRoute.flightNonce, attemptId: ackRoute.attemptId }
      : { ...payload, via: [w] };
    const sent = this._send(type, out);
    Promise.resolve(sent).then((r) => {
      const v = dispatchVerdict(r, declares);
      if (v === 'violation') {
        this._log('error', 'pubsub:dispatch-contract-violation', { topic: idHex(topicBig).slice(0, 12), peer: w.slice(0, 12), detail: `adapter declares verdictsSupported but returned no verdict on ${type}` });
        return;
      }
      if (v !== 'failed') return;                              // consumed / unsupported: nothing moves
      const role = this.axonRoles.get(topicBig);
      if (role?.merge?.gate?.corr === gate.corr) this._mergeAbort(role, 'winner-unreachable', { type });
    });
  },

  /** Where the flight learns the round it belongs to is still open. */
  _mergeFlightLive(f) {
    if (!f?.merge) return false;
    const role = this.axonRoles.get(f.topicBig);
    return !!role?.merge?.gate && role.merge.gate.corr === f.merge.corr;
  },
  /** A merge flight's ack at the wrong epoch from the right node: W re-minted after the bind. */
  _mergeWinnerReminted(f, gotEpoch) {
    const role = this.axonRoles.get(f.topicBig);
    if (!role?.merge?.gate || role.merge.gate.corr !== f.merge.corr) return;
    role.merge.candidate = { id: f.rootHex, epoch: gotEpoch | 0, source: 'reminted', at: this._now() };   // the next OFFER carries the new epoch
    this._mergeAbort(role, 'winner-reminted', { got: gotEpoch | 0, want: f.epoch });
  },
  /** A merge flight passed INGEST_ACK_MS with no ack from W: the round ends. */
  _mergeFlightSilent(f) {
    const role = this.axonRoles.get(f.topicBig);
    if (!role?.merge?.gate || role.merge.gate.corr !== f.merge.corr) return;
    this._mergeAbort(role, 'winner-no-ack', { winner: f.rootHex.slice(0, 10) });
  },

  // ══════════════════════ THE EXCHANGE (stage d3) ══════════════════════
  _mergeTiming() { return { version: MERGE_TIMING_VERSION, A: MERGE_TIMEOUT_MS, E: FUTURE_TOLERANCE_MS }; },
  _mergeTimingAccepted(t) {
    return !!t && t.version === MERGE_TIMING_VERSION && Number.isInteger(t.A) && t.A >= MERGE_A_MIN_MS && t.A <= MERGE_A_MAX_MS && Number.isInteger(t.E) && t.E >= 0 && t.E <= MERGE_E_MAX_MS;
  },
  async _mergeManifestHash(bodies, dels) {
    return sha256Hex(`${bodies.join(',')}|${dels.join(',')}`);
  },

  // ── step 1 (L): OFFER from the recorded candidate ─────────────────────
  /**
   * Run from _mergeCandidate once a candidate is recorded (and from any
   * later trigger naming the same winner). One open merge per (topic, W):
   * a new OFFER to the same W with a higher loserEpoch supersedes.
   */
  _mergeOffer(topicBig, role) {
    const c = role.merge?.candidate;
    if (!this._mergeCapable || !role?.isRoot || !c || role.merge.gate || role.merge.completing) return false;
    for (const [corr, m] of role.sync.openMerges) {
      if (m.winner === c.id) {
        if (m.loserEpoch >= (role.epoch | 0)) return false;    // standing offer to this winner at this incarnation
        role.sync.openMerges.delete(corr);                      // superseded by a higher loserEpoch
      }
    }
    const corr = randomNonce() + randomNonce();
    const timing = this._mergeTiming();
    role.sync.openMerges.set(corr, { winner: c.id, winnerEpoch: c.epoch | 0, loserEpoch: role.epoch | 0, state: 'offered', snapshot: null, manifest: null, lastCursorSent: null, offeredAt: this._now(), boundAt: 0, tB: 0, timing });
    const rec = role.sync.openMerges.get(corr);
    const sent = this._route(idBig(c.id), T.MERGE_OFFER, {
      topicId: idHex(topicBig), corr, loser: idHex(this.nodeId), loserEpoch: role.epoch | 0,
      hw: this._highWater(role), lw: this._lowWater(role), count: role.cache.length, timing,
    });
    // Did the offer reach anyone? A failed verdict means W is unreachable, not
    // legacy: the sweep then drops the record instead of falling back.
    Promise.resolve(sent).then((r) => { const v = dispatchVerdict(r, this.dht?.verdictsSupported); rec.reached = v === 'failed' ? false : true; });
    this._log('info', 'merge-offer', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), winner: c.id.slice(0, 10), winnerEpoch: c.epoch | 0, loserEpoch: role.epoch | 0, count: role.cache.length });
    return true;
  },

  // ── step 2 (W): the OFFER arrives; evaluate the relation, then pull ────
  _onMergeOffer(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    if (!this._mergeCapable) return 'consumed';
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return 'consumed'; }
    const loser = lc(payload.loser);
    if (!isHexId(loser) || typeof payload.corr !== 'string' || idBig(loser) === this.nodeId) return 'consumed';
    const role = this.axonRoles.get(topicBig);
    if (!role || !role.isRoot) {
      // NOT A ROOT HERE: answer, never stay silent. Silence reads as a legacy
      // winner at the loser (the fallback) and would demote it toward a node
      // that holds no claim — seen on the sim when a hand-forked root at a
      // higher epoch had already absorbed the true root and then named it.
      this._log('info', 'merge-offer-not-root', { topic: idHex(topicBig).slice(0, 12), from: loser.slice(0, 10) });
      this._route(idBig(loser), T.REPLAYACK, { topicId: payload.topicId, corr: payload.corr, winner: idHex(this.nodeId), winnerEpoch: role?.epoch | 0, loser, loserEpoch: Number.isInteger(payload.loserEpoch) ? payload.loserEpoch : 0, disposition: 'not-root' });
      return 'consumed';
    }
    const loserEpoch = Number.isInteger(payload.loserEpoch) ? payload.loserEpoch : 0;
    // THE RELATION FIRST: W pulls only from a claim it wins over. If the
    // offered claim wins over W's own, W is the loser and offers instead.
    if (this._rootClaim.wins(topicBig, { id: loser, epoch: loserEpoch })) {
      this._log('info', 'merge-offer-loses', { topic: idHex(topicBig).slice(0, 12), from: loser.slice(0, 10), loserEpoch, myEpoch: role.epoch | 0 });
      // ANSWER, as the not-root branch does. Silence here reads as a legacy
      // winner at the offerer, whose sweep then demotes the claim that
      // outranks ours toward us (merge-legacy-fallback). Fleet logs: 3 against
      // 299 merge-step5 rounds in a 4.121.0 window, 1 against 198 in a
      // 4.122.0 window (by event name); the logs do not say which were this silence and which
      // were lost routes. The offerer keeps its claim; we offer. A lost answer
      // still falls back (the offer timeout); this closes the silent path only.
      this._route(idBig(loser), T.REPLAYACK, { topicId: payload.topicId, corr: payload.corr, winner: idHex(this.nodeId), winnerEpoch: role.epoch | 0, loser, loserEpoch, disposition: 'outranked' });
      this._mergeCandidate(topicBig, loser, loserEpoch, 'offer');
      return 'consumed';
    }
    // TIMING LIMITS, refused before any data.
    if (!this._mergeTimingAccepted(payload.timing)) {
      this._log('warn', 'merge-refused-timing', { topic: idHex(topicBig).slice(0, 12), corr: payload.corr.slice(0, 8), timing: payload.timing ?? null });
      this._route(idBig(loser), T.REPLAYACK, { topicId: payload.topicId, corr: payload.corr, winner: idHex(this.nodeId), winnerEpoch: role.epoch | 0, loser, loserEpoch, disposition: 'refused-timing' });
      return 'consumed';
    }
    if (role.sync.openPulls.has(payload.corr)) return 'consumed';
    if (!role.merge) role.merge = makeMergeState();
    role.sync.openPulls.set(payload.corr, { loser, loserEpoch, cursor: null, framesIn: 0, held: 0, sent: 0, startedAt: this._now(), timing: { ...payload.timing }, state: 'pulling', manifest: { bodies: [], dels: [] }, header: null, tS: 0 });
    this._mergePull(topicBig, role, payload.corr, null);
    this._log('info', 'merge-pull-start', { topic: idHex(topicBig).slice(0, 12), corr: payload.corr.slice(0, 8), loser: loser.slice(0, 10), loserEpoch, offered: payload.count ?? null });
    return 'consumed';
  },
  _mergePull(topicBig, role, corr, cursor) {
    const p = role.sync.openPulls.get(corr);
    if (!p) return;
    this._emitPullUp(idBig(p.loser), { topicId: idHex(topicBig), corr, parentId: idHex(this.nodeId), winnerEpoch: role.epoch | 0, cursor, sinceHw: 0, timing: { ...p.timing } });
  },

  // ── step 3 (L): the bound PULLUP, the cut, the header, the frames ──────
  _mergeAnswerPull(payload, meta) {
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return; }
    const role = this.axonRoles.get(topicBig);
    const corr = payload.corr;
    const requester = lc(payload.parentId);
    if (!role || !isHexId(requester)) return;
    const winnerEpoch = Number.isInteger(payload.winnerEpoch) ? payload.winnerEpoch : 0;
    let m = role.sync.openMerges.get(corr);
    if (m && m.winner !== requester) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'requester' }); return; }
    if (!m) {
      if (role.merge?.closedCorrs?.has(corr)) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'closed-corr' }); return; }
      // No offer of ours: open the record only when the requester's claim wins
      // over L's own under the relation; anything else is stale.
      if (!this._mergeCapable || !role.isRoot || !this._rootClaim.wins(topicBig, { id: requester, epoch: winnerEpoch })) {
        this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'no-offer-and-not-winner', from: requester.slice(0, 10) });
        return;
      }
      if (!this._mergeTimingAccepted(payload.timing)) { this._log('warn', 'merge-refused-timing', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), timing: payload.timing ?? null }); return; }
      m = { winner: requester, winnerEpoch, loserEpoch: role.epoch | 0, state: 'offered', snapshot: null, manifest: null, lastCursorSent: null, offeredAt: this._now(), boundAt: 0, tB: 0, timing: { ...payload.timing } };
      role.sync.openMerges.set(corr, m);
    }
    if (m.state === 'offered') {
      // THE BIND. Timing must equal what was offered, as numbers.
      if (!sameTiming(payload.timing, m.timing)) {
        this._log('warn', 'merge-refused-timing', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), offered: m.timing, echoed: payload.timing ?? null });
        role.sync.openMerges.delete(corr);
        return;
      }
      if (!role.isRoot || role.merge?.gate || role.merge?.completing) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: role.isRoot ? 'busy' : 'not-root' }); return; }
      m.winnerEpoch = winnerEpoch;
      // CUT + GATE, then the export, all in this synchronous run.
      const gate = this._mergeCut(topicBig, { winner: requester, winnerEpoch, corr });
      if (!gate) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'cut-refused' }); return; }
      const bodies = role.cache.slice().sort((a, b) => a.publishTs - b.publishTs || (a.msgId < b.msgId ? -1 : a.msgId > b.msgId ? 1 : 0))
        .map((c) => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq, bytes: c.bytes }));
      const dels = this._activeDels(role).sort((a, b) => a.killTs - b.killTs || (a.msgId < b.msgId ? -1 : a.msgId > b.msgId ? 1 : 0))
        .map((d) => ({ ...d, publishTs: d.killTs }));
      const manifest = { bodies: bodies.map((b) => b.msgId), dels: dels.map((d) => d.msgId) };
      const bytes = JSON.stringify({ bodies, dels, manifest }).length;
      if (bytes > EXPORT_BYTES_MAX) {
        this._log('warn', 'refused-export-bound', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), bytes, bound: EXPORT_BYTES_MAX });
        this._mergeCloseCorr(role, corr);
        this._mergeGateClear(role, 'export-bound');
        this._emitReplayUp(idBig(requester), { topicId: payload.topicId, corr, loserEpoch: role.epoch | 0, refused: 'export-bound', msgs: [], dels: [] });
        return;
      }
      m.snapshot = { bodies, dels, items: [...bodies.map((b) => ({ kind: 'body', e: b })), ...dels.map((d) => ({ kind: 'del', e: d }))] };
      m.manifest = manifest; m.bytes = bytes;
      m.state = 'bound'; m.boundAt = this._now(); m.tB = this._mono(); m.tBWall = this._now();
      m.lastCursorSent = 'h';
      this._mergeManifestHash(manifest.bodies, manifest.dels).then((hash) => {
        m.hash = hash;
        this._emitReplayUp(idBig(requester), { topicId: payload.topicId, corr, loserEpoch: role.epoch | 0, header: { bodies: manifest.bodies.length, dels: manifest.dels.length, bytes, hash }, cursor: null, more: true, msgs: [], dels: [] });
        this._log('info', 'merge-bound', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), winner: requester.slice(0, 10), winnerEpoch, bodies: manifest.bodies.length, dels: manifest.dels.length, bytes });
      });
      return;
    }
    if (m.state !== 'bound') { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: m.state }); return; }
    // A DATA FRAME. The cursor must equal the last one we sent.
    const want = m.lastCursorSent;
    const got = payload.cursor == null ? null : String(payload.cursor);
    if (got !== want) {
      this._log('warn', 'merge-cursor-invalid', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), got: got ? got.slice(-12) : null, want: want ? want.slice(-12) : null });
      return;                                                   // W restarts with a new corr after its timeout
    }
    const items = m.snapshot.items;
    let start = 0;
    if (want !== 'h') { while (start < items.length && cursorOf(items[start].e) !== want) start++; start++; }
    const sizeOf = (it) => (it.e.json ? it.e.json.length : 0) + 160;
    // Each frame is at most REPLAY_CHUNK_BYTES and NEVER ENDS INSIDE AN
    // EQUAL-STAMP GROUP: when the next entry would overflow the frame, the
    // frame ends BEFORE the group that entry belongs to (members of that group
    // already placed move to the next frame), unless the group alone exceeds a
    // frame, in which case the frame may end inside it and the (publishTs,
    // msgId) pair still advances within it.
    const groupBytes = (j) => { let b = 0; const ts = items[j].e.publishTs; let k = j; while (k >= start && items[k].e.publishTs === ts) k--; k++; for (; k < items.length && items[k].e.publishTs === ts; k++) b += sizeOf(items[k]); return b; };
    let i = start, bytes = 0;
    while (i < items.length) {
      const size = sizeOf(items[i]);
      if (bytes + size > REPLAY_CHUNK_BYTES && i > start) {
        const ts = items[i].e.publishTs;
        if (items[i - 1].e.publishTs === ts && groupBytes(i) <= REPLAY_CHUNK_BYTES) {
          while (i > start && items[i - 1].e.publishTs === ts) i--;   // end before the group
        }
        break;
      }
      bytes += size; i++;
    }
    if (i === start && start < items.length) i = start + 1;             // one entry alone never stalls the export
    const msgs = [], dels = [], slice = { bodies: [], dels: [] };
    for (let k = start; k < i; k++) {
      const it = items[k];
      if (it.kind === 'body') { msgs.push({ json: it.e.json, publishTs: it.e.publishTs, msgId: it.e.msgId, seq: it.e.seq }); slice.bodies.push(it.e.msgId); }
      else { const { publishTs: _p, ...d } = it.e; dels.push(d); slice.dels.push(it.e.msgId); }
    }
    const more = i < items.length;
    const last = i > start ? cursorOf(items[i - 1].e) : want;
    m.lastCursorSent = last;
    const frame = { topicId: payload.topicId, corr, loserEpoch: role.epoch | 0, cursor: want, lastCursor: last, more, msgs, dels, slice };
    if (!more) frame.manifest = { bodies: m.manifest.bodies.length, dels: m.manifest.dels.length, hash: m.hash };
    this._emitReplayUp(idBig(requester), frame);
    this._log('debug', 'merge-frame', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), msgs: msgs.length, dels: dels.length, more });
  },

  // ── step 3 (W): ingest a frame under corr; header admission; pins ──────
  async _mergeIngestFrame(payload, meta) {
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return; }
    const role = this.axonRoles.get(topicBig);
    const corr = payload.corr;
    const p = role?.sync.openPulls.get(corr);
    if (!role || !p) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: String(corr).slice(0, 8), why: 'no-pull' }); return; }
    if (p.state !== 'pulling') return;
    if (Number.isInteger(payload.loserEpoch) && payload.loserEpoch !== p.loserEpoch) {
      this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'loserEpoch', got: payload.loserEpoch, want: p.loserEpoch });
      return;
    }
    if (payload.refused) {
      this._log('warn', 'merge-pull-refused', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: payload.refused });
      role.sync.openPulls.delete(corr);
      return;
    }
    if (payload.header) {
      // ADMISSION, SIZED AT THE CUT: baseline (every entry and active
      // tombstone W holds now) + the header's bodies and bytes +
      // MERGE_WRITE_RESERVE must fit under the caps. Fail closed.
      const h = payload.header;
      const baselineN = role.cache.length, baselineB = role.cacheBytes;
      const needN = baselineN + (h.bodies | 0) + MERGE_WRITE_RESERVE_ENTRIES;
      const needB = baselineB + (h.bytes | 0) + MERGE_WRITE_RESERVE_BYTES;
      if (needN > this._cacheMax || needB > this._cacheBytes) {
        this._log('warn', 'refused-capacity', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), baselineN, baselineB, bodies: h.bodies, bytes: h.bytes, cacheMax: this._cacheMax, cacheBytes: this._cacheBytes });
        this._route(idBig(p.loser), T.REPLAYACK, { topicId: payload.topicId, corr, winner: idHex(this.nodeId), winnerEpoch: role.epoch | 0, loser: p.loser, loserEpoch: p.loserEpoch, disposition: 'refused-capacity', retained: 0, delsActive: 0 });
        role.sync.openPulls.delete(corr);
        return;
      }
      p.header = { ...h };
      if (!role.merge) role.merge = makeMergeState();
      for (const c of role.cache) role.merge.pins.add(c.msgId);        // pin the baseline
      role.merge.reserve = { entries: MERGE_WRITE_RESERVE_ENTRIES, bytes: MERGE_WRITE_RESERVE_BYTES };
      p.cursor = 'h';
      this._log('info', 'merge-admitted', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), bodies: h.bodies, dels: h.dels, bytes: h.bytes, baselineN });
      this._mergePull(topicBig, role, corr, 'h');
      return;
    }
    // A DATA FRAME: cursor continuity, pins before ingest, counts, next pull or terminal.
    const got = payload.cursor == null ? null : String(payload.cursor);
    if (got !== p.cursor) { this._log('warn', 'merge-cursor-invalid', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), side: 'winner' }); return; }
    const slice = payload.slice || { bodies: [], dels: [] };
    for (const id of slice.bodies) { role.merge.pins.add(id); p.manifest.bodies.push(id); }
    for (const id of slice.dels) { role.merge.pins.add(id); p.manifest.dels.push(id); }   // a manifest tombstone is pinned too: it must be active at the ack and until completion
    const cut = this._cutCapture(role);
    const delsOk = await this._applyDels(role, topicBig, payload.dels, cut);
    const tally = delsOk ? await this._ingestStampedBatch(role, payload.msgs, cut) : { sent: (payload.msgs || []).length, held: 0, rejected: (payload.msgs || []).length, refused: true };
    p.framesIn++; p.held += tally.held; p.sent += tally.sent;
    if (!delsOk || tally.refused) { this._log('warn', 'merge-frame-refused', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8) }); }
    p.cursor = payload.lastCursor == null ? p.cursor : String(payload.lastCursor);
    if (payload.more) { this._mergePull(topicBig, role, corr, p.cursor); return; }
    // TERMINAL: the retention check against the manifest, at ack time.
    const man = payload.manifest || { bodies: 0, dels: 0, hash: null };
    const hash = await this._mergeManifestHash(p.manifest.bodies, p.manifest.dels);
    let retained = 0, delsActive = 0;
    const now = this._now();
    for (const id of p.manifest.bodies) if (role.cacheIds.has(id) || (role.tombstones.get(id)?.exp ?? 0) > now) retained++;
    for (const id of p.manifest.dels) if ((role.tombstones.get(id)?.exp ?? 0) > now) delsActive++;
    const full = retained === man.bodies && delsActive === man.dels && p.manifest.bodies.length === man.bodies && p.manifest.dels.length === man.dels && hash === man.hash;
    p.state = 'acked'; p.tS = now; p.hash = hash; p.full = full;
    this._route(idBig(p.loser), T.REPLAYACK, { topicId: payload.topicId, corr, winner: idHex(this.nodeId), winnerEpoch: role.epoch | 0, loser: p.loser, loserEpoch: p.loserEpoch, retained, delsActive, hash, disposition: full ? 'full' : 'short' });
    this._log(full ? 'info' : 'warn', 'merge-acked', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), loser: p.loser.slice(0, 10), retained, delsActive, bodies: man.bodies, dels: man.dels, full, frames: p.framesIn });
  },
  _mergeReleasePins(role, p, why) {
    if (!role.merge) return;
    role.merge.pins.clear();
    role.merge.reserve = null;
    this._log('info', 'merge-pins-released', { topic: idHex(role.topicId).slice(0, 12), why });
  },

  // ── step 5 (L): the terminal ack; the lease; the flip with the token ───
  _onReplayAck(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return 'consumed'; }
    const role = this.axonRoles.get(topicBig);
    const corr = payload.corr;
    const m = role?.sync.openMerges.get(corr);
    if (!role || !m) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: String(corr).slice(0, 8), why: 'no-record' }); return 'consumed'; }
    // A REFUSAL binds on corr, winner and loser: a refusing node may not hold
    // the epoch the offer named (a non-root has none), so the six-field match
    // below is for the terminal ack only.
    const three = lc(payload.winner) === m.winner && lc(payload.loser) === lc(idHex(this.nodeId)) && lc(payload.topicId) === idHex(topicBig);
    if (payload.disposition === 'refused-timing' || payload.disposition === 'refused-capacity' || payload.disposition === 'not-root' || payload.disposition === 'outranked') {
      if (!three) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'refusal-fields' }); return 'consumed'; }
      this._log('warn', 'merge-refused', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: payload.disposition });
      this._mergeCloseCorr(role, corr);
      if (role.merge?.gate?.corr === corr) this._mergeGateClear(role, payload.disposition);
      // A winner that holds no claim is no candidate: drop the record so the
      // next trigger evaluates afresh instead of re-offering to a child. A
      // winner our claim outranks is no candidate either: it offers to us.
      if ((payload.disposition === 'not-root' || payload.disposition === 'outranked') && role.merge?.candidate?.id === m.winner) role.merge.candidate = null;
      return 'consumed';
    }
    const six = three && (payload.winnerEpoch | 0) === (m.winnerEpoch | 0) && (payload.loserEpoch | 0) === (m.loserEpoch | 0);
    if (!six) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: 'fields' }); return 'consumed'; }
    if (m.state !== 'bound') { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), why: m.state }); return 'consumed'; }
    // THE LEASE, anchored at the bind: monotonic now ≤ tB + A, and no wall-clock jump past E.
    const leaseOk = () => {
      const dm = this._mono() - m.tB, dw = this._now() - m.tBWall;
      return dm <= m.timing.A && Math.abs(dw - dm) <= m.timing.E;
    };
    if (!leaseOk()) {
      this._log('warn', 'merge-ack-expired', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), sinceBindMs: Math.round(this._mono() - m.tB), A: m.timing.A });
      this._mergeAbort(role, 'ack-expired');
      return 'consumed';
    }
    // THE RETENTION CHECK: every manifest body held or tombstoned at W, every
    // manifest tombstone active at W, the hash matches. Anything less is short.
    const full = payload.disposition === 'full' && (payload.retained | 0) === m.manifest.bodies.length && (payload.delsActive | 0) === m.manifest.dels.length && payload.hash === m.hash;
    if (!full) {
      this._log('warn', 'merge-ack-short', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), retained: payload.retained, delsActive: payload.delsActive, bodies: m.manifest.bodies.length, dels: m.manifest.dels.length, hashOk: payload.hash === m.hash });
      this._mergeAbort(role, 'ack-short');
      return 'consumed';
    }
    // STEP 5, synchronous from here to the flip: re-check the lease, pin the
    // export, mint the token, demote with it. The SUB demote() sends is an
    // ordinary renewal; completion is claimed AFTER the flip, below.
    if (!leaseOk()) { this._log('warn', 'merge-ack-expired', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), at: 'commit' }); this._mergeAbort(role, 'ack-expired'); return 'consumed'; }
    for (const id of m.manifest.bodies) role.merge.pins.add(id);
    for (const id of m.manifest.dels) role.merge.pins.add(id);
    role.merge.releaseToken = randomNonce() + randomNonce();
    const tok = role.merge.releaseToken;
    const gateCorr = role.merge.gate?.corr;
    const stepAt = this._now();
    role.merge.acceptedWinner = { id: m.winner, epoch: m.winnerEpoch | 0, at: stepAt, exp: stepAt + (this._stepDownHoldMs | 0) };
    const ok = this._rootClaim.demote(topicBig, m.winner, 'merge-step5', { releaseToken: tok, holdAlways: true });
    if (!ok) {
      role.merge.releaseToken = null; role.merge.acceptedWinner = null;
      this._log('error', 'merge-flip-refused', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8) });
      this._mergeAbort(role, 'flip-refused');
      return 'consumed';
    }
    if (gateCorr === corr) this._mergeGateClear(role, 'completed');
    m.state = 'completed';
    // THE COMMIT GUARD (assumption 7): the synchronous run from the recheck to
    // the completed flip must not have been suspended past the lease. L cannot
    // prevent it and detects it only here, after the fact: the flip stands (the
    // claim is W's) but NO COMPLETION IS CLAIMED — no merge-bearing SUB goes
    // out, the export stays pinned at L as W's child until L's own TTL (the
    // custodian rule), and W's record fails at tS + R. The data is safe; W's
    // union is not claimed.
    const late = this._mono() - m.tB > m.timing.A;
    this._log(late ? 'warn' : 'info', late ? 'merge-late-flip' : 'merge-step5', { topic: idHex(topicBig).slice(0, 12), corr: corr.slice(0, 8), winner: m.winner.slice(0, 10), winnerEpoch: m.winnerEpoch | 0, bodies: m.manifest.bodies.length, dels: m.manifest.dels.length, sinceBindMs: Math.round(this._mono() - m.tB) });
    if (late) return 'consumed';
    // COMPLETION IS CLAIMED: the merge block rides every SUB from here until
    // MERGE_CLOSED; the first asks for everything (since-all) so L converges.
    role.merge.completing = { corr, winner: m.winner, winnerEpoch: m.winnerEpoch | 0, loserEpoch: m.loserEpoch | 0, hash: m.hash, sinceAll: true };
    this._sendSubscribe(topicBig);
    return 'consumed';
  },

  // ── completion (W): the merge-bearing SUB; MERGE_CLOSED ────────────────
  _mergeCompleteOnSub(role, subHex, payload) {
    const b = payload.merge;
    const corr = typeof b.corr === 'string' ? b.corr : null;
    const p = corr ? role.sync.openPulls.get(corr) : null;
    const selfHex = idHex(this.nodeId);
    if (!p || p.state !== 'acked') { this._log('warn', 'merge-stale', { topic: idHex(role.topicId).slice(0, 12), corr: String(corr).slice(0, 8), why: p ? p.state : 'no-acked-record', from: subHex.slice(0, 10) }); return false; }
    const via = Array.isArray(payload.via) && payload.via.length ? lc(payload.via[0]) : null;
    const match = lc(b.winner) === selfHex && (b.winnerEpoch | 0) === (role.epoch | 0) && (b.loserEpoch | 0) === (p.loserEpoch | 0) && b.hash === p.hash && subHex === p.loser && (via === null || via === selfHex);
    if (!match || !p.full) { this._log('warn', 'merge-stale', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8), why: p.full ? 'block-mismatch' : 'short-ack', from: subHex.slice(0, 10) }); return false; }
    this._mergeReleasePins(role, p, 'completed');                 // pinned entries become ordinary custody
    role.sync.openPulls.delete(corr);
    this._route(idBig(subHex), T.MERGE_CLOSED, { topicId: idHex(role.topicId), corr, winnerEpoch: role.epoch | 0 });
    this._log('info', 'merge-complete', { topic: idHex(role.topicId).slice(0, 12), corr: corr.slice(0, 8), loser: subHex.slice(0, 10), held: p.held, sent: p.sent, sinceAckMs: this._now() - p.tS });
    return true;
  },
  _onMergeClosed(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return 'consumed'; }
    const role = this.axonRoles.get(topicBig);
    const c = role?.merge?.completing;
    if (!c || c.corr !== payload.corr || (payload.winnerEpoch | 0) !== c.winnerEpoch) { this._log('warn', 'merge-stale', { topic: idHex(topicBig).slice(0, 12), corr: String(payload.corr).slice(0, 8), why: 'closed-mismatch' }); return 'consumed'; }
    role.merge.completing = null;
    role.merge.pins.clear();
    this._mergeCloseCorr(role, payload.corr);
    this._log('info', 'merge-closed', { topic: idHex(topicBig).slice(0, 12), corr: payload.corr.slice(0, 8), winner: c.winner.slice(0, 10) });
    return 'consumed';
  },
};

export default mergeMachineMethods;
