/**
 * DeadPeers — the dead-peer MARK TABLE, and (row 10) its automaton.
 *
 * Hold-and-Fill v0.5 row 1 (axona-docs 4334504) made a mark carry a reason;
 * v0.7 "Marks" (95c2ff4, Aster c34f3c85 P1-A, f5237636, fb63049f) made the
 * mark an automaton. This file is both. `node._deadPeers` was a bare
 * Set<bigint>; it is this Map, and every reader that used has/delete/size
 * sees what it saw.
 *
 *   mark = { kind, cause, at, attempts, dueAt, token }
 *
 *   kind     'loss' (every writer in the kernel today) or 'policy' (accepted
 *            by the structure; no kernel writer at 270835d).
 *   cause    the transport's close cause or 'unknown'.
 *   at       WALL CLOCK, descriptive, for the log line. Nothing is timed
 *            from it.
 *   attempts failures counted so far, 1..A_max. The counter counts FAILURES:
 *            a success is free (it deletes the mark).
 *   dueAt    MONOTONIC clock (performance.now), the instant the identity is
 *            next eligible. A wall-clock step moves no schedule.
 *   token    exhausted only: 1 once the refill window has passed and the
 *            one allowed attempt has not been issued; 0 otherwise.
 *
 * THE PREDICATE, one for nomination and dialing alike:
 *
 *   ELIGIBLE(id) := no mark  → not (MARKS-FULL or POLICY-FULL)
 *                   policy   → false
 *                   loss     → now ≥ dueAt   (and, at attempts = A_max, the
 *                                             lazy REFILL sets token := 1)
 *
 * THE INPUTS:
 *   FAIL(id, cause)  an attempt ended without a bind, or an open channel
 *                    died, and no OPEN channel to id remains. First failure
 *                    writes {attempts 1, dueAt now + B}; a later one does
 *                    attempts := min(attempts + 1, A_max) and ONE schedule by
 *                    the new count: < A_max → now + B·factor^(attempts−1);
 *                    = A_max → now + R_refill (the transition into
 *                    exhaustion takes the refill schedule, not the doubling
 *                    one). Never lowers attempts, never sets the token.
 *   CONSUME(id)      the attempt is ISSUED (the offer goes out). Below A_max
 *                    nothing (charged at FAIL). At A_max: token := 0 and
 *                    dueAt := now + R_refill — the window advances at ISSUE,
 *                    so a second evaluation in the same window is false
 *                    whatever ran between (f5237636).
 *   BIND(id)         the identity bound on our own channel: delete the mark.
 *                    The next loss starts at attempts 1.
 *
 * So for a continuously retained exhausted mark, under atomic ISSUE and a
 * monotonic clock, AT MOST one attempt issues per R_refill (fb63049f);
 * BIND, eviction and restart reset the accounting by policy.
 *
 * TWO FULL STATES, exact, no Bloom filter:
 *   MARKS-FULL   LOSS marks ≥ M_marks latches; lifts below M_marks − M_hyst
 *                (the Map holds loss and policy marks together; each kind
 *                has its own bound and count, Aster 1816f5e6). While
 *                latched no UNMARKED identity is eligible. A new loss mark at
 *                the bound evicts the OLDEST exhausted-and-waiting mark
 *                (attempts = A_max, token 0), and only such a mark; with none
 *                to evict the new mark is NOT written and the identity is
 *                refused by MARKS-FULL. The evicted identity returns as a
 *                stranger: stated, bounded forgetting (it changes which
 *                identity the next dial goes to, never how many dials).
 *   POLICY-FULL  policy marks ≥ M_policy: a new policy mark is not written,
 *                `policyRefused` counts it, and no unmarked identity is
 *                eligible until a policy mark is deleted or the process
 *                restarts. A policy mark is never evicted.
 *
 * WRITERS WITHOUT INFORMATION never overwrite a mark that has it (row 1,
 * Aster fb79c09e): add(id) is membership only and, on an unmarked id, writes
 * a mark with attempts 0 (no failure counted; the kernel's FAIL for the same
 * death then counts the first).
 *
 * The inbound side: v0.7 has `identify` consult ELIGIBLE before a bind. The
 * web transport binds (hello-ack → bindPeer) before the kernel sees the
 * identity, so an inbound bind of a marked identity is accepted and deletes
 * the mark, as the kernel did before this row. Refusing it would need a
 * pre-bind hook in the transport; that is recorded as a deviation to carry
 * into the design, not done here.
 */

export const MARK_DEFAULTS = Object.freeze({
  B: 30_000,          // first backoff, ms
  factor: 2,
  A_max: 4,           // failures to exhaustion
  R_refill: 60_000,   // one attempt per window after exhaustion, ms
  M_marks: 256,
  M_hyst: 32,
  M_policy: 1024,
});

const monotonic = () => {
  try { const p = globalThis.performance; if (p && typeof p.now === 'function') return p.now(); } catch { /* fall through */ }
  return Date.now();
};

export class DeadPeers extends Map {
  /**
   * @param {object} [opts]  overrides of MARK_DEFAULTS, plus `now` (monotonic
   *   clock) and `wall` (wall clock) for tests.
   */
  constructor(opts = {}) {
    super();
    const o = opts && typeof opts === 'object' ? opts : {};
    const num = (v, d, min) => { const n = Number(v); return Number.isFinite(n) ? Math.max(min, n) : d; };
    this.cfg = {
      B:        num(o.B,        MARK_DEFAULTS.B,        0),
      factor:   num(o.factor,   MARK_DEFAULTS.factor,   1),
      A_max:    num(o.A_max,    MARK_DEFAULTS.A_max,    1),
      R_refill: num(o.R_refill, MARK_DEFAULTS.R_refill, 0),
      M_marks:  num(o.M_marks,  MARK_DEFAULTS.M_marks,  1),
      M_hyst:   num(o.M_hyst,   MARK_DEFAULTS.M_hyst,   0),
      M_policy: num(o.M_policy, MARK_DEFAULTS.M_policy, 1),
    };
    this._now  = typeof o.now  === 'function' ? o.now  : monotonic;
    this._wall = typeof o.wall === 'function' ? o.wall : Date.now;
    this._fullLatched = false;
    this._policyCount = 0;
    this._seq = 0;
    this._stats = { fails: 0, consumed: 0, refilled: 0, evicted: 0, refusedFull: 0, policyRefused: 0, ineligibleUnmarked: 0 };
  }

  // ── row 1 API, kept ────────────────────────────────────────────────

  /**
   * Write a mark. For kind 'loss' this IS the FAIL input (the latest cause
   * wins, the schedule advances). For kind 'policy' it writes or refreshes a
   * policy mark. `at` may be supplied for tests.
   * @param {bigint} id
   * @param {{ kind?: string, cause?: string, at?: number }} [mark]
   * @returns {this}
   */
  mark(id, mark = {}) {
    const kind  = (typeof mark.kind === 'string' && mark.kind) ? mark.kind : 'loss';
    const cause = (typeof mark.cause === 'string' && mark.cause) ? mark.cause : 'unknown';
    if (kind === 'policy') this._markPolicy(id, cause, mark.at);
    else this.fail(id, cause, mark.at);
    return this;
  }

  /**
   * Set-compatible writer for consumers that predate the mark (the bridge).
   * MEMBERSHIP ONLY: an id already marked keeps its mark (Aster fb79c09e).
   * An unmarked id gets a loss mark with NO failure counted (attempts 0),
   * due after one backoff; the kernel's FAIL for the same death counts the
   * first failure.
   * @param {bigint} id
   * @returns {this}
   */
  add(id) {
    if (this.has(id)) return this;
    if (!this._admitWrite(id)) return this;
    const now = this._now();
    this.set(id, { kind: 'loss', cause: 'unknown', at: this._wall(), attempts: 0, dueAt: now + this.cfg.B, token: 0, seq: ++this._seq });
    this._latch();
    return this;
  }

  delete(id) {
    const m = this.get(id);
    const ok = super.delete(id);
    if (ok && m?.kind === 'policy') this._policyCount = Math.max(0, this._policyCount - 1);
    this._latch();
    return ok;
  }

  clear() { super.clear(); this._policyCount = 0; this._fullLatched = false; }

  // ── the automaton ──────────────────────────────────────────────────

  /** ELIGIBLE(id). Evaluated lazily; the exhausted row refills the token. */
  eligible(id) {
    const m = this.get(id);
    if (!m) {
      const ok = !(this._fullLatched || this.policyFull());
      if (!ok) this._stats.ineligibleUnmarked++;
      return ok;
    }
    if (m.kind === 'policy') return false;
    const now = this._now();
    if (now < m.dueAt) return false;
    if (m.attempts < this.cfg.A_max) return true;
    if (!m.token) { m.token = 1; this._stats.refilled++; }
    return true;
  }

  /**
   * FAIL(id, cause). Writes the first mark or advances the schedule. A
   * policy mark is untouched. Returns the mark, or null when MARKS-FULL
   * refused a new one.
   */
  fail(id, cause = 'unknown', at = undefined) {
    const now = this._now();
    const wall = Number.isFinite(at) ? at : this._wall();
    const c = (typeof cause === 'string' && cause) ? cause : 'unknown';
    const m = this.get(id);
    if (m) {
      if (m.kind === 'policy') return m;
      m.attempts = Math.min(m.attempts + 1, this.cfg.A_max);
      m.cause = c; m.at = wall; m.token = 0;
      m.dueAt = this._schedule(now, m.attempts);
      this._stats.fails++;
      return m;
    }
    if (!this._admitWrite(id)) return null;
    const rec = { kind: 'loss', cause: c, at: wall, attempts: 1, dueAt: this._schedule(now, 1), token: 0, seq: ++this._seq };
    this.set(id, rec);
    this._stats.fails++;
    this._latch();
    return rec;
  }

  /** CONSUME(id): the attempt is issued. Exhausted marks advance their window. */
  consume(id) {
    const m = this.get(id);
    if (!m || m.kind !== 'loss') return;
    this._stats.consumed++;
    if (m.attempts >= this.cfg.A_max) { m.token = 0; m.dueAt = this._now() + this.cfg.R_refill; }
  }

  /** BIND(id): the identity bound on our own channel. Same as delete. */
  bind(id) { return this.delete(id); }

  policyFull() { return this._policyCount >= this.cfg.M_policy; }
  marksFull() { return this._fullLatched; }

  stats() {
    let exhausted = 0, waiting = 0;
    for (const m of this.values()) if (m.kind === 'loss' && m.attempts >= this.cfg.A_max) { exhausted++; if (!m.token) waiting++; }
    return { size: this.size, loss: this.lossCount(), policy: this._policyCount, exhausted, exhaustedWaiting: waiting, marksFull: this._fullLatched, policyFull: this.policyFull(), ...this._stats, cfg: { ...this.cfg } };
  }

  // ── internals ──────────────────────────────────────────────────────

  _schedule(now, attempts) {
    if (attempts >= this.cfg.A_max) return now + this.cfg.R_refill;
    return now + this.cfg.B * Math.pow(this.cfg.factor, attempts - 1);
  }

  /** Loss marks in the table. The two bounds are separate (Aster 1816f5e6
   *  R10/13-C): M_marks bounds LOSS marks, M_policy bounds POLICY marks; the
   *  Map holds both, and size is their sum. */
  lossCount() { return this.size - this._policyCount; }

  _latch() {
    const loss = this.lossCount();
    if (loss >= this.cfg.M_marks) this._fullLatched = true;
    else if (loss < this.cfg.M_marks - this.cfg.M_hyst) this._fullLatched = false;
  }

  /** May a NEW loss mark be written? At the loss bound, evict the oldest
   *  exhausted-and-waiting loss mark; with none, refuse. Policy marks are
   *  never candidates and never counted here. */
  _admitWrite(id) {
    if (this.lossCount() < this.cfg.M_marks) return true;
    let victim = null;
    for (const [k, m] of this) {
      if (m.kind !== 'loss' || m.attempts < this.cfg.A_max || m.token) continue;
      if (!victim || m.seq < victim.m.seq) victim = { k, m };
    }
    if (!victim) { this._stats.refusedFull++; return false; }
    super.delete(victim.k);
    this._stats.evicted++;
    return true;
  }

  _markPolicy(id, cause, at) {
    const m = this.get(id);
    if (m && m.kind === 'policy') { m.cause = cause; m.at = Number.isFinite(at) ? at : this._wall(); return; }
    if (this.policyFull()) { this._stats.policyRefused++; return; }
    if (m) super.delete(id);           // a loss mark becomes a policy mark (leaves the loss count)
    this.set(id, { kind: 'policy', cause, at: Number.isFinite(at) ? at : this._wall(), attempts: 0, dueAt: Infinity, token: 0, seq: ++this._seq });
    this._policyCount++;
    this._latch();                     // the loss count may have dropped
  }
}
