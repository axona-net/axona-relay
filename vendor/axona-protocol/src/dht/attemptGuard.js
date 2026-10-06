// =====================================================================
// attemptGuard.js — the candidate attempt guard + deficit backoff
// (Connection-Quality v0.7, axona-docs 66f50bc; implementation slice 3).
//
// The storm this exists to kill, measured before it existed: a maintenance
// loop re-probing a never-binding near-successor at maxPerTick EVERY tick,
// forever (kernel test c16d12b — 3.0 probes/tick sustained). Two brakes,
// each with its release valve:
//
//   ATTEMPT GUARD — per-candidate: in-flight dedup, bounded retry with
//   exponential backoff, expiry on bind or exhaustion. The valve is the
//   dht:presence record (slice 2): a verified fresh record clears ONE
//   budget — paced, at most one refill per identity per window, however
//   many valid gens arrive (v0.4 receiver step 4; matrix scenario 2c).
//
//   DEFICIT BACKOFF — the search itself: a maintenance pass that finds
//   nothing to attempt backs the next search off exponentially (an empty
//   deficit is usually an UNPOPULATED band — searching cannot fill it).
//   The valve: any attempt, any fresh presence record, resets it.
//
// All candidate state keys on the 256-BIT IDENTITY SUFFIX (v0.6 "What the
// key is"): the geo-prefix byte is the only churn possible under one key.
// Nothing here persists; a new session is fresh state by design.
// =====================================================================

const MASK_256 = (1n << 256n) - 1n;

/** Normalize a candidate id (BigInt or 66-hex string) to its 64-hex
 *  256-bit identity suffix — THE guard key. Anything else: null. */
export function identitySuffix(id) {
  if (typeof id === 'bigint') return (id & MASK_256).toString(16).padStart(64, '0');
  if (typeof id === 'string' && id.length === 66 && /^[0-9a-f]+$/.test(id)) return id.slice(2);
  if (typeof id === 'string' && id.length === 64 && /^[0-9a-f]+$/.test(id)) return id;
  return null;
}

export class AttemptGuard {
  constructor({ maxAttempts = 4, baseMs = 30000, factor = 2, refillWindowMs = 60000, inflightMaxMs = 45000 } = {}) {
    this.maxAttempts = maxAttempts;
    this.baseMs = baseMs;
    this.factor = factor;
    this.refillWindowMs = refillWindowMs;
    // Row 8 (Hold-and-Fill v0.15, axona-docs e4809d2): an attempt whose dial
    // went out holds its token until BIND, CANCEL or DEADLINE. The deadline
    // normally arrives as the mesh's negotiation failure (row 13); this bound
    // is the fail-safe for a lost signal, so an identity can never stay
    // in flight (and therefore undialable) for ever. NEGOTIATION_DEADLINE_MS
    // is 30 s; 45 s leaves the signal room to arrive first.
    this.inflightMaxMs = inflightMaxMs;
    this._state = new Map();        // suffix -> { attempts, inflight, k, beganAt, nextAt, expired }
    this._lastRefillAt = new Map(); // suffix -> ts of last granted refill
    this._seq = 0;                  // completion-token counter (k)
    this.refills = 0; this.coalesced = 0; this.staleEnded = 0; this.ignoredEnds = 0;
  }

  _s(key) {
    let s = this._state.get(key);
    if (!s) { s = { attempts: 0, inflight: false, k: 0, beganAt: 0, nextAt: 0, expired: false }; this._state.set(key, s); }
    return s;
  }

  /** May a probe toward this candidate go out now? */
  allow(id, t = Date.now()) {
    const key = identitySuffix(id);
    if (key === null) return true;             // structurally unreadable: not ours to block
    const s = this._s(key);
    return !s.expired && !s.inflight && t >= s.nextAt;
  }

  /** Open an attempt. Returns the completion token `k` the attempt owns
   *  (row 8, case 16: `begin(id, k)` once; `end(id, k, ·)` exactly once). */
  begin(id, t = Date.now()) {
    const key = identitySuffix(id);
    if (key === null) return 0;
    const s = this._s(key);
    s.inflight = true; s.k = ++this._seq; s.beganAt = t;
    return s.k;
  }

  /** Record the attempt's outcome, EXACTLY ONCE. Bind clears the entry
   *  (expiry-on-bind); failure schedules the exponential backoff; exhaustion
   *  expires. An `end` with no attempt in flight, or carrying a token that is
   *  not the live one, is ignored and counted: a bind of an identity this
   *  guard never dialed, a second deadline for one dial, or a stale
   *  completion after a newer attempt must not count a failure or clear a
   *  live attempt. Returns true when it acted. */
  end(id, bound, t = Date.now(), k = undefined, inc = undefined) {
    const key = identitySuffix(id);
    if (key === null) return false;
    const s = this._state.get(key);
    if (!s || !s.inflight || (k !== undefined && k !== s.k)) { this.ignoredEnds++; return false; }
    // Row 8 (Aster a2c1d79f R8-2): a terminal event names the CHANNEL
    // INCARNATION it came from. When the attempt recorded the incarnation of
    // the negotiation it started (`attach`) and the event carries one, they
    // must match: an old channel's deadline or bind after a newer dial to
    // the same identity ends nothing and counts nothing.
    if (inc != null && s.inc != null && inc !== s.inc) { this.ignoredEnds++; this.staleIncEnds = (this.staleIncEnds ?? 0) + 1; return false; }
    s.inflight = false; s.k = 0; s.inc = null;
    if (bound) { this._state.delete(key); return true; }
    s.attempts++;
    if (s.attempts >= this.maxAttempts) { s.expired = true; return true; }
    s.nextAt = t + this.baseMs * Math.pow(this.factor, s.attempts - 1);
    return true;
  }

  /** Record the channel incarnation the live attempt's dial started (row 8,
   *  R8-2), so a terminal event from another incarnation is ignored. */
  attach(id, k, inc) {
    const key = identitySuffix(id);
    if (key === null) return false;
    const s = this._state.get(key);
    if (!s || !s.inflight || s.k !== k) return false;
    s.inc = (typeof inc === 'string' && inc.length) ? inc : null;
    return true;
  }

  /** Release a live token WITHOUT counting an attempt: the dial site found,
   *  after its awaited open, that nothing may go out (the identity became
   *  ineligible). Nothing was issued, so nothing is counted; the identity is
   *  simply no longer in flight. Stale or absent tokens are ignored as in
   *  `end`. Returns true when it acted. */
  release(id, k = undefined) {
    const key = identitySuffix(id);
    if (key === null) return false;
    const s = this._state.get(key);
    if (!s || !s.inflight || (k !== undefined && k !== s.k)) { this.ignoredEnds++; return false; }
    s.inflight = false; s.k = 0; this.released = (this.released ?? 0) + 1;
    if (s.attempts === 0 && !s.expired) this._state.delete(key);
    return true;
  }

  /** The fail-safe deadline: end, as a failure, every attempt in flight
   *  longer than `inflightMaxMs`. Called by the dial sites before they ask
   *  `allow`. Returns how many it ended. */
  sweep(t = Date.now(), maxMs = this.inflightMaxMs) {
    let n = 0;
    for (const [key, s] of this._state) {
      if (s.inflight && t - s.beganAt > maxMs) { this.end(key, false, t, s.k); this.staleEnded++; n++; }
    }
    return n;
  }

  inflightOf(id) { return this._state.get(identitySuffix(id))?.inflight ?? false; }
  /** Row 12: peer(PENDING) — attempts whose dial went out and have not ended. The pending-slot half of the reservation. */
  inflightCount() { let n = 0; for (const s of this._state.values()) if (s.inflight) n++; return n; }

  /** The presence valve. Watermark monotonicity is enforced UPSTREAM (the
   *  presence handler fires hooks only on a fresh gen); this method paces:
   *  at most one budget refill per identity per window. A coalesced record
   *  refills nothing — freshness was already recorded upstream. */
  onFreshRecord(id, t = Date.now()) {
    const key = identitySuffix(id);
    if (key === null) return false;
    const last = this._lastRefillAt.get(key) ?? -Infinity;
    if (t - last < this.refillWindowMs) { this.coalesced++; return 'coalesced'; }
    this._lastRefillAt.set(key, t);
    // Row 8 (Aster a2c1d79f R8-3): freshness refills the BUDGET (attempts,
    // expiry, backoff). It does not end an attempt: a live token keeps its
    // in-flight state, k, incarnation and start time, so nothing becomes
    // allowed by freshness while a dial is out; the dial's own bind,
    // deadline or sweep ends it.
    const s = this._state.get(key);
    if (s && s.inflight) { s.attempts = 0; s.expired = false; s.nextAt = 0; }
    else this._state.delete(key);              // one fresh budget, re-eligible
    this.refills++;
    return true;
  }

  attemptsOf(id) { return this._state.get(identitySuffix(id))?.attempts ?? 0; }
  expiredOf(id)  { return this._state.get(identitySuffix(id))?.expired ?? false; }
}

export class DeficitBackoff {
  constructor({ deficitBaseMs = 30000, deficitFactor = 2 } = {}) {
    this.baseMs = deficitBaseMs;
    this.factor = deficitFactor;
    this._empties = 0;
    this._nextAt = 0;
  }
  allow(t = Date.now()) { return t >= this._nextAt; }
  onEmpty(t = Date.now()) {
    this._empties++;
    this._nextAt = t + this.baseMs * Math.pow(this.factor, this._empties - 1);
  }
  reset() { this._empties = 0; this._nextAt = 0; }
}
