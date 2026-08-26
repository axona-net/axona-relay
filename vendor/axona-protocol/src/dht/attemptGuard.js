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
  constructor({ maxAttempts = 4, baseMs = 30000, factor = 2, refillWindowMs = 60000 } = {}) {
    this.maxAttempts = maxAttempts;
    this.baseMs = baseMs;
    this.factor = factor;
    this.refillWindowMs = refillWindowMs;
    this._state = new Map();        // suffix -> { attempts, inflight, nextAt, expired }
    this._lastRefillAt = new Map(); // suffix -> ts of last granted refill
    this.refills = 0; this.coalesced = 0;
  }

  _s(key) {
    let s = this._state.get(key);
    if (!s) { s = { attempts: 0, inflight: false, nextAt: 0, expired: false }; this._state.set(key, s); }
    return s;
  }

  /** May a probe toward this candidate go out now? */
  allow(id, t = Date.now()) {
    const key = identitySuffix(id);
    if (key === null) return true;             // structurally unreadable: not ours to block
    const s = this._s(key);
    return !s.expired && !s.inflight && t >= s.nextAt;
  }

  begin(id) {
    const key = identitySuffix(id);
    if (key === null) return;
    this._s(key).inflight = true;
  }

  /** Record the probe outcome. Bind clears the entry (expiry-on-bind);
   *  failure schedules the exponential backoff; exhaustion expires. */
  end(id, bound, t = Date.now()) {
    const key = identitySuffix(id);
    if (key === null) return;
    const s = this._s(key);
    s.inflight = false;
    if (bound) { this._state.delete(key); return; }
    s.attempts++;
    if (s.attempts >= this.maxAttempts) { s.expired = true; return; }
    s.nextAt = t + this.baseMs * Math.pow(this.factor, s.attempts - 1);
  }

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
    this._state.delete(key);                   // one fresh budget, re-eligible
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
