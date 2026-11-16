// rtt.js — a smoothed round-trip estimate for one channel.
//
// Hop Receipt and Removal v0.3 (axona-docs b5dac77), build step 1; David
// 2026-11-15 "Start step 1, the round-trip measurement". A receipt deadline
// has to be derived from a round trip that was MEASURED on the channel. Until
// 4.148.0 the bridge and WebSocket transports answered getLatency() with the
// constant 50 and the mesh answered with one unsmoothed sample.
//
// THIS FILE MEASURES AND DECIDES NOTHING. Nothing in the kernel reads an
// estimate to choose a hop, time a request or remove a peer. getLatency() on
// every transport answers exactly as it did.
//
// The smoothing is the classic pair (RFC 6298 §2): on the first sample
// srtt = R, rttvar = R/2; after that rttvar = 3/4·rttvar + 1/4·|srtt − R| and
// srtt = 7/8·srtt + 1/8·R. No retransmission timeout is computed here: the
// multiple and the floor of any deadline are not this file's to choose.

/** A sample above this is a clock jump or a stalled process, not a round trip. */
export const RTT_SAMPLE_MAX_MS = 60_000;

export class RttEstimator {
  constructor() { this.reset(); }

  /** Forget everything: a new channel instance starts cold. */
  reset() {
    this.srtt = null;        // smoothed round trip, ms
    this.rttvar = null;      // smoothed mean deviation, ms
    this.last = null;        // the newest accepted sample, ms
    this.min = null;         // the smallest accepted sample, ms
    this.max = null;         // the largest accepted sample, ms
    this.samples = 0;        // accepted samples, all sources
    this.own = 0;            // measured on this end
    this.reported = 0;       // measured by the far end and carried to us
    this.rejected = 0;       // refused: not a number, negative, or over the cap
    this.lastAt = null;      // when the newest sample was accepted (caller's clock)
  }

  /**
   * Offer one round trip. `source` is 'own' (this end timed it) or 'reported'
   * (the far end timed it and told us). Returns true when it was accepted.
   */
  sample(ms, { now = Date.now(), source = 'own' } = {}) {
    if (!Number.isFinite(ms) || ms < 0 || ms > RTT_SAMPLE_MAX_MS) {   // Number.isFinite is false for every non-number
      this.rejected++;
      return false;
    }
    if (this.srtt === null) {
      this.srtt = ms;
      this.rttvar = ms / 2;
    } else {
      this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - ms);
      this.srtt = 0.875 * this.srtt + 0.125 * ms;
    }
    this.last = ms;
    this.min = this.min === null ? ms : Math.min(this.min, ms);
    this.max = this.max === null ? ms : Math.max(this.max, ms);
    this.samples++;
    if (source === 'reported') this.reported++; else this.own++;
    this.lastAt = now;
    return true;
  }

  /** Has this channel produced no sample yet? */
  get cold() { return this.samples === 0; }

  /** A plain copy for a reader. `ageMs` is null while cold. */
  snapshot(now = Date.now()) {
    return {
      cold: this.samples === 0,
      srtt: this.srtt, rttvar: this.rttvar, last: this.last, min: this.min, max: this.max,
      samples: this.samples, own: this.own, reported: this.reported, rejected: this.rejected,
      ageMs: this.lastAt === null ? null : Math.max(0, now - this.lastAt),
    };
  }
}

export default RttEstimator;
