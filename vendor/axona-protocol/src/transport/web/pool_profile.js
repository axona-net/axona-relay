// =====================================================================
// pool_profile.js — the TWO-POOL PROFILE (G4 of the two-pool packet,
// axona-docs ca52384; Aster amendment v1.1 bc4fe433 §2, v1.2 b395fe8f G4).
//
// WHAT IT IS. One validated, frozen object that every cap consumer reads:
//
//   M           managed peers this node selects and retains     (≤ M)
//   I           incoming peers accepted to serve remote requests (≤ I)
//   H           physical channels, charged plus reserved, all classes (≤ H)
//   pPending    outbound attempts in flight (the guard's bound)
//   cInbound    inbound negotiations not yet bound
//   serveFloor  usable peers this node needs before it serves
//   promoteMs   the bound on a PROMOTION_PENDING window (G6)
//   hostBudget  sockets the host declares for this process, or null
//   doorSockets sockets the launcher reserves OUTSIDE the mesh pools
//               (a bridge's door), charged against hostBudget only
//
// WHAT IT IS NOT. Not a default. A node constructed without a profile is
// NOT ARMED: the ledger keeps its direction-only predicate, the kernel reads
// MAX_SYNAPTOME as it always has, and no class is recorded on any channel.
// The rows below (`POOL_PROFILE_EXAMPLES`) are the council's REVIEW rows,
// carried for the fence and the reader; none is adopted by this module.
//
// FEASIBILITY IS REJECTED, NEVER TRIMMED. A profile that would overcommit
// (M + I > H, pPending > M, cInbound > I, serveFloor > M, hostBudget below
// H + doorSockets) throws PoolProfileError naming the field. Silent trimming
// or borrowing is the failure v1.1 §2 forbids ("no cross-pool borrowing").
// =====================================================================

export class PoolProfileError extends Error {
  constructor(field, message) { super(`pool profile: ${field}: ${message}`); this.name = 'PoolProfileError'; this.field = field; }
}

/** The council's review rows (Vega 675c730b §4, Aster b395fe8f G4). Examples, not adopted. */
export const POOL_PROFILE_EXAMPLES = Object.freeze({
  relay:   Object.freeze({ M: 50, I: 50, H: 100, pPending: 8, cInbound: 4, serveFloor: 3 }),
  desktop: Object.freeze({ M: 50, I: 50, H: 100, pPending: 8, cInbound: 4, serveFloor: 3 }),   // only with a declared hostBudget
  leaf:    Object.freeze({ M: 6,  I: 6,  H: 12,  pPending: 2, cInbound: 2, serveFloor: 1 }),
});

const DEFAULT_PROMOTE_MS = 30_000;

function int(field, v, { min = 0, required = false } = {}) {
  if (v === undefined || v === null) {
    if (required) throw new PoolProfileError(field, 'required');
    return null;
  }
  const n = Number(v);
  if (!Number.isInteger(n)) throw new PoolProfileError(field, `must be an integer, got ${JSON.stringify(v)}`);
  if (n < min) throw new PoolProfileError(field, `must be ≥ ${min}, got ${n}`);
  return n;
}

/**
 * Build and validate a two-pool profile. Throws PoolProfileError on any
 * infeasible combination; returns a frozen object with `armed: true`.
 *
 * @param {object} opts
 * @param {number} opts.M           ≥ 1
 * @param {number} opts.I           ≥ 0
 * @param {number} [opts.H]         default M + I; must be ≥ M + I
 * @param {number} [opts.pPending]  default min(8, M); must be ≤ M
 * @param {number} [opts.cInbound]  default min(4, I); must be ≤ I
 * @param {number} [opts.serveFloor] default min(3, M); must be ≤ M
 * @param {number} [opts.promoteMs] default 30 000; ≥ 0
 * @param {number|null} [opts.hostBudget] if given, must be ≥ H + doorSockets
 * @param {number} [opts.doorSockets] default 0
 */
export function makePoolProfile(opts) {
  if (!opts || typeof opts !== 'object') throw new PoolProfileError('profile', 'must be an object');
  const M = int('M', opts.M, { min: 1, required: true });
  const I = int('I', opts.I, { min: 0, required: true });
  const H = int('H', opts.H, { min: 1 }) ?? (M + I);
  const pPending   = int('pPending',   opts.pPending,   { min: 1 }) ?? Math.min(8, M);
  const cInbound   = int('cInbound',   opts.cInbound,   { min: 0 }) ?? Math.min(4, I);
  const serveFloor = int('serveFloor', opts.serveFloor, { min: 0 }) ?? Math.min(3, M);
  const promoteMs  = int('promoteMs',  opts.promoteMs,  { min: 0 }) ?? DEFAULT_PROMOTE_MS;
  const doorSockets = int('doorSockets', opts.doorSockets, { min: 0 }) ?? 0;
  const hostBudget  = int('hostBudget', opts.hostBudget, { min: 1 });

  if (M + I > H) throw new PoolProfileError('H', `M + I = ${M + I} exceeds H = ${H} (no borrowing, no 101st allocation)`);
  if (pPending > M) throw new PoolProfileError('pPending', `${pPending} exceeds M = ${M}`);
  if (cInbound > I) throw new PoolProfileError('cInbound', `${cInbound} exceeds I = ${I}`);
  if (serveFloor > M) throw new PoolProfileError('serveFloor', `${serveFloor} exceeds M = ${M}`);
  if (hostBudget !== null && hostBudget < H + doorSockets) {
    throw new PoolProfileError('hostBudget', `${hostBudget} is below H + doorSockets = ${H + doorSockets}`);
  }
  return Object.freeze({ armed: true, M, I, H, pPending, cInbound, serveFloor, promoteMs, hostBudget, doorSockets });
}

/** True for an object makePoolProfile produced (or one shaped like it). */
export function isPoolProfile(p) {
  return !!p && typeof p === 'object' && p.armed === true
    && Number.isInteger(p.M) && Number.isInteger(p.I) && Number.isInteger(p.H);
}
