// =====================================================================
// signedExpiry.js — REF-1.1 S2.0c: signed immutable message expiry, the
// Option-1 V2 content-address + committed-deadline machinery.
//
// STATUS: PURE + FENCED. This module provides the V2 msgId + expiry functions
// ONLY. It does NOT change the default envelope wire path: buildEnvelope /
// verifyEnvelope / computeMsgId in envelope.js still produce the V1 content
// address, so the running network (all on msgId V1) is unaffected and the full
// suite stays green. Making V2 the active envelope format is the FLAG-DAY
// cutover — a separate, David-gated deploy step, not this tranche. Building the
// crypto core fenced-and-tested first, then cutting over under its own gate, is
// the same review shape the council accepted for Phase 1 (tombstoneAuth) and
// for S2.0a/b/c.
//
// WHAT THIS IS (the accepted design, REF-1.1-S2.0c-Signed-Expiry-Design-v6.md):
//   msgId(V2) = sha256( canonical({ d, exp, message, publisher, topicId }) )
//     d        = "axona:pubsub-msgid:v2"   (literal domain tag)
//     publisher = exactly 64 lowercase hex (32-byte Ed25519 key) OR null
//                 (the fenced anonymous case — local-only, non-transferable)
//     topicId   = exactly 66 lowercase hex (region byte + 32)
//   Both widths are validated BEFORE hashing; any other form (66-hex publisher,
//   64-hex topicId, upper-case, non-hex, wrong length) is rejected pre-hash.
//   canonical() key-sorts, so the serialized order is d, exp, message,
//   publisher, topicId. The legacy V1 id is byte-preserved: no `d` field,
//   sha256(canonical({publisher, message})), never re-hashed. V1-vs-V2 is
//   decided at the flag-day cutoff.
//   Lifetime: ts < exp <= ts + TTL_CEILING; one effectiveDeath = exp + CLOCK_SKEW
//   used everywhere; a body is fresh iff now <= effectiveDeath.
//
// Committing exp into the content address is what makes the deadline immutable
// and COLD-VERIFIABLE: a receiver recomputes msgId from (publisher, message,
// topicId, exp) and any tampered exp yields a different id, so a migrated kill
// naming an msgId cannot silently carry a longer life than the author signed.
//
// PROFILE NOTE: 64/66 are the normative PRODUCTION widths. Under the shrunk sim
// keyspace profile (configureKeyspace / AUTH_VERIFY_RELAXED) the ids are the
// profile's truncated widths and enter computeMsgIdV2 at that width. A caller on
// that path passes the EXPLICIT sim widths ({ pubWidth, topicWidth }); it does
// NOT get to turn validation off. The relaxed profile permits a narrower
// fixed-width canonical id — never an arbitrary type, a non-hex value, an
// upper-case alias, or an unconstrained length: string + lowercase-hex + exact
// configured width is always enforced. The sim profile is not a production
// identity and its records are non-transferable to production across the
// flag-day cutoff.
//
// TIMESTAMP CONTRACT (D1): ts and exp are finite SAFE integers. A value past
// Number.MAX_SAFE_INTEGER loses precision, so it can never be committed into the
// content address (computeMsgIdV2) or the deadline (validateExp/clampExp); those
// fail closed, and the guard also rejects an exp whose exp + CLOCK_SKEW would
// itself leave the safe range.
//
// PURITY: no Date.now() here — every time function takes `now`/`ts` from the
// caller, so it is deterministic and testable.
// =====================================================================

import { canonical, sha256Hex } from './post.js';
import { TTL_MS } from './constants.js';

export const MSGID_DOMAIN_V2 = 'axona:pubsub-msgid:v2';

// Lifetime constants. NOTE: tombstoneAuth.js (Phase 1) currently carries its
// own CLOCK_SKEW / TTL_CEILING copies with these same values; the flag-day
// integration collapses those to a single import from this module (this is the
// natural home for the expiry layer). Tracked; not reconciled here because
// Phase 1 is already accepted and both modules are unwired.
export const TTL_CEILING = TTL_MS;   // 24h absolute hold ceiling (constants.TTL_MS)
export const CLOCK_SKEW  = 5_000;    // symmetric clock-skew allowance (ms)

// Normative PRODUCTION identity widths. Lowercase-hex only, exact length.
export const PUBLISHER_WIDTH = 64;   // 32-byte Ed25519 verification key
export const TOPICID_WIDTH   = 66;   // region byte + 32
const HEX_RE = /^[0-9a-f]+$/;        // lowercase hex ONLY — rejects upper-case + non-hex

/** True iff `s` is a lowercase-hex STRING of exactly `width` chars. */
function isHexOfWidth(s, width) {
  return typeof s === 'string' && s.length === width && HEX_RE.test(s);
}

/** Typed error for pre-hash / lifetime rejections. `.code` names the failure. */
export class SignedExpiryError extends Error {
  constructor(message, code) { super(message); this.name = 'SignedExpiryError'; this.code = code; }
}

/**
 * Fail closed unless `exp` is a finite SAFE integer whose committed death
 * (exp + CLOCK_SKEW) is also safe — so nothing past Number.MAX_SAFE_INTEGER is
 * ever hashed into the identity or used as a deadline (D1). Internal helper.
 */
function assertSafeExp(exp) {
  if (!Number.isSafeInteger(exp))
    throw new SignedExpiryError('exp must be a finite safe-integer ms timestamp', 'BAD_EXP');
  if (!Number.isSafeInteger(exp + CLOCK_SKEW))
    throw new SignedExpiryError('exp + CLOCK_SKEW exceeds the safe-integer range', 'BAD_EXP');
}

/**
 * Validate the V2 identity widths BEFORE hashing.
 * publisher: exactly `pubWidth` lowercase hex, or JSON null (anonymous, local-only).
 * topicId:   exactly `topicWidth` lowercase hex.
 * The widths default to the production 64/66; a sim-relaxed caller passes its
 * EXPLICIT narrower widths. There is no validation-off mode: string +
 * lowercase-hex + exact width is enforced at every width, so an object, an
 * array, a non-hex value, an upper-case alias, or a wrong length is rejected on
 * every profile. Throws SignedExpiryError on any other form. This is the
 * pre-hash gate the golden/rejection vectors exercise.
 */
export function assertV2Widths(publisher, topicId, { pubWidth = PUBLISHER_WIDTH, topicWidth = TOPICID_WIDTH } = {}) {
  if (!Number.isInteger(pubWidth) || pubWidth <= 0 || !Number.isInteger(topicWidth) || topicWidth <= 0)
    throw new SignedExpiryError('profile widths must be positive integers', 'BAD_PROFILE_WIDTH');
  if (publisher !== null && !isHexOfWidth(publisher, pubWidth))
    throw new SignedExpiryError(`publisher must be exactly ${pubWidth} lowercase hex or null`, 'BAD_PUBLISHER_WIDTH');
  if (!isHexOfWidth(topicId, topicWidth))
    throw new SignedExpiryError(`topicId must be exactly ${topicWidth} lowercase hex`, 'BAD_TOPICID_WIDTH');
}

/**
 * The exact V2 preimage string that will be hashed. Exposed so tests and
 * reviewers can pin it byte-for-byte. canonical() key-sorts to
 * d, exp, message, publisher, topicId.
 */
export function msgIdV2Preimage({ publisher, message, topicId, exp }) {
  return canonical({ d: MSGID_DOMAIN_V2, exp, message, publisher, topicId });
}

/**
 * Compute the V2 (Option-1) content address.
 * @param {{publisher: string|null, message: *, topicId: string, exp: number}} core
 * @param {{pubWidth?: number, topicWidth?: number}} [opts]  identity widths; default
 *        the production 64/66. A sim-relaxed caller passes its explicit narrower
 *        widths — validation is never disabled, only re-sized (see assertV2Widths).
 * @returns {Promise<string>} 64-char hex sha256
 */
export function computeMsgIdV2({ publisher, message, topicId, exp }, { pubWidth, topicWidth } = {}) {
  assertSafeExp(exp);
  assertV2Widths(publisher, topicId, { pubWidth, topicWidth });
  return sha256Hex(msgIdV2Preimage({ publisher, message, topicId, exp }));
}

/**
 * The legacy V1 content address, byte-preserved: NO `d` field,
 * sha256(canonical({publisher, message})). Retained so V1 records are never
 * re-hashed across the flag day and V1-vs-V2 can be proven distinct.
 */
export function computeMsgIdV1({ publisher = null, message }) {
  return sha256Hex(canonical({ publisher, message }));
}

// ---- committed lifetime (D1/D2) --------------------------------------------

/**
 * The maximal exp a publisher may sign for a message minted at `ts`.
 * Fails closed on an unsafe ts, or if the clamped exp + CLOCK_SKEW would leave
 * the safe-integer range (D1) — a clamped deadline is never silently imprecise.
 */
export function clampExp(ts) {
  if (!Number.isSafeInteger(ts))
    throw new SignedExpiryError('ts must be a finite safe-integer ms timestamp', 'BAD_EXP');
  const exp = ts + TTL_CEILING;
  if (!Number.isSafeInteger(exp + CLOCK_SKEW))
    throw new SignedExpiryError('clamped exp + CLOCK_SKEW exceeds the safe-integer range', 'BAD_EXP');
  return exp;
}

/**
 * Validate a committed exp against its mint ts: ts < exp <= ts + TTL_CEILING.
 * A normal 24h message sets exp = ts + TTL_CEILING. Both ts and exp must be
 * finite SAFE integers, and exp + CLOCK_SKEW must stay safe (D1). Throws on
 * out-of-range so an ingress can fail closed; returns the exp on success.
 */
export function validateExp(ts, exp) {
  if (!Number.isSafeInteger(ts) || !Number.isSafeInteger(exp))
    throw new SignedExpiryError('ts and exp must be finite safe-integer ms timestamps', 'BAD_EXP');
  if (!(exp > ts))            throw new SignedExpiryError('exp must be strictly after ts', 'EXP_NOT_AFTER_TS');
  if (exp > ts + TTL_CEILING) throw new SignedExpiryError('exp exceeds ts + TTL_CEILING', 'EXP_OVER_CEILING');
  if (!Number.isSafeInteger(exp + CLOCK_SKEW))
    throw new SignedExpiryError('exp + CLOCK_SKEW exceeds the safe-integer range', 'BAD_EXP');
  return exp;
}

/**
 * The one committed death used everywhere: effectiveDeath = exp + CLOCK_SKEW.
 * Enforces the safe-exp guard itself (D1): this is an EXPORTED helper a Phase 3
 * caller can reach directly with an unvalidated envelope exp, so it must fail
 * closed rather than return an imprecise (unsafe-integer) deadline. The upstream
 * compute/validate/clamp guards do not cover a direct call here.
 */
export function effectiveDeath(exp) {
  assertSafeExp(exp);
  return exp + CLOCK_SKEW;
}

/**
 * A body is fresh (deliverable / suppressible) iff now <= effectiveDeath(exp).
 * Inherits effectiveDeath's safe-exp guard, so an unsafe exp THROWS rather than
 * yielding a bogus verdict (isBodyFresh(MAX_SAFE_INTEGER, MAX_SAFE_INTEGER) must
 * not return true).
 */
export function isBodyFresh(now, exp) { return now <= effectiveDeath(exp); }
