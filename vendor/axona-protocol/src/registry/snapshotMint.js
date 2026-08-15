// registry/snapshotMint.js — the DECODER-PRIVATE snapshot capability (refactor
// Phase 1, REF-1.1; S1i per Aster's S1h disposition + trust-model steer).
//
// TRUST BOUNDARY (Option B — NORMATIVE). The security properties of this module
// hold under INTACT REALM INTRINSICS AND PROTOTYPES for the ENTIRE
// certification-and-dispatch lifetime — not merely at module load. This is the
// same assumption the rest of the kernel already relies on throughout: the DHT,
// transport, wire codec, and the routed handlers themselves all call mutable
// realm globals with no post-load capture. This module therefore does NOT claim
// general post-load intrinsic-tamper resistance: same-realm post-load
// replacement of a relied-upon intrinsic (JSON.parse, Object.keys,
// WeakSet.prototype.has, Object.getOwnPropertyDescriptor, WeakMap.prototype.get,
// Array.isArray, …) is EXPLICITLY OUT OF SCOPE. Hardening only this observation
// module against that would not create a system-level boundary — a window in a
// house with no walls. A future requirement for same-realm tamper resistance
// would need a kernel-wide hardened compartment / SES-style realm, not an
// intrinsic-capture checklist local to this file.
//
// IN SCOPE and enforced (preserved from S1h): a certified value that an ATTACKER
// MUTATES AFTER certification — including swapping its prototype to a Proxy, or
// inserting a nested Proxy — must not cause shadow observation to fire a trap,
// mutate handler-visible state, suppress the handler, or change its arguments.
// Structural classification reads decoder-private CONSTRUCTION-TIME tags
// (`kindOf`), never `instanceof` or a live prototype walk.
//
// PROVENANCE. `certify` parses a serialized frame (the decoder's own output from
// wire bytes) and brands every reachable object/array node in a WeakSet (an
// identity check, trap-free to read). It is NOT re-exported by registry/index.js
// and its package subpath is blocked — that is API ENCAPSULATION / hygiene, not a
// security boundary (the file is reachable by URL). The security property does
// not rely on unreachability: certify takes TEXT, so under the intact-realm
// assumption above its parse output is a plain graph, and classification never
// touches a prototype.

// S2.0c — FIXED per-variant certifying decoders (REF-1.1 S2.0b Gate-2 v2 cleared;
// Aster seq 700 authorized this as a reviewable code tranche). The core exposes
// TWO fixed string-only decoder variants matching the transport wire decoders:
//   certifyPlain(text)   — JSON.parse, no reviver           (node WS, web-bridge WS)
//   certifyBigint(text)  — JSON.parse + the SAME internal   (WebRTC mesh)
//                          bigintReviver decode() already uses
// There is NO caller-supplied reviver/callback and NO general object-branding
// export — an object-accepting mint would recreate the branding capability S1
// forbids. Each variant NORMALIZES its runtime input type (only a string is a
// valid wire frame; anything else → null → uncertified → observation no-op) and
// enforces an F7 pre-parse UTF-8 BYTE ceiling BEFORE JSON.parse, so an oversized
// frame is rejected before it can allocate a parse graph. The ceiling is the
// transport contract's own MAX_FRAME_BYTES (wire.js), NOT the registry's
// per-scalar budget; a caller may only TIGHTEN it, never raise it.
// `MAX_DEPTH`/`MAX_NODES` bound the brand WALK; F7 bounds the PARSE.
import { bigintReviver, MAX_FRAME_BYTES } from '../transport/wire.js';

const _certified = new WeakSet();
const _kind = new WeakMap();        // node -> frozen structural tag { k, len? }
const MAX_DEPTH = 8;
const MAX_NODES = 4096;
// Distinguishes a genuinely OMITTED cap argument from an explicitly supplied
// `undefined`. The public wrappers substitute this via arguments.length so that
// forwarding a named `maxBytes` parameter cannot erase the distinction (Aster
// seq 708 finding 3): only an omitted cap defaults; an explicit undefined rejects.
const OMITTED = Symbol('omittedCap');
// F7 pre-parse ceiling. The HARD cap is owned and justified by the transport
// contract (wire.js MAX_FRAME_BYTES) — the frame/envelope guard, sized against
// the largest legitimate frame per ingress variant. It is NOT the registry's
// per-scalar MAX_BYTES_CEILING (a different, smaller resource) that S2.0c v1
// wrongly reused. A caller (a decoder variant / registered row) MAY supply a
// TIGHTER ceiling; the hard cap can never be raised past it, and an invalid
// supplied ceiling is rejected rather than silently defaulted (Aster seq 704,
// findings 1-3).

export function isCertified(x) { try { return _certified.has(x); } catch { return false; } }
// Construction-time structural tag (trap-free WeakMap read). null for a plain
// object (classified as 'obj' without prototype traversal) or an uncertified value.
export function kindOf(x) { try { return _kind.get(x) || null; } catch { return null; } }

// Return true if the UTF-8 byte length of `s` exceeds `cap`, counted exactly and
// with an early stop. This is a BYTE ceiling, not a JS string-length ceiling
// (Aster Gate-2 correction). A valid surrogate pair is 4 bytes; a lone surrogate
// is the 3-byte replacement. Every UTF-16 code unit contributes >= 1 byte, so
// `s.length > cap` already implies over-cap — used as an O(1) fast reject before
// the O(<=cap) exact count.
function utf8Over(s, cap) {
  if (s.length > cap) return true;
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xD800 && c <= 0xDBFF) {
      const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (d >= 0xDC00 && d <= 0xDFFF) { n += 4; i++; } else n += 3;
    } else n += 3;
    if (n > cap) return true;
  }
  return false;
}

// The shared certifying-decoder core. `reviver` is null (plain) or the fixed
// internal bigintReviver. Not exported; callers use the two fixed variants.
function _certify(reviver, serialized, maxBytes) {
  // Input-type normalization: only a string is a valid wire frame.
  if (typeof serialized !== 'string') return null;
  // F7 ceiling resolution (Aster seq 704 & 708, findings 1 & 3):
  //   - OMITTED (the caller passed no cap → the wrapper substitutes the OMITTED
  //     sentinel via arguments.length) → the transport hard cap MAX_FRAME_BYTES.
  //     Defaulting is legal ONLY on a genuinely omitted argument.
  //   - SUPPLIED (any value, INCLUDING an explicit `undefined`) → must be a
  //     positive integer that does NOT raise the hard cap. Every other supplied
  //     value — undefined, null, 0, negative, fractional, NaN, Infinity,
  //     non-number, or a value above MAX_FRAME_BYTES — is REJECTED (return null →
  //     uncertified → observation no-op), never silently defaulted. The hard cap
  //     is therefore invariant: it can only be tightened, and an explicitly
  //     supplied undefined does not fall open to the default (seq 708 finding 3).
  let cap;
  if (maxBytes === OMITTED) {
    cap = MAX_FRAME_BYTES;
  } else if (Number.isInteger(maxBytes) && maxBytes > 0 && maxBytes <= MAX_FRAME_BYTES) {
    cap = maxBytes;
  } else {
    return null;
  }
  // F7: pre-parse UTF-8 byte ceiling, enforced BEFORE JSON.parse.
  if (utf8Over(serialized, cap)) return null;
  let g;
  try { g = reviver ? JSON.parse(serialized, reviver) : JSON.parse(serialized); }
  catch { return null; }
  brandWalk(g, 0, { n: 0 });
  return g;
}

// certifyPlain / certifyBigint: parse a serialized frame with the fixed decoder
// for its transport variant and brand every reachable node, tagging its
// structural kind at construction. Input is text, so under intact realm
// intrinsics the parse output is a plain graph and a Proxy cannot enter the
// certified set. Returns the graph, or null on non-string / over-ceiling /
// malformed input. Internal callers only (the frame decoders).
export function certifyPlain(serialized, maxBytes)  { return _certify(null, serialized, arguments.length < 2 ? OMITTED : maxBytes); }
export function certifyBigint(serialized, maxBytes) { return _certify(bigintReviver, serialized, arguments.length < 2 ? OMITTED : maxBytes); }

// Back-compat alias: `certify` is the plain variant (the S1i callers + the
// 48-gate core suite predate the split and use no bigint reviver).
export const certify = certifyPlain;

function brandWalk(v, depth, budget) {
  if (v === null || typeof v !== 'object') return;
  if (depth > MAX_DEPTH || budget.n >= MAX_NODES) return;   // beyond bounds: unbranded → skipped at read
  budget.n++;
  _certified.add(v);
  // v is fresh parser output here (plain under the intact-realm assumption), so
  // Array.isArray and v.length are safe AT CONSTRUCTION. The result is frozen
  // into a tag that the dispatcher reads later without touching v's prototype.
  if (Array.isArray(v)) {
    _kind.set(v, Object.freeze({ k: 'arr', len: v.length }));
    for (let i = 0; i < v.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[i], depth + 1, budget); }
  } else {
    const ks = Object.keys(v);
    for (let i = 0; i < ks.length; i++) { if (budget.n >= MAX_NODES) break; brandWalk(v[ks[i]], depth + 1, budget); }
  }
}

export default certify;
