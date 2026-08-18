// =====================================================================
// registerDirectFrame.js — REF-1.1 E3 decision 2: the ONE named registrar for the
// onDirectMessage `direct_${type}` family (Aster ASTER-E3-DESIGN / council-ratified).
//
// registerFrame's literal-wire gate [V2] refuses a COMPUTED wire, and direct
// messaging needs `direct_${type}` for an app-chosen `type`. Rather than grow a
// parameterized variant into the canonical door, the direct family gets this single
// named low-level registrar — an enumerated mechanism shim, frozen by MODULE IDENTITY
// in the S5 ownership fence (decision 3). It is the SOLE place in the kernel that
// constructs a `direct_*` wire.
//
// The seal-relevant invariant it enforces is a SHAPE, not a hardcoded type list
// (council decision 2, option (b), David 2026-08-17): the wire is always
// `direct_` + a well-formed literal type token, never a computed/arbitrary string.
// A non-string, empty, or already-`direct_`-prefixed `type` is refused. App-level
// admission (which types are allowed) stays with the caller's finite, immutable-at-
// construction directMessageTypes gate (AxonaPeer._gateDirectType); this registrar
// adds the structural guarantee that no computed wire escapes the sealed primitive.
//
// It reaches the notification primitive through the allowlisted capability reader
// (readDispatchCapability) ONLY. E3c removed the transitional literal-name fallback:
// capability presence is mandatory, matching the canonical door.
// =====================================================================
import { readDispatchCapability } from './registerFrame.js';

/**
 * Bind `handler` to the `direct_${type}` notification leg of `recv`.
 * @param {object} recv    the transport carrying the notification primitive
 * @param {string} type    a well-formed direct-message type (NOT `direct_`-prefixed)
 * @param {(fromId, payload) => void} handler
 * @returns {void}
 */
export function registerDirectFrame(recv, type, handler) {
  if (!recv || (typeof recv !== 'object' && typeof recv !== 'function')) {
    throw new TypeError('registerDirectFrame: recv (the transport) required');
  }
  // SHAPE invariant (negative tests): the wire is `direct_` + a literal type token.
  // Reject non-string / empty / already-prefixed / computed-looking types so no
  // arbitrary or computed wire can reach the sealed primitive through this registrar.
  if (typeof type !== 'string' || type.length === 0 || type.startsWith('direct_')) {
    throw new TypeError(`registerDirectFrame: malformed direct-message type ${JSON.stringify(type)} — must be a non-empty string, not 'direct_'-prefixed`);
  }
  if (typeof handler !== 'function') {
    throw new TypeError(`registerDirectFrame(${type}): handler function required`);
  }
  const wire = `direct_${type}`;

  // Read the sealed receiver's notification closure through the allowlisted reader.
  // E3c (SEAL): capability presence is MANDATORY — no literal-name fallback. A receiver
  // that has not deposited cannot register a direct frame; it throws. This is the same
  // mandatory-capability rule the canonical door enforces (E3b.4, Aster option 1
  // 39012d73), applied to the one parameterized registrar. After E3c no `recv.onX(...)`
  // named-primitive call survives anywhere in src.
  const cap = readDispatchCapability(recv);
  if (!cap) {
    throw new TypeError(`registerDirectFrame(${type}): receiver has no deposited dispatch capability — every direct-message receiver must deposit through depositDispatchCapability at construction (E3 seal: capability presence is mandatory, no literal-name fallback). Test doubles opt in via test/lib/testCapability.mjs.`);
  }
  if (typeof cap.notification !== 'function') {
    throw new TypeError(`registerDirectFrame(${type}): sealed recv has no notification dispatch capability`);
  }
  return cap.notification(wire, handler);
}

export default registerDirectFrame;
