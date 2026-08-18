// =====================================================================
// registerFrame.js — REF-1.1 E1: the ONE canonical frame-registration door.
//
// The enforcement cutover makes this function the sole path by which a frame
// handler is wired to a raw dispatch primitive. E1 lands the door and proves it;
// it does NOT migrate the existing call sites (E2) and does NOT seal the raw
// primitives (E3). At E1 the raw methods are still public and still used by the
// frozen baseline of legacy sites; registerFrame is the intended sole door going
// forward, exercised and proven on Boundary-1's rows.
//
// Design: REF-1.1-Enforcement-Cutover-Design-v5.md. This module realizes exit
// criterion 1 (one canonical path), the shadow-wrap reuse [V5], and the
// registration-time ownership refuse (exit criterion 4).
//
//   registerFrame(recv, wire, handler, { registry })
//
// - `recv`     the receiver carrying the raw dispatch primitive (e.g. the DHT
//              adapter for Boundary-1; a transport for Boundary-2/3/4 at E2).
// - `wire`     the frame-type constant. The AST wire-literal gate [V2] enforces
//              that call sites pass a literal / frozen-constant here; this function
//              enforces at runtime that the registry declares it.
// - `handler`  the frame handler; wrapped by the shared ShadowRegistry.wrap [V5].
// - registry   the boundary ShadowRegistry (carries `.wiring` wire->row, `.wrap`,
//              and `.mintLive`). The row's `transportKind` selects the primitive
//              INTERNALLY — one door, not two (exit criterion 1 [V2]).
//
// THE OBSERVATION CERTIFIER IS REGISTRY-OWNED, NOT A CALLER ARGUMENT (Aster E1
// review F1). Each boundary registry carries its own `mintLive` (B1 =
// certifyBigint∘encode); registerFrame reads it from the registry and threads it
// into the wrap. There is NO public caller-supplied path to live-observation
// certification. And observation itself only fires when the runtime shadow flag is
// on (default off), so a public registerFrame call is shadow-only regardless.
//
// SHADOW-MODE ONLY, wire unchanged, WIRE_VERSION 4.0. When the shadow flag is off
// (default) the wrap runs the handler verbatim, so flag-off is byte-identical.
// =====================================================================

// REF-1.1 E2.0 (council: Aster ASTER-E2-SITE-IDENTITY, Vega 4b66abe0): a wire may
// bind on more than one dispatch primitive within one boundary — `axona:direct`
// is registered on onRequest (AxonaPeer.js:2599) AND onNotification (:2615), one
// wire, two primitives. So the wiring key is (wire, transportKind), not the wire
// alone. A registry that carries a colliding wire keys those rows by frameWiringKey
// and the migration NAMES the primitive it is binding. A bare wire still resolves
// when the caller omits transportKind and the wire is unambiguous (the B1–B5 case,
// keyed by plain wire), so single-primitive migrations are unchanged.
const DISPATCH_KINDS = new Set(['routed', 'notification', 'request']);

// =====================================================================
// REF-1.1 E3 (SEAL) — the capability channel.
//
// The seal removes the ability to reach a transport's raw dispatch primitive
// (onRequest / onNotification / onRoutedMessage) without going through this door.
// A sealed transport no longer carries the primitive as a public method; instead
// its constructor DEPOSITS the three dispatch closures into this module-private
// WeakMap, keyed by the transport instance. registerFrame is the ONLY reader.
//
// "READ SOLELY BY registerFrame" (Aster ASTER-E3-DESIGN condition 1) is structural
// here, not merely allowlisted: the WeakMap is module-private state of this file
// and no read accessor is exported. Transports receive a WRITE-ONLY deposit; they
// cannot read their own capability back. E3a introduced the channel and sealed one
// transport (the node WebSocket transport); E3b sealed the rest + the Transport.js
// contract and REMOVED the transitional named fallback — capability presence is now
// MANDATORY (Aster boundary ruling 39012d73 / option 1). A receiver that has not
// deposited cannot register a frame; the deposited closure is the only path.
// =====================================================================
const DISPATCH_CAPABILITY = new WeakMap();

/**
 * Deposit a sealed receiver's raw dispatch closures into the capability channel.
 * Called by a sealed transport's constructor with the subset of dispatch slots it
 * carries, keyed by transport KIND — { request?, notification?, routed? } — NOT by
 * the public method names. The slot keys are the row.transportKind values, so the
 * seal genuinely removes the onRequest/onNotification/onRoutedMessage names from the
 * running program: they survive nowhere, not even as channel property keys. Each
 * closure is this-bound at construction, so registerFrame calls it bare.
 * WRITE ONLY: there is no companion reader export — the channel is read solely by
 * registerFrame, in this module.
 */
export function depositDispatchCapability(recv, caps) {
  if (!recv || (typeof recv !== 'object' && typeof recv !== 'function')) {
    throw new TypeError('depositDispatchCapability: recv (the sealed receiver) required');
  }
  if (!caps || typeof caps !== 'object') {
    throw new TypeError('depositDispatchCapability: caps ({ request?, notification?, routed? }) required');
  }
  const channel = {};
  for (const slot of ['request', 'notification', 'routed']) {
    if (caps[slot] == null) continue;
    if (typeof caps[slot] !== 'function') {
      throw new TypeError(`depositDispatchCapability: caps.${slot} must be a function`);
    }
    channel[slot] = caps[slot];
  }
  DISPATCH_CAPABILITY.set(recv, channel);
}

// REF-1.1 E3b — the allowlisted capability READER for the enumerated mechanism-shim
// set (Aster ASTER-E3-DESIGN decision 3: the surviving shims are frozen by MODULE
// IDENTITY). registerFrame reads the module-private WeakMap directly; the mechanism
// shims that legitimately replay/forward a sealed primitive read through this export:
//   - the CompositeTransport fan-out (replays a handler to each sub-transport),
//   - the AxonaPeer routed demux (delegates onRoutedMessage to the peer),
//   - registerDirectFrame (the onDirectMessage direct_* parameterized registrar).
// The S5 ownership fence freezes the SET of importers to exactly those modules; a new
// importer fails closed. Returns { request?, notification?, routed? } (kind-keyed, the
// same channel registerFrame reads) or undefined for an unsealed receiver.
export function readDispatchCapability(recv) {
  return DISPATCH_CAPABILITY.get(recv);
}

export const frameWiringKey = (wire, transportKind) => `${transportKind}\0${wire}`;

export function registerFrame(recv, wire, handler, { registry, transportKind } = {}) {
  if (!recv || (typeof recv !== 'object' && typeof recv !== 'function')) {
    throw new TypeError('registerFrame: recv (an object carrying the dispatch primitive) required');
  }
  if (typeof wire !== 'string' || !wire) {
    throw new TypeError('registerFrame: wire (non-empty frame-type string) required');
  }
  if (typeof handler !== 'function') {
    throw new TypeError(`registerFrame(${wire}): handler function required`);
  }
  if (!registry || typeof registry.wiring?.get !== 'function' || typeof registry.wrap !== 'function') {
    throw new TypeError(`registerFrame(${wire}): a boundary registry with .wiring and .wrap required`);
  }
  if (transportKind != null && !DISPATCH_KINDS.has(transportKind)) {
    throw new TypeError(`registerFrame(${wire}): transportKind must be one of routed|notification|request`);
  }

  // Ownership refuse (exit criterion 4): the registry rows are the single source
  // of truth for which (boundary, wire) pairs exist. A wire no row declares is a
  // mis-bound registration — refuse it AT REGISTRATION, do not silently wire it.
  //
  // STRICT COMPOSITE/BARE PARTITION (Aster E2.0 re-review 2795636a cond.1): the two
  // lookup paths are EXACTLY two, non-overlapping, with NO fallback between them.
  //   * transportKind SUPPLIED → composite-key lookup ONLY. A composite miss
  //     REFUSES. It does NOT fall back to a bare-wire row — not even one whose own
  //     transportKind happens to match. A "bare row of the same primitive" fallback
  //     is still a second lookup path, and two paths is exactly the ambiguity this
  //     door exists to forbid. Composite keys exist only where a wire binds >1
  //     primitive within one registry (B6 axona:direct, the sole such collision).
  //   * transportKind OMITTED → bare-wire lookup ONLY (the unambiguous B1–B5
  //     single-primitive path). The row's own transportKind then selects the leg.
  // Consequence: supplying a transportKind COMMITS the caller to the composite
  // path, so a single-primitive registry (B1–B5, no composite keys) REFUSES any
  // supplied transportKind and is bound only by OMITTING it; a composite registry
  // (B6) is bound only by NAMING the primitive, and an omitted kind there refuses
  // rather than silently picking a leg. One wire, one path, no fallback.
  let row = null;
  if (transportKind != null) {
    row = registry.wiring.get(frameWiringKey(wire, transportKind)) || null;   // composite ONLY — a miss refuses
  } else {
    row = registry.wiring.get(wire) || null;                                  // bare ONLY
  }
  if (!row) {
    throw new Error(`registerFrame: no registry row declares wire "${wire}"${transportKind ? ` on transportKind "${transportKind}" (strict composite lookup — omit transportKind to bind a single-primitive B1–B5 wire)` : ''} — refusing an undeclared (recv, wire) binding`);
  }

  // The observation certifier is REGISTRY-OWNED (Aster F1): read it from the
  // registry, never from a caller argument. undefined is fine (the wrap treats a
  // missing mintLive as the unbranded-source path).
  const mintLive = registry.mintLive;

  // [V5] Reuse the EXISTING shadow wrap — not a second wrapper. Flag-off it runs
  // the handler verbatim (byte-identical); flag-on it observes a certified snapshot
  // beside the handler and emits a trace, never mutating args or the return value.
  const wrapped = registry.wrap(row.type, handler, {
    ...(mintLive ? { mintLive } : {}),
    ...(row.variantBy ? { variantBy: row.variantBy } : {}),
  });

  // Select the raw dispatch primitive INTERNALLY from the row's transport kind —
  // one door, not two (exit criterion 1 [V2]).
  //
  // REF-1.1 E3 (SEAL) — CAPABILITY PRESENCE IS MANDATORY (Aster boundary ruling
  // 39012d73 / option 1; Vega 94cdf8ec; Orion b86f4c0d). Every frame receiver — the
  // five transports, the AxonaPeer, the CompositeTransport, and the default-DHT
  // adapter — DEPOSITS its dispatch closures into the module-private capability
  // channel (DISPATCH_CAPABILITY) at construction. registerFrame reaches the raw
  // primitive ONLY through that deposit. There is NO literal-name fallback: a
  // receiver that has not deposited cannot register a frame — it throws. This closes
  // the source-level seal gap the E3a/E3b transitional fallback left open: no
  // `recv.onRoutedMessage(...)` / `recv.onNotification(...)` / `recv.onRequest(...)`
  // branch survives in this door, so no future or unsealed receiver can reach a raw
  // primitive by name. Test doubles that must act as a sealed receiver deposit
  // explicitly through test/lib/testCapability.mjs — the production path gains no
  // test affordance.
  //
  // The channel is keyed by transport KIND (request|notification|routed), matching
  // row.transportKind — closures were deposited under those slots, NOT under the
  // public method names, so the seal removes those names from the running program.
  // registerFrame reads the slot by a LITERAL name (never `cap[row.transportKind]`,
  // which would be a computed access the runtime boundary forbids) and calls the
  // this-bound closure bare.
  const cap = DISPATCH_CAPABILITY.get(recv);
  if (!cap) {
    throw new TypeError(`registerFrame(${wire}): receiver has no deposited dispatch capability — every frame receiver must deposit through depositDispatchCapability at construction (E3 seal: capability presence is mandatory, no literal-name fallback). Test doubles opt in via test/lib/testCapability.mjs.`);
  }
  switch (row.transportKind) {
    case 'routed':
      if (typeof cap.routed !== 'function') throw new TypeError(`registerFrame(${wire}): sealed recv has no routed dispatch capability`);
      return cap.routed(wire, wrapped);
    case 'notification':
      if (typeof cap.notification !== 'function') throw new TypeError(`registerFrame(${wire}): sealed recv has no notification dispatch capability`);
      return cap.notification(wire, wrapped);
    case 'request':
      if (typeof cap.request !== 'function') throw new TypeError(`registerFrame(${wire}): sealed recv has no request dispatch capability`);
      return cap.request(wire, wrapped);
    default:
      throw new Error(`registerFrame(${wire}): row.transportKind "${row.transportKind}" has no known dispatch primitive`);
  }
}

export default registerFrame;
