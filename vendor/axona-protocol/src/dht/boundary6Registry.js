// boundary6Registry.js — REF-1.1 E2.0: the Boundary-6 (direct messaging)
// frame-contract registry TABLE plus the (wire, transportKind) wiring map. The
// second of the two new registries E2.0 adds (council: David ruled two; Aster
// ASTER-E2-SITE-IDENTITY, Vega b524f5af). It owns the three A2/A3 direct-message
// frames no B1–B4 registry declared.
//
// SHADOW / CONTRACT-DECLARATION ONLY (E2.0). No handler moves here.
//
// THE THREE FRAMES (verified against the E0 manifest):
//   onRequest       axona:direct        (AxonaPeer.js:2599) — request/reply leg
//   onNotification  axona:direct        (AxonaPeer.js:2615) — fire-and-forget leg
//   onRoutedMessage __tunneled_direct__ (AxonaPeer.js:3131) — DHT-relayed direct
//
// WHY THIS BOUNDARY NEEDS THE (wire, transportKind) KEY (Vega b524f5af): the two
// `axona:direct` legs are ONE wire on TWO primitives. A wire-only wiring map
// (`wiring.get('axona:direct')`) cannot own both — it would silently keep one and
// drop the other. So Boundary-6 keys every row by frameWiringKey(wire,
// transportKind), and the E2.4 migration NAMES the primitive it binds. This is the
// only same-wire collision in the whole 38-site set; `hello` (B2 vs B3) is two
// sites on two DIFFERENT registries, which registerFrame already disambiguates by
// the registry it is handed — not a door change.
//
// MODELING (council-ratified, Aster ASTER-E2-CHANNEL-SUBJECT / Vega / Orion):
//   * The axona:direct REQUEST leg is REQUEST_RESPONSE carrying the
//     CorrelationSubjectKind.TransportRpcRef channel subject — the reply obligation
//     is the transport request-return pair, not a payload field (empty requires +
//     fixed transportScope 'request-return'). The notify + tunneled routed legs are
//     fire-and-forget → ONE_WAY. transportKind 'request' still selects onRequest at
//     registration.
//   * No new wire fields. Rows describe the EXISTING frames.

import { defineRow, FrameKind, Retry, NOT_APPLICABLE as NA, ShadowRegistry, frameWiringKey, CorrelationSubjectKind } from '../registry/index.js';

const V = { min: 4, max: 4 };                 // Kernel-4 wire version (matches B1)

function rowDefs() {
  // The axona:direct REQUEST leg awaits a reply on the transport leg → REQUEST_RESPONSE
  // with a TransportRpcRef channel subject (Aster ASTER-E2-CHANNEL-SUBJECT). The
  // notify leg and the tunneled routed leg are fire-and-forget → ONE_WAY.
  const direct = (type, wire, transportKind, note) => {
    const isRequest = transportKind === 'request';
    return {
      type, wire, transportKind,
      kind: isRequest ? FrameKind.REQUEST_RESPONSE : FrameKind.ONE_WAY, owningService: 'DirectMessaging', versionRange: V,
      ...(isRequest ? { correlation: { kind: CorrelationSubjectKind.TransportRpcRef, requires: [], transportScope: 'request-return' } } : {}),
      retry: Retry.NONE, topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      admissionGuard: NA, placementGuard: NA, note,
    };
  };
  return [
    // ── axona:direct — ONE wire, TWO primitives (the (wire, transportKind) case) ──
    direct('direct:axona-direct-request', 'axona:direct', 'request',
      'app direct-message REQUEST leg: sender awaits a reply. onRequest(:2599). Shares the wire with the notify leg; disambiguated by transportKind.'),
    direct('direct:axona-direct-notify',  'axona:direct', 'notification',
      'app direct-message NOTIFY leg: fire-and-forget. onNotification(:2615). Same wire as the request leg, different primitive.'),
    // ── tunneled direct — the DHT-relayed variant (routed) ──
    direct('direct:tunneled',             '__tunneled_direct__', 'routed',
      'a direct message relayed through the DHT to a terminus that has no direct channel. onRoutedMessage(:3131).'),
  ];
}

// Mint the Boundary-6 rows (defineRow-branded). Throws if any def fails validation.
function boundary6Rows() { return rowDefs().map(defineRow); }

// The wiring map: (wire, transportKind) -> { type, transportKind }. The composite
// key is what lets the two `axona:direct` legs coexist; a bare `wiring.get(wire)`
// for axona:direct therefore misses, forcing the migration to name the primitive.
function frameWiring(defs) {
  const out = new Map();
  for (const d of defs) {
    const key = frameWiringKey(d.wire, d.transportKind);
    if (out.has(key)) throw new Error(`boundary6Registry: duplicate (wire,transportKind) ${d.wire}/${d.transportKind}`);
    out.set(key, { type: d.type, wire: d.wire, transportKind: d.transportKind });
  }
  return out;
}

// Build a Boundary-6 ShadowRegistry with every row registered, plus the wiring the
// E2.4 migration site will read. Construction throws on any defineRow failure.
export function buildBoundary6Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'direct-messaging', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

export { boundary6Rows, rowDefs, frameWiring };
export default buildBoundary6Registry;
