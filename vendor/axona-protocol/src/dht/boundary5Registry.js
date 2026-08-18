// boundary5Registry.js — REF-1.1 E2.0: the Boundary-5 (DHT routing / core)
// frame-contract registry TABLE plus the (wire, transportKind) wiring map that
// E2.5 will route through registerFrame. Boundary-5 is one of the two new
// registries E2.0 adds to home the E0 orphan set (council: David ruled two;
// Aster ASTER-E2-SITE-IDENTITY, Vega 4b66abe0): it owns the ten A1 DHT-core
// frames that no B1–B4 registry declared.
//
// SHADOW / CONTRACT-DECLARATION ONLY (E2.0). No handler moves here; this file
// declares the ten routing frames so registerFrame and the ownership fence will
// accept the E2.5 migration. Every flag-off path stays byte-identical; there is
// no live observer wired to Boundary-5 yet.
//
// THE TEN FRAMES (AxonaPeer.start, verified against the E0 manifest):
//   onRequest      lookup_step (:465) lookahead_probe (:483) local_probe (:598)
//                  find_closest_set (:622) route_msg (:648)
//   onNotification reinforce (:498) triadic_introduce (:526) hop_cache (:535)
//                  lateral_spread (:536) peer-leaving (:563)
// Each wire is bound on exactly ONE primitive, so unlike Boundary-6 there is no
// same-wire collision inside this boundary.
//
// MODELING (council-ratified, Aster ASTER-E2-CHANNEL-SUBJECT / Vega / Orion):
//   * THE onRequest RPCs ARE REQUEST_RESPONSE, NOT ONE_WAY. lookup_step /
//     lookahead_probe / local_probe / find_closest_set / route_msg return a value
//     over the transport request/response leg, so the contract must record the
//     reply obligation. They carry CorrelationSubjectKind.TransportRpcRef — a
//     CHANNEL subject: the request↔return pairing is the transport RPC channel, not
//     a payload field, so correlation.requires is EMPTY and transportScope is the
//     fixed 'request-return'. The onNotification legs (reinforce, triadic_introduce,
//     hop_cache, lateral_spread, peer-leaving) are genuinely fire-and-forget →
//     ONE_WAY. `transportKind:'request'` still selects onRequest at registration.
//   * No new wire fields. Rows describe the EXISTING frames.
//   * owningService groups the ten by plane: DhtRouting (the iterative lookup /
//     route legs), SynaptomeLearning (synapse maintenance), PeerLifecycle.

import { defineRow, FrameKind, Retry, NOT_APPLICABLE as NA, ShadowRegistry, frameWiringKey, CorrelationSubjectKind } from '../registry/index.js';

const V = { min: 4, max: 4 };                 // Kernel-4 wire version (matches B1)

// The Boundary-5 row DEFINITIONS. `wire` (the transport registration label) and
// `transportKind` (the dispatch primitive) are carried on the def so the wiring
// map derives from these; defineRow drops both as unknown keys.
function rowDefs() {
  // A request leg is REQUEST_RESPONSE with a TransportRpcRef channel subject (the
  // reply is the transport return); a notification leg is ONE_WAY (Aster
  // ASTER-E2-CHANNEL-SUBJECT — the contract must record the reply obligation).
  const routing = (type, wire, transportKind, owningService, note) => {
    const isRequest = transportKind === 'request';
    return {
      type, wire, transportKind,
      kind: isRequest ? FrameKind.REQUEST_RESPONSE : FrameKind.ONE_WAY, owningService, versionRange: V,
      ...(isRequest ? { correlation: { kind: CorrelationSubjectKind.TransportRpcRef, requires: [], transportScope: 'request-return' } } : {}),
      retry: Retry.NONE, topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      admissionGuard: NA, placementGuard: NA, note,
    };
  };
  return [
    // ── iterative lookup / routing legs (onRequest) ──
    routing('dht:lookup_step',      'lookup_step',      'request', 'DhtRouting',
      'one hop of the iterative lookup: given a target, return this node\'s closest known peers. REQUEST_RESPONSE carrying the TransportRpcRef channel subject: the reply is the transport return, associated by the request-return pair, not a payload correlation.'),
    routing('dht:lookahead_probe',  'lookahead_probe',  'request', 'DhtRouting',
      'speculative next-hop probe used to prime the routing table ahead of the walk.'),
    routing('dht:local_probe',      'local_probe',      'request', 'DhtRouting',
      'liveness/identity probe of a directly-known peer; D-4 trimmed its disclosure.'),
    routing('dht:find_closest_set', 'find_closest_set', 'request', 'DhtRouting',
      'return the K closest known peers to a target id (K clamp is issue #433, out of scope here).'),
    routing('dht:route_msg',        'route_msg',        'request', 'DhtRouting',
      'forward an application/routed payload one hop toward its destination id.'),
    // ── synaptome learning (onNotification) ──
    routing('dht:reinforce',        'reinforce',        'notification', 'SynaptomeLearning',
      'strengthen an EXISTING first-party synapse (capped weight); does not create table entries.'),
    routing('dht:triadic_introduce','triadic_introduce','notification', 'SynaptomeLearning',
      'gossip a candidate peer into the introduction pool, not directly into the table.'),
    routing('dht:hop_cache',        'hop_cache',        'notification', 'SynaptomeLearning',
      'cache a freshly-observed hop for later reuse; shares its handler with lateral_spread.'),
    routing('dht:lateral_spread',   'lateral_spread',   'notification', 'SynaptomeLearning',
      'lateral propagation of a hop observation; same handler as hop_cache.'),
    // ── peer lifecycle (onNotification) ──
    routing('dht:peer-leaving',     'peer-leaving',     'notification', 'PeerLifecycle',
      'a peer announces departure; we delete its synapse and drop it from the table.'),
  ];
}

// Mint the Boundary-5 rows (defineRow-branded). Throws if any def fails validation.
function boundary5Rows() { return rowDefs().map(defineRow); }

// The wiring map: wire -> { type, transportKind }. Every Boundary-5 wire is
// single-primitive, so it keys by the BARE wire, like Boundary-1 — the composite
// (wire, transportKind) key is scoped to Boundary-6 alone, where two axona:direct
// sites collide on one wire (Aster ASTER-E2-KEY-SCOPE: "make the composite lookup
// change only in B6"). registerFrame resolves a bare-wire key when the caller
// omits transportKind, and reads transportKind off the row to select the primitive.
function frameWiring(defs) {
  const out = new Map();
  for (const d of defs) {
    if (out.has(d.wire)) throw new Error(`boundary5Registry: duplicate wire ${d.wire} (Boundary-5 is single-primitive per wire)`);
    out.set(d.wire, { type: d.type, wire: d.wire, transportKind: d.transportKind });
  }
  return out;
}

// Build a Boundary-5 ShadowRegistry with every row registered, plus the wiring
// the E2.5 migration site will read. `enabled` gates observation (default-off).
// Construction throws if any row fails defineRow validation.
export function buildBoundary5Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'dht-routing', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

export { boundary5Rows, rowDefs, frameWiring };
export default buildBoundary5Registry;
