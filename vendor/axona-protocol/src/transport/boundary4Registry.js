// boundary4Registry.js — REF-1.1 S4c: the Boundary-4 (bridge administration)
// frame-contract registry TABLE, plus the wiring map that will shadow-wrap the
// bridge-administration ingress surface in transport/web. These are the peer<->
// bridge control frames on the bridge WebSocket that are NOT transport auth
// (Boundary-2: welcome/hello/hello-ack/cap-attest) and NOT WebRTC signalling
// (Boundary-3: peer-list/peer-joined/peer-left/signal/mesh hello/hello-sig):
//   admission  — client-hello (peer->bridge), version-gate (bridge->peer)
//   heartbeat  — ping (peer->bridge), pong (bridge->peer)
//   turn       — turn-refresh (peer->bridge), turn (bridge->peer)
//   discovery  — peer-list-request (peer->bridge; its reply peer-list is B3)
//
// SHADOW MODE ONLY. Changes NO acceptance behavior. Built only when the owning
// transport is constructed with the frame-registry flag; the observe path runs
// nothing when the runtime shadow flag is off (default) — flag-off is byte-
// identical. Flag-on OBSERVES a decoder-certified snapshot beside each handler
// and emits a trace; never mutating, suppressing, or reordering a handler or its
// arguments. Dispatch is NOT migrated (§4.3).
//
// GROUNDING (code-refactor-plan §4.3): Boundary-4 is registry #4 of four —
// "bridge administration frames". Cross-checked against the live wire:
//   client-hello        web/index.js:291   (SEND) / bridge server.js:1068 (HANDLE)
//   version-gate        bridge server.js:931 (EMIT) / web/index.js:546   (no-op)
//   ping                web/index.js:1070  (SEND) / bridge server.js:1163 (HANDLE)
//   pong                bridge server.js:1175 (EMIT) / web/index.js:541  (recordPong)
//   turn-refresh        web/index.js:794   (SEND) / bridge server.js:1187 (HANDLE)
//   turn                bridge server.js:1195 (EMIT) / web/index.js:498  (applyTurnFrame)
//   peer-list-request   web/index.js:1220  (SEND) / bridge server.js:1150 (HANDLE)
//
// DELIBERATE MODELING DECISIONS (flagged for review):
//   * EVIDENCE AXIS NULL. Like Boundary-2/3, these frames administer the bridge
//     SESSION (admission, heartbeat, credential refresh, discovery request); they
//     prove connection/administration state, not pub/sub DATA movement (the #28
//     evidence hierarchy is the data plane). So `evidence`/`proves` are null (the
//     wrong axis) and each row carries a named `outcome` + `terminalOutcome`.
//   * DIRECTION-SPLIT INGRESS SCOPE. The TABLE is the full boundary contract for
//     all seven frames, BOTH directions. But only THREE are ingested by the
//     KERNEL (this transport): version-gate / pong / turn, dispatched in
//     signaling.dispatch. The other four (client-hello / ping / turn-refresh /
//     peer-list-request) are SENT by the kernel and ingested by the BRIDGE SERVER
//     (axona-bridge server.js), a separate surface outside the kernel. So the
//     kernel-side live wiring (S4c increment 2) can observe only the three
//     peer-RECEIVED frames; the four peer-SENT frames have no kernel ingress
//     handler to wrap (a bridge-side registry is out of REF-1.1's kernel scope).
//     The rows still document them so the boundary contract is complete.
//   * ADMISSION GUARD, NOT AUTH GUARD. Boundary-2/3 carry a cryptographic
//     `authGuard` (verifyAuthHello / verifyCapAttest). Bridge administration is
//     gated on PROTOCOL VERSION and ADMITTED STATE, not identity. So client-hello
//     carries an `admissionGuard` — the THREE-stage version gate (F1): REQUIRED_WIRE_MAJOR,
//     then MIN_PEER_VERSION, then flagDayFloor(peerVersion) which picks a namespace floor
//     (MIN_KERNEL_VERSION or MIN_PEER_APP_VERSION) and rejects below effectiveMin = max of
//     the two (server.js:1123); optional STRICT_MIN_KERNEL on kernelVersion; a miss closes
//     4426 before admission. The post-admission frames (ping/turn-refresh/peer-list-request)
//     carry admissionGuard 'admitted' (dropped pre-hello). `authGuard` stays NA across this
//     boundary — none of these frames is signed.
//   * PING/PONG IS A `t`-KEYED CONVERSATION ON THE PAYLOAD LEG. pong echoes the
//     ping's `t` timestamp unchanged (server.js:1175) so the client can sample
//     RTT (recordPong). That is a REAL correlation key, so ping is the REQUEST leg
//     and pong the RESPONSE leg, paired on `t` — but on the PAYLOAD side (B3's
//     mutual-auth paired on the META leg meshId; this complements it). The KIND
//     stays ONE_WAY (each heartbeat leg is fire-and-forget), NOT REQUEST_RESPONSE
//     (which defineRow would force a correlation-SUBJECT union member onto — none
//     of LegacyAuthorityRef/IngressRef/HolderRef/AuthorLaneRef fits a heartbeat).
//   * TURN AND PEER-LIST-REQUEST ARE SOLICITED WITH NO WIRE KEY. turn-refresh->turn
//     and peer-list-request->peer-list are request/reply, but neither reply echoes
//     a wire field to correlate on (turn-refresh carries nothing; the peer-list
//     reply is a full snapshot). Like B3's signal offer/answer, the pairing is the
//     single in-flight socket round-trip, not a frame field — so NO conversation is
//     declared (declaring one would require a pairing key that does not exist). The
//     solicitation is noted in prose. peer-list-request's reply `peer-list` is a
//     Boundary-3 row (cross-boundary), so it is not paired here either.
//   * CLIENT-HELLO IS ADMINISTRATION, NOT SESSION AUTH. welcome (its bridge->peer
//     answer) is Boundary-2 (the session/auth context: connId + serverNonce).
//     client-hello + version-gate are the VERSION-GATE ADMISSION-CONTROL pair, so
//     they are assigned here (bridge administration), not to B2. Flagged: if the
//     council prefers client-hello beside welcome in B2, a row moves between tables
//     cleanly before acceptance.
//   * NOT-A-FRAME ADMIN EXCLUDED. Graduation (WS close 4200) and upgrade-required
//     (WS close 4426) are conveyed by close CODE + reason string, not a wire
//     `type`, so they are not rows. The bridge-directory beacon rides Boundary-1
//     pub/sub (BRIDGE_DIRECTORY_TOPIC), and /healthz + /diag are HTTP endpoints —
//     all excluded from this WS-admin boundary.
//   * TYPE NAMESPACE. Row types are prefixed `bridge:` so they never collide with
//     the other registries' labels (the axona-layer onRequest('ping') is a
//     Boundary-1 axona-wire frame, distinct from this WS-admin `bridge:ping`).
//   * No new wire fields. Rows describe the EXISTING frames.

import {
  defineRow, FrameKind, Retry, NOT_APPLICABLE as NA,
  ConversationRole, PairSide, ShadowRegistry, shadowEnabled,
} from '../registry/index.js';
import { certifyPlain } from '../registry/snapshotMint.js';

const V = { min: 4, max: 4 };                 // WIRE_VERSION major = '4.x' (the bridge version-gate axis)
const REQ = ConversationRole.REQUEST, RESP = ConversationRole.RESPONSE;
const budget = (leaves, maxBytes = 1024) => ({ maxLeaves: leaves, maxBytes });

// The Boundary-4 row DEFINITIONS. `wire` (the dispatch label) is carried on the
// def so the wiring map derives from it; defineRow drops unknown keys.
function rowDefs() {
  return [
    // ── admission: client-hello (peer->bridge; version-gate admission request) ──
    ({
      type: 'bridge:client-hello', wire: 'client-hello', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeAdmission', versionRange: V,
      outcome: 'AdmissionOutcome', terminalOutcome: 'CONNECTION_ADMITTED',
      retry: Retry.NONE,   // one-shot admission handshake; a resend after admission is dropped
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,       // NOT cryptographic — admission is version-gated
      // F1 (Aster/Vega): the live gate is THREE stages, not two — the third is
      // flagDayFloor(peerVersion) at server.js:1123, which picks a namespace floor
      // and rejects below effectiveMin = max(MIN_PEER_VERSION, that floor).
      admissionGuard: 'version gate: REQUIRED_WIRE_MAJOR; peerVersion >= max(MIN_PEER_VERSION, flagDayFloor floor MIN_KERNEL_VERSION|MIN_PEER_APP_VERSION by ns); STRICT_MIN_KERNEL optional; below closes 4426 (full detail in note)',
      placementGuard: NA,
      projection: { payload: ['version', 'wireVersion', 'kernelVersion', 'capabilities'] },
      // `capabilities` is a non-scalar (string[]) — required-present + projected but
      // NOT typed, mirroring B3's ICE `candidate` object (type only the scalars).
      schema: { require: ['version', 'wireVersion', 'kernelVersion'], types: { version: 'string', wireVersion: 'string', kernelVersion: 'string' } },
      errorContract: [], traceFields: ['wireVersion'], budget: budget(6),
      note: 'the peer\'s opening frame; the ADMISSION GATE the bridge waits for — until it passes conn.admitted is false and every other frame is dropped. THREE stages: REQUIRED_WIRE_MAJOR, then MIN_PEER_VERSION, then flagDayFloor(peerVersion) picks a namespace floor (MIN_KERNEL_VERSION|MIN_PEER_APP_VERSION) and rejects below max of the two; a miss closes 4426. Success sets admitted + answers with `welcome` (Boundary-2). Carries version, wireVersion, kernelVersion, capabilities.',
    }),

    // ── admission: version-gate (bridge->peer; pre-admit gate announcement) ──
    ({
      type: 'bridge:version-gate', wire: 'version-gate', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'BridgeAdmission', versionRange: V,
      outcome: 'AdmissionOutcome', terminalOutcome: 'VERSION_GATE_ANNOUNCED',
      retry: Retry.NATURAL,   // idempotent no-op announcement; order-independent
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,   // pre-admission informational
      projection: { payload: ['minPeerVersion', 'serverT'] },
      // F3 (Aster): the bridge emits minPeerVersion as a semver STRING (server.js:931),
      // so type it — a numeric value must be schema-invalid, not silently certified.
      schema: { require: ['minPeerVersion', 'serverT'], types: { minPeerVersion: 'string', serverT: 'number' } },
      errorContract: [], traceFields: [], budget: budget(3),
      note: 'the bridge announces its MIN_PEER_VERSION once on connect, BEFORE admission, so an old client gets an obvious failure mode instead of a silent ghost. The client no-ops it (index.js:546); the real gate runs on the bridge over client-hello.',
    }),

    // ── heartbeat: ping (peer->bridge; REQUEST leg, + meshBound vitality) ──
    ({
      type: 'bridge:ping', wire: 'ping', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeHeartbeat', versionRange: V,
      outcome: 'HeartbeatOutcome', terminalOutcome: 'VITALITY_REPORTED',
      retry: Retry.NONE,   // each ping is a distinct RTT probe (its own t); not a dedup-able retry
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,
      admissionGuard: 'admitted: reached only inside the post-admission switch; dropped pre-hello',
      placementGuard: NA,
      projection: { payload: ['t', 'meshBound'] },
      schema: { require: ['t'], types: { t: 'number', meshBound: 'number' } },
      // REQUEST leg of the ping/pong heartbeat, paired with pong on the PAYLOAD leg `t`
      // (pong echoes it unchanged for RTT). KIND stays ONE_WAY (fire-and-forget leg).
      conversation: { role: REQ, opposite: 'bridge:pong', pairing: [{ local: 't', remote: 't', from: PairSide.payload }] },
      errorContract: [], traceFields: [], budget: budget(3),
      note: '~1s heartbeat (startBridgePingLoop, index.js:1070). Additive meshBound = count of authenticated bound mesh peers; the bridge records it (server.js:1163) to graduate the best-meshed peer on evidence, not an uptime guess (#374). REQUEST leg of the t-keyed ping/pong conversation.',
    }),

    // ── heartbeat: pong (bridge->peer; RESPONSE leg, RTT + liveness) ──
    ({
      type: 'bridge:pong', wire: 'pong', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeHeartbeat', versionRange: V,
      outcome: 'HeartbeatOutcome', terminalOutcome: 'RTT_SAMPLED',
      retry: Retry.NONE,   // distinct reply per ping
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,   // no guard on receipt; client samples RTT only
      projection: { payload: ['t', 'serverT'] },
      schema: { require: ['t'], types: { t: 'number', serverT: 'number' } },
      // RESPONSE leg: echoes the ping's `t`, paired on the PAYLOAD leg. KIND ONE_WAY.
      conversation: { role: RESP, opposite: 'bridge:ping', pairing: [{ local: 't', remote: 't', from: PairSide.payload }] },
      errorContract: [], traceFields: [], budget: budget(3),
      note: 'the bridge echoes the ping `t` unchanged plus its own serverT (server.js:1175). The client feeds `t` to recordPong (index.js:541) for an RTT + liveness sample; no socket or mesh change. RESPONSE leg of the t-keyed ping/pong conversation.',
    }),

    // ── turn: turn-refresh (peer->bridge; in-band TURN credential request) ──
    ({
      type: 'bridge:turn-refresh', wire: 'turn-refresh', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeTurn', versionRange: V,
      outcome: 'TurnOutcome', terminalOutcome: 'TURN_REFRESH_REQUESTED',
      // F2/F5 (Aster): NOT Retry.NATURAL. The kernel runs up to TURN_REFRESH_MAX_TRIES=3
      // ATTEMPTS (attempts 0,1,2 = at most 2 retries; requestTurnRefreshInBand, index.js:788)
      // and each bridge handling mints a FRESH random credential (makeTurnCredential), so a
      // resend is bounded and NON-idempotent — not naturally idempotent (no key, not
      // order-independent), and not NONE (the kernel does retry). F5: the bound is declared
      // STRUCTURALLY as retryMaxAttempts (defineRow requires it for BOUNDED_N), not comment-only.
      retry: Retry.BOUNDED_N,
      retryMaxAttempts: 3,   // total attempts (= at most 2 retries)
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,
      admissionGuard: 'admitted: admitted peers only (server.js:1193)',
      placementGuard: NA,
      projection: {},
      schema: { require: [] },   // carries only {type}
      errorContract: [], traceFields: [], budget: budget(2),
      note: 'a long-lived client whose 2h TURN credential is about to lapse asks for a fresh one in-band (index.js:794). The kernel runs up to TURN_REFRESH_MAX_TRIES=3 attempts (at most 2 retries), then defers; each bridge handling mints a FRESH credential (makeTurnCredential) — a bounded, NON-idempotent resend (Retry.BOUNDED_N, retryMaxAttempts=3). Solicited: the bridge replies with `turn`. No wire correlation key (socket round-trip), so no conversation.',
    }),

    // ── turn: turn (bridge->peer; TURN credential reply) ──
    ({
      type: 'bridge:turn', wire: 'turn', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeTurn', versionRange: V,
      outcome: 'TurnOutcome', terminalOutcome: 'TURN_CREDENTIAL_APPLIED',
      retry: Retry.NONE,   // apply latest credential: order-dependent replace, no dedup key
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['turn', 'serverT'] },
      // `turn` is a non-scalar (the credential object) — required-present + projected
      // but NOT typed, mirroring B3's ICE `candidate` object (type only the scalars).
      schema: { require: ['turn'], types: { serverT: 'number' } },
      errorContract: [], traceFields: [], budget: budget(8, 4096),
      note: 'the bridge mints a fresh credential and returns it (server.js:1195). The client installs it via applyTurnFrame (index.js:498) and reschedules — no socket or mesh change. DISTINCT from the `turn` FIELD carried inside `welcome` (Boundary-2): this standalone frame never re-runs welcome\'s connId/nonce bookkeeping.',
    }),

    // ── discovery: peer-list-request (peer->bridge; mesh re-warm bootstrap) ──
    ({
      type: 'bridge:peer-list-request', wire: 'peer-list-request', kind: FrameKind.ONE_WAY,
      owningService: 'BridgeDiscovery', versionRange: V,
      outcome: 'DiscoveryOutcome', terminalOutcome: 'PEER_LIST_REQUESTED',
      // F4 (Aster/Vega): a re-request is safe, so Retry.NATURAL holds — but the
      // reply is NOT a "full replace". Its consumer MeshManager.onPeerList is
      // ADDITIVE (mesh.js:458, hasPeer-guarded): skips existing peers, initiates
      // only missing ones, never removes peers absent from the snapshot.
      retry: Retry.NATURAL,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,
      admissionGuard: 'admitted: admitted peers only (server.js:1151)',
      placementGuard: NA,
      projection: {},
      schema: { require: [] },   // carries only {type}
      errorContract: [], traceFields: [], budget: budget(2),
      note: 'mesh-rewarm asks the bridge to re-introduce the signalling peers (requestPeerIntroductions, index.js:1220). Solicited: the bridge resends the admitted `peer-list` (server.js:1150), a BOUNDARY-3 row — spans two registries, no wire correlation key, so no conversation. The reply is additive, not a wholesale replacement: MeshManager.onPeerList (mesh.js:458) skips existing peers, initiates only missing ones, never removes absent ones; Retry.NATURAL via the hasPeer-guarded ingest.',
    }),
  ];
}

// Mint the Boundary-4 rows (defineRow-branded). Throws if any def fails
// validation — a table defect fails loud at build, never silently.
function boundary4Rows() { return rowDefs().map(defineRow); }

// The wiring map: dispatch label -> { type }. Built from the raw defs' `wire`
// tags. Boundary-4 has no multi-variant frame (no signal-style split), so every
// wire maps to a single row type.
function frameWiring(defs) {
  const out = new Map();
  for (const d of defs) out.set(d.wire, { type: d.type });
  return out;
}

// Build a Boundary-4 ShadowRegistry with every row registered, plus the
// wire->row wiring the bridge-admin observe sites use. `enabled` gates
// observation (default-off); `sink` receives trace records; `now` is the clock.
// Construction throws if any row fails defineRow validation.
export function buildBoundary4Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'bridge-admin', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

// ── S4c increment 2 (WIRED into web/index.js signaling.dispatch): the LIVE observe side-channel ──
// makeBoundary4Observers returns `observe(wire, scope, body)` — a pure side-
// channel (never receives/wraps/returns the handler), called before the three
// kernel-ingested bridge-admin handlers (version-gate / pong / turn). Flag-off it
// returns immediately (byte-identical). Flag-on it certifies a snapshot of `body`
// and a certified (empty) meta, and emits a shape-only trace (verdict 'unobserved';
// NO handler runs). Boundary-4 rows carry NO projection.meta (all correlation is on
// the payload leg — ping/pong's `t`), so the meta is always {} and `scope` is only
// STAMPED onto the trace, never certified as a field. Every live B4 site passes
// scope=null — bridge administration is session-wide, with no per-frame subject —
// so the stamped scope is null on the wired path; the parameter is kept for observer
// symmetry with B2/B3, which stamp a per-frame subject (connId / signalling peer).
export function makeBoundary4Observers({ sink = () => {}, now, sampleEvery } = {}) {
  let curScope = null;
  const reg = buildBoundary4Registry({ sink: (r) => sink({ ...r, scope: curScope }), now, sampleEvery });
  const wiringByWire = reg.wiring;
  const observe = (wire, scope, body) => {
    if (!shadowEnabled()) return;                    // flag-off: no work, no trace, byte-identical
    const info = wiringByWire.get(wire); if (!info) return;   // unknown wire → not our frame
    try {
      curScope = scope == null ? null : String(scope);
      const snap = certifyPlain(JSON.stringify(body ?? {}));
      const meta = certifyPlain(JSON.stringify({}));   // B4 rows project no meta leg; scope is stamped, not certified-as-a-field
      // SHAPE-ONLY observation — verdict 'unobserved', NO handler runs.
      reg.observeShape(info.type, snap, meta, {});
    } catch { /* observation must NEVER affect the transport */ } finally { curScope = null; }
  };
  return { reg, observe };
}

export { boundary4Rows, rowDefs, frameWiring };
export default buildBoundary4Registry;
