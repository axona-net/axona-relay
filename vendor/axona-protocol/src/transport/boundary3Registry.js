// boundary3Registry.js — REF-1.1 S4b: the Boundary-3 (WebRTC signalling +
// mesh-auth) frame-contract registry TABLE, plus the wiring map that will
// shadow-wrap the two mesh-establishment ingress surfaces in transport/web:
//   (1) BRIDGE SIGNALLING frames — top-level `frame.type` dispatched by
//       signaling.dispatch in index.js: peer-list / peer-joined / peer-left /
//       signal (SDP offer/answer + ICE). These carry fields on the FRAME
//       (frame.peers / frame.peerId / frame.from + frame.payload), not a body.
//   (2) MESH BASE-AUTH notifications — webrtc.onNotification('hello'/'hello-sig')
//       → mesh-auth.js (nonce exchange then the signed authenticated hello). A
//       `k:'ntf'` mesh data-channel notification with a `body`.
//
// SHADOW MODE ONLY. Changes NO acceptance behavior. Built only when the owning
// transport is constructed with the frame-registry flag; the observe path runs
// nothing when the runtime shadow flag is off (default) — flag-off is byte-
// identical. Flag-on OBSERVES a decoder-certified snapshot beside each handler
// and emits a trace; never mutating, suppressing, or reordering a handler or its
// arguments. Dispatch is NOT migrated (§4.3).
//
// GROUNDING (code-refactor-plan §4.3): Boundary-3 is registry #3 of four —
// "WebRTC signalling and mesh-auth frames". CAP_ATTEST is NOT here: it was
// assigned to Boundary-2 (§4.3) though it rides the mesh channel. The bridge
// `welcome` frame is also Boundary-2 (transport session), not here.
//
// DELIBERATE MODELING DECISIONS (flagged for review):
//   * EVIDENCE AXIS NULL. Like Boundary-2, these frames establish a mesh EDGE
//     (discovery → SDP/ICE negotiation → base auth); they prove connection state,
//     not pub/sub data movement (the #28 evidence hierarchy is the data plane).
//     So `evidence`/`proves` are null (the wrong axis) and each row carries a
//     named `outcome` + `terminalOutcome` instead.
//   * SIGNAL FRAMES ARE UNAUTHENTICATED AT THIS BOUNDARY. SDP offer/answer and
//     ICE candidates carry NO signature; a bridge (or relay) can read or tamper
//     with them in flight. The channel identity is bound DOWNSTREAM: mesh-auth's
//     hello-sig signs a CBV that folds the DTLS fingerprints of the negotiated
//     channel (task A-1), so a MITM that rewrote the SDP derives a different
//     fingerprint and the mutual signature fails. So signal rows declare
//     authGuard=null with that note; the real guard lives on mesh:hello-sig.
//   * PEER DISCOVERY IS BOUNDARY-3. peer-list / peer-joined / peer-left are the
//     bridge's signalling-channel announcements that DRIVE the WebRTC state
//     machine (peer-joined → await offer). They are assigned here as the
//     discovery half of "WebRTC signalling", not to Boundary-4 (bridge
//     administration). They are bridge-ASSERTED: trust is the authenticated
//     bridge session, there is no per-frame auth, and a wrong peer set costs at
//     most a failed/duplicate edge (mesh-auth still gates admission).
//   * SIGNAL IS ONE FRAME WITH THREE VARIANTS. `signal` carries an SDP offer, an
//     SDP answer, or an ICE candidate, discriminated by payload.kind
//     (sdp-offer/sdp-answer/ice) — one row per variant (mirrors Boundary-1's
//     signed/legacy INGESTACK split), value-gated via variantBy.cases. NO wire
//     correlation subject: the offer/answer relationship is tracked by the
//     RTCPeerConnection state machine, not a frame field, and candidates trickle
//     ONE_WAY. No CorrelationSubjectKind fits "negotiating channel peer". The
//     signalling peer id `from` (a meta leg) is projected AND schema-required, so a
//     trace cannot pass while it is absent (Aster F2 — the projection is now
//     observable, not decorative).
//   * MESH MUTUAL AUTH IS A meshId-KEYED CONVERSATION (Aster F3). Each side sends
//     hello (its nonce) then hello-sig (its signed proof over the nonce+fingerprint
//     CBV). On the ingest side the two are CAUSALLY ordered: onHello stores the peer
//     nonce that _progress/verifyAuthHello needs before onHelloSig can verify+bind
//     (mesh-auth.js). So hello is the REQUEST leg and hello-sig the RESPONSE leg,
//     paired on the meshId (a meta leg) — the accepted Boundary-2 mutual-auth shape.
//     The frame KIND stays ONE_WAY (each leg is fire-and-forget, not a payload-
//     correlated request/response); the pairing is declared via `conversation`, and
//     meshId is projected + schema-required so the conversation key is observable.
//     Base auth completes on hello-sig (verifyAuthHello + fingerprint CBV bind →
//     bindPeer): terminalOutcome CHANNEL_AUTHENTICATED.
//   * ICE IS Retry.NONE, NOT NATURAL (Aster F1). The live handler drops an ICE
//     candidate whose peer entry does not yet exist (mesh.js `ice-for-unknown`), so
//     arrival order changes the outcome and there is no registry-level dedup key.
//     That is fire-and-forget, not an order-independent set-union.
//   * TYPE NAMESPACE. Row types are prefixed `mesh:` so they never collide with
//     Boundary-2's `transport:hello`/`transport:hello-ack` — the wire label
//     'hello' means bridge auth in the B2 registry and mesh auth in this one,
//     and each registry's observer is wired only to its own ingress site.
//   * No new wire fields. Rows describe the EXISTING frames.

import {
  defineRow, FrameKind, Retry, NOT_APPLICABLE as NA,
  ConversationRole, PairSide, ShadowRegistry, shadowEnabled,
} from '../registry/index.js';
import { certifyPlain } from '../registry/snapshotMint.js';

const V = { min: 5, max: 5 };                 // AUTH_PROTO = 'axona/5' (mesh base auth)
const REQ = ConversationRole.REQUEST, RESP = ConversationRole.RESPONSE;
const budget = (leaves, maxBytes = 1024) => ({ maxLeaves: leaves, maxBytes });

// The Boundary-3 row DEFINITIONS. `wire` (the dispatch label) and `variant`
// (for the signal SDP/ICE split) are carried on the def so the wiring map
// derives from these; defineRow drops unknown keys.
function rowDefs() {
  return [
    // ── bridge signalling discovery: current peer snapshot ──
    ({
      type: 'mesh:peer-list', wire: 'peer-list', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'MeshDiscovery', versionRange: V,
      outcome: 'DiscoveryOutcome', terminalOutcome: 'PEER_SET_ANNOUNCED',
      retry: Retry.NATURAL,   // full snapshot: order-independent replace, no key
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,   // bridge-asserted; trust = the authed bridge session
      projection: { payload: ['peers'] },
      schema: { require: ['peers'] },
      errorContract: [], traceFields: [], budget: budget(64, 4096),
      note: 'bridge announces the current signalling-channel peer set on connect; drives which peers we open edges to. Bridge-asserted, no per-frame auth; a wrong set costs a failed/duplicate edge and mesh-auth still gates admission.',
    }),

    // ── bridge signalling discovery: a peer joined ──
    ({
      type: 'mesh:peer-joined', wire: 'peer-joined', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'MeshDiscovery', versionRange: V,
      outcome: 'DiscoveryOutcome', terminalOutcome: 'PEER_ANNOUNCED',
      retry: Retry.NATURAL,   // re-announce of a known peer is idempotent
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['peerId'] },
      schema: { require: ['peerId'], types: { peerId: 'string' } },
      errorContract: [], traceFields: ['peerId'], budget: budget(4),
      note: 'a new signalling-channel peer; we transition to awaiting-offer for it. peerId is the bridge connId (a 3-char signalling id), not an authenticated nodeId.',
    }),

    // ── bridge signalling discovery: a peer left (+ optional #364-B nodeId hint) ──
    ({
      type: 'mesh:peer-left', wire: 'peer-left', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'MeshDiscovery', versionRange: V,
      // F4 (Vega found, Aster confirmed): the frame is a bridge (third-party) DEPARTURE
      // HINT, not proof the peer left the mesh — so it must not claim departure or edge
      // teardown (#374 bridge-independence invariant). It is a PROCESSED HINT, and its
      // effect is state-dependent, so Retry is NONE, not NATURAL.
      outcome: 'DiscoveryOutcome', terminalOutcome: 'DEPARTURE_HINT_PROCESSED',
      retry: Retry.NONE,   // F4: state-dependent — the same hint is ignored while the channel is open and retires state once it is non-open; the nodeId fan-out has no dedup key. NOT naturally idempotent/order-independent.
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['peerId', 'nodeId'] },
      schema: { require: ['peerId'], types: { peerId: 'string', nodeId: 'string' } },
      errorContract: [], traceFields: ['peerId'], budget: budget(4),
      note: 'bridge-asserted DEPARTURE HINT, not proof of mesh departure. onPeerLeft KEEPS an OPEN first-party channel (peer-left-ignored-live, no onPeerLost) and lets its own vitality reaper govern teardown; only NON-OPEN state is retired, unknown is a no-op (#374). The optional hex nodeId (reportPeerDeparted) is likewise IGNORED while a live channel to the subject exists; only an unreachable subject purges pub/sub ghosts (#364-B). Old bridges omit nodeId.',
    }),

    // ── WebRTC signal: SDP offer (responder builds the answer) ──
    ({
      type: 'mesh:signal', variant: 'offer', wire: 'signal', kind: FrameKind.ONE_WAY,
      owningService: 'MeshSignalling', versionRange: V,
      outcome: 'SignalOutcome', terminalOutcome: 'OFFER_APPLIED',
      retry: Retry.NONE,   // a re-offer renegotiates the PC, not a replay
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,   // UNAUTHENTICATED at this boundary; channel identity is bound by mesh:hello-sig (DTLS-fingerprint CBV)
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['kind', 'sdp'], meta: ['from'] },
      schema: { require: ['kind', 'sdp', 'from'], types: { kind: 'string', sdp: 'string', from: 'string' } },   // F2: `from` (meta) is REQUIRED, not decorative
      capabilityRange: { kind: 'sdp-offer' },
      errorContract: [], traceFields: ['kind'], budget: budget(3, 16384),
      note: 'inbound WebRTC SDP offer from `from` (a signalling peer id, meta leg); we build/reuse the RTCPeerConnection and answer. Unsigned — the offered DTLS fingerprint is bound into the mesh-auth CBV downstream, so a rewritten offer fails the mutual signature.',
    }),

    // ── WebRTC signal: SDP answer (to our offer) ──
    ({
      type: 'mesh:signal', variant: 'answer', wire: 'signal', kind: FrameKind.ONE_WAY,
      owningService: 'MeshSignalling', versionRange: V,
      outcome: 'SignalOutcome', terminalOutcome: 'ANSWER_APPLIED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['kind', 'sdp'], meta: ['from'] },
      schema: { require: ['kind', 'sdp', 'from'], types: { kind: 'string', sdp: 'string', from: 'string' } },   // F2: `from` (meta) is REQUIRED, not decorative
      capabilityRange: { kind: 'sdp-answer' },
      errorContract: [], traceFields: ['kind'], budget: budget(3, 16384),
      note: 'inbound SDP answer to an offer we sent; setRemoteDescription on the matching PC (matched by the RTCPeerConnection state, not a wire field). Same fingerprint-CBV binding note as offer.',
    }),

    // ── WebRTC signal: ICE candidate (trickle) ──
    ({
      type: 'mesh:signal', variant: 'candidate', wire: 'signal', kind: FrameKind.ONE_WAY,
      owningService: 'MeshSignalling', versionRange: V,
      outcome: 'SignalOutcome', terminalOutcome: 'CANDIDATE_APPLIED',
      retry: Retry.NONE,   // F1 (Aster): NOT order-independent — mesh.js drops an ICE whose peer entry is absent (ice-for-unknown), no dedup key; fire-and-forget, order matters
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['kind', 'candidate'], meta: ['from'] },
      schema: { require: ['kind', 'candidate', 'from'], types: { kind: 'string', from: 'string' } },   // candidate is an RTCIceCandidateInit object, not a scalar; `from` (meta) required (F2)
      capabilityRange: { kind: 'ice' },
      errorContract: [], traceFields: ['kind'], budget: budget(4, 4096),
      note: 'trickled ICE candidate. Applied via addIceCandidate once the PC has a remote description, else buffered on the peer entry (pendingCandidates, flushed after the answer). An ICE arriving BEFORE the peer entry exists is DROPPED (mesh.js ice-for-unknown): arrival order changes the outcome and there is no dedup key, so Retry.NONE. `candidate` is an RTCIceCandidateInit object.',
    }),

    // ── DHT-routed mesh-signal carrier (REF-1.1 E2.0) ──
    // The E0 migration-target onRoutedMessage('mesh:signal') at AxonaPeer.js:729.
    // DISTINCT from the direct WebRTC `signal` SDP rows above (wire 'signal'): this is
    // the ROUTED envelope that relays a signalling payload {from, signal} to a targetId
    // terminus, not the SDP frame itself. Aster ASTER-E2-KEY-SCOPE: add a distinct row
    // keyed { wire: 'mesh:signal', transportKind: 'routed' }; do not reuse `signal`.
    ({
      type: 'mesh:signal-routed', wire: 'mesh:signal', transportKind: 'routed', kind: FrameKind.ONE_WAY,
      owningService: 'MeshSignalling', versionRange: V,
      outcome: 'SignalOutcome', terminalOutcome: 'SIGNAL_RELAYED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,   // unauthenticated carrier; the relayed inner signal is bound by mesh:hello-sig downstream
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['from', 'signal'], meta: ['targetId'] },
      schema: { require: ['from', 'signal'], types: { from: 'string' } },
      errorContract: [], traceFields: [], budget: budget(3, 16384),
      note: 'DHT-routed mesh-signal carrier: onRoutedMessage(mesh:signal) at AxonaPeer.js:729. If meta.targetId === node.id, deliver payload.signal to the local transport via deliverMeshSignal(payload.from, payload.signal); otherwise forward. The E0 migration-target for mesh:signal — DISTINCT from the direct WebRTC signal SDP rows (wire "signal").',
    }),

    // ── mesh base auth: hello (nonce leg) ──
    ({
      type: 'mesh:hello', wire: 'hello', kind: FrameKind.ONE_WAY,
      owningService: 'MeshAuth', versionRange: V,
      outcome: 'MeshAuthOutcome', terminalOutcome: 'NONCE_EXCHANGED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA,   // the nonce leg carries no proof; the proof is hello-sig
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['proto', 'nonce'], meta: ['meshId'] },
      schema: { require: ['proto', 'nonce', 'meshId'], types: { proto: 'string', nonce: 'string', meshId: 'string' } },   // F2: meshId (meta) required — it is the conversation key
      // F3: hello is the REQUEST leg — it supplies the peer nonce onHelloSig's verify
      // consumes; paired with hello-sig on the meshId (a meta leg), the accepted
      // Boundary-2 mutual-auth shape. KIND stays ONE_WAY (fire-and-forget leg).
      conversation: { role: REQ, opposite: 'mesh:hello-sig', pairing: [{ local: 'meshId', remote: 'meshId', from: PairSide.meta }] },
      capabilityRange: { proto: 'axona/5' },
      errorContract: [], traceFields: ['proto'], budget: budget(3),
      note: 'first leg of the mesh mutual auth over the data channel: our per-link nonce. Both sides send one; the pair of nonces (+ DTLS fingerprints) forms the channel CBV. No proof yet. REQUEST leg of the meshId-keyed hello/hello-sig conversation.',
    }),

    // ── mesh base auth: hello-sig (signed proof leg → CHANNEL_AUTHENTICATED) ──
    ({
      type: 'mesh:hello-sig', wire: 'hello-sig', kind: FrameKind.ONE_WAY,
      owningService: 'MeshAuth', versionRange: V,
      outcome: 'MeshAuthOutcome', terminalOutcome: 'CHANNEL_AUTHENTICATED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: 'verifyAuthHello: pubkey->nodeId 256-bit bind + ed25519 sig over {proto,nodeId,pubkey,cbv} where cbv = cbvFromNonces(myNonce,peerNonce,"mesh") | cbvFromFingerprints(localFp,remoteFp); id-mismatch to the channel peer rejects',
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['proto', 'nodeId', 'pubkey', 'sig', 'pow'], meta: ['meshId'] },
      schema: { require: ['proto', 'nodeId', 'pubkey', 'sig', 'meshId'], types: { proto: 'string', nodeId: 'string', pubkey: 'string', sig: 'string', pow: 'string', meshId: 'string' } },   // F2: meshId (meta) required
      // F3: hello-sig is the RESPONSE leg — its verify consumes the nonce hello supplied;
      // paired with hello on the meshId (a meta leg). KIND stays ONE_WAY.
      conversation: { role: RESP, opposite: 'mesh:hello', pairing: [{ local: 'meshId', remote: 'meshId', from: PairSide.meta }] },
      capabilityRange: { proto: 'axona/5' },
      errorContract: [], traceFields: ['nodeId', 'proto'], budget: budget(6),
      note: 'second leg: the authenticated hello signed over the nonce+fingerprint CBV. On verify we bindPeer (the channel becomes a routing member) and retain the peer pubkey + cbv for a later CAP_ATTEST (Boundary-2). The fingerprint fold is the MITM defense over the unsigned SDP. RESPONSE leg of the meshId-keyed hello/hello-sig conversation.',
    }),
  ];
}

// Mint the Boundary-3 rows (defineRow-branded). Throws if any def fails
// validation — a table defect fails loud at build, never silently.
function boundary3Rows() { return rowDefs().map(defineRow); }

// The wiring map: dispatch label -> { type, variantBy? }. Built from the raw
// defs' `wire`/`variant` tags. `signal` carries a VALUE-gated variant
// discriminator on payload.kind (offer/answer/candidate); every other wire maps
// to a single row type.
function frameWiring(defs) {
  const byWire = new Map();
  for (const d of defs) {
    const cur = byWire.get(d.wire);
    // transportKind (E2.0): carried on the value so registerFrame can select the
    // primitive and the coverage gate can read each row's (wire, transportKind).
    // The existing B3 frames are all onNotification; mesh:signal is 'routed'. Bare
    // wire keys (composite keying is scoped to Boundary-6 alone).
    if (!cur) byWire.set(d.wire, { type: d.type, variants: d.variant != null, transportKind: d.transportKind || 'notification' });
    else cur.variants = true;
  }
  const out = new Map();
  for (const [wire, info] of byWire) {
    const base = { type: info.type, transportKind: info.transportKind };
    if (info.variants) {
      // signal: the observe site passes the frame.payload as the observed body,
      // so `kind` is a top-level leaf; value-gated to the three SDP/ICE variants.
      out.set(wire, { ...base, variantBy: { path: 'kind', valueType: 'string', cases: { 'sdp-offer': 'offer', 'sdp-answer': 'answer', 'ice': 'candidate' } } });
    } else {
      out.set(wire, base);
    }
  }
  return out;
}

// Build a Boundary-3 ShadowRegistry with every row registered, plus the
// wire->row wiring the mesh-establishment observe sites use. `enabled` gates
// observation (default-off); `sink` receives trace records; `now` is the clock.
// Construction throws if any row fails defineRow validation.
export function buildBoundary3Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'webrtc+mesh-auth', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

// ── S4b increment 2 (NOT YET WIRED): the LIVE observe side-channel ───────────
// makeBoundary3Observers returns `observe(wire, scope, body)` — a pure side-
// channel (never receives/wraps/returns the handler). Flag-off it returns
// immediately (byte-identical). Flag-on it certifies a snapshot of `body` + a
// certified `{scope}` meta (the peer/meshId the observation ran under) and emits
// a shape-only trace (verdict 'unobserved'; NO handler runs). The observed
// `scope` is stamped onto each trace, mirroring Boundary-2's connId stamping.
export function makeBoundary3Observers({ sink = () => {}, now, sampleEvery } = {}) {
  let curScope = null;
  const reg = buildBoundary3Registry({ sink: (r) => sink({ ...r, scope: curScope }), now, sampleEvery });
  const wiringByWire = reg.wiring;
  // F2 (Aster): the certified meta must carry the SAME field the rows project — the
  // signal rows project meta.from, the mesh-auth rows project meta.meshId — or the
  // required projection is unobservable and a green trace hides an absent field. Map
  // each wire to its declared meta key (its single projection.meta field) so the one
  // `scope` value the caller passes (the peer id / meshId the observation ran under)
  // is certified under the RIGHT name. The schema then requires it, and the mesh-auth
  // conversation keys on it. Discovery wires have no meta projection → empty meta.
  const metaKeyByWire = new Map();
  for (const d of rowDefs()) { const mk = d.projection?.meta?.[0]; if (mk && !metaKeyByWire.has(d.wire)) metaKeyByWire.set(d.wire, mk); }
  const observe = (wire, scope, body) => {
    if (!shadowEnabled()) return;                    // flag-off: no work, no trace, byte-identical
    const info = wiringByWire.get(wire); if (!info) return;   // unknown wire → not our frame
    try {
      curScope = scope == null ? null : String(scope);
      const metaKey = metaKeyByWire.get(wire) || null;   // 'from' (signal) | 'meshId' (mesh auth) | none (discovery)
      const snap = certifyPlain(JSON.stringify(body ?? {}));
      const meta = certifyPlain(JSON.stringify(metaKey ? { [metaKey]: curScope } : {}));
      // SHAPE-ONLY observation — verdict 'unobserved', NO handler runs. The
      // variantBy (signal's payload.kind) is carried on the wiring info.
      reg.observeShape(info.type, snap, meta, { variantBy: info.variantBy });
    } catch { /* observation must NEVER affect the transport */ } finally { curScope = null; }
  };
  return { reg, observe };
}

export { boundary3Rows, rowDefs, frameWiring };
export default buildBoundary3Registry;
