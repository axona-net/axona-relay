// boundary2Registry.js — REF-1.1 S4a: the Boundary-2 (transport hello / auth /
// session, plus CAP_ATTEST) frame-contract registry TABLE, plus the wiring map
// that shadow-wraps the transport-layer notification handlers (bridge auth
// hello/hello-ack + welcome in transport/web/index.js; cap-attest dispatched via
// webrtc.onNotification → meshAuth.onCapAttest).
//
// SHADOW MODE ONLY. Changes NO acceptance behavior. Built only when the owning
// transport is constructed with the frame-registry flag; the wrap runs each
// handler verbatim whenever the runtime shadow flag is off (default) — flag-off
// is byte-identical. Flag-on OBSERVES a decoder-certified snapshot beside each
// handler and emits a trace; never mutating, suppressing, or reordering a handler
// or its arguments. Dispatch is NOT migrated (§4.3).
//
// GROUNDING (code-refactor-plan §4.3):
//   * Boundary-2 is registry #2 of four: "transport hello / auth / session
//     frames". CAP_ATTEST is explicitly assigned to THIS boundary (§4.3, "CAP_ATTEST
//     belongs to the transport/auth boundary") even though the frame rides the mesh
//     channel — its trust is the base-authenticated channel key + a locally derived
//     current-channel CBV digest, expiring on channel loss and never persisted.
//
// DELIBERATE MODELING DECISIONS (flagged for review):
//   * The EVIDENCE HIERARCHY (ROUTED/INGESTED/RETAINED/COMMITTED/OBSERVED) is the
//     pub/sub DATA plane (#28). Transport auth/session frames prove CHANNEL
//     authentication, not data movement, so `evidence`/`proves` are left null (the
//     wrong axis for this boundary) and the row instead carries a named `outcome` +
//     `terminalOutcome`. This is distinct from the descriptor-NA convention: evidence
//     is an enum-or-null axis, and null here states "this axis does not apply".
//   * CORRELATION IS CHANNEL-SCOPED. hello and hello-ack are a MUTUAL authentication
//     over one channel: each side INDEPENDENTLY authenticates (verifyAuthHello over
//     the shared CBV), and each carries its OWN nodeId — there is no payload field
//     where hello.X === hello-ack.Y. So they are ONE_WAY (not REQUEST_RESPONSE, which
//     would force a payload correlation subject), and their pair relationship is
//     declared via `conversation` keyed on the transport channel (connId, a `meta`
//     leg). No CorrelationSubjectKind fits "authenticated channel peer", so no
//     authority correlation subject is claimed.
//   * No new wire fields. Rows describe the EXISTING frames.

import {
  defineRow, FrameKind, Retry, NOT_APPLICABLE as NA,
  ConversationRole, PairSide, ShadowRegistry, shadowEnabled,
} from '../registry/index.js';
// certifyPlain is the FIXED string-only decoder variant for the WS bridge / node
// path (JSON.parse, no reviver) — the same decode the transport already performs.
// It is deliberately NOT re-exported by registry/index.js (encapsulation, not a
// security boundary); a kernel-internal path import is the sanctioned consumer.
import { certifyPlain } from '../registry/snapshotMint.js';

const V = { min: 5, max: 5 };                 // the axona/5 transport generation (AUTH_PROTO = 'axona/5')
const REQ = ConversationRole.REQUEST, RESP = ConversationRole.RESPONSE;
const budget = (leaves, maxBytes = 1024) => ({ maxLeaves: leaves, maxBytes });

// The Boundary-2 row DEFINITIONS. `wire` (the transport notification label) is
// carried on the def so the wiring map derives from these, not from the minted
// rows (defineRow drops unknown keys). Single source of truth for both
// registration and handler wrapping.
function rowDefs() {
  return [
    // ── bridge session greeting (server → client, unsolicited) ──
    ({
      type: 'transport:welcome', wire: 'welcome', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'TransportSession', versionRange: V,
      outcome: 'SessionOutcome', terminalOutcome: 'SESSION_WELCOMED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      // connId + serverNonce are the bridge-link channel-binding material (both sides
      // fold them into the signed hello CBV); version/kernelVersion/turn are observability.
      projection: { payload: ['connId', 'serverNonce', 'version', 'kernelVersion', 'turn'] },
      schema: { require: ['connId'], types: { connId: 'string', serverNonce: 'string' } },
      capabilityRange: { proto: 'axona/5' },
      errorContract: [], traceFields: ['connId', 'version', 'kernelVersion'], budget: budget(5, 2048),
      note: 'bridge greeting on connect; carries connId+serverNonce (the bridge-link CBV both sides fold into the signed hello) plus version/kernelVersion/TURN. Informational — the bridge identity is proven by the hello exchange, not by welcome.',
    }),

    // ── bridge WS mutual auth: hello (REQUEST leg) ──
    ({
      type: 'transport:hello', wire: 'hello', kind: FrameKind.ONE_WAY,
      owningService: 'TransportAuth', versionRange: V,
      outcome: 'AuthOutcome', terminalOutcome: 'CHANNEL_AUTHENTICATED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: 'verifyAuthHello: pubkey->nodeId 256-bit bind + ed25519 sig over {proto,nodeId,pubkey,cbv} with the VERIFIER cbv',
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['proto', 'nodeId', 'pubkey', 'sig', 'pow'], meta: ['connId'] },
      schema: { require: ['proto', 'nodeId', 'pubkey', 'sig'], types: { proto: 'string', nodeId: 'string', pubkey: 'string', sig: 'string', pow: 'string' } },
      // channel-scoped pairing: same transport channel (connId, a meta leg). pow is a
      // sibling PoW field, NOT part of the signed transcript.
      conversation: { role: REQ, opposite: 'transport:hello-ack', pairing: [{ local: 'connId', remote: 'connId', from: PairSide.meta }] },
      capabilityRange: { proto: 'axona/5' },
      errorContract: [], traceFields: ['nodeId', 'proto'], budget: budget(6),
      note: 'inbound authenticated hello over the bridge socket; on receipt we reply with our own authenticated hello-ack. Mutual, channel-bound — not a payload-correlated request/response.',
    }),

    // ── bridge WS mutual auth: hello-ack (RESPONSE leg, same frame shape) ──
    ({
      type: 'transport:hello-ack', wire: 'hello-ack', kind: FrameKind.ONE_WAY,
      owningService: 'TransportAuth', versionRange: V,
      outcome: 'AuthOutcome', terminalOutcome: 'CHANNEL_AUTHENTICATED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: 'verifyAuthHello: pubkey->nodeId 256-bit bind + ed25519 sig over {proto,nodeId,pubkey,cbv} with the VERIFIER cbv',
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['proto', 'nodeId', 'pubkey', 'sig', 'pow'], meta: ['connId'] },
      schema: { require: ['proto', 'nodeId', 'pubkey', 'sig'], types: { proto: 'string', nodeId: 'string', pubkey: 'string', sig: 'string', pow: 'string' } },
      conversation: { role: RESP, opposite: 'transport:hello', pairing: [{ local: 'connId', remote: 'connId', from: PairSide.meta }] },
      capabilityRange: { proto: 'axona/5' },
      errorContract: [], traceFields: ['nodeId', 'proto'], budget: budget(6),
      note: 'our authenticated reply to an inbound hello; the peer verifies it symmetrically. Same builder/verifier as hello.',
    }),

    // ── CAP_ATTEST: post-bind capability decoration (§4.3: transport/auth boundary) ──
    ({
      type: 'transport:cap-attest', wire: 'cap-attest', kind: FrameKind.UNSOLICITED_EVENT,
      owningService: 'TransportAuth', versionRange: V,
      outcome: 'CapabilityOutcome', terminalOutcome: 'CAPABILITY_ATTESTED',
      retry: Retry.NONE,
      topicProfile: NA, eventIdScheme: NA, replayCursorType: NA, orderingModel: NA,
      authGuard: 'verifyCapAttest: ed25519 sig over buildCapTranscript(nodeId, cbvDigest(cbv)) vs the BASE-AUTHENTICATED peerKey + expectedNodeId + verifier cbv',
      admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['capId', 'nodeId', 'sig'], meta: ['connId'] },
      schema: { require: ['capId', 'nodeId', 'sig'], types: { capId: 'string', nodeId: 'string', sig: 'string' } },
      capabilityRange: { capId: 'write-flight-ack-v1' },
      errorContract: [], traceFields: ['nodeId', 'capId'], budget: budget(4),
      note: 'write-flight-ack-v1 capability, attested post-bind and assigned to Boundary-2 (§4.3) though it rides the mesh channel: channel-key + current-CBV-digest bound, expires on channel loss, never persisted, absent/fail-closed on old peers.',
    }),
  ];
}

// Mint the Boundary-2 rows (defineRow-branded). Throws if any def fails
// validation — a table defect fails loud at build, never silently.
function boundary2Rows() { return rowDefs().map(defineRow); }

// The wiring map: transport notification label -> { type }. Built from the raw
// defs' `wire` tag (minted rows drop it). Boundary-2 has no signed/legacy variant
// split (unlike Boundary-1's INGESTACK), so every wire maps to a single row type.
function frameWiring(defs) {
  const out = new Map();
  for (const d of defs) {
    if (out.has(d.wire)) throw new Error(`boundary2Registry: duplicate wire ${d.wire}`);
    // transportKind (E2.0): every Boundary-2 frame is a transport onNotification;
    // carried on the value for registerFrame + the coverage gate. Bare wire key.
    out.set(d.wire, { type: d.type, wire: d.wire, transportKind: d.transportKind || 'notification' });
  }
  return out;
}

// Build a Boundary-2 ShadowRegistry with every row registered, plus the
// wire->row wiring the transport handler-wrap site uses. `enabled` gates
// observation (default-off); `sink` receives trace records; `now` is the clock.
// Construction throws if any row fails defineRow validation.
export function buildBoundary2Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'transport+auth', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  return reg;
}

// ── S4a increment 2: the LIVE-WIRING observe side-channel ───────────────────
// makeBoundary2Observers builds a Boundary-2 registry and returns `observe(wire,
// connId, body)` — the ONLY thing the live transport calls. It is a pure
// side-channel: it never receives, wraps, or returns the handler, so it cannot
// change what the handler sees or does. Flag-off (the default) it returns
// immediately — zero certify work, zero traces, byte-identical. Flag-on it feeds a
// certified snapshot of `body` plus a certified `{connId}` meta (certified from the
// ACTUAL channel/session scope the caller passes) to a no-op-handler wrap, which
// observes and emits a trace. Stateless per call — no per-connId state is retained
// — so reusing a fixed channel sentinel (BRIDGE_CONN_ID) across reconnects carries
// nothing between sessions.
export function makeBoundary2Observers({ sink = () => {}, now, sampleEvery } = {}) {
  // F3: stamp the ACTUAL channel/session connId the observation ran under onto each
  // emitted trace. observeShape is synchronous, so a transient set-before / reset-
  // after around the one call reaches the sink with the right scope. This is what
  // lets two reconnect sessions with distinct welcome connIds be told apart, and it
  // keeps the bridge-auth channel scope out of the certified body and in the trace.
  let curConnId = null;
  const reg = buildBoundary2Registry({ sink: (r) => sink({ ...r, connId: curConnId }), now, sampleEvery });
  const typeByWire = new Map();
  for (const [wire, info] of reg.wiring) typeByWire.set(wire, info.type);
  const observe = (wire, connId, body) => {
    if (!shadowEnabled()) return;                    // flag-off: no work, no trace, byte-identical
    const type = typeByWire.get(wire); if (!type) return;   // unknown wire → not our frame
    try {
      curConnId = connId == null ? null : String(connId);
      const snap = certifyPlain(JSON.stringify(body ?? {}));
      const meta = certifyPlain(JSON.stringify({ connId: curConnId }));
      // F1: SHAPE-ONLY observation — verdict 'unobserved', NO handler runs, so a
      // later async rejection of the real handler is never overclaimed as 'passed'.
      reg.observeShape(type, snap, meta);
    } catch { /* observation must NEVER affect the transport */ } finally { curConnId = null; }
  };
  return { reg, observe };
}

export { boundary2Rows, rowDefs, frameWiring };
export default buildBoundary2Registry;
