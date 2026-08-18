// boundary1Registry.js — REF-1.1 S2 (recut-3): the Boundary-1 (pub/sub + DHT
// control) frame-contract registry TABLE, plus the wiring map that shadow-wraps
// the 19 routed handlers registered in wireHandlers.js `_registerHandlers`.
//
// SHADOW MODE ONLY. This module changes NO acceptance behavior. The registry is
// built ONLY when AxonaManager is constructed with `frameRegistry:true`; the wrap
// runs the handler verbatim whenever the runtime shadow flag is off (the default),
// so flag-off is byte-identical. Flag-on OBSERVES a decoder-certified snapshot
// beside each handler and emits a trace — never mutating, suppressing, or
// reordering a handler or its arguments. Dispatch is NOT migrated (§4.3).
//
// MODELING grounds each row in code-refactor-plan §4.3 (row contract + evidence
// hierarchy + correlation subject union) and §4.4 (retry/ordering). Recut-3
// addresses Aster's recut-2 disposition (d1b1d060, which closed F4/F5; F1 fixed
// in the core):
//   * F2 — the Phase-1 row contract is COMPLETE per row: authentication /
//     admission / placement guards, error contract, trace fields, normalized
//     `outcome` + `terminalOutcome`, `retry` classification, `topicProfile`,
//     `eventIdScheme`, `replayCursorType`, `orderingModel`, a `capabilityRange`,
//     and a per-row observation `budget`. A field with no meaning for a frame is
//     marked NOT_APPLICABLE (NA) explicitly — never silently left null.
//   * F3 — correlation is a PAIR ALGEBRA, not a presence list. Read/catch-up/
//     handoff pairs declare `conversation { role, opposite, pairing:[{local,
//     remote, from}] }`; where the return destination (routing, not payload)
//     supplies the peer identity — REPLAYUP↔PULLUP, HANDOFFACK↔HANDOFF — the leg
//     is `from:'meta'`. The authority subject is claimed only where a frame binds
//     one: signed INGESTACK binds the exact D1 flight + incarnation + proof signer
//     (`correlation.binding`), legacy INGESTACK binds flight + incarnation, PUB/
//     KILL bind the ingress ATTEMPT (topicId+attemptId+flightNonce, not topicId
//     alone), and REPLICATE (unsigned cohort spray) binds nothing.
//   * F4 — INGESTACK variant selection is type-gated on `typeof sig === 'string'`.
//   * F5 — PUB frame-level idempotency is left undeclared (msgId is nested in the
//     signed envelope); KILL keeps kill.msgId.
//   * No new wire fields. Rows describe the EXISTING frames.

import { T } from './constants.js';
import {
  defineRow, FrameKind, EvidenceLevel, Proves, CorrelationSubjectKind,
  Retry, NOT_APPLICABLE as NA, ConversationRole, PairSide, ShadowRegistry,
} from '../registry/index.js';
import { certifyBigint } from '../registry/snapshotMint.js';   // REF-1.1 E1: the B1 observation certifier lives WITH the B1 registry
import { encode } from '../transport/wire.js';

const V = { min: 4, max: 4 };                 // Kernel-4 wire version
const LAR = CorrelationSubjectKind.LegacyAuthorityRef;
const INGRESS = CorrelationSubjectKind.IngressRef;
const PROFILE = 'LEGACY_ROOT_V4';             // every Boundary-1 frame runs under the Kernel-4 singleton-root profile
const REQ = ConversationRole.REQUEST, RESP = ConversationRole.RESPONSE;
const budget = (leaves, maxBytes = 1024) => ({ maxLeaves: leaves, maxBytes });

// The Boundary-1 row DEFINITIONS. `wire` (a routed-message T.* string) is carried
// on the def so the wiring map derives from these, not from the minted rows
// (defineRow drops unknown keys). Single source of truth for both registration
// and handler wrapping.
function rowDefs() {
  return [
    // ── subscription lease (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:sub', wire: T.SUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'LEASE_ESTABLISHED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: 'HIGHWATER', orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: 'regionOk+admitRole+meshBareSelfRootGuard',
      projection: { payload: ['topicId', 'subscriberId', 'since', 'latest', 'hw', 'lw'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['refuse-region', 'refuse-role-budget'], traceFields: ['topicId', 'subscriberId'], budget: budget(6),
      note: 'establishes/renews a delivery lease routed toward topicId; advertises the subscriber replay cursor (hw/lw)',
    }),
    ({
      type: 'pubsub:unsub', wire: T.UNSUB, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'LEASE_RELEASED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: NA, orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: 'regionOk',
      projection: { payload: ['topicId', 'subscriberId'] },
      schema: { require: ['topicId', 'subscriberId'], types: { topicId: 'string', subscriberId: 'string' } },
      idempotency: { from: ['topicId', 'subscriberId'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['refuse-region'], traceFields: ['topicId', 'subscriberId'], budget: budget(2),
    }),

    // ── write ingress: PUB / KILL bind the ingress ATTEMPT (IngressRef), not authority ──
    ({
      type: 'pubsub:pub', wire: T.PUB, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION, outcome: 'IngestOutcome', terminalOutcome: 'INGEST_PROOF_OR_EVICT',
      retry: Retry.SINGLE_FLIGHT, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: 'verifyEnvelope', admissionGuard: 'checkFreshness+writePolicy+topicBinding', placementGuard: 'regionOk+admitRole',
      projection: { payload: ['topicId', 'json', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'json'], types: { topicId: 'string', json: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'attemptId', 'flightNonce'], binding: { flight: ['topicId', 'attemptId', 'flightNonce'] } },
      // F5: no frame-level idempotency key — msgId hashes author+message and lives inside the signed json.
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-unparseable', 'drop-bad-envelope', 'drop-stale', 'drop-topic-mismatch', 'drop-write-policy'],
      traceFields: ['topicId'], budget: budget(6),
      note: 'signed envelope routed to the root; the ingest attempt (topicId+attemptId+flightNonce) is the IngressRef; proof returns as a separate INGESTACK',
    }),
    ({
      type: 'pubsub:kill', wire: T.KILL, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION, outcome: 'IngestOutcome', terminalOutcome: 'INGEST_PROOF_OR_EVICT',
      retry: Retry.SINGLE_FLIGHT, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: 'verifyKill', admissionGuard: 'authorship+topicBinding', placementGuard: 'regionOk+admitRole',
      projection: { payload: ['topicId', 'kill.msgId', 'kill.signerPubkey', 'via', 'ackTo', 'attemptId', 'flightNonce'] },
      schema: { require: ['topicId', 'kill.msgId'], types: { topicId: 'string', 'kill.msgId': 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'attemptId', 'flightNonce'], binding: { flight: ['topicId', 'attemptId', 'flightNonce'] } },
      idempotency: { from: ['kill.msgId'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-bad-kill-sig', 'drop-authorship-mismatch'], traceFields: ['topicId', 'kill.msgId'], budget: budget(7),
    }),

    // ── ingest proof: D1 signed + legacy unsigned variants ──
    ({
      type: 'pubsub:ingestack', variant: 'signed', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'WriteIngress', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION, outcome: 'IngestOutcome', terminalOutcome: 'PROOF_DELIVERED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: 'verifyAckProof', admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'attemptId', 'ackTo', 'flightNonce', 'sig', 'rootPub', 'purpose'] },
      schema: { require: ['topicId', 'msgId', 'sig', 'rootPub'], types: { topicId: 'string', msgId: 'string', sig: 'string', rootPub: 'string' } },
      // F3: the D1 subject binds the EXACT open flight, the incarnation, and the proof signer — grouped, not a flat list.
      correlation: {
        kind: LAR, requires: ['topicId', 'msgId', 'op', 'epoch', 'attemptId', 'ackTo', 'flightNonce', 'rootPub'],
        binding: {
          flight: ['topicId', 'msgId', 'op', 'attemptId', 'ackTo', 'flightNonce'], authority: ['epoch'], proofSigner: ['rootPub'],
          // F3.2: the real authority relation — the proof signer's pubkey must hash to
          // the open flight's expected root node id (rootPubMatchesNodeHash), not merely be present.
          relations: [{ subject: 'rootPub', derives: 'nodeIdHash=hashComponent(SHA-256(rootPub))', boundTo: 'flight.expectedRootHex' }],
        },
      },
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      capabilityRange: { proofModule: 'ackProof.js', profile: PROFILE },
      errorContract: ['drop-bad-proof', 'drop-flight-mismatch', 'drop-wrong-signer'], traceFields: ['topicId', 'msgId', 'op'], budget: budget(10),
      note: 'D1 signed proof; byte construction/verification owned by ackProof.js; binds (topicId,msgId,op,attemptId,ackTo,flightNonce)+epoch+rootPub',
    }),
    ({
      type: 'pubsub:ingestack', variant: 'legacy', wire: T.INGESTACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      evidence: EvidenceLevel.INGESTED, proves: Proves.INGESTION, outcome: 'IngestOutcome', terminalOutcome: 'PROOF_DELIVERED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: 'adjacentSenderAuth', admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'msgId', 'op', 'epoch', 'sig'], meta: ['fromId'] },   // `sig` projected (absent at runtime) for the type-gated discriminator; `fromId` = the authenticated adjacent sender
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      // F3.2: legacy completion binds the authenticated ADJACENT SENDER (meta.fromId) to the open flight's expected root, plus the intended incarnation (epoch).
      correlation: {
        kind: LAR, requires: ['topicId', 'msgId', 'op', 'epoch', 'fromId'],
        binding: { flight: ['topicId', 'msgId', 'op'], authority: ['epoch'], relations: [{ subject: 'fromId', derives: 'authenticatedAdjacentSender', boundTo: 'flight.expectedRootHex' }] },
      },
      idempotency: { from: ['topicId', 'msgId', 'op'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-wrong-sender', 'drop-wrong-incarnation'], traceFields: ['topicId', 'msgId', 'op'], budget: budget(5),
      note: 'legacy unsigned one-hop; completion binds the authenticated adjacent sender + intended incarnation (epoch), no proof signer',
    }),

    // ── write-flight receipt probe / nack (writeFlight) ──
    ({
      type: 'pubsub:receiptprobe', wire: T.RECEIPTPROBE, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      outcome: 'ProbeOutcome', terminalOutcome: 'PROBE_ANSWERED',
      retry: Retry.BOUNDED_ONCE, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'msgId', 'op'] },
      schema: { require: ['topicId', 'msgId', 'op'], types: { topicId: 'string', msgId: 'string', op: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'msgId', 'op'], binding: { flight: ['topicId', 'msgId', 'op'] } },
      capabilityRange: { profile: PROFILE },
      errorContract: ['probe-no-hit'], traceFields: ['topicId', 'msgId', 'op'], budget: budget(3),
      note: 'pure read of a suspect root’s held state; answered by INGESTACK (held) or RECEIPTNACK (not); no evidence produced',
    }),
    ({
      type: 'pubsub:receiptnack', wire: T.RECEIPTNACK, kind: FrameKind.ONE_WAY, owningService: 'writeFlight', versionRange: V,
      outcome: 'ProbeOutcome', terminalOutcome: 'ABSENCE_ASSERTED',
      retry: Retry.BOUNDED_ONCE, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'SINGLE_FLIGHT',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'msgId', 'op', 'reason'] },
      schema: { require: ['topicId', 'msgId'], types: { topicId: 'string', msgId: 'string' } },
      correlation: { kind: INGRESS, requires: ['topicId', 'msgId', 'op'], binding: { flight: ['topicId', 'msgId', 'op'] } },
      capabilityRange: { profile: PROFILE },
      errorContract: ['reason'], traceFields: ['topicId', 'msgId', 'reason'], budget: budget(4),
      note: 'explicit non-retention; earns exactly one direct retry; asserts absence, not a positive evidence level',
    }),

    // ── delivery + adoption (TopicDeliveryPlane) ──
    ({
      type: 'pubsub:deliver', wire: T.DELIVER, kind: FrameKind.MULTICAST, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.OBSERVED, proves: Proves.OBSERVATION, outcome: 'DeliveryOutcome', terminalOutcome: 'ENTRIES_DELIVERED_AT_MOST_ONCE',
      retry: Retry.NONE, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'UNION_IDEMPOTENT',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'from', 'msgs'] },
      schema: { require: ['topicId', 'msgs'], types: { topicId: 'string', msgs: 'arr' } },
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-duplicate-msgid'], traceFields: ['topicId'], budget: budget(3),
      note: 'fan-out down the tree; each entry keyed by its own msgId (per-entry idempotency, not a frame key)',
    }),
    ({
      type: 'pubsub:adopt', wire: T.ADOPT, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'SUBTREE_ADOPTED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: NA, orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: 'admitDelegatedSubs', placementGuard: 'regionOk',
      projection: { payload: ['topicId', 'parent', 'subs'] },
      schema: { require: ['topicId', 'parent'], types: { topicId: 'string', parent: 'string', subs: 'arr' } },
      idempotency: { from: ['topicId', 'parent'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['refuse-region'], traceFields: ['topicId', 'parent'], budget: budget(3),
      note: 'delegation command: adopt these subscribers under this parent',
    }),

    // ── read path: PULL / PULLRESP conversation keyed by (corrId, requesterId) (F3 pair algebra) ──
    ({
      type: 'pubsub:pull', wire: T.PULL, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      outcome: 'ReadOutcome', terminalOutcome: 'READ_ANSWERED',
      retry: Retry.NATURAL, topicProfile: PROFILE, eventIdScheme: 'CORR_ID', replayCursorType: NA, orderingModel: 'NONE',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'postHash', 'corrId', 'requesterId'] },
      schema: { require: ['topicId', 'corrId', 'requesterId'], types: { topicId: 'string', corrId: 'string', requesterId: 'string' } },
      // The LIVE handler now matches a response on the WHOLE pair: _onPullResp
      // resolves _pending.get(corrId) AND requires the response's requesterId to
      // fold to the requester recorded when the PULL was issued (requester gate,
      // Aster council d17ece0b). The conversation pair is therefore honestly
      // corrId+requesterId — a foreign-requester response with our corrId does
      // NOT settle the read.
      conversation: { role: REQ, opposite: 'pubsub:pullresp', pairing: [{ local: 'corrId', remote: 'corrId' }, { local: 'requesterId', remote: 'requesterId' }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['no-hit'], traceFields: ['topicId', 'corrId'], budget: budget(4),
      note: 'read request; the (corrId, requesterId) pair is the conversation id the handler matches on; pure read, no idempotency',
    }),
    ({
      type: 'pubsub:pullresp', wire: T.PULLRESP, kind: FrameKind.ONE_WAY, owningService: 'TopicDeliveryPlane', versionRange: V,
      outcome: 'ReadOutcome', terminalOutcome: 'RESPONSE_MATCHED',
      retry: Retry.NONE, topicProfile: PROFILE, eventIdScheme: 'CORR_ID', replayCursorType: NA, orderingModel: 'NONE',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['corrId', 'json', 'publishTs', 'requesterId'] },
      schema: { require: ['corrId', 'requesterId'], types: { corrId: 'string', requesterId: 'string' } },
      // _onPullResp resolves _pending.get(corrId) AND enforces that the response's
      // requesterId folds to the recorded requester (requester gate, Aster council
      // d17ece0b) — the response pairs on corrId+requesterId, not corrId alone. A
      // foreign-requester response with a matching corrId is dropped unmatched.
      conversation: { role: RESP, opposite: 'pubsub:pull', pairing: [{ local: 'corrId', remote: 'corrId' }, { local: 'requesterId', remote: 'requesterId' }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-unmatched-corrid', 'drop-foreign-requester'], traceFields: ['corrId'], budget: budget(4),
      note: 'read response matched to _pending by the (corrId, requesterId) pair; a foreign requesterId is dropped unmatched; json:null is a genuine no-hit',
    }),

    // ── sync engine: catch-up + cohort + handoff (SyncEngine / TopicRoleLifecycle) ──
    ({
      type: 'pubsub:pullup', wire: T.PULLUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'REPLAY_SENT',
      retry: Retry.NATURAL, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: 'HIGHWATER', orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'sinceHw', 'parentId'] },
      schema: { require: ['topicId', 'parentId'], types: { topicId: 'string', parentId: 'string' } },
      // catch-up conversation; parentId is the return destination the REPLAYUP is routed back to (its routing target).
      conversation: { role: REQ, opposite: 'pubsub:replayup', pairing: [{ local: 'topicId', remote: 'topicId' }, { local: 'parentId', remote: 'targetId' }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['no-newer-history'], traceFields: ['topicId', 'parentId'], budget: budget(3),
      note: 'catch-up trigger to the parent holder from cursor sinceHw; answered by REPLAYUP routed back to parentId',
    }),
    ({
      type: 'pubsub:replayup', wire: T.REPLAYUP, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION, outcome: 'RetentionOutcome', terminalOutcome: 'ENTRIES_APPLIED_UNION',
      retry: Retry.NATURAL, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: 'HIGHWATER', orderingModel: 'UNION_IDEMPOTENT',
      authGuard: 'verifyEnvelope', admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'msgs', 'dels'], meta: ['targetId'] },
      schema: { require: ['topicId'], types: { topicId: 'string', msgs: 'arr', dels: 'arr' } },
      // RESPONSE leg: REPLAYUP carries only topicId; the requester identity is the ROUTING return target (meta), matched to PULLUP.parentId.
      conversation: { role: RESP, opposite: 'pubsub:pullup', pairing: [{ local: 'topicId', remote: 'topicId' }, { local: 'targetId', remote: 'parentId', from: PairSide.meta }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['drop-bad-envelope-entry'], traceFields: ['topicId'], budget: budget(4),
      note: 'replay-up response routed to the requesting parent; per-entry msgId idempotency (union) at ingest; requester identity supplied by routing meta',
    }),
    ({
      type: 'pubsub:handoff', wire: T.HANDOFF, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION, outcome: 'RetentionOutcome', terminalOutcome: 'STANDING_STATE_RECEIVED',
      retry: Retry.BOUNDED_ONCE, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'UNION_IDEMPOTENT',
      authGuard: NA, admissionGuard: 'admitPushedRole', placementGuard: NA,
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId', 'from'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      // REQUEST leg: the departing node `from` is the return destination the HANDOFFACK is routed back to.
      conversation: { role: REQ, opposite: 'pubsub:handoffack', pairing: [{ local: 'topicId', remote: 'topicId' }, { local: 'from', remote: 'targetId' }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['refuse-role-budget'], traceFields: ['topicId', 'from'], budget: budget(4),
      note: 'standing-state transfer to the heir; answered by HANDOFFACK routed back to `from`',
    }),
    ({
      type: 'pubsub:handoffack', wire: T.HANDOFFACK, kind: FrameKind.ONE_WAY, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'ACK_RECEIVED',
      retry: Retry.NONE, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: NA, orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'held', 'sent'], meta: ['targetId'] },
      schema: { require: ['topicId'], types: { topicId: 'string', held: 'number', sent: 'number' } },
      // RESPONSE leg: the ack carries only topicId; it is routed back to the departing node (meta), matched to HANDOFF.from.
      conversation: { role: RESP, opposite: 'pubsub:handoff', pairing: [{ local: 'topicId', remote: 'topicId' }, { local: 'targetId', remote: 'from', from: PairSide.meta }] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['ignore-short-ack'], traceFields: ['topicId'], budget: budget(3),
      note: 'handoff completion ack; a short ack (held<sent) is deliberately not recorded; departing-node identity from routing meta',
    }),
    ({
      type: 'pubsub:replicate', wire: T.REPLICATE, kind: FrameKind.MULTICAST, owningService: 'SyncEngine', versionRange: V,
      evidence: EvidenceLevel.RETAINED, proves: Proves.RETENTION, outcome: 'RetentionOutcome', terminalOutcome: 'REPLICA_UNION_APPLIED',
      retry: Retry.NATURAL, topicProfile: PROFILE, eventIdScheme: 'MSGID_CONTENT_V1', replayCursorType: NA, orderingModel: 'UNION_IDEMPOTENT',
      authGuard: 'verifyEnvelope', admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId', 'from', 'msgs', 'dels'] },
      schema: { require: ['topicId'], types: { topicId: 'string', from: 'string', msgs: 'arr', dels: 'arr' } },
      // No correlation and no conversation: a cohort spray keyed by an unsigned `from` is not an authenticated holder
      // subject and has no request/response pairing (F3). COUNT_HIGHWATER_HINT is rate gating, never an equality proof.
      capabilityRange: { profile: PROFILE, summaryStrategy: 'COUNT_HIGHWATER_HINT' },
      errorContract: ['drop-bad-envelope-entry'], traceFields: ['topicId'], budget: budget(4),
      note: 'cohort spray to K-closest; empty msgs+dels = liveness keepalive; union ingest is order-independent',
    }),

    // ── locator + lifecycle: beacon / metrics demand / deprecated touch ──
    ({
      type: 'pubsub:rootbeacon', wire: T.ROOTBEACON, kind: FrameKind.UNSOLICITED_EVENT, owningService: 'TopicLocator', versionRange: V,
      outcome: 'LocatorOutcome', terminalOutcome: 'SOFT_STATE_REFRESHED',
      retry: Retry.FLOOD_DEDUP, topicProfile: PROFILE, eventIdScheme: 'BEACON_ID', replayCursorType: NA, orderingModel: 'NONE',
      authGuard: NA, admissionGuard: NA, placementGuard: 'verifyClosenessGate',
      projection: { payload: ['root', 'topics', 'epochs', 'beaconId', 'layer'] },
      schema: { require: ['root', 'beaconId'], types: { root: 'string', beaconId: 'string', topics: 'arr', epochs: 'arr' } },
      idempotency: { from: ['beaconId'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['reject-closeness', 'reject-stale-epoch'], traceFields: ['root', 'beaconId'], budget: budget(5),
      note: 'soft-state root advertisement; no opposite → registers with no correlation (§4.3); flood dedup by beaconId',
    }),
    ({
      type: 'pubsub:metricson', wire: T.METRICSON, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      outcome: 'RoutingOutcome', terminalOutcome: 'LEASE_ARMED',
      retry: Retry.IDEMPOTENT, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: NA, orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: 'regionOk+admitRole',
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      idempotency: { from: ['topicId'] },
      capabilityRange: { profile: PROFILE },
      errorContract: ['refuse-region'], traceFields: ['topicId'], budget: budget(1),
      note: 'demand signal arming a renewable metrics-publish lease at the root; no durability',
    }),
    ({
      type: 'pubsub:touch', wire: T.TOUCH, kind: FrameKind.ONE_WAY, owningService: 'TopicRoleLifecycle', versionRange: V,
      evidence: EvidenceLevel.ROUTED, proves: Proves.ROUTING, outcome: 'RoutingOutcome', terminalOutcome: 'NOOP',
      retry: Retry.NONE, topicProfile: PROFILE, eventIdScheme: NA, replayCursorType: NA, orderingModel: 'ARRIVAL',
      authGuard: NA, admissionGuard: NA, placementGuard: NA,
      projection: { payload: ['topicId'] },
      schema: { require: ['topicId'], types: { topicId: 'string' } },
      capabilityRange: { profile: NA },
      errorContract: [], traceFields: ['topicId'], budget: budget(1),
      note: 'registered wire-compat no-op (peer.touch deprecated v4.3.0) — governed deprecation exception; cannot fail',
    }),
  ];
}

// Mint the Boundary-1 rows (defineRow-branded, ready to register). Throws if any
// def fails validation — a table defect fails loud at build, never silently.
function boundary1Rows() { return rowDefs().map(defineRow); }

// The wiring map: routed-message T.* string -> { type, variantBy? }. Built from
// the raw defs' `wire` tag (the minted rows drop it). INGESTACK carries a
// TYPE-GATED variant discriminator (F4): `signed` only when `typeof sig ===
// 'string'` — a present-but-non-string sig selects `legacy`, mirroring the live
// handler. Every other frame maps to a single row type.
function frameWiring(defs) {
  const byWire = new Map();
  for (const d of defs) {
    const cur = byWire.get(d.wire);
    if (!cur) byWire.set(d.wire, { type: d.type, variants: d.variant != null });
    else cur.variants = true;   // more than one variant registered for this wire
  }
  const out = new Map();
  for (const [wire, info] of byWire) {
    // REF-1.1 E1: `transportKind` selects the raw dispatch primitive registerFrame
    // uses (routed -> onRoutedMessage). Every Boundary-1 frame is routed (registered
    // via the DHT adapter's onRoutedMessage in wireHandlers). Additive: the existing
    // wrap site reads only .type/.variantBy.
    if (info.variants) {
      out.set(wire, { type: info.type, transportKind: 'routed', variantBy: { path: 'sig', valueType: 'string', whenPresent: 'signed', whenAbsent: 'legacy' } });
    } else {
      out.set(wire, { type: info.type, transportKind: 'routed' });
    }
  }
  return out;
}

// Build a per-node Boundary-1 ShadowRegistry with every row registered, plus the
// wire->row wiring the handler-registration site uses. `enabled` gates observation
// (default-off); `sink` receives trace records; `now` is the clock. Construction
// throws if any row fails defineRow validation.
export function buildBoundary1Registry({ sink = () => {}, enabled, now, sampleEvery } = {}) {
  const defs = rowDefs();
  const reg = new ShadowRegistry({ boundary: 'pubsub+dht', sink, enabled, now, sampleEvery });
  for (const d of defs) reg.register(defineRow(d));
  reg.wiring = frameWiring(defs);
  // REF-1.1 E1 (Aster F1): the OBSERVATION CERTIFIER belongs to the boundary, not
  // to a caller. B1's is the bigint-faithful re-encode certifier (M1c). registerFrame
  // reads reg.mintLive; there is no public caller-supplied certifier path. It is
  // still inert unless the runtime shadow flag is armed (default off).
  reg.mintLive = (x) => certifyBigint(encode(x));
  return reg;
}

export { boundary1Rows, rowDefs, frameWiring };
export default buildBoundary1Registry;
