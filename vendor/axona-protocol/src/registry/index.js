// registry/index.js — frame contract registries (refactor Phase 1, REF-1.1).
// Shadow-mode only in Phase 1: validate + trace beside the handlers, change no
// acceptance behavior. See types.js (the row shape) and shadowRegistry.js (the
// wrapper + flag).
//
// LOCATION (S2.0a): this module lives at src/registry/, NOT src/pubsub/registry/.
// The certified-snapshot mint, construction tags, safe leaf reader/classifier,
// and observation budgets are boundary-NEUTRAL: all four boundary registries
// (pub/sub+DHT, transport/auth, WebRTC signalling, bridge admin) consume this one
// implementation. Relocated out of src/pubsub so no boundary keeps a local copy
// (Phase-1 post-mortem F1; Aster seq 664, Orion seq 670). The 48-gate suite is
// the shared conformance gate every boundary re-runs unchanged.
export {
  defineRow, FrameKind, EvidenceLevel, CorrelationSubjectKind, Proves, FactType,
  Retry, NOT_APPLICABLE, ConversationRole, PairSide,
} from './types.js';
export { ShadowRegistry, shadowEnabled, setShadowEnabled, frameRegistryCanaryVerdict } from './shadowRegistry.js';
export { registerFrame, frameWiringKey, depositDispatchCapability, readDispatchCapability } from './registerFrame.js';   // REF-1.1 E1: the one canonical frame-registration door; E2.0: (wire,transportKind) key; E3: capability-channel deposit (write-only) + allowlisted reader (mechanism shims)
export { registerDirectFrame } from './registerDirectFrame.js';   // REF-1.1 E3 decision 2: the named direct_${type} parameterized registrar (enumerated mechanism shim)
// NOTE: the snapshot mint (certify) is deliberately NOT re-exported, and its
// subpath is blocked in package.json. That is API ENCAPSULATION / hygiene, NOT a
// security boundary — a consumer can still resolve the file by URL (Aster S1g).
// The security property does not rely on unreachability: certify takes text, and
// the dispatcher classifies nodes without touching any prototype or constructor,
// so even a reachable certify cannot produce a value whose observation fires a
// trap. This holds under the module's stated trust boundary — intact realm
// intrinsics/prototypes for the whole certification-and-dispatch lifetime (see
// snapshotMint.js). This module does not claim post-load intrinsic-tamper
// resistance; same-realm intrinsic replacement is out of scope kernel-wide.
