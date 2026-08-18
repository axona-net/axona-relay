// registry/types.js — the per-boundary frame CONTRACT ROW (refactor Phase 1,
// REF-1.1; S1e re-cut per Aster's S1d disposition). A row is DECLARATIVE DATA.
// It carries NO executable code: the S1d escape was that schema / correlation /
// idempotencyKey were still functions the dispatcher ran, so freezing their
// arguments did not make them pure. S1e removes them.
//
// A row now declares, as pure data interpreted by a fixed vetted evaluator
// (shadowRegistry.js), only:
//   - `projection` : dotted LEAF PATHS the shadow layer may read (own DATA
//     descriptors only; accessors never invoked). Path segments __proto__ /
//     prototype / constructor are rejected.
//   - `schema`     : { require:[paths], forbid:[paths], types:{path:TypeName} }
//     over projected facts. No function accepted.
//   - `correlation`: { kind, requires:[paths] } — subject present iff all
//     `requires` paths are present. No function accepted.
//   - `idempotency`: { from:[paths] } — inputs present iff all `from` present.
//     No function accepted (the old `idempotencyKey` callback is rejected).
//
// defineRow REJECTS (never normalizes): functions where a spec is required;
// over-limit projection / capability-key / variant-case / errorContract /
// traceField / schema lists; non-object budget / capability / projection /
// spec; oversized or non-finite capability values; non-string notes; evidence/
// proof contradictions; and any declared path (projection, schema, correlation,
// idempotency) not answerable from — or, for the latter three, not present in —
// the projection.

export const FrameKind = Object.freeze({
  REQUEST_RESPONSE: 'REQUEST_RESPONSE', ONE_WAY: 'ONE_WAY', MULTICAST: 'MULTICAST', UNSOLICITED_EVENT: 'UNSOLICITED_EVENT',
});
const FRAME_KINDS = new Set(Object.values(FrameKind));

// Orthogonal fact labels — NOT an ordinal scale (Aster). No comparison exists.
export const EvidenceLevel = Object.freeze({
  ROUTED: 'ROUTED', INGESTED: 'INGESTED', RETAINED: 'RETAINED', COMMITTED: 'COMMITTED', OBSERVED: 'OBSERVED',
});
const EVIDENCE_LEVELS = new Set(Object.values(EvidenceLevel));

export const Proves = Object.freeze({ ROUTING: 'routing', INGESTION: 'ingestion', RETENTION: 'retention', OBSERVATION: 'observation' });
const PROVES = new Set(Object.values(Proves));

// The ONLY evidence↔proof pairings that are not a contradiction.
const EVIDENCE_FOR_PROOF = Object.freeze({
  routing: new Set(['ROUTED']),
  ingestion: new Set(['INGESTED']),
  retention: new Set(['RETAINED', 'COMMITTED']),
  observation: new Set(['OBSERVED']),
});

export const CorrelationSubjectKind = Object.freeze({
  LegacyAuthorityRef: 'LegacyAuthorityRef', IngressRef: 'IngressRef', HolderRef: 'HolderRef', AuthorLaneRef: 'AuthorLaneRef',
  // REF-1.1 E2.0 (Aster ASTER-E2-CHANNEL-SUBJECT, all three seats): the transport
  // request-return pairing. The association is supplied by the transport RPC
  // channel itself — NOT a separately dispatched frame and NOT a payload field. It
  // is the honest correlation subject for an onRequest RPC whose reply is the
  // transport return value (lookup_step, find_closest_set, axona:direct request …).
  // A CHANNEL subject: it carries a fixed transportScope and an EMPTY requires (no
  // payload projection is a correlation id), a schema exception available ONLY to
  // FrameKind.REQUEST_RESPONSE + TransportRpcRef.
  TransportRpcRef: 'TransportRpcRef',
});
const CORRELATION_KINDS = new Set(Object.values(CorrelationSubjectKind));

// Retry classification (§4.4 retry/backoff bound), as a closed vocabulary — the
// row DECLARES how the frame is retried, it does not run retry code (S1e).
export const Retry = Object.freeze({
  NONE: 'NONE',                   // no retry concept — fire-and-forget event or fan-out leg
  IDEMPOTENT: 'IDEMPOTENT',       // resend-safe, deduped BY THE DECLARED idempotency key (requires idempotency)
  NATURAL: 'NATURAL',             // naturally idempotent WITHOUT a frame key — a read, a catch-up, or an order-independent set-union (Aster recut-3 F2.2)
  SINGLE_FLIGHT: 'SINGLE_FLIGHT', // bounded writer flight (defer→probe→evict→promote→retry)
  BOUNDED_ONCE: 'BOUNDED_ONCE',   // earns exactly one direct retry, then gives up
  BOUNDED_N: 'BOUNDED_N',         // a bounded number of retries then gives up; NOT idempotent — a resend may produce a DISTINCT effect. The bound is declared STRUCTURALLY as retryMaxAttempts (total attempts; N attempts = at most N-1 retries), required by defineRow (S4c F2/F5: turn-refresh runs up to 3 attempts and each mints a fresh credential)
  FLOOD_DEDUP: 'FLOOD_DEDUP',     // gossip flood; dedup by id, no targeted retry
});
const RETRY_CLASSES = new Set(Object.values(Retry));

// The explicit "considered and genuinely inapplicable" marker for a descriptor
// field (Aster S2/S3 recut-2 F2). A descriptor left `null` is UNCONSIDERED; a
// descriptor set to NOT_APPLICABLE is a deliberate declaration that the field has
// no meaning for this frame. The two are distinct; a complete table leaves no
// applicable descriptor silently null.
export const NOT_APPLICABLE = 'n/a';

// The conversation leg role — which half of a request/response pair a row is.
export const ConversationRole = Object.freeze({ REQUEST: 'REQUEST', RESPONSE: 'RESPONSE' });
const CONVERSATION_ROLES = new Set(Object.values(ConversationRole));
// Where a paired field is read from: the certified payload, or the routing meta
// (used when the return destination — not the payload — supplies the peer
// identity, e.g. REPLAYUP/HANDOFFACK routed back to the requester).
export const PairSide = Object.freeze({ payload: 'payload', meta: 'meta' });
const PAIR_SIDES = new Set(Object.values(PairSide));

// Fact type names a schema spec may assert over a projected leaf.
export const FactType = Object.freeze({
  string: 'string', number: 'number', boolean: 'boolean', bigint: 'bigint',
  arr: 'arr', bytes: 'bytes', obj: 'obj', present: 'present',
});
const FACT_TYPES = new Set(Object.values(FactType));

const _minted = new WeakSet();
export const isRow = (x) => { try { return _minted.has(x); } catch { return false; } };

export const MAX_PROJECTION_FIELDS = 24;
export const MAX_PATH = 96;
export const MAX_NOTE = 500;
export const MAX_CAP_STR = 256;
export const MAX_CAP_KEYS = 16;
export const MAX_LIST = 16;          // errorContract, traceFields, schema/correlation/idempotency lists
export const MAX_VARIANT_CASES = 32;
export const MAX_BYTES_CEILING = 1 << 16;   // 65536 — hard global cap on a per-scalar byte budget

const isStr = (x) => typeof x === 'string' && x.length > 0 && x.length <= 256;
const isBoundedStr = (x, m) => typeof x === 'string' && x.length > 0 && x.length <= m;
const isPosInt = (x) => Number.isInteger(x) && x > 0;
const isPlainObject = (x) => x != null && typeof x === 'object' && !Array.isArray(x) &&
  (Object.getPrototypeOf(x) === Object.prototype || Object.getPrototypeOf(x) === null);
const fail = (type, msg) => { throw new TypeError(`defineRow(${type ?? '?'}): ${msg}`); };
const UNSAFE_SEG = new Set(['__proto__', 'prototype', 'constructor']);

function validPaths(type, name, arr, maxLen = MAX_LIST) {
  if (!Array.isArray(arr)) fail(type, `${name} must be an array`);
  if (arr.length > maxLen) fail(type, `${name} exceeds ${maxLen} entries`);
  for (const s of arr) {
    if (!isBoundedStr(s, MAX_PATH)) fail(type, `${name} entries must be non-empty strings <= ${MAX_PATH} chars`);
    for (const seg of s.split('.')) if (UNSAFE_SEG.has(seg)) fail(type, `${name} path segment "${seg}" is not allowed`);
  }
  if (new Set(arr).size !== arr.length) fail(type, `${name} entries must be unique`);
  return Object.freeze([...arr]);
}

const notFn = (type, name, v) => { if (typeof v === 'function') fail(type, `${name} must be a declarative spec object, not a function (S1e: no row code runs in the dispatch thread)`); };

export function defineRow(row) {
  if (!isPlainObject(row)) throw new TypeError('defineRow: a plain row object required');
  const { type, kind, owningService } = row;
  if (!isStr(type)) fail(type, 'type required');
  if (!FRAME_KINDS.has(kind)) fail(type, `invalid frame kind ${String(kind)}`);
  if (!isStr(owningService)) fail(type, 'owningService (§4.9) required');
  if (row.variant != null && !isBoundedStr(row.variant, 64)) fail(type, 'variant must be a bounded string or null');

  const vr = row.versionRange;
  if (!isPlainObject(vr) || !Number.isInteger(vr.min) || !Number.isInteger(vr.max) || vr.min < 1 || vr.max < vr.min) {
    fail(type, 'versionRange { min, max } required with integer min>=1 and max>=min');
  }
  for (const g of ['authGuard', 'admissionGuard', 'placementGuard']) if (row[g] != null && !isStr(row[g])) fail(type, `${g} must be a string`);
  for (const p of ['topicProfile', 'eventIdScheme', 'replayCursorType', 'orderingModel', 'producedPolicy', 'requiredPolicy', 'outcome', 'terminalOutcome']) {
    if (row[p] != null && !isStr(row[p])) fail(type, `${p} must be a string or null`);
  }
  // retry classification (§4.4) — a closed vocabulary, or the explicit N/A marker.
  if (row.retry != null && row.retry !== NOT_APPLICABLE && !RETRY_CLASSES.has(row.retry)) {
    fail(type, `retry must be one of ${[...RETRY_CLASSES].join('|')} or ${NOT_APPLICABLE}`);
  }
  if (row.evidence != null && !EVIDENCE_LEVELS.has(row.evidence)) fail(type, `invalid evidence ${String(row.evidence)}`);
  if (row.proves != null && !PROVES.has(row.proves)) fail(type, `invalid proves ${String(row.proves)}`);
  if (row.evidence != null && row.proves != null && !EVIDENCE_FOR_PROOF[row.proves].has(row.evidence)) {
    fail(type, `evidence ${row.evidence} contradicts proves ${row.proves}`);
  }

  // projection — plain object, path arrays, HARD cap (reject, never truncate).
  const proj = row.projection ?? {};
  if (!isPlainObject(proj)) fail(type, 'projection must be a plain object');
  const pPay = validPaths(type, 'projection.payload', proj.payload ?? [], MAX_PROJECTION_FIELDS);
  const pMeta = validPaths(type, 'projection.meta', proj.meta ?? [], MAX_PROJECTION_FIELDS);
  if (pPay.length + pMeta.length > MAX_PROJECTION_FIELDS) fail(type, `projection exceeds ${MAX_PROJECTION_FIELDS} fields (declare fewer; the runtime rejects, it does not truncate)`);
  // A recipe path is unqualified (schema/correlation/idempotency name a field by
  // path only); if the same path is declared on BOTH payload and meta, resolution
  // is ambiguous (Aster S1e #4). Reject the collision so a path names one side.
  const payloadSet = new Set(pPay);
  for (const m of pMeta) if (payloadSet.has(m)) fail(type, `projection path ${m} is declared on both payload and meta; recipe resolution would be ambiguous (declare it on one side)`);
  const projection = Object.freeze({ payload: pPay, meta: pMeta });
  const projSet = new Set([...pPay, ...pMeta]);
  const inProjection = (p) => projSet.has(p);

  const errorContract = validPaths(type, 'errorContract', row.errorContract ?? [], MAX_LIST);
  const traceFields = validPaths(type, 'traceFields', row.traceFields ?? [], MAX_LIST);

  // schema — DECLARATIVE spec, never a function (S1e).
  notFn(type, 'schema', row.schema);
  let schema = Object.freeze({ require: Object.freeze([]), forbid: Object.freeze([]), types: Object.freeze({}) });
  if (row.schema != null) {
    if (!isPlainObject(row.schema)) fail(type, 'schema must be a plain spec object');
    const require_ = validPaths(type, 'schema.require', row.schema.require ?? [], MAX_LIST);
    const forbid = validPaths(type, 'schema.forbid', row.schema.forbid ?? [], MAX_LIST);
    const typesIn = row.schema.types ?? {};
    if (!isPlainObject(typesIn)) fail(type, 'schema.types must be a plain object');
    const typeKeys = Object.keys(typesIn);
    if (typeKeys.length > MAX_LIST) fail(type, `schema.types exceeds ${MAX_LIST} entries`);
    const types = Object.create(null);
    for (const k of typeKeys) {
      if (!isBoundedStr(k, MAX_PATH)) fail(type, 'schema.types keys must be bounded path strings');
      for (const seg of k.split('.')) if (UNSAFE_SEG.has(seg)) fail(type, `schema.types path segment "${seg}" is not allowed`);
      if (!FACT_TYPES.has(typesIn[k])) fail(type, `schema.types.${k} must be one of ${[...FACT_TYPES].join('|')}`);
      types[k] = typesIn[k];
    }
    for (const p of [...require_, ...forbid, ...Object.keys(types)]) if (!inProjection(p)) fail(type, `schema path ${p} is not in the declared projection`);
    schema = Object.freeze({ require: require_, forbid, types: Object.freeze(types) });
  }

  // correlation — DECLARATIVE spec, never a function (S1e). The authority subject
  // (LegacyAuthorityRef/IngressRef/HolderRef/AuthorLaneRef) is present iff every
  // `requires` path is present. `binding` (Aster S2/S3 recut-2 F3) additionally
  // GROUPS those paths into the exact identity they bind — the open flight, the
  // authority/incarnation, and the proof signer — so a LegacyAuthorityRef declares
  // the D1 flight match, not a flat presence list. Every binding path must also be
  // a `requires` path (the group cannot bind a field the subject does not require).
  notFn(type, 'correlation', row.correlation);
  let correlation = null, subjectShape = null;
  if (row.correlation != null) {
    if (!isPlainObject(row.correlation)) fail(type, 'correlation must be a plain spec object');
    if (!CORRELATION_KINDS.has(row.correlation.kind)) fail(type, 'correlation.kind (CorrelationSubjectKind) required');
    const requires_ = validPaths(type, 'correlation.requires', row.correlation.requires ?? [], MAX_LIST);
    // TransportRpcRef is a CHANNEL subject (Aster ASTER-E2-CHANNEL-SUBJECT): the
    // request↔return pairing comes from the transport RPC channel, not a payload
    // field. The empty-requires + fixed-transportScope exception is available ONLY
    // to FrameKind.REQUEST_RESPONSE + TransportRpcRef; every other subject keeps the
    // non-empty projected-requires rule, and no generic empty-requires escape hatch
    // exists. transportScope is likewise rejected on any other kind/subject.
    const isRpcSubject = row.correlation.kind === CorrelationSubjectKind.TransportRpcRef;
    if (row.correlation.transportScope != null && !(isRpcSubject && kind === FrameKind.REQUEST_RESPONSE)) {
      fail(type, 'correlation.transportScope is valid ONLY for FrameKind.REQUEST_RESPONSE with CorrelationSubjectKind.TransportRpcRef');
    }
    let transportScope = null;
    if (isRpcSubject) {
      if (kind !== FrameKind.REQUEST_RESPONSE) fail(type, 'CorrelationSubjectKind.TransportRpcRef is valid only on FrameKind.REQUEST_RESPONSE');
      if (row.correlation.transportScope !== 'request-return') fail(type, 'TransportRpcRef requires an explicit correlation.transportScope === "request-return" (the transport request-return channel)');
      if (requires_.length !== 0) fail(type, 'TransportRpcRef must NOT claim a payload projection as a correlation id — correlation.requires must be empty (the pairing is the transport RPC channel)');
      transportScope = 'request-return';
    } else {
      if (requires_.length === 0) fail(type, 'correlation must declare non-empty requires');
      for (const p of requires_) if (!inProjection(p)) fail(type, `correlation.requires path ${p} is not in the declared projection`);
    }
    const requiresSet = new Set(requires_);
    let binding = null;
    if (row.correlation.binding != null) {
      if (!isPlainObject(row.correlation.binding)) fail(type, 'correlation.binding must be a plain object');
      const bindOut = {};
      for (const group of ['flight', 'authority', 'proofSigner']) {
        if (row.correlation.binding[group] == null) continue;
        const g = validPaths(type, `correlation.binding.${group}`, row.correlation.binding[group], MAX_LIST);
        for (const p of g) if (!requiresSet.has(p)) fail(type, `correlation.binding.${group} path ${p} is not a correlation.requires path`);
        bindOut[group] = g;
      }
      // F3.2 (Aster recut-3): model the actual authority RELATION, not just
      // field presence. Each relation names a `subject` (a requires path), how it
      // `derives` the authority (e.g. the signer pubkey hashing to the expected
      // root node id, or an authenticated adjacent sender), and the `boundTo`
      // expected authority reference (e.g. the open flight's rootHex). Declarative
      // — the shadow layer records the contract; the live handler enforces it.
      if (row.correlation.binding.relations != null) {
        if (!Array.isArray(row.correlation.binding.relations)) fail(type, 'correlation.binding.relations must be an array');
        if (row.correlation.binding.relations.length > MAX_LIST) fail(type, `correlation.binding.relations exceeds ${MAX_LIST}`);
        const rels = [];
        for (const r of row.correlation.binding.relations) {
          if (!isPlainObject(r)) fail(type, 'correlation.binding.relations entries must be plain objects');
          if (!isBoundedStr(r.subject, MAX_PATH) || !requiresSet.has(r.subject)) fail(type, 'binding relation subject must be a correlation.requires path');
          if (!isStr(r.derives) || !isStr(r.boundTo)) fail(type, 'binding relation requires `derives` and `boundTo` strings');
          rels.push(Object.freeze({ subject: r.subject, derives: r.derives, boundTo: r.boundTo }));
        }
        bindOut.relations = Object.freeze(rels);
      }
      binding = Object.freeze(bindOut);
    }
    correlation = Object.freeze({ kind: row.correlation.kind, requires: requires_, binding, ...(transportScope ? { transportScope } : {}) });
    subjectShape = row.correlation.kind;
  }
  if (kind === FrameKind.REQUEST_RESPONSE && !correlation) fail(type, 'REQUEST_RESPONSE requires a correlation spec');

  // idempotency — DECLARATIVE spec; the old idempotencyKey callback is gone.
  if (row.idempotencyKey != null) fail(type, 'idempotencyKey (callback) is removed in S1e; declare idempotency:{ from:[paths] }');
  notFn(type, 'idempotency', row.idempotency);
  let idempotency = null;
  if (row.idempotency != null) {
    if (!isPlainObject(row.idempotency)) fail(type, 'idempotency must be a plain spec object');
    const from = validPaths(type, 'idempotency.from', row.idempotency.from ?? [], MAX_LIST);
    if (from.length === 0) fail(type, 'idempotency must declare non-empty from');
    for (const p of from) if (!inProjection(p)) fail(type, `idempotency.from path ${p} is not in the declared projection`);
    idempotency = Object.freeze({ from });
  }

  // conversation — a DECLARATIVE request/response PAIR ALGEBRA, separate from the
  // authority correlation subject (Aster S2/S3 recut-2 F3). Presence is not
  // correlation: a conversation names the OPPOSITE frame type, this leg's `role`,
  // and a `pairing` — an ordered list of { local, remote, from } field
  // correspondences. Two frames pair iff, for every entry, this frame's `local`
  // field equals the opposite frame's `remote` field. `from` says where the local
  // field is read: the certified `payload`, or the routing `meta` when the return
  // destination (not the payload) supplies the peer identity — e.g. REPLAYUP and
  // HANDOFFACK are routed back to the requester, so their requester/parent identity
  // is a meta field, not a payload field. A meta-sourced local path must be
  // projected on meta; a payload-sourced one on payload. No function (S1e).
  notFn(type, 'conversation', row.conversation);
  let conversation = null;
  if (row.conversation != null) {
    const c = row.conversation;
    if (!isPlainObject(c)) fail(type, 'conversation must be a plain spec object');
    if (!CONVERSATION_ROLES.has(c.role)) fail(type, 'conversation.role (REQUEST|RESPONSE) required');
    if (!isStr(c.opposite)) fail(type, 'conversation.opposite (the paired frame type) required');
    if (!Array.isArray(c.pairing) || c.pairing.length === 0) fail(type, 'conversation.pairing must be a non-empty array');
    if (c.pairing.length > MAX_LIST) fail(type, `conversation.pairing exceeds ${MAX_LIST} entries`);
    const pairing = [], localKey = [];
    for (const pr of c.pairing) {
      if (!isPlainObject(pr)) fail(type, 'conversation.pairing entries must be plain objects');
      if (!isBoundedStr(pr.local, MAX_PATH) || !isBoundedStr(pr.remote, MAX_PATH)) fail(type, 'conversation.pairing local/remote must be bounded path strings');
      for (const seg of [...pr.local.split('.'), ...pr.remote.split('.')]) if (UNSAFE_SEG.has(seg)) fail(type, `conversation.pairing path segment "${seg}" is not allowed`);
      const from = pr.from ?? PairSide.payload;
      if (!PAIR_SIDES.has(from)) fail(type, 'conversation.pairing.from must be payload|meta');
      if (from === PairSide.payload && !pPay.includes(pr.local)) fail(type, `conversation.pairing local ${pr.local} is not in projection.payload`);
      if (from === PairSide.meta && !pMeta.includes(pr.local)) fail(type, `conversation.pairing local ${pr.local} is not in projection.meta`);
      pairing.push(Object.freeze({ local: pr.local, remote: pr.remote, from }));
      if (from === PairSide.payload) localKey.push(pr.local);   // payload legs are shadow-observable; meta is unbranded
    }
    conversation = Object.freeze({ role: c.role, opposite: c.opposite, pairing: Object.freeze(pairing), localKey: Object.freeze(localKey) });
  }

  // budget — plain object, positive ints. maxBytes has a HARD global ceiling
  // (a count cap alone doesn't bound a dispatch-thread scan — Aster S1f #5).
  // maxLeaves caps how many projected leaf paths are read (renamed from maxWork,
  // which mis-implied a per-operation charge; the per-operation ceiling is the
  // fixed MAX_REFLECT_OPS in shadowRegistry.js — Aster S1f #5).
  const b = row.budget ?? {};
  if (!isPlainObject(b)) fail(type, 'budget must be a plain object');
  if (b.maxBytes != null && (!isPosInt(b.maxBytes) || b.maxBytes > MAX_BYTES_CEILING)) fail(type, `budget.maxBytes must be a positive integer <= ${MAX_BYTES_CEILING}`);
  if (b.maxLeaves != null && !isPosInt(b.maxLeaves)) fail(type, 'budget.maxLeaves must be a positive integer or null');
  if (b.maxWork != null) fail(type, 'budget.maxWork is renamed to budget.maxLeaves (S1f)');

  // capability range — plain object; cap KEY COUNT, key length, and each value
  // (a retained declaration key must itself be bounded — Aster S1f #5).
  const cap = row.capabilityRange ?? {};
  if (!isPlainObject(cap)) fail(type, 'capabilityRange must be a plain object');
  const capKeys = Object.keys(cap);
  if (capKeys.length > MAX_CAP_KEYS) fail(type, `capabilityRange exceeds ${MAX_CAP_KEYS} keys`);
  for (const k of capKeys) {
    if (k.length > MAX_CAP_STR) fail(type, `capabilityRange key exceeds ${MAX_CAP_STR} chars`);
    const v = cap[k];
    if (v == null) continue;
    if (typeof v === 'number') { if (!Number.isFinite(v)) fail(type, `capabilityRange.${k} must be finite`); }
    else if (typeof v === 'string') { if (v.length > MAX_CAP_STR) fail(type, `capabilityRange.${k} exceeds ${MAX_CAP_STR} chars`); }
    else fail(type, `capabilityRange.${k} must be a finite number, bounded string, or null`);
  }

  if (row.note != null && !isBoundedStr(row.note, MAX_NOTE)) fail(type, `note must be a string <= ${MAX_NOTE} chars`);
  if (row.evidence === EvidenceLevel.COMMITTED && !isStr(row.producedPolicy)) fail(type, 'COMMITTED evidence requires a producedPolicy');
  // F2.2 (Aster recut-3): retry IDEMPOTENT means "deduped by the declared idempotency
  // key" — it is a contradiction to select it with no idempotency spec. A frame that
  // is resend-safe WITHOUT a frame key (a read, a catch-up, an order-independent
  // set-union) declares Retry.NATURAL instead.
  if (row.retry === Retry.IDEMPOTENT && !idempotency) fail(type, 'retry IDEMPOTENT requires an idempotency key; use Retry.NATURAL for a frame that is naturally idempotent without a key');
  // S4c F5 (Aster/Vega): Retry.BOUNDED_N declares "a bounded number of retries" — the
  // bound must be a MACHINE-CHECKABLE field, not comment text. Require retryMaxAttempts
  // (total attempts, so >= 2 means at least one retry) iff BOUNDED_N, and forbid it
  // otherwise, exactly as IDEMPOTENT requires its key. Mirrors the recut-3 F2.2 shape.
  const rma = row.retryMaxAttempts;
  if (row.retry === Retry.BOUNDED_N) {
    if (!(Number.isInteger(rma) && rma >= 2)) fail(type, 'retry BOUNDED_N requires retryMaxAttempts: an integer >= 2 (total attempts; N attempts = at most N-1 retries)');
  } else if (rma != null) {
    fail(type, 'retryMaxAttempts is only valid with retry BOUNDED_N');
  }

  const norm = {
    type, variant: row.variant ?? null,
    versionRange: Object.freeze({ min: vr.min, max: vr.max }),
    kind, owningService,
    authGuard: row.authGuard ?? 'none', admissionGuard: row.admissionGuard ?? 'none', placementGuard: row.placementGuard ?? 'none',
    topicProfile: row.topicProfile ?? null, eventIdScheme: row.eventIdScheme ?? null,
    replayCursorType: row.replayCursorType ?? null, orderingModel: row.orderingModel ?? null,
    projection,
    schema, correlation, idempotency, conversation,
    subjectShape,
    evidence: row.evidence ?? null, producedPolicy: row.producedPolicy ?? null, requiredPolicy: row.requiredPolicy ?? null,
    proves: row.proves ?? null, outcome: row.outcome ?? null, terminalOutcome: row.terminalOutcome ?? null,
    retry: row.retry ?? null,
    retryMaxAttempts: row.retryMaxAttempts ?? null,   // S4c F5: the declared BOUNDED_N bound, frozen into the row
    errorContract, traceFields,
    budget: Object.freeze({ maxBytes: b.maxBytes ?? null, maxLeaves: b.maxLeaves ?? null }),
    capabilityRange: Object.freeze({ ...cap }),
    note: row.note ?? '',
  };
  Object.freeze(norm);
  _minted.add(norm);
  return norm;
}

export default defineRow;
