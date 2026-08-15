// registry/shadowRegistry.js — the SHADOW dispatcher (refactor Phase 1, REF-1.1;
// S1f re-cut per Aster's S1e disposition). The shadow layer NEVER reflects on a
// live handler argument: JavaScript reflection (getPrototypeOf,
// getOwnPropertyDescriptor, Array.isArray+.length) is itself trap-capable, so
// inspecting a Proxy to decide whether it is "plain" can execute or throw. The
// only thing the evaluator ever reads is a decoder-produced SNAPSHOT whose
// provenance is a module-private brand (a WeakSet) — an identity check that
// fires no trap. Anything unbranded is observed as nothing; the handler runs
// verbatim.
//
// Invariants (each an adversarial gate case in smoke_registry_core.mjs):
//   * NO reflection on untrusted live args. Only a branded snapshot is read.
//     A root/nested/array/revoked Proxy is never branded → observation skipped →
//     handler runs verbatim: no trap fires, no mutation, no suppression, no
//     observer exception, arguments unchanged.
//   * schema/correlation/idempotency are DECLARATIVE recipes a fixed evaluator
//     interprets over frozen facts. No row code runs.
//   * UTF-8 accounting counts 4 bytes only for a VALID surrogate pair; a lone
//     surrogate is the 3-byte replacement and does not skip the next unit.
//   * Budgets: maxLeaves caps projected leaf paths; MAX_REFLECT_OPS is a fixed
//     hard ceiling on descriptor reads (including the variant discriminator);
//     maxBytes has a hard global ceiling (enforced in defineRow); bigint is a
//     structural fact (no unbounded conversion).
//   * Recipe paths resolve unambiguously — payload/meta collisions are rejected
//     at defineRow, so a path names exactly one side.
//   * Telemetry emits FIXED allowlisted codes only. Registry keys are a nested
//     Map on (type, variant) with a Symbol base sentinel — no delimiter, no NUL.

import { isRow, MAX_VARIANT_CASES, MAX_CAP_STR } from './types.js';
import { isCertified, kindOf } from './snapshotMint.js';

let _override = null;
function envFlag() {
  try { const v = (typeof process !== 'undefined' && process.env && process.env.AXONA_REGISTRY_SHADOW) || ''; return v === '1' || v === 'true' || v === 'on'; }
  catch { return false; }
}
export function shadowEnabled() { return _override === null ? envFlag() : _override; }
export function setShadowEnabled(on) { _override = (on === null || on === undefined) ? null : !!on; }

const DEFAULT_MAX_BYTES = 256;   // per-scalar UTF-8 byte cap when a row declares none
const DEFAULT_MAX_LEAVES = 64;   // projected-leaf cap when a row declares none
const MAX_REFLECT_OPS = 256;     // fixed hard ceiling on descriptor reads per observation (incl. discriminator)
const MAX_DEPTH = 8;             // leaf-path segment depth cap
const BASE = Symbol('registry.base');   // inner-map key for the variant-less row — a private Symbol, never a real variant
const VARIANT_VALUE_TYPES = new Set(['string', 'number', 'boolean']);   // F4: scalar type gate for a variant discriminator

// Provenance is certified by the decoder-private mint (snapshotMint.js). The
// shadow layer reflects on a value ONLY if isCertified(value) — a WeakSet
// identity lookup that fires no Proxy trap. The mint is not publicly importable
// and brands every reachable node, so a hostile or nested Proxy is never in the
// set and is never touched. The mint (`certify`) is deliberately NOT re-exported
// here or by registry/index.js.

// UTF-8 byte length with early stop. A high surrogate counts as a 4-byte pair
// ONLY when followed by a low surrogate; a lone surrogate is the 3-byte
// replacement sequence and does not consume the next code unit (Aster S1f #3).
function utf8LenCapped(s, cap) {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xD800 && c <= 0xDBFF) {
      const d = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (d >= 0xDC00 && d <= 0xDFFF) { n += 4; i++; }   // valid pair
      else n += 3;                                        // lone high surrogate → U+FFFD
    } else n += 3;                                        // BMP ≥ 0x800, or lone low surrogate → U+FFFD
    if (n > cap) return n;
  }
  return n;
}

// Read a declared leaf path from a SNAPSHOT (Proxy-free by construction) using
// only own data-property descriptors. `ops` counts every descriptor read against
// the fixed hard ceiling. Never invokes an accessor, never enumerates.
function readLeaf(root, path, maxBytes, ops) {
  let cur = root;
  const segs = path.split('.');
  if (segs.length > MAX_DEPTH) return { fault: 'path-too-deep' };
  for (let i = 0; i < segs.length; i++) {
    if (cur == null || typeof cur !== 'object') return { absent: true };
    if (!isCertified(cur)) return { fault: 'unbranded' };   // never reflect on an uncertified node (nested Proxy / post-mint insertion)
    if (ops.n >= MAX_REFLECT_OPS) return { fault: 'ops' };
    ops.n++;
    const d = Object.getOwnPropertyDescriptor(cur, segs[i]);   // safe: certified node is not a Proxy
    if (!d) return { absent: true };
    if (!('value' in d)) return { fault: 'accessor' };
    cur = d.value;
  }
  return represent(cur, maxBytes);
}

function represent(v, maxBytes) {
  const cap = Number.isInteger(maxBytes) && maxBytes > 0 ? maxBytes : DEFAULT_MAX_BYTES;
  const t = typeof v;
  if (v === null || v === undefined) return { absent: true };
  // F2.1 (Aster recut-3): an over-cap scalar is PRESENT and typed, not malformed.
  // utf8LenCapped stops at cap+1, so this stays bounded (we never hold the full
  // value). Return a typed struct with `truncated:true` — the field counts as
  // present, its type is known, and schema does NOT fail; the truncation is
  // reported separately from a schema fault so a legitimate large `json`/`msgs`
  // frame under the reliable-publish limit is never mislabeled malformed.
  if (t === 'string') return utf8LenCapped(v, cap) > cap ? { struct: Object.freeze({ k: 'string', truncated: true }) } : { value: v };
  if (t === 'number') return Number.isFinite(v) ? { value: v } : { fault: 'nonfinite' };
  if (t === 'boolean') return { value: v };
  if (t === 'bigint') return { struct: Object.freeze({ k: 'bigint' }) };   // no toString — no unbounded conversion
  if (t !== 'object') return { fault: 'unsupported' };   // function / symbol
  // Reflect ONLY on a certified node (a nested Proxy or a post-mint insertion is
  // uncertified). Classification uses the decoder's CONSTRUCTION-TIME tag, never
  // `instanceof` or `Array.isArray` on the live value — so a prototype swapped
  // after certification is never consulted and fires no getPrototypeOf trap
  // (Aster S1g #1). A certified plain object has no tag → classified 'obj'
  // without touching its prototype.
  if (!isCertified(v)) return { fault: 'unbranded' };
  const tag = kindOf(v);
  if (tag) return { struct: tag };   // e.g. { k:'arr', len } (a future binary decoder tags { k:'bytes', len } at construction)
  return { struct: Object.freeze({ k: 'obj' }) };
}

// ── fixed, vetted evaluators over frozen facts ──
function typeOfFact(v) {
  if (v === null || v === undefined) return 'absent';
  if (typeof v === 'object' && typeof v.k === 'string') return v.k;
  return typeof v;
}
function factGet(payloadFacts, metaFacts, p) {
  // A path names exactly one side (payload/meta collisions are rejected at
  // defineRow), so this lookup is unambiguous.
  if (Object.prototype.hasOwnProperty.call(payloadFacts, p)) return payloadFacts[p];
  if (Object.prototype.hasOwnProperty.call(metaFacts, p)) return metaFacts[p];
  return undefined;
}
function evalSchema(spec, payloadFacts, metaFacts) {
  for (const p of spec.require) if (factGet(payloadFacts, metaFacts, p) === undefined) return { ok: false, code: 'missing-required' };
  for (const p of spec.forbid) if (factGet(payloadFacts, metaFacts, p) !== undefined) return { ok: false, code: 'forbidden-present' };
  for (const p of Object.keys(spec.types)) {
    const want = spec.types[p];
    if (want === 'present') continue;
    const v = factGet(payloadFacts, metaFacts, p);
    if (v === undefined) continue;
    if (typeOfFact(v) !== want) return { ok: false, code: 'type-mismatch' };
  }
  return { ok: true, code: null };
}
function evalPresence(paths, payloadFacts, metaFacts) {
  for (const p of paths) if (factGet(payloadFacts, metaFacts, p) === undefined) return false;
  return true;
}

export class ShadowRegistry {
  constructor(opts = {}) {
    this.boundary = opts.boundary || 'unknown';
    this._byType = new Map();
    this._sink = typeof opts.sink === 'function' ? opts.sink : () => {};
    this._enabled = typeof opts.enabled === 'function' ? opts.enabled : shadowEnabled;
    this._now = typeof opts.now === 'function' ? opts.now : defaultNow;
    const s = Number(opts.sampleEvery);
    this._sampleEvery = Number.isInteger(s) && s >= 1 ? s : 1;
    this._seen = 0;
  }

  register(row) {
    if (!isRow(row)) throw new TypeError('ShadowRegistry.register: a defineRow()-branded row required');
    let inner = this._byType.get(row.type);
    if (!inner) { inner = new Map(); this._byType.set(row.type, inner); }
    const vk = row.variant == null ? BASE : row.variant;
    if (inner.has(vk)) throw new TypeError(`ShadowRegistry.register: duplicate (type=${row.type}, variant=${row.variant ?? 'base'})`);
    inner.set(vk, row);
    return this;
  }

  row(type, variant = null) { const inner = this._byType.get(type); return inner ? (inner.get(variant == null ? BASE : variant) || null) : null; }
  size() { let n = 0; for (const inner of this._byType.values()) n += inner.size; return n; }

  wrap(type, handler, opts = {}) {
    if (typeof type !== 'string' || !type) throw new TypeError('ShadowRegistry.wrap: type (non-empty string) required at construction');
    if (typeof handler !== 'function') throw new TypeError(`ShadowRegistry.wrap(${type}): handler function required`);
    const variantBy = validateVariantBy(type, opts.variantBy, this._byType.get(type));
    // REF-1.1 M1c (Decision 1): the LIVE-path certifying re-encoder. On the routed
    // dispatch path the handler args are freshly DECODED plain objects (never
    // decoder-certified), so without this the wrap observes nothing on live traffic
    // (100% unbranded-source; the M1 canary finding). mintLive re-encodes an arg
    // through the CANONICAL wire codec and certifies the TEXT — same discipline as
    // B2-B4 (`certifyPlain(JSON.stringify(body))`), but the caller supplies a
    // bigint-faithful encoder so pubsub ids survive (a plain JSON.stringify drops
    // BigInt). It is only ever called on a freshly-decoded plain arg, and it is
    // GUARDED at the call site: any throw (a hostile getter/Proxy trap fired by the
    // re-encode) yields no snapshot → unbranded no-op → handler verbatim. The mint
    // still accepts only TEXT; the wrap never brands a live object.
    const mintLive = typeof opts.mintLive === 'function' ? opts.mintLive : null;
    const self = this;

    return function shadowWrapped(...args) {
      let on; try { on = self._enabled(); } catch { on = false; }
      if (!on) return handler.apply(this, args);

      // Observed payload/meta are CERTIFIED snapshots — never the live args. Prefer
      // an already-certified arg (decoder-minted); else, if a mintLive re-encoder is
      // provided, mint one from the live arg (guarded). The identity check fires no
      // Proxy trap; an uncertified, un-mintable arg is never reflected on — handler
      // runs verbatim. Contained so nothing here can suppress the handler or escape.
      let obsPayload = isCertified(args[0]) ? args[0] : null;
      let obsMeta = isCertified(args[1]) ? args[1] : null;
      if (!obsPayload && mintLive) {
        try { const s = mintLive(args[0]); if (isCertified(s)) obsPayload = s; } catch { /* → unbranded */ }
        if (obsPayload && args.length > 1 && args[1] != null && !obsMeta) {
          try { const m = mintLive(args[1]); if (isCertified(m)) obsMeta = m; } catch { /* meta stays uncertified */ }
        }
      }
      if (!obsPayload) {
        try { self._emitUnbranded(type); } catch { /* never break dispatch */ }
        return handler.apply(this, args);
      }

      let pre = null, variant = null;
      try {
        const t0 = self._safeNow();
        const ops = { n: 0 };
        let variantFault = null;
        if (variantBy) {
          const leaf = readLeaf(obsPayload, variantBy.path, DEFAULT_MAX_BYTES, ops);
          const r = pickVariant(variantBy, leaf);
          variant = r.variant; variantFault = r.fault;
        }
        const row = variantFault ? null : self.row(type, variant);
        const unknownVariant = (!variantFault && variant != null && !row);
        pre = self._observe(row, obsPayload, obsMeta, { variantFault, unknownVariant }, ops);
        pre._t0 = t0;
      } catch { pre = null; }   // an observer fault must never change delivery

      let result, threw = false;
      try { result = handler.apply(this, args); } catch (e) { threw = true; result = e; }
      if (pre) {
        // S1 INERTNESS (Aster recut-2 F1): the generic core inspects ONLY the
        // synchronous return, by primitive type. It NEVER touches a returned object —
        // no `.then`, no `instanceof`-driven branch — because a native Promise
        // subclass with an own `then` getter would have `instanceof Promise` true and
        // any subsequent `.then()` would invoke that getter, and attaching a
        // settlement observer would mark an otherwise-ignored rejection handled and
        // suppress Node's unhandledRejection. Both are observable, so neither is inert.
        // A Promise return is therefore verdict 'object' (verdictOf): settlement is NOT
        // observed here, and declaredEvidence marks evidence as a contract-on-success,
        // so a deferred handler is not overclaimed as achieved. Real async-outcome
        // observation belongs in an explicit owning-service adapter on a known effect
        // path, never in this generic wrapper.
        try { self._emit(type, variant, pre, threw ? 'threw' : verdictOf(result), self._safeNow() - pre._t0); } catch { /* */ }
      }
      if (threw) throw result;
      return result;
    };
  }

  // Shape-only observation for a side-channel that must NEVER run or touch the
  // real handler (REF-1.1 S4a F1). Unlike wrap(type, NOOP) — which emits the
  // no-op's verdict 'passed' and thus OVERCLAIMS handler success — observeShape
  // runs no handler and emits verdict 'unobserved': it records the certified
  // frame's SHAPE (registered / schemaOk / correlation / conversation) and makes
  // NO claim about the real handler's disposition (which may be async and later
  // reject). Flag-off: no-op. Uncertified arg: unbranded-source, no reflection.
  // Never throws out — observation must not affect the caller.
  observeShape(type, payload, meta, opts = {}) {
    let on; try { on = this._enabled(); } catch { on = false; }
    if (!on) return;
    if (!isCertified(payload)) { try { this._emitUnbranded(type); } catch { /* */ } return; }
    try {
      const t0 = this._safeNow();
      const ops = { n: 0 };
      const variantBy = validateVariantBy(type, opts.variantBy, this._byType.get(type));
      let variant = null, variantFault = null;
      if (variantBy) {
        const leaf = readLeaf(payload, variantBy.path, DEFAULT_MAX_BYTES, ops);
        const r = pickVariant(variantBy, leaf);
        variant = r.variant; variantFault = r.fault;
      }
      const row = variantFault ? null : this.row(type, variant);
      const unknownVariant = (!variantFault && variant != null && !row);
      const metaObj = isCertified(meta) ? meta : null;
      const pre = this._observe(row, payload, metaObj, { variantFault, unknownVariant }, ops);
      this._emit(type, variant, pre, 'unobserved', this._safeNow() - t0);   // NEVER 'passed' — no handler ran
    } catch { /* an observer fault must never reach the caller */ }
  }

  _observe(row, payload, metaObj, faults, ops) {
    const out = { registered: !!row, variantFault: faults.variantFault || null, unknownVariant: !!faults.unknownVariant };
    if (!row) return out;
    out.kind = row.kind; out.owningService = row.owningService; out.evidence = row.evidence; out.proves = row.proves; out.subjectShape = row.subjectShape;

    const facts = { payload: Object.create(null), meta: Object.create(null) };
    let projFault = null, truncated = false;
    const maxLeaves = row.budget.maxLeaves ?? DEFAULT_MAX_LEAVES;
    let leaves = 0;
    const sides = [['payload', payload], ['meta', metaObj]];
    for (const [side, obj] of sides) {
      for (const path of row.projection[side]) {
        if (obj == null) { continue; }   // unbranded/absent side — no reflection
        if (leaves >= maxLeaves) { projFault = projFault || 'leaves'; break; }
        leaves++;
        const leaf = readLeaf(obj, path, row.budget.maxBytes, ops);
        if (leaf.absent) continue;
        if (leaf.fault) { projFault = projFault || leaf.fault; continue; }
        if (leaf.struct && leaf.struct.truncated) truncated = true;   // F2.1: present-but-over-budget, NOT a fault
        facts[side][path] = leaf.value !== undefined ? leaf.value : leaf.struct;
      }
      Object.freeze(facts[side]);
    }
    Object.freeze(facts);
    out.projFault = projFault;
    out.truncated = truncated;   // F2.1: bounded-observation artifact, reported separately from schema failure

    const s = evalSchema(row.schema, facts.payload, facts.meta);
    out.schemaOk = s.ok; if (!s.ok) out.schemaCode = s.code;
    if (row.correlation) out.correlationPresent = evalPresence(row.correlation.requires, facts.payload, facts.meta);
    if (row.conversation) {
      // F3.1 (Aster recut-3): a two-leg conversation key cannot be reported present
      // when a leg is unknown. Evaluate the FULL pairing as true/false/'unknown':
      // a payload leg absent ⇒ false; else if a meta-sourced leg exists and the meta
      // side is unbranded (the normal routing-meta case) ⇒ 'unknown' (never claim
      // present from the payload leg alone); a CERTIFIED meta snapshot lets the meta
      // legs be observed as genuinely present/absent.
      const c = row.conversation;
      const metaLegs = c.pairing.filter((p) => p.from === 'meta');
      if (!evalPresence(c.localKey, facts.payload, facts.meta)) out.conversationPresent = false;
      else if (metaLegs.length === 0) out.conversationPresent = true;
      else if (metaObj == null) out.conversationPresent = 'unknown';
      else out.conversationPresent = metaLegs.every((p) => factGet(facts.payload, facts.meta, p.local) !== undefined);
    }
    if (row.idempotency) out.idempotencyPresent = evalPresence(row.idempotency.from, facts.payload, facts.meta);
    return out;
  }

  _emitUnbranded(type) {
    this._seen++;
    let rec;
    try { rec = { boundary: this.boundary, type, registered: null, faults: ['unbranded-source'], verdict: 'unobserved', durationMs: null }; }
    catch { return; }
    try { this._sink(rec); } catch { /* */ }
  }

  _emit(type, variant, pre, verdict, durationMs) {
    const isFault = verdict === 'threw' || pre.schemaOk === false || pre.schemaCode
      || pre.variantFault || pre.unknownVariant || pre.projFault || pre.registered === false;
    this._seen++;
    if (!isFault && this._sampleEvery > 1 && (this._seen % this._sampleEvery) !== 0) return;
    let rec;
    try {
      rec = {
        boundary: this.boundary,
        type,
        variant: variant == null ? null : variant,
        registered: pre.registered,
        kind: pre.kind ?? null, owningService: pre.owningService ?? null,
        // F1 (Aster): these are the row's DECLARED contract — what the frame proves
        // ON SUCCESS — NOT an observed outcome of this trace. The observed facts are
        // verdict / schemaOk / faults below; a rejected, rerouted, or merely queued
        // frame does not ACHIEVE its declared evidence just by being seen.
        declaredEvidence: pre.evidence ?? null, declaredProves: pre.proves ?? null,
        correlationKind: pre.subjectShape ?? null,
        schemaOk: pre.schemaOk ?? null,
        schemaCode: pre.schemaCode ?? null,
        correlationPresent: pre.correlationPresent ?? null,
        conversationPresent: pre.conversationPresent ?? null,
        idempotencyPresent: pre.idempotencyPresent ?? null,
        truncated: pre.truncated ?? null,   // F2.1: a projected scalar exceeded its byte budget (present, typed, bounded) — NOT a schema fault

        faults: faultCodes(pre),
        verdict,
        durationMs: Number.isFinite(durationMs) ? Math.round(durationMs * 1000) / 1000 : null,
      };
    } catch { rec = { boundary: this.boundary, type: 'unknown', verdict: 'trace-fault' }; }
    try { this._sink(rec); } catch { /* a broken sink must never break dispatch */ }
  }

  _safeNow() { try { const n = this._now(); return Number.isFinite(n) ? n : 0; } catch { return 0; } }
}

function faultCodes(pre) {
  const f = [];
  if (pre.registered === false) f.push('unregistered');
  if (pre.unknownVariant) f.push('unknown-variant');
  if (pre.variantFault) f.push('variant-' + pre.variantFault);
  if (pre.projFault) f.push('projection-' + pre.projFault);
  if (pre.schemaCode) f.push('schema:' + pre.schemaCode);
  return f.length ? f : null;
}

function validateVariantBy(type, vb, inner) {
  if (vb == null) return null;
  if (typeof vb !== 'object' || Array.isArray(vb)) throw new TypeError(`wrap(${type}): variantBy must be a plain object`);
  if (typeof vb.path !== 'string' || !vb.path) throw new TypeError(`wrap(${type}): variantBy.path (string) required`);
  const isVar = (x) => x == null || (typeof x === 'string' && x.length > 0 && x.length <= 64);
  if (!isVar(vb.whenPresent) || !isVar(vb.whenAbsent) || !isVar(vb.default)) throw new TypeError(`wrap(${type}): variantBy variant names must be bounded strings`);
  // F4 (Aster): an optional scalar TYPE gate so the discriminator mirrors a handler
  // that checks `typeof payload.<path> === valueType`. A present-but-wrong-type leaf
  // is then treated as absent (selects whenAbsent), never as whenPresent.
  if (vb.valueType != null && !VARIANT_VALUE_TYPES.has(vb.valueType)) throw new TypeError(`wrap(${type}): variantBy.valueType must be one of string|number|boolean`);
  const out = { path: vb.path, valueType: vb.valueType ?? null, whenPresent: vb.whenPresent ?? null, whenAbsent: vb.whenAbsent ?? null, default: vb.default ?? null, cases: null };
  if (vb.cases != null) {
    if (typeof vb.cases !== 'object' || Array.isArray(vb.cases)) throw new TypeError(`wrap(${type}): variantBy.cases must be a plain object`);
    const caseKeys = Object.keys(vb.cases);
    if (caseKeys.length > MAX_VARIANT_CASES) throw new TypeError(`wrap(${type}): variantBy.cases exceeds ${MAX_VARIANT_CASES} entries`);
    const cases = Object.create(null);
    for (const k of caseKeys) {
      if (k.length > MAX_CAP_STR) throw new TypeError(`wrap(${type}): variantBy.cases key exceeds ${MAX_CAP_STR} chars`);
      if (!isVar(vb.cases[k])) throw new TypeError(`wrap(${type}): variantBy.cases values must be variant names`);
      cases[k] = vb.cases[k];
    }
    out.cases = cases;
  }
  if (!inner || inner.size === 0) throw new TypeError(`wrap(${type}): variantBy requires rows registered for ${type} before wrapping`);
  for (const row of inner.values()) {
    if (!row.projection.payload.includes(out.path)) throw new TypeError(`wrap(${type}): variantBy.path "${out.path}" is not a declared payload projection field of every registered variant`);
  }
  const registered = new Set([...inner.keys()].filter((k) => typeof k === 'string'));
  const results = [out.whenPresent, out.whenAbsent, out.default, ...(out.cases ? Object.values(out.cases) : [])].filter((x) => x != null);
  for (const r of results) if (!registered.has(r)) throw new TypeError(`wrap(${type}): variantBy result "${r}" is not a registered variant of ${type}`);
  return Object.freeze(out);
}

function pickVariant(vb, leaf) {
  if (leaf.fault) return { variant: null, fault: leaf.fault };
  let present = !leaf.absent;
  // F4 (Aster): a type-gated discriminator mirrors a handler that checks
  // `typeof payload.<path> === valueType`. A present-but-wrong-type leaf (e.g. a
  // numeric `sig` where the handler wants a string) is treated as absent, so it
  // selects whenAbsent/default — never the whenPresent variant.
  if (present && vb.valueType) present = (typeof leaf.value === vb.valueType);
  if (vb.cases && present && leaf.value !== undefined) {
    const key = String(leaf.value);
    if (Object.prototype.hasOwnProperty.call(vb.cases, key)) return { variant: vb.cases[key], fault: null };
  }
  if (present && vb.whenPresent != null) return { variant: vb.whenPresent, fault: null };
  if (!present && vb.whenAbsent != null) return { variant: vb.whenAbsent, fault: null };
  return { variant: vb.default, fault: null };
}

function defaultNow() { try { if (typeof performance !== 'undefined' && typeof performance.now === 'function') return performance.now(); } catch { /* */ } return 0; }

function verdictOf(r) {
  const t = typeof r;
  if (t === 'string') return r === 'consumed' ? 'consumed' : 'other';
  if (r === undefined || r === null || r === false) return 'passed';
  if (t === 'boolean' || t === 'number' || t === 'bigint' || t === 'symbol') return 'other';
  return 'object';
}

// REF-1.1 M1b: the ONE canonical canary invariant over a frameRegistrySummary()
// (peer- or manager-level). FAIL-CLOSED safety predicate — the single rollout
// gate, so it must reject invalid telemetry and both blind and dirty windows, and
// never reinterpret garbage as clean. Passes iff ALL hold:
//   built===true      — the registry object was constructed;
//   observing===true  — the runtime shadow gate (AXONA_REGISTRY_SHADOW) is ON, so
//                       the wrap actually emits traces. built without observing is
//                       a BLIND window: zero traces, faults:0 — looks clean but
//                       observed nothing (council F1, Aster/Vega). BOTH gates.
//   faults===0        — a present, non-negative safe integer, and zero. As of M1c
//                       `faults` counts CONTRACT violations only (schema / projection
//                       / variant / unregistered); a frame that took the unbranded no-op
//                       is NOT a fault — it is a COVERAGE miss counted in `unobserved`.
//   no 'threw'/'trace-fault' verdict — a threw can carry an empty faults[], so the
//                       faults counter alone is insufficient (council carry-forward).
//   total>0           — LIVENESS: frames actually flowed (M1c; Vega — this lived only
//                       in the canary script, so the predicate could pass an empty window).
//   unobserved===0    — COVERAGE (covered===total): every observed frame reached a
//                       certified snapshot and exercised its contract. An unbranded
//                       no-op frame counts 'unobserved' and MUST fail. This gate is
//                       what makes splitting 'unbranded-source' out of `faults` safe:
//                       an all-unbranded window still fails (F1 cannot be renamed),
//                       and a residual unbranded live frame fails even after the mint.
// Telemetry validation (council F2, Aster/Vega): summary must be a plain object;
// faults a non-negative safe integer; verdicts a plain counter object; each own
// verdict count a non-negative safe integer. Missing / null / negative / non-finite
// / wrong-typed fields → pass:false with an invalid-summary reason, NOT a coerced
// clean pass. Pure; every consumer (ops drain, canary slot, tests) reads THIS.
//
// TOTAL over arbitrary input (council F3, Aster/Vega): the advertised contract is
// that this NEVER throws — a rollout gate that raises on a hostile value is worse
// than one that fails closed. Totality is delivered by TWO layers so it does not
// rest on any single line:
//   (1) an OUTER fail-closed boundary: any reflection trap (a Proxy getPrototypeOf
//       / ownKeys trap that throws) or any unforeseen fault returns
//       pass:false/ready:false. This alone makes the sentence true.
//   (2) DESCRIPTOR-based field reads (Object.getOwnPropertyDescriptor +
//       Reflect.ownKeys): only OWN DATA properties are read; an accessor (getter)
//       is never invoked and is rejected as invalid; a Symbol key in verdicts is
//       seen by Reflect.ownKeys (Object.keys hides it) and rejected, so it cannot
//       bypass the string→count contract. No JSON.stringify / coercion of
//       untrusted values; only static, field-specific reasons.
// Returns { pass:boolean, ready:boolean, reasons:string[] }.
export function frameRegistryCanaryVerdict(summary) {
  try {
    return _frameRegistryCanaryVerdict(summary);
  } catch {
    // A reflection trap (throwing getPrototypeOf/ownKeys) is unreachable from an
    // honest frameRegistrySummary(), but the predicate is advertised TOTAL and is
    // the single rollout gate: an escape must fail closed, never propagate.
    return { pass: false, ready: false, reasons: ['invalid-summary: reflection fault (fail-closed)'] };
  }
}

function _frameRegistryCanaryVerdict(summary) {
  // isCount rejects bigint, string, NaN, Infinity, negative, and fractional —
  // typeof===number FIRST so a BigInt (typeof 'bigint') can never reach a throw.
  const isCount = (x) => typeof x === 'number' && Number.isSafeInteger(x) && x >= 0;
  // Own DATA-property read. Returns {value} for an own data property, {absent:true}
  // when there is no own property, {accessor:true} for a getter/setter (NEVER
  // invoked). getOwnPropertyDescriptor does not run accessors, so a throwing getter
  // is reported, not triggered (council F3, Aster/Vega).
  const ownData = (o, key) => {
    const d = Object.getOwnPropertyDescriptor(o, key);
    if (!d) return { absent: true };
    if (!('value' in d)) return { accessor: true };
    return { value: d.value };
  };
  // A plain counter record: prototype Object.prototype or null, not an Array.
  // getPrototypeOf on a Proxy can trap+throw — caught by the outer boundary.
  const isPlainRecord = (o) => {
    if (o === null || typeof o !== 'object' || Array.isArray(o)) return false;
    const p = Object.getPrototypeOf(o);
    return p === Object.prototype || p === null;
  };
  if (!isPlainRecord(summary)) {
    return { pass: false, ready: false, reasons: ['invalid-summary: not a plain object'] };
  }
  const reasons = [];
  const builtD = ownData(summary, 'built');
  const obsD = ownData(summary, 'observing');
  if (builtD.accessor || obsD.accessor) {
    return { pass: false, ready: false, reasons: ['invalid-summary: accessor field (built/observing)'] };
  }
  const built = builtD.value === true;
  const observing = obsD.value === true;   // runtime shadow gate is on
  if (!built)     reasons.push('not-armed (built!==true)');
  if (!observing) reasons.push('not-observing (shadow gate off — blind window)');
  // faults MUST be a present own-data valid count; accessor/absence/garbage rejected.
  const faultsD = ownData(summary, 'faults');
  if (faultsD.accessor)            reasons.push('invalid-summary: faults is an accessor');
  else if (!isCount(faultsD.value)) reasons.push('invalid-summary: faults not a non-negative safe integer');
  else if (faultsD.value > 0)       reasons.push(`faults=${faultsD.value}`);
  // verdicts MUST be an own-data plain record AND every OWN key a string mapping to
  // a valid count via an own data property — Reflect.ownKeys so a Symbol key cannot
  // slip past (council F3.2). Only then read threw/trace-fault.
  const vD = ownData(summary, 'verdicts');
  if (vD.accessor) {
    reasons.push('invalid-summary: verdicts is an accessor');
  } else if (!isPlainRecord(vD.value)) {
    reasons.push('invalid-summary: verdicts not a plain counter object');
  } else {
    const v = vD.value;
    let bad = false;
    for (const key of Reflect.ownKeys(v)) {
      if (typeof key === 'symbol') { bad = true; break; }   // Symbol key violates string→count shape
      const cD = ownData(v, key);
      if (cD.accessor || !isCount(cD.value)) { bad = true; break; }
    }
    if (bad) {
      reasons.push('invalid-summary: verdicts has a non-integer or non-string-keyed counter');
    } else {
      const threw = ownData(v, 'threw').value;             // validated count or absent(undefined)
      const traceFault = ownData(v, 'trace-fault').value;
      if (threw > 0)      reasons.push(`threw=${threw}`);
      if (traceFault > 0) reasons.push(`trace-fault=${traceFault}`);
    }
  }
  // COVERAGE (REF-1.1 M1c — Decision-1 synthesis, Aster + Vega). A window that
  // observed NO contracts must fail even with faults===0. Two own-data counts,
  // both fail-closed on accessor/absence/garbage:
  //   total>0        — LIVENESS. Frames actually flowed. Vega: the canary script
  //                    required this, the predicate did not — a consumer reading
  //                    only the predicate could pass an empty window. Now it can't.
  //   unobserved===0 — COVERAGE (covered===total). Every observed frame reached a
  //                    certified snapshot and exercised its contract. A frame that
  //                    took the unbranded-source no-op is counted 'unobserved' and
  //                    MUST fail. This is why splitting 'unbranded-source' out of
  //                    `faults` (M1c fold change) is safe: the coverage gate keeps
  //                    an all-unbranded window failing — F1 cannot be renamed. A
  //                    residual unbranded live frame after the mint lands still fails.
  const totalD = ownData(summary, 'total');
  if (totalD.accessor)             reasons.push('invalid-summary: total is an accessor');
  else if (!isCount(totalD.value)) reasons.push('invalid-summary: total not a non-negative safe integer');
  else if (totalD.value === 0)     reasons.push('no-traffic (total===0 — nothing observed)');
  const unobsD = ownData(summary, 'unobserved');
  if (unobsD.accessor)             reasons.push('invalid-summary: unobserved is an accessor');
  else if (!isCount(unobsD.value)) reasons.push('invalid-summary: unobserved not a non-negative safe integer');
  else if (unobsD.value > 0)       reasons.push(`uncovered (unobserved=${unobsD.value} — frames bypassed contract certification)`);
  const ready = built && observing;
  return { pass: ready && reasons.length === 0, ready, reasons };
}

export default ShadowRegistry;
