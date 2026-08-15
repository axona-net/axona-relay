// =====================================================================
// tombstoneAuth.js — REF-1.1 S2.0c-AUTH-B: provisional co-located
// tombstone authorization, the bounded deletion-state core.
//
// STATUS: PURE + UNWIRED. This module is the isolated state machine only.
// Nothing imports it yet; it is NOT exported from the public surface and it
// does NOT touch the wire, AxonaManager, or topicStore. Wiring is a later,
// separately-gated tranche (Phase 3). Building the core in isolation first is
// the same review shape the council accepted for S2.0a/b/c: land the fenced
// mechanism, test it to the plan, then wire it under its own gate.
//
// WHAT THIS IS (the accepted design, in one paragraph)
// Authoritative deletion is CO-LOCATED: a node may install a tombstone for a
// message only when it has itself B-4-verified the message body and the kill's
// `signerPubkey` equals that body's publisher. Authority is LOCAL and
// NON-TRANSFERABLE — never trusted from another node. Migration carries only
// the SIGNED kill as a bounded, non-authoritative candidate; the receiver
// re-earns authority by its own co-location check when the body arrives. The
// deletion state is two bounded stores (tombstones + candidates) plus the body
// cache; every store refuses admission at capacity and never evicts a live
// entry. `suppress()` is fail-closed on the committed deadline BEFORE any side
// effect, and commits {tombstone admit -> body remove -> fanout -> candidate
// purge} atomically.
//
// PROVENANCE: this is a faithful port of the Gate-A saturation reference model
// (axona-docs `REF-1.1-S2.0c-Tombstone-Saturation-Sim.mjs`, 23/23 behavioral,
// heap-measured), adapted to kernel ESM and the kernel's own `canonical()` so
// the byte accounting reflects real serialization (Aster's representation-
// fidelity requirement). Governing designs, all council-accepted:
//   - REF-1.1-S2.0c-Signed-Expiry-Design-v6.md  (committed effectiveDeath)
//   - REF-1.1-S2.0c-AUTH-B-Design-v8.md          (security model)
//   - REF-1.1-S2.0c-AUTH-B-Design-v12.md         (SUPPRESS fail-closed invariant)
//   - REF-1.1-S2.0c-Tombstone-Saturation-HostRuns.md (Gate A ACCEPTED caps)
// Gate B implementation test plan classes exercised at the module level:
//   A (commit-order/atomicity), C (retry precondition re-checks), E (capacity),
//   K (final-slot contention), L (receive-path dominance), M (crypto-prefilter
//   + local-receipt ClaimRetention), N (retry scheduling). Wire-dependent
//   classes (B, D wire integration, F, G, H golden vectors, J restart) land
//   with Phase 2 (signed-expiry) and Phase 3 (wiring).
//
// PURITY: every method that reasons about time takes `now` from the caller.
// No Date.now() here — deterministic and testable, and it lets the eventual
// call site pass a single consistent clock read per ingest.
// =====================================================================

import { canonical } from './post.js';
import { TTL_MS } from './constants.js';

const _enc = new TextEncoder();

// ---- AUTH-B capacity + lifetime constants (Gate A ACCEPTED sizing) ----------
// The committed-death horizon: a message's authorization can never outlive its
// signed `exp`, and `effectiveDeath = exp + CLOCK_SKEW` is used everywhere so
// there is no skew-gap between the freshness window and the death check.
export const TTL_CEILING          = TTL_MS;   // 24h absolute hold ceiling (constants.TTL_MS)
export const CLOCK_SKEW           = 5_000;    // symmetric clock-skew allowance (ms)
export const FUTURE_TOLERANCE_MS  = 30_000;   // live-ingress future window (ms)

// Per-record hard cap. Gate A measured a full canonical record at 725 B; 768 is
// that worst case plus margin. A record over this is refused (never truncated).
export const TOMBSTONE_RECORD_MAX = 768;
const SUBLIMIT_FRACTION           = 16;       // per-signer / per-topic sublimit = count / 16

// Cap PROFILES. Relay caps are Gate-A ACCEPTED and normative for the relay
// runtime (39.52 MiB worst-case = 62% of the 64 MiB budget, headroom preserved).
// The browser profile is measured (44.5% of 4 MiB at 512/128) but stays DISABLED
// and NON-NORMATIVE per Aster's Gate A disposition: enabling it requires a
// cross-origin-isolated measurement first. `enabled:false` is advisory metadata
// for the eventual wiring layer — this pure core simply builds to whatever caps
// it is handed.
export const RELAY_CAPS = Object.freeze({
  tombMaxCount: 32768,
  candMax:       8192,
  recordMax:    TOMBSTONE_RECORD_MAX,
  enabled:       true,
});
export const BROWSER_CAPS = Object.freeze({
  tombMaxCount:  512,
  candMax:       128,
  recordMax:    TOMBSTONE_RECORD_MAX,
  enabled:      false,   // DISABLED pending a COI measureUserAgentSpecificMemory() run
});

// ClaimRetention: how long a body-absent candidate is held before it is
// reclaimed. Derived from the LOCAL receipt time, never from attacker-supplied
// kill.ts (Gate B class M). Bounds the resource, not the authorization.
export const claimRetention = (localReceipt) =>
  localReceipt + FUTURE_TOLERANCE_MS + TTL_CEILING + CLOCK_SKEW;

// ---- canonical byte accounting (kernel serializer, faithful sizing) ---------
const canonicalBytes = (s) => _enc.encode(s).length;
const recBytesOf     = (key, rec) => canonicalBytes(key) + canonicalBytes(canonical(rec));

// A tombstone/candidate is keyed by (topicId, msgId). The topicId prefix lets
// the per-topic accounting recover the topic from the key without a second map.
export const authKey = (topicId, msgId) => topicId + '|' + msgId;
const topicOf        = (k) => k.slice(0, k.indexOf('|'));

// =====================================================================
// BodyCache — bounded, insertion-ordered; auto-evicts the oldest on overflow
// and REPORTS the evicted key so the owner can demote that key's pending
// candidate (it just lost its co-location basis). This is the real body store
// the deletion state reasons against; overflow here is what drives Gate B's
// "body evicted before retry" demotion path.
// =====================================================================
export class BodyCache {
  constructor(max) { this.max = max; this.map = new Map(); }
  has(k) { return this.map.has(k); }
  get(k) { return this.map.get(k); }
  put(k, body) {                        // returns the evicted key, or null
    let evicted = null;
    if (!this.map.has(k) && this.map.size >= this.max) {
      evicted = this.map.keys().next().value;   // oldest
      this.map.delete(evicted);
    }
    this.map.set(k, body);
    return evicted;
  }
  evict(k) { return this.map.delete(k); }
}

// =====================================================================
// CandidateStore — holds BOTH plain body-absent candidates (a signed kill we
// cannot yet authorize) AND pending-capacity ones (a body-verified kill that a
// full tombstone store made us defer). Bounded on global count + bytes +
// per-signer + per-topic. Deduplicated by (key, signer). Expires at
// ClaimRetention. REFUSAL ONLY — it never evicts a non-expired entry.
// =====================================================================
export class CandidateStore {
  constructor({ max, maxBytes, perSignerMax, perTopicMax, recordMax }) {
    Object.assign(this, { max, maxBytes, perSignerMax, perTopicMax, recordMax });
    this.map       = new Map();   // key -> [ {signer, killBytes, tag, bodyArrivedAt, claimRet, _bytes} ]
    this.total     = 0;
    this.bytes     = 0;
    this.perSigner = new Map();
    this.perTopic  = new Map();
  }
  _cnt(m, k) { return m.get(k) || 0; }
  _inc(m, k) { m.set(k, (m.get(k) || 0) + 1); }
  _dec(m, k) { const c = m.get(k) - 1; if (c <= 0) m.delete(k); else m.set(k, c); }

  reclaimExpired(now) {
    let n = 0;
    for (const [k, list] of this.map) {
      for (let i = list.length - 1; i >= 0; i--) {
        if (now > list[i].claimRet) {
          const c = list[i];
          list.splice(i, 1);
          this.total--; this.bytes -= c._bytes;
          this._dec(this.perSigner, c.signer); this._dec(this.perTopic, topicOf(k));
          n++;
        }
      }
      if (!list.length) this.map.delete(k);
    }
    return n;
  }

  // Admit a candidate. `tag` is 'plain' (body-absent) or 'pending' (body-
  // verified, tombstone-slot-deferred). Dedup drops a repeated matching kill.
  admit(k, signer, killBytes, now, tag = 'plain', bodyArrivedAt = null) {
    this.reclaimExpired(now);
    const list = this.map.get(k) || [];
    if (list.some((c) => c.signer === signer)) return 'DUP';   // dedup repeated matching KILLs
    const _bytes = recBytesOf(k, { signer, killBytes });
    if (_bytes > this.recordMaxOr()) return 'REFUSED_RECORD_TOO_LARGE';
    if (this.total >= this.max)                                   return 'REFUSED_CAND_GLOBAL';
    if (this.bytes + _bytes > this.maxBytes)                      return 'REFUSED_CAND_BYTES';
    if (this._cnt(this.perSigner, signer) >= this.perSignerMax)   return 'REFUSED_CAND_SIGNER';
    if (this._cnt(this.perTopic, topicOf(k)) >= this.perTopicMax) return 'REFUSED_CAND_TOPIC';
    list.push({ signer, killBytes, tag, bodyArrivedAt, claimRet: claimRetention(now), _bytes });
    this.map.set(k, list);
    this.total++; this.bytes += _bytes;
    this._inc(this.perSigner, signer); this._inc(this.perTopic, topicOf(k));
    return 'ADMITTED';
  }
  // Record cap is carried on the store for locality; expose via a hook so a
  // profile with a different record cap can override without touching admit().
  recordMaxOr() { return this.recordMax ?? TOMBSTONE_RECORD_MAX; }

  get(k)          { return this.map.get(k) || []; }
  find(k, signer) { return this.get(k).find((c) => c.signer === signer); }
  setTag(k, signer, tag, bodyArrivedAt) {
    const c = this.find(k, signer);
    if (c) { c.tag = tag; c.bodyArrivedAt = bodyArrivedAt; }
    return !!c;
  }
  demoteKey(k) { for (const c of this.get(k)) { c.tag = 'plain'; c.bodyArrivedAt = null; } }   // body gone -> all revert
  remove(k, signer) {
    const list = this.map.get(k); if (!list) return false;
    const i = list.findIndex((c) => c.signer === signer); if (i < 0) return false;
    const c = list[i]; list.splice(i, 1);
    this.total--; this.bytes -= c._bytes;
    this._dec(this.perSigner, c.signer); this._dec(this.perTopic, topicOf(k));
    if (!list.length) this.map.delete(k);
    return true;
  }
  purge(k) {
    const list = this.map.get(k); if (!list) return 0;
    for (const c of list) {
      this.total--; this.bytes -= c._bytes;
      this._dec(this.perSigner, c.signer); this._dec(this.perTopic, topicOf(k));
    }
    this.map.delete(k);
    return list.length;
  }
  // Pending candidates, oldest body-arrival first — the retry scheduler serves
  // in this order so a long-waiting candidate is not starved (Gate B class N).
  pendingOldestFirst() {
    const out = [];
    for (const [k, list] of this.map) for (const c of list) if (c.tag === 'pending') out.push({ k, c });
    return out.sort((a, b) => a.c.bodyArrivedAt - b.c.bodyArrivedAt);
  }
}

// =====================================================================
// TombstoneStore — the AUTHORITATIVE deletions. Bounded count + bytes +
// per-signer + per-topic. `effectiveDeath` is the body's COMMITTED value
// (exp + CLOCK_SKEW), never recomputed from local now+TTL. Reclamation is
// minDeath-guarded so a full-store admit is O(1) amortized, not O(n).
// =====================================================================
export class TombstoneStore {
  constructor({ maxCount, maxBytes, perSignerMax, perTopicMax, recordMax }) {
    Object.assign(this, { maxCount, maxBytes, perSignerMax, perTopicMax, recordMax });
    this.map       = new Map();   // key -> { signerPubkey, effectiveDeath, killBytes, _bytes }
    this.bytes     = 0;
    this.perSigner = new Map();
    this.perTopic  = new Map();
    this.minDeath  = Infinity;    // earliest effectiveDeath held; guards reclaim
  }
  _dec(m, k) { const c = m.get(k) - 1; if (c <= 0) m.delete(k); else m.set(k, c); }
  _inc(m, k) { m.set(k, (m.get(k) || 0) + 1); }

  reclaimExpired(now) {
    if (now <= this.minDeath) return 0;      // nothing can be dead yet
    let n = 0, min = Infinity;
    for (const [k, rec] of this.map) {
      if (now > rec.effectiveDeath) {
        this.map.delete(k); this.bytes -= rec._bytes;
        this._dec(this.perSigner, rec.signerPubkey); this._dec(this.perTopic, topicOf(k));
        n++;
      } else if (rec.effectiveDeath < min) min = rec.effectiveDeath;
    }
    this.minDeath = min;
    return n;
  }
  wouldAdmit(topicId, k, rec) {
    const b = recBytesOf(k, rec);
    if (b > (this.recordMax ?? TOMBSTONE_RECORD_MAX))          return 'REFUSED_RECORD_TOO_LARGE';
    if (this.map.size >= this.maxCount)                        return 'REFUSED_GLOBAL_COUNT';
    if (this.bytes + b > this.maxBytes)                        return 'REFUSED_GLOBAL_BYTES';
    if ((this.perSigner.get(rec.signerPubkey) || 0) >= this.perSignerMax) return 'REFUSED_SIGNER';
    if ((this.perTopic.get(topicId) || 0) >= this.perTopicMax)            return 'REFUSED_TOPIC';
    return 'OK';
  }
  insert(topicId, k, rec) {
    const b = recBytesOf(k, rec); rec._bytes = b;
    this.map.set(k, rec); this.bytes += b;
    if (rec.effectiveDeath < this.minDeath) this.minDeath = rec.effectiveDeath;
    this._inc(this.perSigner, rec.signerPubkey); this._inc(this.perTopic, topicId);
  }
  has(k) { return this.map.has(k); }
  get(k) { return this.map.get(k); }
}

// Build the store caps for a profile. Per-signer/per-topic sublimits are the
// count divided by SUBLIMIT_FRACTION so no single signer or topic can occupy
// the whole store.
function tombCapsFor(profile) {
  return {
    maxCount:     profile.tombMaxCount,
    maxBytes:     profile.tombMaxCount * profile.recordMax,
    perSignerMax: Math.max(1, Math.floor(profile.tombMaxCount / SUBLIMIT_FRACTION)),
    perTopicMax:  Math.max(1, Math.floor(profile.tombMaxCount / SUBLIMIT_FRACTION)),
    recordMax:    profile.recordMax,
  };
}
function candCapsFor(profile) {
  return {
    max:          profile.candMax,
    maxBytes:     profile.candMax * profile.recordMax,
    perSignerMax: Math.max(1, Math.floor(profile.candMax / SUBLIMIT_FRACTION)),
    perTopicMax:  Math.max(1, Math.floor(profile.candMax / SUBLIMIT_FRACTION)),
    recordMax:    profile.recordMax,
  };
}

// =====================================================================
// TombstoneAuthority — the deletion state machine over the three stores.
// `suppress()` is the local commit boundary: fail-closed on the committed
// deadline first, then an all-or-nothing {admit -> remove body -> fanout ->
// purge}. Network effects (fanout) are counted here but performed by the
// caller in the wired layer; within this pure core `fanout` is just the
// post-commit marker the wiring hooks onto.
// =====================================================================
export class TombstoneAuthority {
  constructor(profile = RELAY_CAPS, { bodyMax = 1024 } = {}) {
    this.profile = profile;
    this.tomb    = new TombstoneStore(tombCapsFor(profile));
    this.cand    = new CandidateStore(candCapsFor(profile));
    this.bodies  = new BodyCache(bodyMax);
    // Observable effects — the oracle surface the tests and the wiring read.
    this.fx = {
      cacheRemovals: 0, fanouts: 0, candidatePurges: 0,
      liveEvictions: 0, suppressions: 0, demotions: 0,
    };
  }

  // SUPPRESS — atomic, fail-closed on the COMMITTED effectiveDeath FIRST (v12).
  // An already-expired authorization can never suppress a body or install an
  // expired tombstone on ANY path (direct KILL, late body, retry).
  suppress(topicId, msgId, signer, killBytes, effectiveDeath, now) {
    const k = authKey(topicId, msgId);
    this.tomb.reclaimExpired(now);                            // drop any expired tombstone for this key first
    // An already-committed authoritative deletion is idempotent: an identical
    // authoritative kill (same signer) is CONFIRMATION with ZERO side effects; a
    // signer mismatch fails closed and never overwrites the authoritative record.
    const existing = this.tomb.get(k);
    if (existing) return existing.signerPubkey === signer ? 'CONFIRMED' : 'REFUSED_MISMATCH_TOMB';
    if (now > effectiveDeath) return 'REFUSED_EXPIRED';       // fail-closed on the committed deadline, before any side effect
    const rec = { signerPubkey: signer, effectiveDeath, killBytes };
    const v = this.tomb.wouldAdmit(topicId, k, rec);
    if (v !== 'OK') return v;                                  // capacity refusal, no side effect
    // ---- commit point: all-or-nothing from here ----
    this.tomb.insert(topicId, k, rec);
    if (this.bodies.evict(k)) this.fx.cacheRemovals++;
    this.fx.fanouts++;                                         // post-commit idempotent network effect
    if (this.cand.purge(k) > 0) this.fx.candidatePurges++;
    this.fx.suppressions++;
    return 'SUPPRESSED';
  }

  // Body arrival into the cache. Auto-eviction of an older body demotes that
  // key's pending candidate — it just lost its co-location basis.
  _putBody(k, body) {
    const ev = this.bodies.put(k, body);
    if (ev) { this.fx.liveEvictions++; this.cand.demoteKey(ev); this.fx.demotions++; }
  }

  // A signed, signature-verified kill arrives (caller has run verifyKill).
  onKill(topicId, msgId, signer, killBytes, now) {
    const k = authKey(topicId, msgId);
    this.tomb.reclaimExpired(now);                            // an expired tombstone must not confirm
    const tomb = this.tomb.get(k);
    if (tomb) return tomb.signerPubkey === signer ? 'CONFIRMED' : 'DROP_MISMATCH_TOMB';
    if (this.bodies.has(k)) {
      const body = this.bodies.get(k);
      if (body.publisher === signer) {                        // co-located authority
        const r = this.suppress(topicId, msgId, signer, killBytes, body.effectiveDeath, now);
        if (r === 'SUPPRESSED')       return 'SUPPRESSED';
        if (r === 'REFUSED_EXPIRED')  return 'DROP_EXPIRED';   // committed death passed: drop, no pending
        // tombstone store full: retain the body-verified matching kill, bounded
        const a = this.cand.admit(k, signer, killBytes, now, 'pending', body.arrivedAt);
        return 'PENDING_CAPACITY:' + r + '/' + a;
      }
      return 'DROP_NONAUTHOR_BODYPRESENT';                    // author mismatch, body present -> drop
    }
    // body absent: cannot yet authorize -> bounded, non-authoritative candidate
    return this.cand.admit(k, signer, killBytes, now);
  }

  // A message body arrives (caller has B-4-verified it). `effectiveDeath` is the
  // body's COMMITTED value. If a matching candidate kill is present, authorize.
  onBody(topicId, msgId, publisher, effectiveDeath, killBytesIfPending, now) {
    const k = authKey(topicId, msgId);
    // Receive-path dominance (Gate B class L): while a tombstone is live, any
    // body for its msgId is DROPPED — not delivered, not re-cached. The msgId
    // uniquely commits (publisher, message, topicId, exp), so this body IS the
    // killed message (or a forgery that cannot recompute the msgId). Once the
    // tombstone passes its committed death it no longer dominates, and the
    // signed-expiry layer rejects the now-past-death body on its own.
    this.tomb.reclaimExpired(now);
    if (this.isSuppressed(topicId, msgId, now)) return 'SUPPRESSED_TOMBSTONE';
    // Fail-closed BEFORE any side effect: a body already past its committed death
    // is dropped WITHOUT caching, so no expired body ever becomes a co-location
    // basis. Any pre-existing bounded candidate for this key is left untouched —
    // it reclaims on its own ClaimRetention schedule.
    if (now > effectiveDeath) return 'DROP_EXPIRED';
    this._putBody(k, { publisher, effectiveDeath, arrivedAt: now });
    const match = this.cand.find(k, publisher);
    if (match) {
      const r = this.suppress(topicId, msgId, publisher, match.killBytes, effectiveDeath, now);
      if (r === 'SUPPRESSED' || r === 'CONFIRMED') return 'SUPPRESSED';  // committed authoritative deletion
      if (r === 'REFUSED_EXPIRED') return 'DROP_EXPIRED';       // (defensive: deadline already checked above)
      this.cand.setTag(k, publisher, 'pending', now);           // stays bounded in cand store
      return 'PENDING_CAPACITY:' + r;
    }
    // no matching kill: any non-author candidates for this key are provably
    // inert (msgId uniquely commits the publisher) -> purge and deliver.
    if (this.cand.get(k).length) { if (this.cand.purge(k) > 0) this.fx.candidatePurges++; }
    return 'DELIVERED';
  }

  // Body eviction from outside (e.g. TTL) demotes the key's pending candidate.
  evictBody(topicId, msgId) {
    const k = authKey(topicId, msgId);
    const had = this.bodies.evict(k);
    this.cand.demoteKey(k);
    if (had) this.fx.demotions++;
    return had;
  }

  // Reclamation + retry: free expired tombstones/candidates, then re-attempt
  // deferred pending candidates oldest-body-first. Each retry RE-CHECKS body
  // present + author match + committed deadline before committing (Gate B C).
  reclaimAndRetry(now) {
    this.tomb.reclaimExpired(now);
    this.cand.reclaimExpired(now);
    for (const { k, c } of this.cand.pendingOldestFirst()) {
      const body = this.bodies.get(k);
      const ok = body && body.publisher === c.signer && now <= body.effectiveDeath;
      if (ok) {
        const [t, m] = k.split('|');
        if (this.suppress(t, m, c.signer, c.killBytes, body.effectiveDeath, now) === 'SUPPRESSED') {
          this.cand.remove(k, c.signer);
        }
      }
    }
  }

  // Is this (topicId, msgId) currently suppressed? Receive-path dominance (L):
  // while a tombstone is live, the caller must not deliver a body for its msgId.
  isSuppressed(topicId, msgId, now) {
    const k = authKey(topicId, msgId);
    const rec = this.tomb.get(k);
    if (!rec) return false;
    if (now > rec.effectiveDeath) return false;   // past committed death -> no longer suppressed
    return true;
  }
}
