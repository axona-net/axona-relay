// =====================================================================
// tombstoneAuthWiring.js — REF-1.1 S2.0c Phase 3: SHADOW-MODE wiring of the
// accepted tombstoneAuth core (src/pubsub/tombstoneAuth.js) into AxonaManager.
//
// STATUS: DEFAULT-OFF, OBSERVE-ONLY. When the `tombstoneAuth` construction flag
// is set, AxonaManager builds ONE per-node TombstoneAuthority and this module
// feeds it the LOCALLY-VERIFIED body / kill stream, plus cache-eviction and
// role-teardown events, so its parallel deletion-state tracks what this node has
// actually verified and still holds. It NEVER mutates role state, the cache, the
// tombstones, the fanout, or app delivery — the legacy path stays the sole
// source of truth. Flag-OFF (default): the authority is null and every hook is a
// guarded no-op, byte-identical to today.
//
// AUTHORITY IS EARNED FROM LOCAL VERIFICATION (Aster Phase-3 review ec7a5a38).
// The accepted invariant is that a tombstone is authoritative ONLY from a
// locally B-4-verified body plus a locally verified signed kill; migrated/fanned
// markers are non-authoritative. So the observers are driven ONLY from the
// verified ingress points and are handed VERIFIED material:
//   - body  ← _ingestPublish / _ingestStamped, AFTER verifyEnvelope() passed,
//             and only if the entry SURVIVED the cache write (see the survived
//             guard at the call sites). The verified envelope's signerPubkey is
//             the publisher; we never parse an unverified JSON body for authority.
//   - kill  ← _onKill, AFTER verifyKill() passed, handed the signed kill object
//             and its verified signerPubkey; the kill's topicId is bound to this
//             topic before it can reach onKill(). A kill that arrives via a
//             PROPAGATION path (fanout _onDeliver / migrate _applyDels) is verified
//             + BOUND (topicId + msgId + signer) by _taVerifyBoundKill BEFORE it may
//             be observed OR retained/re-fanned on the live path (Aster blocker b) —
//             an UNSIGNED marker, a forged proof, or a proof naming a different
//             carrier is stripped and never observed or transported.
// Anything without local proof never becomes a candidate or a tombstone.
//
// TOPIC BINDING (Aster blocker a). A stamped body is bound to its role topic at the
// LIVE path (_ingestStamped requires deriveTopicId(body.topic)===role.topicId,
// mirroring _ingestPublish), and _taObserveBody re-derives it independently — so a
// cross-topic re-stamp cannot corrupt a role's history or seed a false co-location.
//
// SIGNED-KILL PROOF TRANSPORT (Aster blocker b, flag-gated so flag-off is byte-
// identical). When the authority is built, a propagated proof is verified + bound to
// its carrier (_taVerifyBoundKill) BEFORE _applyKill may retain it; only a bound proof
// is stored in the tombstone and carried by the fanout / replay / replicate / handoff /
// pull emitters, so every node holding the propagated tombstone can verifyKill() it
// locally (B1). A verified proof also UPGRADES an existing proof-less tombstone and
// re-converges downstream, so transport survives reordering / mixed-flag arrival (B2).
//
// CACHE FIDELITY (Aster Phase-3 review, class 2). The shadow body mirror must not
// retain a body the live cache no longer holds, or it would be a false
// co-location basis. So: body observation reflects the FINAL cache outcome
// (survived-guard); TTL and byte-cap evictions call _taObserveEvict; a role
// teardown purges that topic's shadow bodies (_taPurgeTopic); and resetState()
// rebuilds the authority (_taReset).
//
// INTERIM DEADLINE (marked, not hidden): the committed effectiveDeath is meant to
// come from the body's SIGNED exp. V1 envelopes carry none yet (that lands at the
// envelope V2 flag day), so the shadow derives an INTERIM, UNSIGNED death from
// the wire publishTs: publishTs + TTL_CEILING + CLOCK_SKEW. This exercises the
// store/expiry/capacity machinery WITHOUT the cold-verifiable immutability the V2
// identity provides. Replaced by the signed exp at one site when V2 ships.
//
// ENFORCEMENT (making the authority the suppression source of truth, pre-gate
// feeding, closing the del-fanout/migration trust gaps) is a SEPARATE later gate
// that also needs the signed exp, so it pairs with the V2 cutover.
//
// SAFETY: every observer is gated on `this._tombAuthority` and wrapped so it can
// never throw into the hot path — an internal error increments a counter and is
// swallowed, because a SHADOW must never be able to affect the live pipeline.
// =====================================================================

import { TombstoneAuthority, RELAY_CAPS, TTL_CEILING, CLOCK_SKEW } from './tombstoneAuth.js';
import { canonical, deriveTopicIdBig } from './post.js';
import { KILL_DOMAIN, verifyKill } from './kill.js';
import { idHex, idBig, lc } from './ids.js';

// Build the per-node authority + observation counters. Called from the
// AxonaManager constructor ONLY when the tombstoneAuth flag is set.
export function makeTombstoneAuthority(profile = RELAY_CAPS) {
  return {
    authority: new TombstoneAuthority(profile),
    stats: { bodies: 0, kills: 0, evicts: 0, reclaims: 0, purges: 0, resets: 0, errors: 0, skipped: 0, verdicts: {} },
  };
}

const bump = (m, k) => { m[k] = (m[k] || 0) + 1; };

export const tombstoneAuthWiringMethods = {
  // Interim, UNSIGNED committed death derived from the wire publishTs (see header).
  _taDeath(publishTs) { return publishTs + TTL_CEILING + CLOCK_SKEW; },

  // A LOCALLY-VERIFIED body just entered this node's cache and SURVIVED the write
  // (callers pass the verifyEnvelope()-verified `env` and gate on cache survival).
  // publisher = the verified envelope's signerPubkey (or null for anonymous). We
  // never derive authority by parsing an unverified JSON body.
  async _taObserveBody(topicBig, env, publishTs) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      if (!env || typeof env.msgId !== 'string') { ta.stats.skipped++; return; }
      // INDEPENDENT topic binding (Aster b188a223): the observer does NOT trust
      // the caller — it derives the body's SIGNED topic and requires it to equal
      // this role's topic before onBody. Otherwise a cross-topic (migrated-from-A)
      // body could seed a false co-location basis under B, since the V1 msgId is
      // topic-agnostic. Fail closed on any mismatch/malformed descriptor.
      const d = env.topic;
      let stid;
      try { stid = await deriveTopicIdBig({ region: d?.region, owner: d?.owner, name: d?.name, write: d?.write }); }
      catch { ta.stats.skipped++; return; }
      if (stid !== topicBig) { ta.stats.skipped++; return; }
      const publisher = env.signerPubkey ? lc(env.signerPubkey) : null;
      const topicId = idHex(topicBig);
      const v = ta.authority.onBody(topicId, env.msgId, publisher, this._taDeath(publishTs), null, this._now());
      ta.stats.bodies++; bump(ta.stats.verdicts, 'body:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // A LOCALLY-VERIFIED signed kill (caller ran verifyKill() and passes its
  // signerPubkey). The signed kill's topicId is bound to THIS topic before it can
  // reach onKill(): a missing signer, a non-string msgId, or a topicId that does
  // not resolve to this topic is skipped — never a candidate, never a tombstone.
  _taObserveKill(topicBig, kill, signerPubkey) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      if (!kill || !signerPubkey || typeof kill.msgId !== 'string') { ta.stats.skipped++; return; }
      let bound = false;
      try { bound = kill.topicId != null && idBig(kill.topicId) === topicBig; } catch { bound = false; }
      if (!bound) { ta.stats.skipped++; return; }          // mismatched/absent topic binding
      const topicId = idHex(topicBig);
      const signer  = lc(signerPubkey);
      // Faithful byte-accounting: size the record from the signed kill's canonical core.
      const killBytes = canonical({ d: KILL_DOMAIN, topicId: kill.topicId, msgId: kill.msgId, ts: kill.ts, seq: kill.seq });
      const v = ta.authority.onKill(topicId, kill.msgId, signer, killBytes, this._now());
      ta.stats.kills++; bump(ta.stats.verdicts, 'kill:' + String(v).split(':')[0]);
    } catch { ta.stats.errors++; }
  },

  // Verify + BIND a propagated proof to its carrier BEFORE it may be retained,
  // re-fanned, or observed (Aster Phase-3 blocker b, B1). A del marker arriving via a
  // PROPAGATION path (fanout _onDeliver, migrate _applyDels) may carry a COMPLETE
  // signed kill. The kill is trusted for THIS tombstone ONLY when it (1) passes
  // verifyKill() LOCALLY, (2) names this marker's target (kill.msgId===marker.msgId),
  // (3) binds to this topic (idBig(kill.topicId)===topicBig), and (4) its verified
  // signer matches the marker's advertised signer. On success it observes the proof
  // into the shadow authority and RETURNS the kill, so the live path may retain +
  // transport it. On any failure it returns null and the caller MUST strip
  // marker.kill — an unverified, forged, or cross-carrier proof is never retained or
  // re-fanned. An unsigned marker (no kill) returns null quietly (preserves D2). The
  // caller AWAITS this so verification completes before _applyKill retains anything.
  async _taVerifyBoundKill(topicBig, marker) {
    const ta = this._tombAuthority; if (!ta) return null;
    try {
      const k = marker && marker.kill;
      if (!k || typeof k.msgId !== 'string') return null;                  // no proof to bind
      if (k.msgId !== marker.msgId) { ta.stats.skipped++; return null; }   // proof must name the carrier's target
      let bound = false; try { bound = k.topicId != null && idBig(k.topicId) === topicBig; } catch { bound = false; }
      if (!bound) { ta.stats.skipped++; return null; }                     // proof bound to this topic
      const v = await verifyKill(k);                                       // LOCAL signature verify
      if (!v.ok) { ta.stats.skipped++; return null; }
      // The verified proof signer MUST be present in AND match the carrier (Aster S1):
      // a proof carried with a missing or conflicting signer is stripped, never retained
      // or re-emitted. Our own emitters always stamp the carrier signer FROM the verified
      // proof (see _applyKill), so a consistent proof-bearing marker always passes here.
      if (marker.signer == null || lc(marker.signer) !== lc(v.signerPubkey)) { ta.stats.skipped++; return null; }
      this._taObserveKill(topicBig, k, v.signerPubkey);                    // shadow earns the verified, bound proof
      return k;                                                            // live path may now retain + transport it
    } catch { ta.stats.errors++; return null; }
  },

  // A cache entry aged out / was byte-capped (the _cachePush + _expireCache
  // funnels). Keep the shadow body mirror in step with the live cache.
  _taObserveEvict(role, msgId) {
    const ta = this._tombAuthority; if (!ta || !role) return;
    try { ta.authority.evictBody(idHex(role.topicId), msgId); ta.stats.evicts++; } catch { ta.stats.errors++; }
  },

  // Periodic reclamation + deferred-candidate retry (driven off the expiry tick).
  _taReclaim() {
    const ta = this._tombAuthority; if (!ta) return;
    try { ta.authority.reclaimAndRetry(this._now()); ta.stats.reclaims++; } catch { ta.stats.errors++; }
  },

  // A role was torn down (empty-role teardown / graceful leave): the node no
  // longer holds this topic's bodies, so purge their shadow co-location basis.
  _taPurgeTopic(topicBig) {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      const prefix = idHex(topicBig) + '|';
      for (const k of [...ta.authority.bodies.map.keys()]) if (k.startsWith(prefix)) ta.authority.bodies.evict(k);
      ta.stats.purges++;
    } catch { ta.stats.errors++; }
  },

  // resetState(): the node dropped ALL roles — rebuild the shadow authority so no
  // stale body/candidate/tombstone survives to seed a later false co-location.
  // Cumulative observation counters carry over (they are a lifetime tally).
  _taReset() {
    const ta = this._tombAuthority; if (!ta) return;
    try {
      const fresh = makeTombstoneAuthority(ta.authority.profile);
      fresh.stats = ta.stats; fresh.stats.resets++;
      this._tombAuthority = fresh;
    } catch { ta.stats.errors++; }
  },

  // Inspectable observation surface (tests + future telemetry). Reading it never
  // affects behavior. Flag-off returns { enabled:false }.
  tombstoneAuthShadow() {
    const ta = this._tombAuthority; if (!ta) return { enabled: false };
    const a = ta.authority;
    return {
      enabled: true,
      profile:  { tombMaxCount: a.tomb.maxCount, candMax: a.cand.max },
      stats:    { ...ta.stats, verdicts: { ...ta.stats.verdicts } },
      fx:       { ...a.fx },
      sizes:    { tombstones: a.tomb.map.size, candidates: a.cand.total, bodies: a.bodies.map.size },
    };
  },
};

export default tombstoneAuthWiringMethods;
