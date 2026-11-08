// topicStore.js — the TOPIC STORE (refactor Phase 2).
//
// One implementation of per-topic storage semantics: the bounded cache
// (count+byte caps, TTL expiry), the tombstone set and its authorization
// rule, the migrated-deletion wire shape (every cache migration carries
// dels — invariant I-8), the exactly-once app-delivery LRU, and the
// since-floor bookkeeping. Methods are mixed into AxonaManager.prototype
// (`this` is the manager); state lives on the manager façade.

import {
  T, RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS, ROOT_CLAIM_MS,
  ROOT_REPLICAS, BACKUP_EVICT_MS, CACHE_MAX, CACHE_BYTES, MAX_DIRECT,
  DELEGATE_BATCH, MAX_VIA, VIA_HOP_BUDGET, TTL_MS, APP_DEDUP_MAX,
  PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES, COLD_BURST_TRIES,
  COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES, COLD_BURST_SLOW_INTERVAL_MS,
  COLD_PEER_THRESHOLD, FIRST_PUBLISH_RESEND_MS, REPLAY_CHUNK_BYTES,
  FUTURE_TOLERANCE_MS, BEACON_MS, BEACON_TTL_MS, BEACON_SEEN_MS,
  ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH, METRICS_LEASE_MS,
  METRICS_PUB_MS, METRICS_COALESCE_MS,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { isMetricTopicName } from './metrics.js';
import { verifyKill } from './kill.js';
import { canonical } from './post.js';

// 4.131.0: is author stamp (ts, seq) strictly AFTER (bTs, bSeq) on ONE author's
// clock? AxonaPeer draws pub and kill seq from one monotone _nextPubSeq while ts
// stays Date.now(), so a same-millisecond pair is ordered by seq (Orion RAK-3).
export function authorAfter(aTs, aSeq, bTs, bSeq) {
  if (!Number.isFinite(aTs) || !Number.isFinite(bTs)) return false;
  if (aTs !== bTs) return aTs > bTs;
  return Number.isFinite(aSeq) && Number.isFinite(bSeq) && bSeq > 0 && aSeq > bSeq;
}

export const topicStoreMethods = {
  // ── cache ────────────────────────────────────────────────────────────
  // RETURNS true when the entry committed, false when refused at the cut
  // (dual-root packet 4(d)): `cut` is the caller's entry-time capture
  // ({role0, gen0}, _cutCapture) and the check runs here, synchronously,
  // immediately before the write; a gated role refuses with or without a
  // capture. Callers that pass no cut (the relay DELIVER path) are checked
  // against the gate alone.
  _cachePush(role, entry, cut = null) {
    if (!this._cutHolds(role, cut)) {
      this._log('info', 'cut-refused', { topic: idHex(role.topicId).slice(0, 12), at: 'cachePush', msgId: String(entry?.msgId).slice(0, 12), gated: !!role.merge?.gate, gen: role.gen | 0, gen0: cut ? cut.gen0 : null });
      return false;
    }
    // LATEST-ONLY (4.119.0): a metric topic carries one live value; every older
    // snapshot is superseded the moment a newer one exists. A body older than the
    // newest held is not cached at all (the cache is in ARRIVAL order, so a late
    // replay of an old snapshot must not displace a newer one).
    if (role.latestOnly === true) {
      let newest = null;
      for (const e of role.cache) if (!newest || e.publishTs > newest.publishTs) newest = e;
      if (newest && (entry.publishTs ?? 0) <= newest.publishTs) return true;   // superseded on arrival
    }
    entry.bytes = (entry.json ? entry.json.length : 0) + 80;
    role.cache.push(entry);
    role.cacheIds.add(entry.msgId);
    role.cacheBytes += entry.bytes;
    // ADVISORY throughput: one distinct message just entered the cache. Callers
    // gate on cacheIds (dedup) before _cachePush, so this counts each message
    // once per holder; monotonic (eviction below never decrements it). Surfaced
    // as metrics().publishes, maxed across the cohort.
    role.publishes = (role.publishes || 0) + 1;
    // PINS (dual-root 4(d)): a pinned entry is never evicted by the caps. The
    // eviction loop takes the OLDEST UNPINNED entry; with nothing unpinned it
    // stops (the admission arithmetic guarantees pins plus reserve fit).
    const pins = role.merge?.pins;
    let guard = role.cache.length + 1;
    while ((role.cache.length > this._cacheMax || role.cacheBytes > this._cacheBytes) && guard-- > 0) {
      let i = 0;
      if (pins && pins.size) { while (i < role.cache.length && pins.has(role.cache[i].msgId)) i++; if (i >= role.cache.length) break; }
      const old = role.cache.splice(i, 1)[0];
      if (!old) break;
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
      if (this._tombAuthority) this._taObserveEvict(role, old.msgId);   // Phase 3 shadow: keep body mirror in step (no-op flag-off)
    }
    // LATEST-ONLY: the entry just pushed is the newest held (checked above), so
    // every other UNPINNED entry goes. Pins (dual-root 4(d)) are exempt as they
    // are from the caps; this also trims history held from before the role knew
    // it was a metric topic.
    if (role.latestOnly === true && role.cache.length > 1) {
      for (let i = role.cache.length - 2; i >= 0; i--) {
        const old = role.cache[i];
        if (pins && pins.has(old.msgId)) continue;
        role.cache.splice(i, 1);
        role.cacheIds.delete(old.msgId);
        role.cacheBytes -= old.bytes;
        if (this._tombAuthority) this._taObserveEvict(role, old.msgId);
      }
    }
    // NOTE: the body OBSERVE is NOT here — a cache write is not a local
    // verification, and _cachePush is also reached from the relay _onDeliver path
    // (unverified). Body observation is driven from the verified ingress
    // (_ingestPublish / _ingestStamped) with a cache-survival guard.
    return true;
  },

  // Decide ONCE per role whether it holds a metric topic (axona:metric:<id>),
  // from a descriptor already BOUND to role.topicId by the caller (the topic id
  // is a hash of the name, so a verified binding proves the name). Never decided
  // from an unbound body: a forged metric name on an ordinary topic would
  // otherwise erase that topic's history at this holder.
  _noteMetricRole(role, desc) {
    if (!role || role.latestOnly !== undefined) return;
    role.latestOnly = isMetricTopicName(desc?.name);
  },

  _expireCache(role, now) {
    // PINS (dual-root 4(d)): pinned entries are exempt from expiry for the
    // round's life, whatever this node's clock says; the cache is ordered by
    // stamp, so a pinned head is skipped and expiry continues behind it.
    const pins = role.merge?.pins;
    let i = 0;
    while (i < role.cache.length && (now - role.cache[i].publishTs) > TTL_MS) {
      if (pins && pins.has(role.cache[i].msgId)) { i++; continue; }
      const old = role.cache.splice(i, 1)[0];
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
      if (this._tombAuthority) this._taObserveEvict(role, old.msgId);   // Phase 3 shadow (no-op flag-off)
    }
    if (this._tombAuthority) this._taReclaim();                          // Phase 3 shadow: reclaim + retry (no-op flag-off)
  },

  // Tombstone expiry (from refreshTick). PINS (dual-root 4(d)): a pinned
  // tombstone — a manifest entry at W for a round's life, or a loser's
  // pinned export — is exempt until the pin releases.
  _expireTombstones(role, now) {
    const pins = role.merge?.pins;
    for (const [msgId, t] of role.tombstones) {
      if ((t?.exp ?? 0) <= now && !(pins && pins.has(msgId))) role.tombstones.delete(msgId);
    }
    if (role.superseded) for (const [msgId, s] of role.superseded) if ((s?.exp ?? 0) <= now) role.superseded.delete(msgId);   // 4.131.0: swept with the tombstones (Orion e47eb97c)
  },

  // My cache high-water = the newest stamp I hold (or have emitted, as root).
  _highWater(role) {
    return Math.max(role.lastTs || 0, role.cache.length ? role.cache[role.cache.length - 1].publishTs : 0);
  },

  // My cache low-water = the OLDEST stamp I hold; 0 when empty. Advertised on
  // SUB beside hw so a root can see a child holding strictly-OLDER history —
  // the post-transition split half that sits below the root's hw and is
  // invisible to the hw rule (the cold-attach "exactly half the timeline"
  // replay class).
  _lowWater(role) {
    return role.cache.length ? role.cache[0].publishTs : 0;
  },

  // A target message arriving (publish / replay / fan-out) for which we hold a
  // tombstone: SUPPRESS it iff the kill was authorized by the message's own author
  // — this is where a PROVISIONAL kill (accepted before we held the target) is
  // enforced. A signer MISMATCH (or an unsigned/anonymous target a signed kill
  // can't own) means the kill was forged or unauthorized → REVOKE the tombstone
  // and accept the message. Returns true to drop the message.
  _tombstoned(role, msgId, json, stampTs = null) {
    const v = this._tombstoneVerdict(role, msgId, json, stampTs, { stampTrusted: true });   // legacy direct callers (fences) vouch for the stamp
    if (v && v.kind !== 'suppress') this._tombstoneCommit(role, msgId, v);
    return v?.kind === 'suppress';
  },

  // 4.131.0 (Aster 61bad8a8 R1/R2): the DECISION, with no side effect. Returns
  // null (no tombstone), {kind:'suppress'}, {kind:'revoke'} (signer mismatch:
  // the kill was forged/unauthorized) or {kind:'supersede'} (a same-author
  // republication newer than the kill). Callers commit a revoke/supersede with
  // _tombstoneCommit ONLY once the body is admitted and cached, and on an
  // unverified path only after verifyEnvelope: a refused or forged write must
  // leave the tombstone standing.
  //
  // Supersede compares two stamps from ONE clock:
  //   a root-stamped copy (replay, fan-out): its publishTs against killTs,
  //     both from the root's monotone stamp. A pre-kill copy is lower.
  //   a fresh write at the root (no stamp yet): the envelope (ts, seq) against
  //     the kill's own signed (ts, seq), both from the author's clock.
  // Anything else (no stamp, no author kill ts) stays dead.
  _tombstoneVerdict(role, msgId, json, stampTs = null, { stampTrusted = false } = {}) {
    if (!role) return null;
    const tomb = role.tombstones.get(msgId);
    if (!tomb) return null;
    if (!tomb.signer) return { kind: 'suppress', tomb };              // signer-less tombstone (defensive)
    let author = null, sentTs = null, sentSeq = null;
    try { const e = JSON.parse(json); author = e?.signerPubkey ?? null; sentTs = e?.ts; sentSeq = e?.seq; } catch { /* */ }
    // 4.131.0 (Orion KP-1 vs the S2 upgrade): a tombstone keeps the signer it was
    // installed with and records verified kill proofs from OTHER signers as claims.
    // Only the body proves authorship (msgId binds the publisher), so the body
    // decides: its author must be the tombstone's signer or a verified claimant.
    // Neither a foreign valid kill nor a poisoned carrier signer can re-attribute it.
    const claim = (author && lc(author) !== lc(tomb.signer)) ? tomb.claims?.get(lc(author)) : null;
    if (!author || (lc(author) !== lc(tomb.signer) && !claim)) return { kind: 'revoke', tomb };
    // The kill's own signed (ts, seq), where this node knows it: authorTs at the
    // root that verified it, or a retained verified proof. A stamp is the
    // sender's word (DELIVER, REPLAYUP carry an unsigned publishTs), so where
    // the author's kill time is known a copy must ALSO be after it on the
    // author's clock: a validly signed pre-kill body replayed with a forged
    // stamp stays dead (Orion e47eb97c).
    const aTs = claim ? claim.ts : (Number.isFinite(tomb.authorTs) ? tomb.authorTs : (Number.isFinite(tomb.kill?.ts) ? tomb.kill.ts : null));
    const aSeq = claim ? claim.seq : (Number.isFinite(tomb.authorTs) ? tomb.authorSeq : tomb.kill?.seq);
    let after = false;
    // Where the author's kill time is NOT known here (every node but the verifying
    // root, while tombstoneAuth proof carriage is off by default), a stamp may
    // lift the kill only when the CALLER vouches for it: an accepted
    // current-generation DELIVER from this node's upstream (armed). REPLAYUP,
    // REPLICATE, handoff and unarmed DELIVER re-send old data and keep the
    // tombstone, as 4.130.0 did (Aster d165d9ce: a promoted backup must not
    // resurrect a pre-kill body on a claimed stamp).
    if (Number.isFinite(stampTs)) after = Number.isFinite(tomb.killTs) && stampTs > tomb.killTs && (aTs === null ? stampTrusted === true : authorAfter(sentTs, sentSeq, aTs, aSeq));
    else if (aTs !== null) after = authorAfter(sentTs, sentSeq, aTs, aSeq);
    if (after) return { kind: 'supersede', tomb, stampTs, sentTs, aTs, aSeq, author: lc(author) };
    // Orion P2: a proof-less node that refuses a NEWER stamp it cannot vouch for
    // did not "hold" that body; say so, so a transfer's accounting stays honest.
    const unvouchedNewer = aTs === null && Number.isFinite(stampTs) && Number.isFinite(tomb.killTs) && stampTs > tomb.killTs;
    const base = { kind: 'suppress', tomb, ...(claim ? { viaClaim: claim, author: lc(author), stampTs } : {}) };
    return unvouchedNewer ? { ...base, unvouchedNewer: true } : base;
  },

  // Orion CL-4: a VERIFIED body suppressed through a claim has proved its author
  // owns that claim. Promote it: the claimant becomes the tombstone's signer and
  // its kill the tombstone's proof; the other claims are cleared (the author is
  // now known), which also frees a cap a Sybil flood filled.
  _promoteClaim(role, msgId, v) {
    if (!role || !v?.viaClaim || role.tombstones.get(msgId) !== v.tomb) return false;
    const tomb = v.tomb;
    tomb.signer = v.author; tomb.kill = v.viaClaim; tomb.claims = new Map();
    delete tomb.authorTs; delete tomb.authorSeq;
    // Orion CL-5a: the promoted kill must reach nodes holding the body, so its
    // stamp moves past that body (a root takes a fresh stamp and seq slot; a
    // non-root moves past the stamp it was shown) and it is re-fanned.
    if (role.isRoot) { const ts = Math.max(role.lastTs + 1, this._now(), Number.isFinite(v.stampTs) ? v.stampTs + 1 : 0); role.lastTs = ts; tomb.killTs = ts; tomb.seq = ++role.seq; }
    else if (Number.isFinite(v.stampTs)) tomb.killTs = Math.max(tomb.killTs ?? 0, v.stampTs + 1);
    this._log?.('info', 'tombstone-claim-promoted', { msgId: String(msgId).slice(0, 12), killTs: tomb.killTs });
    try {
      this._fanout?.(role, { del: true, msgId, killTs: tomb.killTs, signer: tomb.signer, publishTs: tomb.killTs, seq: tomb.seq, kill: tomb.kill }, null);
      if (role.isRoot && this._rootReplicas) { const bridge = (typeof this.dht?.bridgeId === 'function') ? this.dht.bridgeId() : null; this._replicateRole?.(role.topicId, role, bridge, this._now())?.catch?.(() => {}); }
    } catch { /* fan-out is best-effort; the renewal replay re-sends every live tombstone */ }
    return true;
  },

  // Commit a verdict. Re-checks that the tombstone is the one the verdict saw.
  _tombstoneCommit(role, msgId, v) {
    if (!role || !v || role.tombstones.get(msgId) !== v.tomb) return false;
    if (v.kind === 'revoke') { role.tombstones.delete(msgId); return true; }
    if (v.kind !== 'supersede') return false;
    const tomb = v.tomb;
    // Remember the superseded kill's signed (ts, seq) so a RE-SENT copy of that
    // kill cannot retract the republication (Orion RAK-1a). Kill against kill,
    // one author clock; bounded by the tombstone's own expiry.
    // Orion P4: the author's kill time comes from authorTs OR a retained verified
    // proof, exactly as _tombstoneVerdict reads it.
    const aTs = Number.isFinite(v.aTs) ? v.aTs : (Number.isFinite(tomb.authorTs) ? tomb.authorTs : (Number.isFinite(tomb.kill?.ts) ? tomb.kill.ts : null));
    const aSeq = Number.isFinite(v.aTs) ? v.aSeq : (Number.isFinite(tomb.authorTs) ? tomb.authorSeq : tomb.kill?.seq);
    if (aTs !== null) {
      const sup = (role.superseded ??= new Map());
      sup.delete(msgId);
      sup.set(msgId, { authorTs: aTs, authorSeq: aSeq, exp: tomb.exp, ...(v.author ? { signer: v.author } : {}) });   // Orion CL-3: whose republication superseded it
      if (sup.size > 4096) sup.delete(sup.keys().next().value);
    }
    role.tombstones.delete(msgId);
    this._log?.('info', 'tombstone-superseded', { msgId: String(msgId).slice(0, 12), stampTs: v.stampTs, sentTs: v.sentTs, killTs: tomb.killTs, authorTs: tomb.authorTs ?? null });
    return true;
  },

  // ── singleton-root replication (warm backup roots) ───────────────────
  // The N reachable neighbours XOR-closest to a topic — its natural successors and
  // therefore the right backup roots (whoever routing re-converges on if the root
  // leaves). Excludes self and the bridge (signaling infra, never a root).
  // The active (non-expired) tombstones as a wire-shaped `dels` array. Every cache
  // MIGRATION (replicate, pull-up/replay-up, graceful handoff) must carry these
  // alongside `msgs`: a node that adopts cached history without the matching tombstones
  // would resurrect a killed message and serve it to a late joiner (the kill-leak class).
  // 4.131.0 SIGNED-KILL PROOF CARRIAGE: verify + BIND a propagated proof to its
  // carrier before anything may retain or re-fan it. Bound = the proof names the
  // carrier's msgId, this topic, and a signer equal to the carrier's. A proof whose
  // canonical bytes equal the one this node already holds was verified when it was
  // retained, so a renewal re-send costs no signature check; any altered field
  // (ts, seq, msgId...) changes the bytes and is verified afresh. Returns the
  // proof, or null (the caller strips it). Observes into the shadow if built.
  async _verifyKillProof(role, topicBig, marker) {
    try {
      const k = marker && marker.kill;
      if (!k || typeof k.msgId !== 'string' || k.msgId !== marker.msgId) return null;
      let bound = false; try { bound = k.topicId != null && idBig(k.topicId) === topicBig; } catch { bound = false; }
      if (!bound || marker.signer == null) return null;
      // A proof is bound to ITS carrier signer here. Whether it becomes the
      // tombstone's proof or only a claim is _applyKill's decision (Orion KP-1):
      // a foreign signer's valid kill never re-attributes a tombstone.
      const cur = role?.tombstones?.get(marker.msgId);
      const held = (cur?.kill && lc(cur.kill.signerPubkey) === lc(marker.signer)) ? cur.kill : cur?.claims?.get(lc(marker.signer));
      if (held && canonical(held) === canonical(k)) return lc(marker.signer) === lc(held.signerPubkey) ? held : null;   // Aster fbfd8256: the shortcut binds the carrier signer too
      const v = await verifyKill(k);
      if (!v.ok || lc(marker.signer) !== lc(v.signerPubkey)) return null;
      if (this._tombAuthority) { try { this._taObserveKill(topicBig, k, v.signerPubkey); } catch { /* shadow never throws into the hot path */ } }
      return k;
    } catch { return null; }
  },

  // Verify the claims a marker carries: each a separate signed kill of THIS msgId
  // on THIS topic, its own signer. Returns the verified subset (cap 4).
  async _verifyKillClaims(role, topicBig, marker) {
    const out = [];
    for (const k of (Array.isArray(marker?.claims) ? marker.claims.slice(0, 4) : [])) {
      const v = await this._verifyKillProof(role, topicBig, { msgId: marker.msgId, signer: k?.signerPubkey ?? null, kill: k });
      if (v) out.push(v);
    }
    return out;
  },

  _activeDels(role) {
    const now = this._now();
    const dels = [];
    for (const [tgt, tomb] of (role?.tombstones ?? [])) {
      if ((tomb?.exp ?? 0) > now) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, seq: tomb.seq, publishTs: tomb.killTs, ...(this._killProofs && tomb.kill ? { kill: tomb.kill } : {}), ...(this._killProofs && tomb.claims?.size ? { claims: [...tomb.claims.values()] } : {}) });
    }
    return dels;
  },

  // Apply a batch of migrated tombstones BEFORE ingesting the migrated bodies, so a
  // killed message in the same batch is suppressed (not briefly fanned/delivered then
  // retracted). Shared by the replay-up and handoff receive paths.
  // RETURNS true when every entry committed, false at the FIRST refusal at the
  // cut (dual-root packet 4(d)): the transfer's entry-time `cut` is passed to
  // every _applyKill and nothing is captured after the proof await; a refusal
  // stops the batch so no later entry commits behind the export.
  async _applyDels(role, topicBig, dels, cut = null, genTag = null) {
    for (const d of (Array.isArray(dels) ? dels : [])) {
      if (d && d.msgId) {
        // Verify + BIND any propagated proof (topicId+msgId+signer) BEFORE _applyKill
        // may retain or re-fan it (Aster Phase-3 blocker b, B1). A proof that fails
        // local verifyKill or does not bind is STRIPPED, so the tombstone migrates
        // WITHOUT an unverified/cross-carrier proof. Awaited so verification completes
        // before retention; a verified proof also observes into the shadow authority.
        let dd = d;
        if (this._killProofs && d.kill && !(await this._verifyKillProof(role, topicBig, d))) { const { kill, ...rest } = d; dd = rest; }
        else if (!this._killProofs && d.kill) { const { kill, ...rest } = d; dd = rest; }
        if (dd.claims) { const { claims, ...rest } = dd; dd = this._killProofs ? { ...rest, claims: await this._verifyKillClaims(role, topicBig, dd) } : rest; }
        if (this._applyKill(role, topicBig, dd, cut, genTag) === false) return false;
      }
    }
    return true;
  },

  // ── app delivery (exactly-once) ──────────────────────────────────────
  _deliverToApp(topicBig, json, msgId, publishTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const key = topicBig.toString(16) + ':' + msgId;
    // 4.131.0 (Orion RAK-2): exactly-once PER PUBLICATION. A republished body
    // (same msgId) whose root stamp is above the kill this app already saw is a
    // new publication: clear the kill record and deliver. A body at or below
    // that kill is a late pre-kill copy and stays dropped. Values are root
    // stamps; a legacy `true` carries none and keeps the old exactly-once.
    const killKey = topicBig.toString(16) + ':kill:' + msgId;
    const killedAt = this._appDelivered.get(killKey);
    if (this._appDelivered.has(key)) {
      if (!(Number.isFinite(killedAt) && Number.isFinite(publishTs) && publishTs > killedAt)) return;   // exactly-once
    } else if (Number.isFinite(killedAt) && Number.isFinite(publishTs) && publishTs <= killedAt) {
      return;                                          // pre-kill copy after its kill
    }
    if (killedAt !== undefined) this._appDelivered.delete(killKey);   // Orion RAK-APP(a): a passing body is a new publication; its kill record goes
    this._appDelivered.delete(key);                    // re-insert at the LRU tail
    this._appDelivered.set(key, Number.isFinite(publishTs) ? publishTs : true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (publishTs > prev) this._lastSeenTsByTopic.set(topicBig, publishTs);
    this._confirmPending(topicBig, msgId);             // a subscribed publisher saw its own msg → stop retrying
    if (this._deliveryCallback) {
      this._latStage(msgId, 'deliver:app');
      try { this._deliveryCallback(topicBig, json, msgId, publishTs, seq); }
      catch (e) { this._log('warn', 'delivery-callback-threw', { err: e?.message }); }
      this._latStage(msgId, 'deliver:cb');
    }
  },

  // Deliver a kill to the local app (exactly-once, keyed distinctly from the
  // target's own delivery) and advance our since-floor to the kill's stamp so a
  // renewal stops re-pulling it once seen — while a MISSED kill (since still below
  // killTs) is re-pulled and self-heals.
  _deliverKillToApp(topicBig, target, killTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (killTs > prev) this._lastSeenTsByTopic.set(topicBig, killTs);
    const key = topicBig.toString(16) + ':kill:' + target;
    if (this._appDelivered.has(key)) {                 // exactly-once (per publication: _deliverToApp clears it on a republish)
      const prevKill = this._appDelivered.get(key);    // Orion RAK-APP(b): a NEWER kill advances the record, so a delayed body below it stays dropped
      if (Number.isFinite(killTs) && (!Number.isFinite(prevKill) || killTs > prevKill)) this._appDelivered.set(key, killTs);
      return;
    }
    // 4.131.0 (Orion RAK-2): a kill stamped at or below the body this app holds
    // retracts an EARLIER publication of the same msgId, not this one.
    const heldAt = this._appDelivered.get(topicBig.toString(16) + ':' + target);
    if (Number.isFinite(heldAt) && Number.isFinite(killTs) && killTs <= heldAt) return;
    this._appDelivered.set(key, Number.isFinite(killTs) ? killTs : true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    // A kill retracts a message the app is holding. If this app NEVER received the body
    // (a since:'all' joiner that arrived after the kill — the killed body is spliced from
    // cache and never replayed), there is nothing to retract: delivering a spurious
    // "deleted" event for a message it never saw is noise. Record the tombstone + advance
    // the floor (above) so we converge and suppress the body if it ever arrives, but only
    // CALL BACK when the body was actually delivered to this app. (Now reliably reproduced
    // once cohort anti-entropy made tombstone propagation dependable.)
    const bodyKey = topicBig.toString(16) + ':' + target;
    if (!this._appDelivered.has(bodyKey)) return;      // never had the body → nothing to retract
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, JSON.stringify({ deleted: true, msgId: target, topic: null }), target, killTs, seq); }
      catch (e) { this._log('warn', 'delete-callback-threw', { err: e?.message }); }
    }
  },

  // The `since` to renew with: max of our cache high-water (relay), last app
  // delivery, and the seeded subscription floor.
  _sinceFor(topicBig) {
    const role = this.axonRoles.get(topicBig);
    const relay = (role && role.cache.length) ? role.cache[role.cache.length - 1].publishTs : 0;
    const seen  = this._lastSeenTsByTopic.get(topicBig);
    const sub   = this.mySubscriptions.get(topicBig)?.since;
    return Math.max(relay, Number.isFinite(seen) ? seen : 0, Number.isFinite(sub) ? sub : 0);
  },

};

export default topicStoreMethods;
