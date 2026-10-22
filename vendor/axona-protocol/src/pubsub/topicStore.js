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
  _tombstoned(role, msgId, json) {
    if (!role) return false;
    const tomb = role.tombstones.get(msgId);
    if (!tomb) return false;
    if (tomb.signer) {
      let author = null; try { author = JSON.parse(json)?.signerPubkey ?? null; } catch { /* */ }
      if (author && lc(author) === lc(tomb.signer)) return true;      // authorized kill → stays dead
      role.tombstones.delete(msgId);                                   // unauthorized/forged → revoke
      return false;
    }
    return true;                                                       // signer-less tombstone (defensive) → suppress
  },

  // ── singleton-root replication (warm backup roots) ───────────────────
  // The N reachable neighbours XOR-closest to a topic — its natural successors and
  // therefore the right backup roots (whoever routing re-converges on if the root
  // leaves). Excludes self and the bridge (signaling infra, never a root).
  // The active (non-expired) tombstones as a wire-shaped `dels` array. Every cache
  // MIGRATION (replicate, pull-up/replay-up, graceful handoff) must carry these
  // alongside `msgs`: a node that adopts cached history without the matching tombstones
  // would resurrect a killed message and serve it to a late joiner (the kill-leak class).
  _activeDels(role) {
    const now = this._now();
    const dels = [];
    for (const [tgt, tomb] of (role?.tombstones ?? [])) {
      if ((tomb?.exp ?? 0) > now) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, seq: tomb.seq, publishTs: tomb.killTs, ...(this._tombAuthority && tomb.kill ? { kill: tomb.kill } : {}) });
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
        if (this._tombAuthority && d.kill && !(await this._taVerifyBoundKill(topicBig, d))) { const { kill, ...rest } = d; dd = rest; }
        if (this._applyKill(role, topicBig, dd, cut, genTag) === false) return false;
      }
    }
    return true;
  },

  // ── app delivery (exactly-once) ──────────────────────────────────────
  _deliverToApp(topicBig, json, msgId, publishTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const key = topicBig.toString(16) + ':' + msgId;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
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
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
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
