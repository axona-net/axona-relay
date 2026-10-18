// =====================================================================
// syncEngine.js — the sync engine, repair side (v0.2 refactor, Phase 8).
//
// ONE operation moves topic state between peers for repair/durability.
// Before this module the six repair policies each hand-rolled their own
// snapshot assembly, verb emission, and receiver composition across
// wireHandlers.js and repairPlane.js — the 4.22.0 lw-pull storm, the 4.24.0
// departure spray, and the #353 ephemeral-publisher strand all lived in the
// seams between those hand-rolled copies. Now:
//
//   · SYNC_POLICIES is the NORMATIVE policy table (Axona-Architecture §VIII).
//     Every row is typed by the role natures (Phase 7): it names the nature
//     it creates on the receiver and that nature's eviction path — the
//     principal-liveness rule (INVARIANTS I-10) made machine-checkable.
//   · _syncPull / _syncPush / _syncAnswerPull are the ONLY emission sites
//     for the repair verbs (PULLUP, REPLAYUP, REPLICATE, HANDOFF) — enforced
//     statically by test/smoke_emission_sites.mjs.
//   · _syncIngest is the ONE receiver composition (tombstones first, then
//     verified stamped ingest) with per-policy hooks.
//   · Per-pair quench ledgers (role.sync, Phase 6) are consulted INSIDE the
//     engine, not at call sites — a new policy cannot forget its quench.
//
// The hot delivery path (fan-out, replay-on-seat, DELIVER) is deliberately
// OUT (plan §7 Phase 8): it is high-rate, latency-sensitive, its quench
// machinery is already unified in topicStore, and no incident has lived
// there. It joins the engine only if it earns its way in.
// =====================================================================

import { T, METRICS_LEASE_MS } from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { makeRole } from './rootClaim.js';

// ── The typed policy table ──────────────────────────────────────────────
// Fields (all required — smoke_sync_engine.mjs asserts completeness):
//   mode              'pull' | 'push' | 'ingest' | 'gate'
//   verb              wire verb the policy emits (or rides on)
//   trigger           when the policy fires (prose, normative)
//   summary           what evidence the decision compares (hw/lw/sig/…)
//   createsOnReceiver role nature planted on the receiving node, or null
//   evictor           that nature's eviction path (principal-liveness, I-10)
//   ledger            the role.sync quench state the engine consults, or null
//   rateBound         what bounds the policy's send rate
export const SYNC_POLICIES = Object.freeze({
  REPLAY_UP: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: "renewal SUB advertises a child hw ABOVE mine (I hold less than my child)",
    summary: 'hw', createsOnReceiver: null,
    evictor: 'natural quench: my hw catches up to the child’s on union',
    ledger: null, rateBound: 'one pull per triggering renewal; delta-only answer',
  }),
  SPLIT_UNION: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: "renewal SUB advertises a child lw BELOW my lw (pre-transition half I never saw)",
    summary: 'lw', createsOnReceiver: null,
    evictor: 'natural quench: my lw drops to the child’s on union',
    ledger: 'pulledLw (one-shot per (child, lw); re-arms only if lw DECREASES — 4.22.1 storm guard)',
    rateBound: 'ledger one-shot',
  }),
  EMPTY_ROOT_PROBE: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: 'a root with an EMPTY cache (birth probe + renewal sweep) pulls the cohort',
    summary: 'empty-cache', createsOnReceiver: null,
    evictor: 'natural quench: probe only while empty',
    ledger: 'probeTries/probeAt (per-topic, max EMPTY_ROOT_PROBE_MAX, rate EMPTY_ROOT_PROBE_INTERVAL_MS — enforced at the trigger sweep)',
    rateBound: 'EMPTY_ROOT_PROBE_MAX tries, EMPTY_ROOT_PROBE_FANOUT targets',
  }),
  COHORT_REPLICATE: Object.freeze({
    mode: 'push', verb: 'REPLICATE',
    trigger: 'live root, every tick (keepalive) + eager on stamp/kill; full state on sig delta',
    summary: 'sig (count:hw:tombstones)', createsOnReceiver: 'backup',
    evictor: 'principal gone + re-homed + BACKUP_EVICT_MS (rootClaim.retireBackup)',
    ledger: 'sig/lastFullAt (delta gate) + replicas (cohort membership)',
    rateBound: 'REPLICATE_FULL_BUDGET full pushes per tick (round-robin cursor)',
  }),
  UNION_AT_ROOT: Object.freeze({
    mode: 'ingest', verb: 'REPLICATE',
    trigger: 'a REPLICATE arriving at a node that itself holds the ROOT claim',
    summary: 'none (full union)', createsOnReceiver: null,
    evictor: 'n/a — receiver keeps its claim; no backup bookkeeping (no usurpation)',
    ledger: null, rateBound: 'bounded time-sliced ingest queue (I-11)',
  }),
  HANDOFF: Object.freeze({
    mode: 'push', verb: 'HANDOFF',
    trigger: 'graceful leave: departing root pushes each rooted cache to its heir',
    summary: 'departure', createsOnReceiver: 'root (heir adopts the claim)',
    evictor: 'demote to a strictly-closer live root (rootClaim.handoffArrived); never back to the leaver (I-2)',
    ledger: 'handoffAcked (leaver-side ack set; HANDOFFACK)',
    rateBound: 'HANDOFF_TRIES rounds × HANDOFF_ACK_MS, heirs re-resolved per round (#340)',
  }),
  PUB_DURABLE: Object.freeze({
    mode: 'gate', verb: 'REPLICATE',
    trigger: 'a SELF-ROOTED stamp (publisher is the topic’s root) with a non-empty cohort',
    summary: 'dispatch of the eager cohort replicate', createsOnReceiver: 'backup',
    evictor: 'same as COHORT_REPLICATE',
    ledger: '_pendingPub (confirm deferred until ≥1 REPLICATE dispatched — #353: leave()’s evidence drain then holds an ephemeral publisher until its history has left the node)',
    rateBound: 'one gate per stamped publish',
  }),
});

export const syncEngineMethods = {
  // ── state assembly: ONE place a role's transferable state is serialized ──
  _syncSnapshot(role) {
    return {
      msgs: role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq })),
      dels: this._activeDels(role),
    };
  },
  _syncDelta(role, sinceHw) {
    return {
      msgs: role.cache.filter(c => c.publishTs > sinceHw)
                      .map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq })),
      dels: this._activeDels(role),
    };
  },

  // ── PULL side: ask `target` to replay its stamped history up to me ──────
  // The per-pair quench ledger is consulted HERE (not at call sites): a
  // SPLIT_UNION pull is one-shot per (child, lw) and re-arms only when the
  // child's lw decreases. Returns true iff the pull was sent.
  // Returns true (sent) / false (quenched by the SPLIT_UNION ledger) — the
  // synchronous contract smoke_sync_engine pins. A caller that needs the
  // route's VERDICT uses _syncPullAwait below.
  _syncPull(targetBig, topicBig, policyName, opts = {}) {
    return this._syncPullAwait(targetBig, topicBig, policyName, opts) !== false;
  },
  // Same send; returns the ROUTE'S PROMISE (dual-root packet (a)) or false
  // when quenched. The promise resolves to the verdict routing already
  // produces ({consumed, …}, or undefined from a non-reporting adapter) and
  // never rejects (_route contains that). The empty-root probe awaits it so a
  // try that reached nobody is not counted as a try.
  _syncPullAwait(targetBig, topicBig, policyName, { sinceHw = 0, lw = null, role = null } = {}) {
    if (policyName === 'SPLIT_UNION') {
      const peerHex = lc(idHex(targetBig));
      const prev = role.sync.pulledLw.get(peerHex);
      if (!(prev === undefined || lw < prev)) return false;
      role.sync.pulledLw.set(peerHex, lw);
    }
    return this._emitPullUp(targetBig, { topicId: idHex(topicBig), sinceHw, parentId: idHex(this.nodeId) });
  },
  // THE ONE PULLUP EMISSION SITE and THE ONE REPLAYUP EMISSION SITE
  // (smoke_emission_sites pins each verb to one `_route` in this engine). The
  // merge machine (dual-root 4(d)) emits its corr-bearing pulls and frames
  // through these two, never through its own route call.
  _emitPullUp(targetBig, payload) {
    return this._route(targetBig, T.PULLUP, payload);
  },
  _emitReplayUp(targetBig, payload) {
    return this._route(targetBig, T.REPLAYUP, payload);
  },

  // Answer a PULLUP: replay my cache delta (and active tombstones) UP to the
  // asking parent. The only REPLAYUP emission site.
  _syncAnswerPull(payload) {
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (!role) return;
    // A TOMBSTONE-ONLY role transfers its dels (dual-root packet 4(d)): the
    // old `!role.cache.length` return answered nothing for a role whose
    // whole state is deletions, and a puller then resurrected the killed body.
    if (!role.cache.length && !this._activeDels(role).length) return;
    const sinceHw = Number.isFinite(payload.sinceHw) ? payload.sinceHw : 0;
    const { msgs, dels } = this._syncDelta(role, sinceHw);
    if ((msgs.length || dels.length) && isHexId(lc(payload.parentId))) {
      this._emitReplayUp(idBig(payload.parentId), { topicId: idHex(role.topicId), msgs, dels });
    }
  },

  // ── PUSH side: move a role's full state to one peer ─────────────────────
  // The only REPLICATE / HANDOFF emission site. `full:false` sends an empty
  // KEEPALIVE (refreshes the backup's lastReplicaAt; empty ingest is a no-op).
  // RETURNS the dispatch promise (Q2/C4). Every caller here is free to ignore it
  // — HANDOFF and the repair-plane retries remain fire-and-forget — but the one
  // caller that writes a durability ledger (_replicateRole) must be able to see
  // whether the send actually went anywhere.
  _syncPush(targetBig, topicBig, role, policyName, { full = true } = {}) {
    const { msgs, dels } = full ? this._syncSnapshot(role) : { msgs: [], dels: [] };
    const payload = { topicId: idHex(topicBig), from: idHex(this.nodeId), msgs, dels };
    // SUBSCRIBER-LIST REPLICATION: on a FULL push, carry the root's direct
    // subscriber/children list so the backup can re-adopt them into its fanout on
    // promotion (collapses the post-migration orphan window). Only from a live root
    // (a backup has none to give), only on full pushes, size-bounded.
    if (full && this._replicateSubs && role.isRoot && role.subscribers && role.subscribers.size) {
      const subs = [];
      for (const [h, s] of role.subscribers) {
        subs.push({ id: h, since: (s && Number.isFinite(s.since)) ? s.since : 0, child: role.children?.has(h) ? 1 : 0 });
        if (subs.length >= 256) break;   // bound the payload
      }
      if (subs.length) payload.subs = subs;
    }
    // METRICS-LEASE REPLICATION (#47): on a full push, carry the root's remaining
    // publish lease so a promoted backup keeps emitting metricTopic(T) snapshots across
    // the transition instead of going metrics-dark until a METRICSON renewal re-routes
    // to it — which a lingering dead-root can still swallow (#28). Sent as a DURATION,
    // not an absolute expiry: the backup re-bases it on its own clock, so a constant
    // offset between separated relays (M4/M1) cancels instead of arming a subscriber-
    // stale lease or refusing a live one (Vega, review of 3e8537f). ALWAYS present on a
    // full push (0 = no/lapsed lease) so a push that finds the lease gone CLEARS the
    // backup's inherited value rather than letting a last-seen expiry outlive demand.
    if (full && this._replicateSubs && role.isRoot) {
      payload.metricsRemainingMs = role.metricsOn > this._now() ? (role.metricsOn - this._now()) : 0;
    }
    if (policyName === 'HANDOFF') return this._route(targetBig, T.HANDOFF, payload);
    return this._route(targetBig, T.REPLICATE, payload);
  },

  // ── INGEST side: the ONE receiver composition ───────────────────────────
  // Tombstones first (a killed body in the same batch must be suppressed),
  // then the verified stamped ingest (B-4 re-verify, msgId dedup, fanout +
  // app delivery). Policy hooks:
  //   REPLAY_UP  — role must already exist (I asked for this history)
  //   HANDOFF    — adopt as root (heir), then departure rules + confirming ack
  //   REPLICATE  — at a root: UNION_AT_ROOT (keep claim, no backup
  //                bookkeeping); else: enter the BACKUP nature (rootClaim)
  async _syncIngest(payload, meta, policyName) {
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return; }

    if (policyName === 'HANDOFF') {
      // ADMISSION (v4.46.0). A handoff is PUSHED at us — we did not choose it,
      // and an alternative demonstrably exists (the leaver picks its heir from a
      // candidate list and retries). So this is where refusal has real teeth.
      //
      // Refusing here is only SAFE because 4.45.0 (#402) made the ack honest:
      // before that, a refusal and a successful adoption were indistinguishable
      // on the wire, so the leaver would mark the topic acked, skip retry and
      // cohort spray, and depart — dropping the last copy. We now hold no role
      // and send no ack, so the leaver's unacked path re-homes the topic.
      let role = this.axonRoles.get(topicBig);
      // A WHOLE transfer at a GATED heir is refused before anything is applied
      // (dual-root packet 4(d)): no del, no body, NO HANDOFFACK — the silence
      // the leaver already understands (it retries another heir and cohort-
      // sprays). A short ack would be wrong: held = sent = 0 on a tombstone-
      // only transfer reads as success at the leaver.
      if (role?.merge?.gate) {
        this._log('warn', 'handoff-refused-gated', { topic: String(payload.topicId).slice(0, 12), from: typeof payload.from === 'string' ? payload.from.slice(0, 12) : null, corr: role.merge.gate.corr.slice(0, 8) });
        return;
      }
      if (!role && !this.admitPushedRole(topicBig)) return;   // refuse: stay silent, leaver retries
      if (!role) role = this._becomeRoot(topicBig, 'handoff-heir');
      if (!role) return;                 // hard refusal (bridge) — no role, no ack, leaver re-homes
      const cut = this._cutCapture(role);                      // THE CUT: before the first await (dual-root 4(d))
      const delsOk = await this._applyDels(role, topicBig, payload.dels, cut);
      const tally = delsOk ? await this._ingestStampedBatch(role, payload.msgs, cut)
                           : { sent: Array.isArray(payload.msgs) ? payload.msgs.length : 0, held: 0, rejected: Array.isArray(payload.msgs) ? payload.msgs.length : 0, refused: true };
      if (tally.refused) {
        // Refused at the cut mid-transfer: the whole transfer is refused, NO
        // HANDOFFACK (the leaver retries elsewhere), nothing further applied.
        this._log('warn', 'handoff-refused-cut', { topic: String(payload.topicId).slice(0, 12), from: typeof payload.from === 'string' ? payload.from.slice(0, 12) : null, sent: tally.sent, held: tally.held });
        return;
      }
      // Departure rules — purge the leaver's ghost beacon, never defer back to
      // the leaver, yield only to a strictly-closer live root (rootClaim).
      const leaver = typeof payload.from === 'string' ? lc(payload.from) : null;
      this._rootClaim.handoffArrived(topicBig, leaver);
      // Confirm receipt, HONESTLY (#402): the ack carries what we actually hold.
      // A leaver on >=4.45.0 treats held < sent as NOT acked and keeps retrying /
      // cohort-spraying. Older leavers ignore the extra fields and behave as
      // before, so this is wire-compatible in a mixed fleet.
      if (leaver && isHexId(leaver)) {
        try {
          this._route(idBig(leaver), T.HANDOFFACK,
            { topicId: payload.topicId, held: tally.held, sent: tally.sent });
        } catch { /* best-effort */ }
      }
      // RECORD, not diagnostic (dual-root packet Part 5.1, axona-docs 14a6bd8):
      // this line is the only record a host keeps of a handoff landing. At
      // debug it was absent from every relay log on six hosts when eleven
      // #council entries arrived at the Windows relay on 2026-10-17 03:04Z,
      // and the carrier of that merge stayed a hypothesis. Info, always; the
      // leaver is named by prefix so the carrier can be matched to its leave.
      this._log(tally.rejected ? 'warn' : 'info', 'handoff-ingested',
        { topic: String(payload.topicId).slice(0, 12), from: leaver ? leaver.slice(0, 12) : null, policy: 'HANDOFF',
          sent: tally.sent, held: tally.held, rejected: tally.rejected, dels: Array.isArray(payload.dels) ? payload.dels.length : 0,
          acked: !!(leaver && isHexId(leaver)) });
      return;
    }

    if (policyName === 'REPLICATE') {
      const mine = this.axonRoles.get(topicBig);
      if (mine?.isRoot) {
        // UNION_AT_ROOT — the cohort anti-entropy contract: co-hosting roots
        // converge to the union of cache+tombstones. We keep our claim.
        const cut = this._cutCapture(mine);
        if (await this._applyDels(mine, topicBig, payload.dels, cut)) await this._ingestStampedBatch(mine, payload.msgs, cut);
        return;
      }
      if (!this._rootReplicas) return;                  // backup duty disabled on this node
      let from = null;
      if (payload.from && isHexId(lc(payload.from))) from = lc(payload.from);
      else if (meta?.fromId != null) { try { from = lc(idHex(idBig(meta.fromId))); } catch { /* */ } }
      let role = this.axonRoles.get(topicBig);
      if (!role) { role = makeRole(topicBig, false, this._now()); this.axonRoles.set(topicBig, role); }
      this._rootClaim.becomeBackup(topicBig, role, from);   // nature transition (I-10)
      const cut = this._cutCapture(role);
      if (await this._applyDels(role, topicBig, payload.dels, cut)) await this._ingestStampedBatch(role, payload.msgs, cut);
      // SUBSCRIBER-LIST REPLICATION: stash the principal's subscriber list so a
      // promoted backup can re-adopt them into its fanout + replay (rootClaim._set).
      // STORED, not applied — a backup serves nobody until it becomes root.
      if (this._replicateSubs && Array.isArray(payload.subs)) {
        const selfHex = lc(idHex(this.nodeId));
        const inh = new Map();
        for (const e of payload.subs) {
          if (!e || typeof e.id !== 'string' || !isHexId(lc(e.id))) continue;
          const h = lc(e.id);
          if (h === selfHex) continue;                       // never seed self
          inh.set(h, { since: Number.isFinite(e.since) ? e.since : 0, child: !!e.child });
        }
        role._inheritedSubs = inh.size ? inh : null;
      }
      // METRICS-LEASE REPLICATION (#47): re-base the principal's remaining lease onto
      // THIS node's clock and store it for promotion (rootClaim._set). Present ⟺ full
      // push (0 = principal's lease lapsed → CLEAR, so a stale expiry can't outlive
      // demand — Vega/Aster release condition). Clamped to METRICS_LEASE_MS so a bad
      // remaining can never arm a longer-than-legal lease (Aster guard 1). Receiver-now
      // + remaining adds at most handoff transit, bounded by latency not clock offset —
      // soft-state tolerance, not exact expiry preservation. Never acted on as a backup.
      if (this._replicateSubs && Number.isFinite(payload.metricsRemainingMs)) {
        const rem = Math.min(Math.max(0, payload.metricsRemainingMs), METRICS_LEASE_MS);
        role._inheritedMetricsOn = rem > 0 ? (this._now() + rem) : 0;
      }
      return;
    }

    // REPLAY_UP — stamped history I asked for; only meaningful if the role lives.
    const role = this.axonRoles.get(topicBig);
    if (!role) return;
    const cut = this._cutCapture(role);                      // THE CUT: before the first await (dual-root 4(d))
    if (await this._applyDels(role, topicBig, payload.dels, cut)) await this._ingestStampedBatch(role, payload.msgs, cut);
  },
};

export default syncEngineMethods;
