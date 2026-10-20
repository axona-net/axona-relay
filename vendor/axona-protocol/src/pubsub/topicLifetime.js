// =====================================================================
// topicLifetime.js — TOPIC LIFETIME: the standby lease, termination, the
// thin-branch step-down, and the two frames that carry them.
//
// Design: axona-docs/architecture/Axona-Topic-Lifetime-v0.1.md (title v0.9,
// 11288d7). David, 2026-10-20: "There is no need for a deletion record. After
// the termination message, all participating nodes will no longer reference
// the topic ID or subscribers. … This re-subscription may regenerate the root
// on some node, but this too will time out quickly and the topic
// infrastructure will be trivially removed in the next termination pass."
// And: "Let's use 2 subscribers and 90 seconds." "Build."
//
// WHAT WAS MEASURED. The 2026-10-19 22:40Z census (ops/role-census-
// 20261019T2240Z.md): 2,997 roles over 802 topics; 387 roots, 385 with a seat,
// none empty; 1,367 backups, 1,147 of them seating nobody; every root's seats
// were its own backups. The loop, in one sentence: the root pushes to two
// backups because it has bodies; the backups seat at the root every tick
// because they exist; the root is not dead because it has seats; the backups
// are not dead because they are backups.
//
// THE RULES (every one names what the node can read in its own state):
//   4.1  a generation is (topic, root id, epoch); every frame here names it.
//   4.2  a holder that leaves its part in a LIVE generation keeps a LEFT record
//        for L + S + D on the monotonic clock; termination writes NO record.
//   4.3  demand is counted once: reader 1, child its declared n, standby 0.
//        lastDemandAt is stamped by a reader SUB, a child SUB with n > 0 and a
//        stamped write — never by a standby SUB, a REPLICATE, a beacon or a
//        duplicate kill.
//   4.4  a standby appointment is a lease on the root's REPLICATE: L = DROP_MS,
//        renewed only by a REPLICATE whose apparent age is within D + S. A
//        lapse ends the APPOINTMENT and nothing else: bodies or tombstones with
//        the root gone keep the 4.94.0 election obligation; a reader or child
//        continues as that; otherwise the reapers take it on the tick.
//   4.5  a root terminates when its cache holds no body, no custody is open,
//        there is no local intent, and demand has been zero for DROP_MS.
//        Tombstones do not hold a topic open. `dismantling` is installed
//        synchronously BEFORE the first TOPIC_TERMINATED; from then the role
//        grants no seat and takes no write (the cut refuses it); the notice is
//        sent to every seat and replica once per tick for up to three ticks,
//        and the role goes on the tick after its last send, tombstones with it.
//        A backup drops the generation; a backup holding a BODY does not obey
//        and elects by 4.4(1); a child forwards to its seats, then retires; a
//        reader's subscription ENDS with one `{ terminated, generation }` row.
//   4.6  nothing survives the tree. The residual, accepted by David: a holder
//        partitioned through BOTH the kill and the notice can regenerate a root
//        with the deleted body, served until the body's own TTL.
//   4.7  a thin child steps down: demand below CHILD_MIN_READERS for
//        CHILD_STEP_DOWN_MS, after CHILD_MIN_STAY_MS, with its upstream
//        reachable; it REHOMEs its readers to that upstream, tells its parent
//        (an UNSUB marked steppedDown → cooldown), writes LEFT, and retires.
//        REHOME is kernel-internal: the app's subscription persists.
//
// THE LATE RENEWAL (David). A seat that renews before its notice arrives
// lands at the node then closest to the topic id, which holds no role, and
// _onSub creates one on first sight: a new, empty root with one seat. The
// notice then reaches the seat, which drops its state and stops renewing;
// the seat lapses at DROP_MS, demand reads zero and lastDemandAt is DROP_MS
// old on the same tick, and the regenerated root terminates by this same
// rule with nothing left to notify. One empty role for one DROP window and
// two ticks, gone on the next pass.
//
// Behind TOPIC_LIFETIME (option `topicLifetime`, env TOPIC_LIFETIME=1).
// Unarmed: _lifetimeTick is a no-op, _emitSubscribe declares nothing, a
// REPLICATE carries no generation and is ingested as 4.116.0 did, and both
// frames are consumed without effect. Methods are mixed into
// AxonaManager.prototype; state lives on the façade.
// =====================================================================

import {
  T, STANDBY_LEASE_MS, APPOINTMENT_DELAY_MS, APPOINTMENT_SKEW_MS, LEFT_RECORD_MAX,
  TOPIC_TERMINATE_TRIES, APP_DEDUP_MAX,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';

export const topicLifetimeMethods = {
  // ── seats and demand (4.3) ────────────────────────────────────────────
  /** What kind of seat a SUB declares. A SUB that declares nothing is a reader. */
  _seatKindOf(payload) {
    if (payload && payload.standby === true) return { kind: 'standby', demand: 0 };
    if (payload && Number.isFinite(payload.demand)) return { kind: 'child', demand: Math.max(0, payload.demand | 0) };
    return { kind: 'reader', demand: 1 };
  },
  /** Demand at a holder: the sum over its seats by the 4.3 table and nothing else. */
  _roleDemand(role) {
    let n = 0;
    for (const s of role.subscribers.values()) {
      if (!s || s.kind === 'standby') continue;
      n += (s.kind === 'child') ? (s.demand | 0) : 1;
    }
    return n;
  },
  _seatIsStandby(role, subHex) { const s = role.subscribers.get(subHex); return !!(s && s.kind === 'standby'); },

  // ── generation (4.1) ──────────────────────────────────────────────────
  _genOf(role) { return { root: lc(idHex(this.nodeId)), epoch: role.epoch | 0 }; },
  /** The generation a holder carries DOWN on its DELIVERs: a root its own, a child the one its upstream named, else null. */
  _genCarried(role) { if (!role) return null; if (role.isRoot) return this._genOf(role); const a = this._attach.get(role.topicId); return a && a.gen ? { ...a.gen } : null; },
  /** The generation this node is attached to for `t` (relay role and own reader alike), or null. */
  _attachedGenOf(t) { const a = this._attach.get(t); return a && a.gen ? a.gen : null; },
  _sameGen(a, b) { return !!a && !!b && a.root === b.root && (a.epoch | 0) === (b.epoch | 0); },
  _genValid(g) { return !!(g && typeof g === 'object' && typeof g.root === 'string' && isHexId(lc(g.root))); },
  _genNorm(g) { return { root: lc(g.root), epoch: (Number.isInteger(g.epoch) && g.epoch >= 0) ? g.epoch : 0 }; },

  // ── LEFT records (4.2) ────────────────────────────────────────────────
  // Kept outside axonRoles, keyed by topic, `until` on the MONOTONIC clock so a
  // wall-clock jump neither shortens nor lengthens it. Bounded: pressure evicts
  // only an EXPIRED record; a map full of live records refuses the write, and
  // the caller then refuses the appointment (delays a grant, never admits a
  // stale one).
  _leftWrite(topicBig, rec) {
    const map = this._leftRecords;
    const mono = this._mono();
    const until = mono + STANDBY_LEASE_MS + APPOINTMENT_SKEW_MS + APPOINTMENT_DELAY_MS;
    if (map.size >= LEFT_RECORD_MAX && !map.has(topicBig)) {
      let victim = null;
      for (const [k, r] of map) { if (r.until <= mono) { victim = k; break; } }
      if (victim === null) { this._log('warn', 'left-record-refused', { topic: idHex(topicBig).slice(0, 12), size: map.size }); return false; }
      map.delete(victim);
    }
    map.set(topicBig, {
      rootId: (typeof rec.rootId === 'string' && rec.rootId) ? lc(rec.rootId) : null,
      epoch: rec.epoch | 0,
      upstream: (typeof rec.upstream === 'string' && rec.upstream) ? lc(rec.upstream) : null,
      until, why: rec.why || null,
    });
    return true;
  },
  _leftGet(topicBig) {
    const r = this._leftRecords.get(topicBig);
    if (!r) return null;
    if (r.until <= this._mono()) { this._leftRecords.delete(topicBig); return null; }
    return r;
  },
  _leftPrune() {
    const mono = this._mono();
    for (const [k, r] of this._leftRecords) if (r.until <= mono) this._leftRecords.delete(k);
    for (const [k, g] of this._genHighWater) if (g.until <= mono) this._genHighWater.delete(k);
  },
  /**
   * True when a grant or standby SUB naming `gen` falls under a live LEFT
   * record: the SAME generation this node left. A different root or a later
   * epoch is a new generation and is never refused by the record.
   */
  _leftRefuses(topicBig, gen) {
    const r = this._leftGet(topicBig);
    if (!r) return false;
    if (!gen) return true;                           // a generation-less frame cannot be told apart from the one we left
    if (r.rootId === null) return false;             // a stepped-down child's record names no root: it forwards readers, it refuses no grant
    return r.rootId === gen.root && (r.epoch | 0) === (gen.epoch | 0);
  },

  // ── the grant rule (4.4) ──────────────────────────────────────────────
  // A conforming grant, issued and delivered within D under skew S, reads at
  // most D + S old by the receiver's clock. Returns true (fresh), false
  // (dropped), or 'legacy' when the frame carries no issue stamp.
  _grantFresh(payload, now) {
    if (!Number.isFinite(payload?.issuedAt)) return 'legacy';
    if (this._grantHoldUntil && now < this._grantHoldUntil) return false;   // a moved wall clock: nothing accepted for one D
    return (now - payload.issuedAt) <= APPOINTMENT_DELAY_MS + APPOINTMENT_SKEW_MS;
  },
  // A wall-clock jump at the receiver moves the apparent age of every grant by
  // the jump. Detected against the monotonic clock each tick; a jump beyond S
  // holds grants for one D.
  _wallJumpCheck(now) {
    const mono = this._mono();
    if (this._lifeLastWall != null) {
      const dw = now - this._lifeLastWall, dm = mono - this._lifeLastMono;
      if (Math.abs(dw - dm) > APPOINTMENT_SKEW_MS) {
        this._grantHoldUntil = now + APPOINTMENT_DELAY_MS;
        this._log('info', 'clock-jump', { wallMs: dw, monoMs: Math.round(dm), holdUntil: this._grantHoldUntil });
      }
    }
    this._lifeLastWall = now; this._lifeLastMono = mono;
  },

  // ── the tick ──────────────────────────────────────────────────────────
  _lifetimeTick(now) {
    if (!this._topicLifetime) return;
    this._wallJumpCheck(now);
    this._leftPrune();
    for (const [h, until] of this._childCooldown) if (until <= now) this._childCooldown.delete(h);
    this._metadataPrune();
    this._standbyLeaseSweep(now);
    this._terminationSweep(now);
    this._thinBranchSweep(now);
  },
  // BOUNDED (Aster 848688cb, 0cfdd82d TL-B09): the held-body set describes
  // bodies in the cache and is pruned to it every tick on EVERY role that can
  // carry it, whatever the role's nature or eligibility for a step-down.
  _metadataPrune() {
    for (const role of this.axonRoles.values()) {
      const s = role.upstreamHeld;
      if (!s || !s.size) continue;
      for (const id of s) if (!role.cacheIds.has(id)) s.delete(id);
    }
  },

  // Custody that holds a topic OPEN for termination: an exchange in flight, a
  // pin, a pending delegation. NOT a tombstone (4.5: tombstones do not hold a
  // topic open; they belong to the generation and go with it).
  _custodyOpen(role, now) {
    const mg = role.merge;
    if (mg) {
      if (mg.completing || mg.gate) return true;
      if (mg.acceptedWinner && mg.acceptedWinner.exp > now) return true;
      if (mg.pins && mg.pins.size) return true;
      if (mg.pendingDelegation && mg.pendingDelegation.size) return true;
    }
    if (role.sync?.openMerges?.size || role.sync?.openPulls?.size) return true;
    return false;
  },

  // ── 4.4 the standby lease ─────────────────────────────────────────────
  _standbyLeaseSweep(now) {
    for (const t of [...this._backupTopics]) {
      const role = this.axonRoles.get(t);
      if (!role || role.isRoot || role.backupOf === null) continue;
      const age = now - (role.lastReplicaAt || 0);
      if (age <= STANDBY_LEASE_MS) continue;
      // 4.4(1): bodies or active tombstones with the root gone → the election
      // obligation, unchanged: it stays subscribed and self-roots at the SUB
      // terminal. A live root keepalives every tick, so a lapse IS the root gone.
      if (role.cache.length > 0 || this._custodyAlive(role, now)) continue;
      const principal = role.backupOf;
      this._leftWrite(t, { rootId: role.standbyGen?.root ?? principal, epoch: role.standbyGen?.epoch ?? 0, upstream: principal, why: 'lease-lapsed' });
      this._rootClaim.retireBackup(t, role, 'lease-lapsed');
      role.standbyGen = null;
      this._standbyLapsed++;
      this._log('info', 'standby-lapsed', { topic: idHex(t).slice(0, 12), ageMs: age, seats: role.subscribers.size, reader: this.mySubscriptions.has(t) });
      // The role now stands or falls on what it still is (a reader seat, a
      // child's seats, local intent): the reapers read it later this tick.
    }
  },

  // ── 4.5 termination ───────────────────────────────────────────────────
  _terminationSweep(now) {
    for (const [t, role] of [...this.axonRoles]) {
      if (!role.isRoot) continue;
      if (role.dismantling) { this._terminateStep(t, role, now); continue; }
      if (role.cache.length > 0) continue;
      if (this._custodyOpen(role, now)) continue;
      if (this.mySubscriptions.has(t) || this._hostedTopics.has(t)) continue;   // local intent
      if (role.metricsOn > now) continue;                                       // a lease is intent too
      if (this._roleDemand(role) > 0) continue;
      const anchor = Number.isFinite(role.lastDemandAt) ? role.lastDemandAt : (role.createdAt || 0);
      if (now - anchor <= this.dropMs) continue;
      // INSTALL BEFORE THE FIRST SEND. From here the role grants no seat and
      // takes no write: _cutHolds reads `dismantling`, and the generation moves
      // so a write parked in its verify await is refused when it resumes.
      role.dismantling = { at: now, sends: 0 };
      role.gen = (role.gen | 0) + 1;
      this._log('info', 'topic-terminating', {
        topic: idHex(t).slice(0, 12), epoch: role.epoch | 0, idleMs: now - anchor,
        seats: role.subscribers.size, replicas: role.replicas?.size ?? 0, tombstones: role.tombstones.size,
      });
      this._terminateStep(t, role, now);
    }
  },
  _terminateStep(t, role, now) {
    const d = role.dismantling;
    const me = lc(idHex(this.nodeId));
    const targets = new Set();
    for (const h of role.subscribers.keys()) targets.add(h);
    if (role.replicas) for (const h of role.replicas.keys()) targets.add(h);
    targets.delete(me);
    if (targets.size === 0 || d.sends >= TOPIC_TERMINATE_TRIES) {
      const tombs = role.tombstones.size;
      // The ended generation's epoch stays a high-water for L + S + D (read by
      // _knownEpoch) so the next generation this node forms mints above it.
      this._genHighWater.set(t, { epoch: role.epoch | 0, until: this._mono() + STANDBY_LEASE_MS + APPOINTMENT_SKEW_MS + APPOINTMENT_DELAY_MS });
      this._rootClaim.retireBackup(t, role, 'terminated');   // nature hygiene; a root wears no backup residue, but be explicit
      this._retireRole(t, role, 'terminated');
      this._rolesTerminated++;
      this._log('info', 'topic-terminated', { topic: idHex(t).slice(0, 12), sends: d.sends, tombstonesDropped: tombs, heldMs: now - d.at });
      return;
    }
    const payload = { topicId: idHex(t), from: idHex(this.nodeId), gen: this._genOf(role) };
    for (const h of targets) { try { this._route(idBig(h), T.TOPIC_TERMINATED, { ...payload }); } catch { /* best-effort; the seat lease bounds a miss */ } }
    d.sends++;
    this._log('info', 'topic-terminated-sent', { topic: idHex(t).slice(0, 12), round: d.sends, targets: targets.size });
  },

  // The notice, received. Bound to the generation: a backup by its principal,
  // a child by its upstream, a reader by the sender being its upstream (or
  // having none). A root ignores another root's notice (a split's two halves
  // each end by their own rule, 4.8).
  _onTopicTerminated(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    if (!this._topicLifetime) return 'consumed';
    let t; try { t = idBig(payload.topicId); } catch { return 'consumed'; }
    if (!this._genValid(payload.gen)) return 'consumed';
    const gen = this._genNorm(payload.gen);
    const from = (typeof payload.from === 'string' && isHexId(lc(payload.from))) ? lc(payload.from) : null;
    const me = lc(idHex(this.nodeId));
    const role = this.axonRoles.get(t);
    const up = (this._upstream.get(t) || [])[0] || null;
    const topic = idHex(t).slice(0, 12);
    if (role?.isRoot) { this._log('info', 'terminated-ignored', { topic, why: 'i-am-root', from: from ? from.slice(0, 10) : null }); return 'consumed'; }
    // TWO OBLIGATIONS, BOUND SEPARATELY (Aster TL-B01, TL-B13). A backup
    // APPOINTMENT binds by its exact (root, epoch) when it has one, by its
    // principal when the grant was legacy. The ATTACHMENT (one per topic: the
    // relay role's parent and the node's own reader share it) binds by the
    // exact generation its DELIVERs named; an UNVERSIONED attachment (an
    // older-kernel upstream that named none) binds by upstream. A notice ends
    // only the obligation it matches: an old appointment's end never ends a
    // reader attached elsewhere, and the role object goes only when no live
    // duty remains on it.
    const att = this._attachedGenOf(t);
    const hasAppointment = !!(role && role.backupOf !== null);
    const appointmentMatch = hasAppointment && (role.standbyGen ? this._sameGen(role.standbyGen, gen) : role.backupOf === gen.root);
    const attachmentMatch = att ? this._sameGen(att, gen) : ((up !== null && (up === gen.root || up === from)) || (!role && up === null));
    if (!appointmentMatch && !attachmentMatch) { this._log('info', 'terminated-ignored', { topic, why: 'foreign-generation', from: from ? from.slice(0, 10) : null, gen: gen.root.slice(0, 10) }); return 'consumed'; }
    // 4.5: a backup holding a body the root lacks does NOT obey — the root's
    // cache was empty, so any body here is one it lacks — and elects by 4.4(1).
    if (appointmentMatch && role.cache.length > 0) { this._log('info', 'terminated-ignored', { topic, why: 'holds-body', bodies: role.cache.length }); return 'consumed'; }
    let tombs = 0, fwd = 0;
    // A tombstone goes with the obligation that delivered it (X1): tagged with
    // the ended generation, or untagged when the ended appointment was legacy
    // (no generation to tag with). A deletion the live attachment delivered
    // stays and keeps suppressing its body.
    // An obligation's end removes ITSELF from each tombstone's owner set and
    // drops the record only when no owner remains (Aster ef073745: the same
    // deletion delivered through two live duties stays while either lives).
    // An untagged record (no owners) belongs to a legacy appointment.
    const dropOwned = (owner) => {
      const key = owner ? `${owner.root}:${owner.epoch | 0}` : null;
      let n = 0;
      for (const [id, tb] of role.tombstones) {
        if (tb.owners && tb.owners.size) {
          if (key && tb.owners.has(key)) { tb.owners.delete(key); if (tb.owners.size === 0) { role.tombstones.delete(id); n++; } }
        } else if (!key) { role.tombstones.delete(id); n++; }
      }
      return n;
    };
    if (appointmentMatch) {
      const ownerGen = role.standbyGen ? { ...role.standbyGen } : null;
      this._rootClaim.retireBackup(t, role, 'terminated');
      role.standbyGen = null;
      tombs += dropOwned(ownerGen);
    }
    if (attachmentMatch) {
      if (role) {
        // A CHILD forwards the notice to its own seats, then drops its list.
        for (const [h, s] of role.subscribers) {
          if (h === me || (s && s.kind === 'standby')) continue;
          try { this._route(idBig(h), T.TOPIC_TERMINATED, { topicId: payload.topicId, from: idHex(this.nodeId), gen }); fwd++; } catch { /* best-effort */ }
        }
        if (fwd) this._log('info', 'terminated-forwarded', { topic, seats: fwd });
        role.subscribers.clear(); role.children.clear();
        role.upstreamHeld = new Set(); role.upstreamHeldFrom = null;
        tombs += dropOwned(att);                                              // the attachment's deletions go with it (X1)
      }
      this._attach.delete(t);
      this._upstream.delete(t);
      const b = this._rootBeacons.get(t);
      if (b && lc(b.root) === gen.root) this._rootBeacons.delete(t);   // the ended root's advert is a ghost now
      this._rootHint.delete(t);
      if (this.mySubscriptions.has(t)) this._endSubscriptionTerminated(t, gen);
    }
    if (role) {
      // The role object goes only when no live duty remains on it: not a backup
      // of another generation, not the relay of a reader attached elsewhere.
      const liveReader = this.mySubscriptions.has(t);
      const stillBackup = role.backupOf !== null;
      if (!stillBackup && !liveReader && role.subscribers.size === 0) {
        tombs += role.tombstones.size;
        this._retireRole(t, role, 'terminated');
        this._log('info', 'role-terminated', { topic, nature: appointmentMatch ? 'backup' : 'child', tombstonesDropped: tombs, appointment: appointmentMatch, attachment: attachmentMatch });
      } else {
        this._log('info', 'obligation-terminated', { topic, appointment: appointmentMatch, attachment: attachmentMatch, tombstonesDropped: tombs, keeps: stillBackup ? 'backup' : (liveReader ? 'reader' : 'seats') });
      }
    }
    return 'consumed';
  },

  // A READER's kernel ends the app's subscription for the generation and
  // delivers one row `{ terminated: true, generation }`. Ended, not paused: an
  // app that still wants the topic subscribes again and forms the next
  // generation. This is the one place the app sees anything. Exactly once per
  // generation.
  _endSubscriptionTerminated(t, gen) {
    const key = t.toString(16) + ':terminated:' + gen.root + ':' + gen.epoch;
    if (!this._appDelivered.has(key)) {
      this._appDelivered.set(key, true);
      if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
      if (this._deliveryCallback) {
        try { this._deliveryCallback(t, JSON.stringify({ terminated: true, msgId: null, topic: null, generation: gen }), null, this._now(), null); }
        catch (e) { this._log('warn', 'terminated-callback-threw', { err: e?.message }); }
      }
    }
    this.mySubscriptions.delete(t);
    this._coldSteerGen?.delete(t);
    this._upstream.delete(t);
    this._unattachedSince.delete(t);
    this._attach.delete(t);
    this._subscriptionsEnded++;
    this._log('info', 'subscription-terminated', { topic: idHex(t).slice(0, 12), gen: gen.root.slice(0, 10), epoch: gen.epoch });
  },

  // ── 4.7 the thin branch ───────────────────────────────────────────────
  _thinBranchSweep(now) {
    for (const [t, role] of [...this.axonRoles]) {
      if (role.isRoot || role.backupOf !== null || this._backupTopics.has(t) || role.readHolder) continue;
      if (role.subscribers.size === 0) continue;                       // not a relay
      const demand = this._roleDemand(role);
      if (demand >= this._childMinReaders) { role.thinSince = null; continue; }
      if (role.thinSince == null) { role.thinSince = now; continue; }
      if (now - role.thinSince <= this._childStepDownMs) continue;          // strictly greater, like the seat lease
      if (now - (role.createdAt || 0) <= this._childMinStayMs) continue;   // a new child is not thin yet
      const up = (this._upstream.get(t) || [])[0] || null;
      if (!up || up === lc(idHex(this.nodeId)) || !this._isReachableId(up)) continue;   // root gone or unknown: not a step-down (4.4(1))
      if (this._custodyOpen(role, now)) continue;
      // BODY CUSTODY (Aster c5adedec TL-B02; packet 4.7 "a body the root lacks").
      // A child steps down only when every body it holds is one its upstream
      // SENT it (role.upstreamHeld, stamped in _onDeliver): those the upstream
      // holds by construction. A body that arrived from below (a reader's
      // REPLAYUP) is held here until the root has pulled it and fanned it back,
      // which marks it; until then the branch keeps its custody and stands.
      if (role.cache.some((c) => !role.upstreamHeld || !role.upstreamHeld.has(c.msgId))) {
        if (!role._heldBodyLogged) { role._heldBodyLogged = true; this._log('info', 'step-down-held-body', { topic: idHex(t).slice(0, 12), bodies: role.cache.length }); }
        continue;
      }
      this._stepDownChild(t, role, up, now, demand);
    }
  },
  _stepDownChild(t, role, up, now, demand) {
    const me = lc(idHex(this.nodeId));
    let n = 0;
    for (const [h, s] of role.subscribers) {
      if (h === me || (s && s.kind === 'standby')) continue;
      try { const ag = this._attachedGenOf(t); this._route(idBig(h), T.REHOME, { topicId: idHex(t), from: idHex(this.nodeId), upstream: up, ...(ag ? { gen: { ...ag } } : {}) }); n++; } catch { /* best-effort */ }
    }
    // Tell the parent now (a seat that lapses in DROP_MS would be re-promoted
    // meanwhile): an UNSUB marked steppedDown arms the parent's cooldown.
    try { this._send(T.UNSUB, { topicId: idHex(t), via: [up], subscriberId: idHex(this.nodeId), steppedDown: true }); } catch { /* best-effort */ }
    this._leftWrite(t, { rootId: null, epoch: 0, upstream: up, why: 'stepped-down' });
    const thinMs = now - (role.thinSince ?? now);
    this._retireRole(t, role, 'stepped-down');
    this._rolesSteppedDown++;
    // This node's OWN reader subscription, if any, re-seats at the root on its fast cadence.
    const s = this.mySubscriptions.get(t);
    if (s) { s.interval = this.renewFastMs; s.lastRenewSent = null; }
    this._log('info', 'child-stepped-down', { topic: idHex(t).slice(0, 12), demand, rehomed: n, thinMs, upstream: up.slice(0, 10) });
  },

  // REHOME, received: kernel-internal. Only from this node's upstream. The
  // kernel keeps the app's subscription and walks to the topic again on its
  // fast cadence; the app sees at most one renewal's gap. Ends nothing.
  _onRehome(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    if (!this._topicLifetime) return 'consumed';
    let t; try { t = idBig(payload.topicId); } catch { return 'consumed'; }
    const from = (typeof payload.from === 'string' && isHexId(lc(payload.from))) ? lc(payload.from) : null;
    const up = (this._upstream.get(t) || [])[0] || null;
    const topic = idHex(t).slice(0, 12);
    if (!from || !up || up !== from) { this._log('info', 'rehome-ignored', { topic, why: 'not-my-upstream', from: from ? from.slice(0, 10) : null }); return 'consumed'; }
    const role = this.axonRoles.get(t);
    if (role?.isRoot) return 'consumed';
    // Generation binding (TL-B01): a REHOME naming a generation other than the
    // one this node is attached to is dropped; an unversioned REHOME from the
    // upstream still re-homes (the older-kernel case).
    const attR = this._attachedGenOf(t);
    const rg = this._genValid(payload.gen) ? this._genNorm(payload.gen) : null;
    if (rg && attR && !this._sameGen(attR, rg)) { this._log('info', 'rehome-ignored', { topic, why: 'foreign-generation', gen: rg.root.slice(0, 10), epoch: rg.epoch }); return 'consumed'; }
    const me = lc(idHex(this.nodeId));
    const next = (typeof payload.upstream === 'string' && isHexId(lc(payload.upstream)) && lc(payload.upstream) !== me) ? lc(payload.upstream) : null;
    if (next) this._upstream.set(t, [next]); else this._upstream.delete(t);
    if (role) { role.upstreamHeld = new Set(); role.upstreamHeldFrom = null; }   // custody evidence does not survive an upstream change (TL-B05)
    const s = this.mySubscriptions.get(t);
    if (s) { s.interval = this.renewFastMs; s.lastRenewSent = null; }
    if (role?.sync) role.sync.lastRenewAt = null;
    this._rehomed++;
    this._log('info', 'rehomed', { topic, from: from.slice(0, 10), to: next ? next.slice(0, 10) : null });
    this._sendSubscribe(t);
    return 'consumed';
  },

  _lifetimeInspect() {
    return {
      armed: !!this._topicLifetime,
      terminated: this._rolesTerminated | 0,
      steppedDown: this._rolesSteppedDown | 0,
      standbyLapsed: this._standbyLapsed | 0,
      grantsRefused: this._grantsRefused | 0,
      rehomed: this._rehomed | 0,
      subscriptionsEnded: this._subscriptionsEnded | 0,
      leftRecords: this._leftRecords ? this._leftRecords.size : 0,
      cooldowns: this._childCooldown ? this._childCooldown.size : 0,
    };
  },
};

export default topicLifetimeMethods;
