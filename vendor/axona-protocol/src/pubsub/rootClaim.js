// =====================================================================
// rootClaim.js — the root-claim state machine (refactor Phase 1).
//
// EVERY flip of `role.isRoot` in the kernel goes through this module: one
// transition function (`_set`), one decision table of guards, one structured
// `root-transition` log line per flip. Before this existed, root-ness was
// decided at ~8 independent sites in AxonaManager and every incident added a
// guard at another site (the patch-interaction bug class —
// axona-docs/architecture/Kernel-Refactor-Analysis-v0.1.md §2A). The rules
// themselves are unchanged; they now live in one place.
//
// The machine holds NO state of its own. Role objects stay in
// manager.axonRoles; beacons/upstream/hints stay on the manager (they move in
// Phase 2). What lives here is the AUTHORITY: who may claim, when a claim
// defers, and the enforced invariants (INVARIANTS.md):
//   I-1  a topic has exactly one root; wrong claims converge without flapping
//   I-2  never defer to a farther node, a ghost, or the node handing off
//
// Transitions (why-codes appear in the root-transition log):
//   become(t, why)            — create the role AS root (routing-terminal fallback)
//   promote(role, p, meta)    — terminal promotion of an existing non-root role
//   demote(t, toHex, why)     — yield the claim to a strictly-closer live root,
//                               pin upstream to it, and re-subscribe under it
//   claimReachable(t)         — reachable-root fallback (unconfirmed deferral
//                               window expired; self is closest REACHABLE)
//   adoptChild(t, parentHex)  — ADOPT: become a non-root child relay
//   handoffArrived(t, leaver) — departing root handed us its history: purge the
//                               leaver's ghost beacon, never defer back to it
//   holdFor(t)                — the step-down hold (4.102.0): after demote(), the
//                               node will not retake the root by promote() or
//                               claimReachable() until STEPDOWN_HOLD_MS passes
// =====================================================================

import { asId } from '../utils/hexid.js';

const idHex = (big) => big.toString(16).padStart(66, '0');
const idBig = asId;
const lc    = (s) => String(s ?? '').toLowerCase();

// ── Role NATURES (v4.26.0, Phase 7 of the v0.2 program) ────────────────────
// A role acts in exactly one PRIMARY nature — ROOT, BACKUP, or CHILD — plus an
// orthogonal HOLDER flag (hosted / app-subscribed retention). The natures are
// DERIVED from the ground facts (isRoot, backupOf), never stored separately:
// a stored copy would drift, and drift here is the #333 bug class (a BACKUP
// whose principal was dead — a state nobody had modeled — self-perpetuated
// into the backbone collapse). Each nature carries obligations and a named
// eviction path; the table is normative in Axona-Architecture §VIII and
// enforced by smoke_role_natures.mjs.
//
//   ROOT    stamps, beacons, verifies, replicates, serves    → demote / idle sweep
//   BACKUP  holds warm copy; subscribes as election standby  → principal gone + re-homed + BACKUP_EVICT_MS
//   CHILD   renews upstream, re-fans down once               → subscriber loss + idle sweep
//   +HOLDER renews; advertises hw                            → unhost / unsub / TTL
export const NATURES = Object.freeze(['root', 'backup', 'child']);

export function roleNature(role) {
  if (role.isRoot) return 'root';
  if (role.backupOf !== null) return 'backup';
  return 'child';
}

/** A relay's per-topic state (root or non-root child relay). */
// dual-root packet 4(d) (axona-docs 14a6bd8): per-role merge state. Allocated
// lazily; a node that is not merge-capable never touches it.
export function makeMergeState() {
  return {
    releaseToken: null,     // minted by the merge's step 5; _set consumes it on the ROOT→non-ROOT flip
    nextCandidate: null,    // the last refused flip {why, ctx, at}: what the machine reconciles toward
    candidate: null,        // accepted winner record {id, epoch, source, at} (d4 arms the hold toward it)
    gate: null,             // (d2) the cut: {winner, winnerEpoch, corr, at} while this role forwards writes
    gateFwd: new Map(),     // (d2) gateFwd nonce -> {at, type, corr}: forwards whose return must not be stamped
    corr: null,             // (d3) the live exchange's correlation id
    pendingDelegation: new Map(), // (d1) ADOPT nonce -> {subs:[hex], at}: subscribers delegated but unacked
    pins: new Set(),        // (d3) msgIds exempt from cap eviction and expiry: W's baseline + manifest for a round's life; L's export after the flip
    reserve: null,          // (d3, W) {entries, bytes} left for W's own writes during a round; null = no round
    completing: null,       // (d3, L) {corr, winner, winnerEpoch, loserEpoch, hash} after the committed flip until MERGE_CLOSED; rides every SUB
    acceptedWinner: null,   // (d4, L) {id, epoch, at, exp}: the hold toward W regardless of closeness
  };
}

export function makeRole(topicId, isRoot, createdAt = null) {
  return {
    topicId,                         // bigint
    createdAt,                       // _now() when this role was admitted (D0 / M4).
                                     // Needed because "never discharged" is only innocent for a role
                                     // that is NEW. A role held for longer than its obligation's
                                     // deadline and no stamp at all (null) has never been serviced,
                                     // which is the WORST case, not an exempt one — capacity measures
                                     // age from birth until the first discharge, then from the stamp.
                                     // Caught by smoke_role_admission.mjs, which builds 96 roles that
                                     // were never serviced and rightly expects saturation.
    isRoot,                          // closest-to-topic node → true; delegated child → false
    epoch: 0,                        // root INCARNATION (Dead-Root Eviction v0.3): minted as
                                     // knownEpoch(topic)+1 every time this role becomes root, 0 while
                                     // not root. Carried in beacons; orders competing/returning claims
                                     // for the same seat (higher epoch wins reconciliation, E4). A
                                     // node that has never heard of the topic mints epoch 1.
    subscribers: new Map(),          // subHex -> { since, lastRenewed }
    children: new Set(),             // subHex of subscribers that are themselves child relays
    cache: [],                       // [{ msgId, publishTs, json, bytes }] asc by publishTs
    cacheIds: new Set(),             // msgId set for O(1) dedup (root-stamp + relay re-fan)
    cacheBytes: 0,
    emptySince: null,                // _now() of the first refreshTick that found the role holding NOTHING
                                     // (no bodies/tombstones/custody/seats/backup/lease/intent); null while it
                                     // holds anything. The empty-role grace is measured from it (4.116.0).
    lastTs: 0,                       // highest stamp emitted (monotonic; root authority)
    seq: 0,                          // DENSE per-topic counter (root authority): ++ per emitted
                                     // message AND kill, so subscribers detect GAPS (env.seq jumps
                                     // ⇒ a message was missed). Distinct from the time-floored
                                     // publishTs (which is monotonic but sparse). Recovered to the
                                     // max seen seq on replay-up/handoff so a new root continues densely.
    tombstones: new Map(),           // msgId -> { exp, killTs, signer, seq }  (kill; thin)
    // The per-topic CONVERGENCE LEDGER (v4.25.0, Phase 6): every guard the data
    // plane keeps about "what have I already exchanged, with whom" lives here —
    // one place, engine semantics (per-pair memory + quench), the seed of the
    // Phase-8 sync engine. Fields were previously scattered as five role-level
    // properties; the 4.22.0 lw-pull storm lived in exactly this kind of guard.
    sync: {
      sig: '',                       // (when ROOT) state signature at the last FULL replica push (4.24.1 delta gate)
      lastFullAt: null,              // (when ROOT) _now() of the last FULL push; null = never (NOT 0 — see C2) (backstop re-arms at ROOT_REPLICATE_FULL_MS)
      lastRenewAt: null,                // _now() of the last SUB actually EMITTED for this role (D0 / M4).
                                     // Completion point for the CHILD / BACKUP / HOLDER obligations — stamped in
                                     // _emitSubscribe AFTER the send, not before, so it records work done rather
                                     // than work attempted. Deadline is DROP_MS: past it the upstream has evicted us.
      lastServicedAt: 0,             // DIAGNOSIS ONLY since D0 (2026-07-30) — drives nothing.
                                     // Was the capacity input, stamped on every role at the TOP of refreshTick
                                     // before any work, which made it mean "a tick began" rather than "the
                                     // obligation was discharged". It read 0 while a role sat 95s past its own
                                     // 60s deadline. Retained (unstamped by the tick) only so a reader diffing
                                     // against an older kernel can see the field is deliberately dead rather than
                                     // accidentally missing. See OBLIGATIONS in constants.js.
      probeTries: 0,                 // empty-self-root cohort pulls fired (4.24.0; quenches at EMPTY_ROOT_PROBE_MAX)
      probeAt: 0,                    // _now() of the last cohort pull (rate-limits the refreshTick re-probe)
      // dual-root packet (a) (axona-docs 14a6bd8): a try whose every pull resolved
      // consumed:false is DEFERRED, not counted — the 2026-10-17 Windows relay spent
      // its three tries inside the 20 s after its restart while its routed sends
      // were failing 6–12 per tick, then never probed again. Deferred tries re-arm
      // from the sweep once the node can route (servingCapable) or after
      // 4 × EMPTY_ROOT_PROBE_INTERVAL_MS, at most EMPTY_ROOT_PROBE_MAX deferrals.
      probeDeferred: false,          // the last try reached nobody; the sweep re-arms it
      probeDeferrals: 0,             // deferred tries so far (bounded by EMPTY_ROOT_PROBE_MAX)
      probeDeferredAt: 0,            // _now() of the last deferral
      probeInflight: false,          // a try is awaiting its verdicts; the sweep does not stack another
      openMerges: new Map(),         // (d3, L) corr -> {winner, winnerEpoch, loserEpoch, state, snapshot, manifest, lastCursorSent, offeredAt, boundAt, tB, timing}
      openPulls: new Map(),          // (d3, W) corr -> {loser, loserEpoch, cursor, framesIn, held, sent, startedAt, timing, state, manifest, header, tS}
      pulledLw: new Map(),           // subHex -> lowest lw already PULLUP'd from that child (4.22.1:
                                     // a refused pull — e.g. the child's oldest is tombstoned here —
                                     // must not re-fire on every renewal; re-arm only if lw DECREASES)
    },
    // dual-root packet 4(d): the merge machine's per-role state; null until the
    // first candidate, gate, pin or token touches it (makeMergeState()).
    merge: null,
    // dual-root packet 4(d): the cut generation. Incremented by the cut in the
    // same synchronous run that sets the gate; never decremented. Commit paths
    // capture it before their first await and re-check it at the mutation.
    gen: 0,
    replicas: new Map(),             // (when ROOT) backupHex -> { at }  nodes holding a warm copy of our cache
    backupOf: null,                  // (when BACKUP) hex of the root replicating to us; null if we're not a backup
    lastReplicaAt: 0,                // (when BACKUP) _now() of the last replica push from our root (staleness → presume root gone)
    metricsOn: 0,                    // (when ROOT) lease expiry ts; while > now, this root publishes snapshots to metricTopic(T)
    metricsLastPub: 0,               // _now() of the last metric snapshot we published (throttle to METRICS_PUB_MS)
  };
}

export class RootClaim {
  /** @param mgr the AxonaManager (state owner); beaconMs = BEACON_MS (corpse-freshness cut) */
  constructor(mgr, { beaconMs = 20_000 } = {}) {
    this.m = mgr;
    this._beaconMs = beaconMs;
  }

  // ────────────────────────── the decision table (guards) ──────────────────────────

  // A live root beacon naming a STRICTLY closer node than self means this node
  // must not (re)take the topic's root — stranded terminal traffic defers to that
  // root instead. This breaks the flap loop observed on the prod relay backbone
  // (4.19.0). `requireReachable` (the default) additionally demands the beaconed
  // root be a DIRECT, authenticated neighbour — when a root dies its channel
  // drops and the gate opens instantly, so backup promotion on churn is never
  // stalled by a stale beacon. The PUB/KILL last-mile correction uses the looser
  // TTL+freshness semantics (requireReachable:false): a publish is one
  // self-healing message, and that looser gate is what fixed cold-publish
  // discovery. Liveness evidence, strongest first: `verified` (network-confirmed
  // by an iterative lookup, 4.19.1), channel-verified neighbour, or a beacon
  // heard within 1.5×BEACON_MS (a DEAD root's beacon goes silent — without this
  // freshness cut a stranded publish ping-pongs toward the corpse until the full
  // TTL; the pre-4.19 latent loop, smoke_root_reconcile phase 4).
  liveCloserRoot(topicBig, { requireReachable = true } = {}) {
    const m = this.m;
    const b = m._rootBeacons.get(topicBig);
    if (!b || m._now() >= b.exp) return null;
    if (b.root === lc(idHex(m.nodeId))) return null;
    let rb; try { rb = idBig(b.root); } catch { return null; }
    if ((rb ^ topicBig) >= (m.nodeId ^ topicBig)) return null;   // never defer to a farther node (I-2)
    // E3 tombstone: a convicted incarnation's beacon is the corpse's ghost.
    // Epoch-bounded — a HIGHER epoch from the same node is a legitimate
    // rebirth and passes (the E4 adopt path mints it).
    if (m._rootTombstoned?.(topicBig, b.root, b.epoch)) return null;
    // A verified record used to return HERE unconditionally — no liveness test,
    // no freshness cut, bounded only by exp (2×ROOT_VERIFY_MS = 90s, 3× the
    // remote-beacon window). Verification proves the root WAS the terminus at
    // lookup time, not that it is alive now, and during the 2026-08-02 outage
    // one such record steered every SUB and PUB into a dead relay for its full
    // life (fence_pub_defers_to_corpse §3; council seq 146/147, option B).
    // The same 1.5×BEACON_MS cut the loose clause has applies now. Safe for
    // live roots: their plain beacons overwrite this record within ≤20s anyway
    // (rootElection.js:63 sets unconditionally) — only a DEAD root leaves a
    // verified record standing long enough to go stale. A stale record still
    // falls through to the reachability test rather than to null: a root that
    // is a live neighbour deserves the defer regardless of the record's age.
    if (b.verified && (m._now() - b.at) < this._beaconMs * 1.5) return b.root;
    if (m._isReachableId(b.root)) return b.root;                 // channel-verified live neighbour
    if (!requireReachable && (m._now() - b.at) < this._beaconMs * 1.5) return b.root;
    return null;
  }

  // True iff this node has NO non-bridge neighbours — it can't route to anyone,
  // so any "terminal at self" verdict is an artifact of isolation, not of
  // closeness (the alone-in-the-dark guard, 4.19.2). When the transport doesn't
  // expose neighbours, assume meshed (never block).
  meshBare() {
    const m = this.m;
    if (typeof m.dht.neighbors !== 'function') return false;
    let bridge = null;
    try { const b = (typeof m.dht.bridgeId === 'function') ? m.dht.bridgeId() : null; bridge = (b != null) ? idBig(b) : null; } catch { /* */ }
    for (const n of (m.dht.neighbors() || [])) {
      let nb; try { nb = idBig(n); } catch { continue; }
      if (bridge === null || nb !== bridge) return false;   // any routable non-bridge neighbour → meshed
    }
    return true;
  }

  // True iff `self` is XOR-closest to `tBig` among the nodes we can ACTUALLY
  // reach (self + direct neighbours), excluding the bridge (signaling infra,
  // never a topic root). The iterative hint may name a node closer in XOR, but
  // if it never adopts us it is effectively unreachable; when self beats every
  // reachable neighbour, self is the best reachable root. Pure local read.
  selfClosestReachable(tBig) {
    const m = this.m;
    const bridge = (typeof m.dht.bridgeId === 'function') ? m.dht.bridgeId() : null;
    const bestD = m.nodeId ^ tBig;
    if (typeof m.dht.neighbors === 'function') {
      for (const n of (m.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        if (nb === m.nodeId || (bridge != null && nb === bridge)) continue;
        if ((nb ^ tBig) < bestD) return false;     // a reachable neighbour is closer → route to it
      }
    }
    return true;
  }

  // ────────────────────────── the transition function ──────────────────────────

  // ── Part 3 of the dual-root packet (axona-docs 14a6bd8): ONE election relation ──
  // A claim is (id, epoch). Between two live claims the HIGHER epoch wins; at
  // equal epoch the XOR-closer to the topic wins. Lexicographic, so total and
  // pairwise consistent: for any two live claims exactly one wins, from either
  // side's view. The yield predicate at rootElection.js (`supersedesMine ||
  // closer`) is two yields and no winner — a closer node at a lower epoch yields
  // to a farther one at a higher epoch, which yields back on closeness — and a
  // merge run on it crosses offers forever. Tombstoned claims are OUT before the
  // comparison (the caller's job: tombstones order rejection, this orders
  // adoption). Epoch before closeness keeps E4; closeness alone re-elects a
  // stale incarnation whenever it is nearer.
  //
  // UNVERSIONED (epoch 0) on EITHER side → closeness alone, the kernel's
  // standing mixed-version rule (rootElection.js: "the whole rule whenever
  // either side is unversioned"). The paths that do not learn the winner's
  // epoch — verify, defer, handoff — pass null and are read as 0. Among
  // VERSIONED claims the relation is a total order (antisymmetric,
  // transitive); with an unversioned claim in a triple it is pairwise
  // antisymmetric only, which is why an unversioned node is never a merge
  // participant (the legacy fallback, packet 4(d)).
  static claimWins(topicBig, a, b) {
    const ea = Number.isInteger(a?.epoch) ? a.epoch : 0;
    const eb = Number.isInteger(b?.epoch) ? b.epoch : 0;
    if (ea > 0 && eb > 0 && ea !== eb) return ea > eb;
    return (idBig(a.id) ^ topicBig) < (idBig(b.id) ^ topicBig);
  }
  /** Does the claim {id, epoch} win over THIS node's claim on the topic? */
  wins(topicBig, claim) {
    const role = this.m.axonRoles.get(topicBig);
    const mine = { id: idHex(this.m.nodeId), epoch: role?.isRoot ? (role.epoch | 0) : 0 };
    return RootClaim.claimWins(topicBig, claim, mine);
  }

  // CUSTODY (Part 3): a role holds custody while it holds a body or an active
  // tombstone. A root with custody sheds its claim only through the merge
  // machine's step 5, which mints the release token the flip below demands.
  hasCustody(role) {
    if (!role) return false;
    if (role.cache && role.cache.length) return true;
    try { return (this.m._activeDels?.(role) || []).length > 0; } catch { return false; }
  }

  // The ONLY place role.isRoot changes. One structured log per flip.
  //
  // RETURNS {committed, reason} (dual-root packet 4(d), "the transition is a
  // transaction"). Every caller branches on it: a refused flip runs none of
  // the side effects that used to follow it unread — demote() pinned upstream,
  // armed the hold and sent the SUB; adoptChild() pinned upstream; _onAdopt
  // then accepted delegated subscribers. The refusal: on a MERGE-CAPABLE node,
  // a ROOT→non-ROOT flip of a role WITH CUSTODY is refused unless ctx carries
  // the release token the merge's step 5 minted. That one gate covers every
  // path that sheds a claim, including ADOPT, which never calls demote().
  // A node that is not merge-capable flips as 4.114.1 did.
  _set(role, isRoot, why, ctx = {}) {
    if (role.isRoot === isRoot) return { committed: true, reason: 'noop' };
    if (!isRoot && this.m._mergeCapable && this.hasCustody(role)) {
      const tok = role.merge?.releaseToken;
      if (!tok || ctx.releaseToken !== tok) {
        if (!role.merge) role.merge = makeMergeState();
        role.merge.nextCandidate = { why, ctx: { ...ctx }, at: this.m._now() };
        this.m._log('info', 'custody-refused', { topic: idHex(role.topicId).slice(0, 12), why, to: ctx.to ?? null, cache: role.cache.length });
        return { committed: false, reason: 'custody' };
      }
      role.merge.releaseToken = null;                          // consumed by this one flip
    }
    // Root INCARNATION mint (v0.3): every entry to the ROOT nature takes the
    // next epoch for this seat. Minted BEFORE the flip so the announce that
    // follows any promotion carries the new incarnation, never a stale one.
    if (isRoot) role.epoch = (this.m._knownEpoch?.(role.topicId) ?? 0) + 1;
    if (isRoot) this.m._stepDownHold?.delete(role.topicId);   // a root holds no step-down
    // Nature hygiene (v4.26.0, Phase 7): a role flipping to ROOT sheds any
    // BACKUP residue in the same transition. Before this, promote() left
    // backupOf + the _backupTopics membership in place forever — a ROOT
    // wearing BACKUP state, exactly the unmodeled-residue class the natures
    // exist to make impossible. (If the root later demotes it re-enters
    // BACKUP only through a fresh REPLICATE from the new live principal.)
    if (isRoot && role.backupOf !== null) this.retireBackup(role.topicId, role, 'promoted');
    role.isRoot = isRoot;
    // OBSERVABILITY (#60): a transition is the moment a seat changes hands, and
    // the one question it must answer is whether anything was seated beneath it.
    // role.subscribers and role.children already hold that; nothing logged them,
    // so production could not tell an empty yield from one that abandoned a
    // subtree. The relay status line's subs= is mySubscriptions (this node's OWN
    // subscriptions) and never measured this — an exclusion drawn from it on
    // 2026-09-07 was wrong for exactly that reason. Counts only: no ids, no
    // sizes that could correlate a subscriber to a topic beyond what the
    // transition already names.
    const seatedSubs = role.subscribers?.size ?? 0;
    const seatedKids = role.children?.size ?? 0;
    const { releaseToken: _rt, ...logCtx } = ctx;               // the token is not a log field
    this.m._log('info', 'root-transition',
      { topic: idHex(role.topicId).slice(0, 12), isRoot, why, subs: seatedSubs, kids: seatedKids, ...logCtx });
    // SUBSCRIBER-LIST REPLICATION: on PROMOTION, re-adopt the principal's inherited
    // subscribers into the fanout and replay the cache to each, so the orphaned
    // readers are served immediately instead of waiting ~9s to re-subscribe (the
    // measured post-migration orphan window). Inherited list is dropped after use.
    if (isRoot && this.m._replicateSubs && role._inheritedSubs && role._inheritedSubs.size) {
      const now = this.m._now();
      let seeded = 0;
      for (const [subHex, info] of role._inheritedSubs) {
        if (role.subscribers.has(subHex)) continue;
        role.subscribers.set(subHex, { since: info.since || 0, lastRenewed: now });
        if (info.child) role.children.add(subHex);
        seeded++;
        try { this.m._replayTo(role, subHex, info.since || 0, true); } catch { /* best-effort */ }
      }
      role._inheritedSubs = null;
      if (seeded) this.m._log('info', 'root-inherited-subs', { topic: idHex(role.topicId).slice(0, 12), seeded });
    }
    // METRICS-LEASE INHERITANCE (#47): a promoted backup adopts the principal's
    // active publish lease so metricTopic(T) snapshots continue without waiting on a
    // METRICSON renewal (which a lingering dead-root can swallow). Complements the
    // path-node seed in promote() (_metricsWanted) — that only covers nodes the
    // METRICSON transited; a replicated backup was never on that path. Take the
    // later expiry so neither source clobbers a fresher lease.
    if (isRoot && this.m._replicateSubs && role._inheritedMetricsOn) {
      const now = this.m._now();
      if (role._inheritedMetricsOn > now && role._inheritedMetricsOn > (role.metricsOn || 0)) {
        role.metricsOn = role._inheritedMetricsOn;
        this.m._log('info', 'root-inherited-metrics', { topic: idHex(role.topicId).slice(0, 12), until: role.metricsOn });
      }
      role._inheritedMetricsOn = 0;
    }
    return { committed: true, reason: why };
  }

  // ── BACKUP nature transitions (v4.26.0, Phase 7) ──────────────────────────
  // Every entry to / exit from the BACKUP nature passes through these two
  // methods — the same single-transition-site discipline rootClaim gave
  // isRoot in Phase 1, extended to the nature whose unmodeled residue caused
  // #333. One structured `role-nature` log per genuine transition (a replica
  // refresh from the SAME principal is bookkeeping, not a transition).
  becomeBackup(topicBig, role, principalHex) {
    const m = this.m;
    const entering = role.backupOf === null;
    const rePrincipaled = !entering && role.backupOf !== principalHex;
    role.backupOf = principalHex;
    role.lastReplicaAt = m._now();
    m._backupTopics.add(topicBig);
    if (entering || rePrincipaled) {
      m._log('info', 'role-nature', {
        topic: idHex(topicBig).slice(0, 12), nature: 'backup',
        principal: String(principalHex).slice(0, 10),
        why: entering ? 'replicate' : 're-principaled',
      });
    }
  }

  retireBackup(topicBig, role, why) {
    const m = this.m;
    if (role.backupOf === null && !m._backupTopics.has(topicBig)) return;
    role.backupOf = null;
    m._backupTopics.delete(topicBig);
    m._log('info', 'role-nature', {
      topic: idHex(topicBig).slice(0, 12), nature: role.isRoot ? 'root' : 'child', why,
    });
  }

  // Create the role AS root — the routing-terminal fallback (the node closest
  // to the topic id acts as its emergent root). formedAt/lastVerify arm the
  // early self-verification (a fresh claim is checked at ROOT_VERIFY_FIRST_MS).
  become(topicBig, why = 'terminal') {
    const m = this.m;
    // ADMISSION (v4.46.0) — S1: one transition site, so one gate.
    //
    // hasAlternative=false is DELIBERATE and load-bearing. Every caller of
    // become() is a routing TERMINUS: routing has already decided that nobody is
    // closer. Refusing here would leave the SUB/PUB/KILL with nowhere to go, so
    // a SOFT reason (not-seated / saturated / paced) is FLOORED — admitted and
    // logged 'admitted-despite'. The value at terminal sites is the honest log
    // plus the HARD bridge fence, not refusal.
    //
    // Real refusal lives where the role is PUSHED and the pusher can re-pick:
    // syncEngine HANDOFF (see there). That asymmetry is the design, not an
    // oversight — a terminus that refuses drops data.
    if (!m.admitRole(topicBig, false)) return null;    // HARD only (bridge)
    const role = makeRole(topicBig, true, m._now());
    role.epoch = (m._knownEpoch?.(topicBig) ?? 0) + 1;   // incarnation mint (v0.3) — born-as-root site
    role.formedAt = m._now(); role.lastVerify = 0;
    m.axonRoles.set(topicBig, role);
    m._log('info', 'root-formed', { topic: idHex(topicBig).slice(0, 12) });
    // Same seated counts as _set (#60). A born root has nothing seated yet, but
    // the fields are emitted as 0 rather than omitted: a consumer parsing
    // root-transition must not have to distinguish "no subtree" from "this
    // emission site forgot to say". This is the SECOND transition emitter —
    // _set is the other — and they must agree on shape.
    m._log('info', 'root-transition', { topic: idHex(topicBig).slice(0, 12), isRoot: true, why,
      subs: role.subscribers?.size ?? 0, kids: role.children?.size ?? 0, born: true });
    m._announceRoot(topicBig);
    // Empty-self-root cohort pull (v4.24.0): a root born with no history must
    // PULL from whoever holds it — nothing reliably tells the holder about a
    // new closer root (the alert-bot read-miss mechanism: reader isRoot,
    // cacheSize:0, sticky). Delayed: a pub-terminal root's own publish fills
    // the cache before the probe fires, so a genuinely-fresh topic stays quiet.
    m._scheduleEmptyRootProbe?.(topicBig);
    return role;
  }

  // I am the root for a topic iff I am the routing terminus for its bare id.
  // A non-root relay that becomes the closest node (e.g. after the old root
  // dies) is promoted here — gated on the closer-live-root defer (a beaconing
  // closer root is never contested). Inherits an active metrics lease so a
  // METRICSON that passed through this node resumes publishing on promotion.
  promote(role, payload, meta) {
    const m = this.m;
    const viaEmpty = !(Array.isArray(payload.via) && payload.via.length);
    if (!(viaEmpty && meta.isTerminal && !role.isRoot)) return;
    // R4: a terminal that cannot serve does not take the seat (same test as _becomeRoot).
    if (typeof m.dht?.servingCapable === 'function' && !m.dht.servingCapable()) return;
    if (this.liveCloserRoot(role.topicId)) return;   // a closer live root is beaconing — don't contest it
    if (this._holdBlocks(role.topicId, 'terminal-promote')) return;   // stepped down: hold the seat (4.102.0)
    this._set(role, true, 'terminal-promote');
    m._upstream.delete(role.topicId);
    m._announceRoot(role.topicId);
    role.formedAt = m._now(); role.lastVerify = 0;   // promoted claims get an early self-verify too
    const w = m._metricsWanted.get(role.topicId) || 0;
    if (w > m._now() && w > (role.metricsOn || 0)) role.metricsOn = w;   // take the later of path-flag vs replicated lease (#47)
  }

  // Yield the claim to a strictly-closer live root: demote, pin upstream to it,
  // and re-subscribe under it — the root must ADOPT us (register us as a
  // downstream child) or our subtree starves; every demotion is paired with a
  // confirming subscribe (the one-sided-link lesson: root subs=0, cache>0).
  // Returns false when there was no live root claim to yield (no-op).
  demote(topicBig, toHex, why, ctx = null) {
    const m = this.m;
    const role = m.axonRoles.get(topicBig);
    if (!role || !role.isRoot) return false;
    let to; try { to = idBig(toHex); } catch { return false; }
    if (to === m.nodeId) return false;               // never "demote toward self"
    // TRANSACTION (dual-root packet 4(d)): a refused flip runs nothing below —
    // no upstream, no hold, no SUB — and returns false. The caller that holds
    // custody is told by _set's log; the merge machine takes it from there.
    const flip = this._set(role, false, why, { to: toHex.slice(0, 10), ...(ctx || {}) });
    if (!flip.committed) return false;
    m._upstream.set(topicBig, [lc(toHex)]);
    // Step-down hold (4.102.0): having yielded to a STRICTLY CLOSER named root,
    // do not retake the seat by self-promotion until the hold expires. Only a
    // closer target arms it: an epoch-superseded demotion can yield to a FARTHER
    // node, and holding there let the true root and a spurious one each defer
    // to the other with neither allowed back — zero roots for the whole hold
    // (smoke_root_reconcile phase 5). The keyspace-closest node may always
    // reclaim; the hold binds only the node that the keyspace says should not
    // own the topic.
    // THE ACCEPTED-WINNER HOLD (dual-root packet 4(d), v0.10): a merge-capable
    // loser that shed its claim at step 5 holds toward W WHETHER OR NOT W IS
    // CLOSER — under epoch-first election a closer loser at a lower epoch would
    // otherwise re-promote on the next bare request, mint knownEpoch + 1 and
    // outrank the live winner at once. `ctx.holdAlways` is set only by step 5.
    if (ctx?.holdAlways || (to ^ topicBig) < (m.nodeId ^ topicBig)) {
      m._stepDownHold?.set(topicBig, { to: lc(toHex), at: m._now(), lastLog: 0 });
    }
    m._sendSubscribe(topicBig);
    return true;
  }

  // The step-down hold for a topic, or null. Expiry is lazy: the first read
  // after STEPDOWN_HOLD_MS deletes the hold and logs that the node may now
  // retake the root (its next claim mints an epoch above any it has heard).
  holdFor(topicBig) {
    const m = this.m;
    const h = m._stepDownHold?.get(topicBig);
    if (!h) return null;
    if (!(m._stepDownHoldMs > 0) || m._now() - h.at >= m._stepDownHoldMs) {
      m._stepDownHold.delete(topicBig);
      // The accepted-winner record expires WITH the hold (dual-root 4(d)): no
      // beacon from W refreshed it, so this node may promote on the next request.
      const role = m.axonRoles.get(topicBig);
      if (role?.merge?.acceptedWinner && role.merge.acceptedWinner.id === h.to) {
        role.merge.acceptedWinner = null;
        m._log('info', 'merge-record-expired', { topic: idHex(topicBig).slice(0, 12), winner: h.to.slice(0, 10) });
      }
      m._log('info', 'root-hold-expired', {
        topic: idHex(topicBig).slice(0, 12), to: h.to.slice(0, 10), heldS: Math.round((m._now() - h.at) / 1000),
      });
      return null;
    }
    return h;
  }

  // Where a held node may send a message it will not root: the held root, on
  // EXACTLY the evidence the existing closer-root gates accept — nothing looser,
  // nothing stricter (Aster dced6098: gate the hold consistently).
  //   requireReachable=true  (SUB): a live channel, or a FRESH VERIFIED record
  //   requireReachable=false (PUB/KILL): also a fresh beacon (< 1.5×BEACON_MS)
  // The loose window is the corpse window council 146/147 already accepted; a
  // failed forward deletes the record (_forwardToRoot), which bounds it. Null =
  // hold, send nothing: a send pinned to a root with no such evidence falls
  // back to topic-id routing and returns to this terminus (4.102.0).
  holdTarget(topicBig, { requireReachable = true } = {}) {
    const m = this.m;
    const h = this.holdFor(topicBig);
    if (!h) return null;
    // The accepted winner IS the target while its record is valid (dual-root
    // 4(d)): the keyspace terminus is this node, the owner is W, and a bare
    // request here is redirected to W by name for the record's life. The
    // forward's own verdict and the re-entry ledger bound a dead W.
    const aw = m.axonRoles.get(topicBig)?.merge?.acceptedWinner;
    if (aw && aw.id === h.to && aw.exp > m._now()) return h.to;
    if (m._isReachableId(h.to)) return h.to;
    const c = this.liveCloserRoot(topicBig, { requireReachable });
    return (c && lc(c) === h.to) ? h.to : null;
  }

  // True (and a rate-limited log) when the hold blocks a claim by `via`.
  _holdBlocks(topicBig, via) {
    const m = this.m;
    const h = this.holdFor(topicBig);
    if (!h) return false;
    if (m._now() - h.lastLog >= 60_000) {
      h.lastLog = m._now();
      m._log('info', 'root-hold', {
        topic: idHex(topicBig).slice(0, 12), to: h.to.slice(0, 10), via,
        remainingS: Math.round((m._stepDownHoldMs - (m._now() - h.at)) / 1000),
      });
    }
    return true;
  }

  // Reachable-root fallback (refreshTick): subscribed-but-unpinned past the
  // confirmation window (the iterative hint named a closer node that never
  // adopted us — unreachable / broken-but-authentic) AND no reachable neighbour
  // is closer → claim root locally rather than defer forever to an unreachable
  // node. A wrongly-claimed farther root self-corrects via the strictly-closer
  // beacon demotion.
  //
  // RETURNS NULL WHEN THE CLAIM IS REFUSED (v4.49.0). become() gained a nullable
  // return in 4.46.0 — the HARD bridge fence. Four of its five callers were
  // taught about it; this one was not, so `_set(null, …)` threw a TypeError.
  // refreshTick is driven by `this.refreshTick().catch(() => {})`, so the throw
  // was swallowed and every tick died at step 1 — no beacons, no replication,
  // no retries, no eviction sweep — silently, for the life of the process. Both
  // production bridges satisfied every precondition (neverRoot on by default,
  // directory topics subscribed in every bridge region). See F1 / N2.
  claimReachable(topicBig) {
    const m = this.m;
    // Step-down hold (4.102.0): null = refused, which leaves this node an
    // ordinary unattached subscriber renewing toward its upstream (see below).
    if (this._holdBlocks(topicBig, 'reachable-fallback')) return null;
    const role = m.axonRoles.get(topicBig) || this.become(topicBig, 'reachable-fallback');
    // Refused (today: only the bridge fence). Change NOTHING: leaving _upstream,
    // _rootHint and _unattachedSince intact keeps this node an ordinary
    // unattached subscriber, which is exactly what it is. The caller must not
    // read a null as "I am root now".
    if (!role) return null;
    this._set(role, true, 'reachable-fallback');
    m._upstream.delete(topicBig);
    m._rootHint.delete(topicBig);          // stop deferring to the unreachable hint
    m._unattachedSince.delete(topicBig);
    m._log('info', 'root-claimed-reachable', { topic: idHex(topicBig).slice(0, 12) });
    return role;
  }

  // ADOPT: become (or remain) a non-root child relay pinned to the parent.
  adoptChild(topicBig, parentHex) {
    const m = this.m;
    let role = m.axonRoles.get(topicBig);
    if (!role) {
      role = makeRole(topicBig, false, m._now());
      m.axonRoles.set(topicBig, role);
      m._log('info', 'relay-formed', { topic: idHex(topicBig).slice(0, 12) });
    }
    // TRANSACTION (dual-root packet 4(d)): ADOPT never called demote(), so the
    // custody gate at _set is the only thing between a delegating parent and a
    // root with custody; a refused flip pins no upstream and returns null, and
    // _onAdopt answers the parent with a refusal instead of accepting its subs.
    const flip = this._set(role, false, 'adopted-child', { to: parentHex.slice(0, 10) });
    if (!flip.committed) return null;
    m._upstream.set(topicBig, [lc(parentHex)]);
    return role;
  }

  // A departing root handed us its history. The sender's root beacon is a
  // ghost the moment the handoff arrives — purge it, or the defer gates would
  // keep pointing the topic at a corpse (observed: heirs adopting the history
  // and immediately demoting back toward the leaver, undoing the handoff).
  // If a strictly-closer live root OTHER than the leaver is beaconing, don't
  // hold a competing claim: demote and push the inherited history up (the SUB
  // carries our hw → the root PULLUPs it). Never defer back to the leaver (I-2).
  handoffArrived(topicBig, leaverHex) {
    const m = this.m;
    if (leaverHex && m._rootBeacons.get(topicBig)?.root === leaverHex) {
      m._rootBeacons.delete(topicBig);
    }
    const closer = this.liveCloserRoot(topicBig);
    if (closer && closer !== leaverHex) this.m._mergeCandidate(topicBig, closer, null, 'handoff-better-heir');
  }
}

export default RootClaim;
