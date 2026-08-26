// =====================================================================
// AxonaManager.js — Axona pub/sub: the routing-only axonic tree.
//
// Design: axona-docs/architecture/Pubsub-Axon-Tree-v0.1.md
//
// CLEAN BREAK (kernel v3.15.0). Routing-only pub/sub. The one rule:
//
//     Axona pub/sub uses ONLY DHT message routing. There are no direct
//     connections. Every interaction is a routed message delivered, hop by
//     hop, to the single live node closest to a 264-bit target.
//
// A message published to a topic is ROUTED toward the topic id; the closest
// live node is the (emergent, never-elected) ROOT. The root assigns a single
// monotonic timestamp — the serialization point that gives the topic a total
// order — caches it, and fans it out to its subscribers by routing a deliver
// to each. Subscribers renew toward the topic id every minute; that renewal
// is at once the keepalive, the failure detector, the self-heal, and (with a
// `since` hint) the gap-recovery. A subscriber carries an ordered `via`
// waypoint list (its `upstream`) so it is pinned to its relay yet always falls
// back to the topic id if that waypoint is gone.
//
// PHASE 2 — THE TREE. When a relay exceeds MAX_DIRECT subscribers it delegates:
// it promotes one of its subscribers to a child relay and hands it a batch of
// the others. A child relay subscribes UP toward the topic id (pinned by its
// parent via), caches the feed, and re-fans each message DOWN to its own
// subscribers exactly once. Delegated subscribers receive their deliveries
// from the child, so they repin to it and renew toward it — the tree is stable
// — but a dead waypoint always falls through to the topic id and re-seats, so
// the tree is self-healing and re-roots if the root itself dies.
//
// (Implementation choice: a relay promotes one of its own SUBSCRIBERS — a
// known-alive participant it can already route to — as the child. The design's
// "one of its connections" is satisfied without the manager needing a
// synaptome/neighbour list; routing reaches the chosen node regardless.)
//
// PHASE 3 — DURABILITY. A SUBSCRIBE advertises the sender's cache high-water; a
// relay/root that is BEHIND a reattaching subscriber pulls its stamped history
// UP (PULLUP → REPLAYUP) and adopts it without re-stamping, advancing lastTs so
// new publishes continue monotonically above it. This carries the topic's recent
// history across abrupt root death (a fresh empty root recovers it from any
// surviving cache-bearing relay) and across graceful migration.
//
// The side functions (kill/unpub/touch/pull/metrics/host) remain thin —
// markers TODO(Phase 4). GONE for good: sendDirect, findKClosest, K-closest
// fan-out, root sets, the old recruit/adopt/promote/dissolve + msgsync/kill-sync.
// =====================================================================

import { extractS2Prefix }   from '../utils/hexid.js';
import { RootClaim, roleNature } from './rootClaim.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { dispatchVerdict, dispatchAttributedTo } from './dispatch.js';
import { DurabilityLedger } from './durability.js';
import { isRegionLockEnforced as _regionLock,
         T, RENEW_MS, RENEW_FAST_MS, DROP_MS, ROOT_REPLICAS, CACHE_MAX,
         CACHE_BYTES, MAX_DIRECT, MAX_VIA, VIA_HOP_BUDGET, BEACON_MS,
         BEACON_FANOUT, BEACON_LAYERS, PENDING_PUB_TTL_MS, COLD_BURST_TRIES,
         COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES,
         COLD_BURST_SLOW_INTERVAL_MS, COLD_PEER_THRESHOLD,
         FIRST_PUBLISH_RESEND_MS, METRICS_LEASE_MS, METRICS_PUB_MS,
         METRICS_COALESCE_MS,
         MAX_ROLES, ROLE_GRACE_MS, ROLE_ADMIT_PER_TICK,
         HELLO_DEADLINE_MS, SATURATION_PRESSURE, ROOT_REPLICATE_FULL_MS,
         TICK_LAG_WINDOW, OBLIGATIONS } from './constants.js';
import { topicStoreMethods }   from './topicStore.js';
import { rootElectionMethods } from './rootElection.js';
import { repairPlaneMethods }  from './repairPlane.js';
import { wireHandlersMethods }  from './wireHandlers.js';
import { syncEngineMethods }    from './syncEngine.js';
import { writeFlightMethods }   from './writeFlight.js';
import { tombstoneAuthWiringMethods, makeTombstoneAuthority } from './tombstoneAuthWiring.js';
import buildBoundary1Registry from './boundary1Registry.js';
import { shadowEnabled } from '../registry/index.js';

// Constants, wire types, and the region-lock switch live in constants.js
// (refactor Phase 2); the caps and region-lock functions are re-exported here
// unchanged — AxonaPeer, std/chunk, and src/index.js import them from this
// module as the stable surface.
export { MAX_PUBLISH_BYTES, MAX_RELIABLE_PUBLISH_BYTES,
         configureRegionLock, isRegionLockEnforced } from './constants.js';



// Role shape (makeRole) lives in rootClaim.js — the role's `isRoot` field is
// the state-machine's state, and EVERY flip of it goes through RootClaim.

export class AxonaManager {
  /**
   * @param {object} o
   * @param {object} o.dht  adapter: { getSelfId(), routeMessage(target,type,payload,opts?),
   *                         onRoutedMessage(type, handler) }. sendDirect/findKClosest unused.
   */
  constructor({
    dht,
    now = () => Date.now(),
    emitLog = null,
    renewMs = RENEW_MS,            // adaptive renewal CEILING (stable state)
    renewFastMs = RENEW_FAST_MS,   // adaptive renewal FLOOR (post-subscribe / post-re-home)
    dropMs = DROP_MS,
    refreshIntervalMs = 5_000,     // tick — must be ≤ renewFastMs so fast renewal can fire
    replayCacheSize = CACHE_MAX,
    replayCacheBytes = CACHE_BYTES,
    maxDirect = MAX_DIRECT,
    beaconFanout = BEACON_FANOUT,  // K XOR-closest neighbors per beacon layer (root-announce reach)
    beaconLayers = BEACON_LAYERS,  // recursive forward depth (reach ≈ K + K² + … + K^layers)
    rootReplicas = ROOT_REPLICAS,  // singleton-root durability: # of nearest backup roots holding the full cache
    maxRoles = MAX_ROLES,          // axonic degree budget — the analogue of MAX_SYNAPTOME
    roleGraceMs = ROLE_GRACE_MS,   // refuse role MANAGEMENT this long after join (still transports)
    roleAdmitPerTick = ROLE_ADMIT_PER_TICK,  // paced admission: new roles per refresh tick
    neverRoot = false,             // HARD refusal: a bridge is a bridge (transport + introduction only)
    identity = null,               // node TRANSPORT identity {pubkey, sign} — signs D1 INGEST-ACK proofs
    tombstoneAuth = false,         // REF-1.1 S2.0c Phase 3: DEFAULT-OFF shadow wiring of the tombstone authorization core (observe-only; no behavior change)
    frameRegistry = false,         // REF-1.1 S2: DEFAULT-OFF Boundary-1 frame-contract registry (shadow-wraps the 19 routed handlers; observe-only; byte-identical flag-off)
    ..._legacy   // accepted-and-ignored clean-break tunables (pickRelayPeer, rootSetSize, …)
  } = {}) {
    if (!dht || typeof dht.routeMessage !== 'function' || typeof dht.getSelfId !== 'function'
        || typeof dht.onRoutedMessage !== 'function') {
      throw new TypeError('AxonaManager: dht with routeMessage + getSelfId + onRoutedMessage required');
    }
    // CAPABILITY IS DECLARED AT CONSTRUCTION — once, loudly, or not at all.
    //
    // v4.58.0 first made a missing declaration a per-send 'violation'. That is
    // fail-closed but it is NOT the contract the council approved: an adapter
    // that never declares would emit one ERROR per message forever and still be
    // silently uncreditable — a log storm standing in for a build error. Aster,
    // council 2026-08-01: "not the once-loud declared-capability contract we
    // approved." A declaration missing at build time is a build failure.
    //
    // Turning this on required auditing all 82 test doubles, because the whole
    // hazard is that blanket-declaring `true` to silence a throw would convert
    // every send from a non-reporting double into a violation and mask the very
    // regressions those tests exist to catch — the "make the doubles pass"
    // mistake with the sign flipped. Audited per file by what routeMessage
    // actually RESOLVES: two report verdicts (one returns {consumed:true}, one
    // delegates to the real AxonaPeer.routeMessage) and declare true; every
    // other returns a push-count or undefined and declares false, which is the
    // honest answer and costs them nothing — a non-reporting adapter simply
    // never credits a replica and never unpins a waypoint.
    if (typeof dht.verdictsSupported !== 'boolean') {
      throw new TypeError(
        'AxonaManager: dht.verdictsSupported must be declared as a boolean.\n' +
        '  true  — routeMessage resolves a routing verdict ({consumed:boolean,…}).\n' +
        '  false — this adapter cannot report outcomes; it will never credit a\n' +
        '          replica and never unpin a waypoint, and that is honest.\n' +
        'Capability is DECLARED, never inferred from what the adapter happens to\n' +
        'return — inferring it let test doubles set a production durability\n' +
        'semantic. Do NOT declare true to silence this: if routeMessage does not\n' +
        'resolve a verdict, true turns every send into a contract violation.\n' +
        'See src/pubsub/dispatch.js.');
    }
    this.dht    = dht;
    this.nodeId = dht.getSelfId();          // bigint, 264-bit
    this._now   = now;

    // ── Axonic admission control (v4.46.0) ───────────────────────────────
    this._maxRoles         = maxRoles;
    this._roleGraceMs      = roleGraceMs;
    this._roleAdmitPerTick = roleAdmitPerTick;
    this._neverRoot        = !!neverRoot;
    // Node TRANSPORT identity {pubkey, sign} — the root signs D1 INGEST-ACK
    // proofs with it (Write-Flight Ack Routing). Absent on sim/test doubles that
    // never carry one; every signed path degrades to the 4.62.1 unsigned one-hop
    // ack, so the write path is correct with or without it.
    this._nodeIdentity     = (identity && typeof identity.sign === 'function' && identity.pubkey) ? identity : null;
    this._joinedAt         = now();     // grace runs from construction
    this._admitTickAt      = 0;         // window start for paced admission
    this._admitTickCount   = 0;         // roles admitted in the current window
    this._admitRefusals    = { bridge: 0, 'not-seated': 0, saturated: 0, paced: 0, floored: 0 };

    // ── Capacity telemetry (v4.47.0) — OBSERVED, never derived from the count ──
    this._tickAt      = 0;   // _now() at the START of the last refresh tick
    this._tickLagMs   = 0;   // observed: (gap between tick starts) - refreshIntervalMs, floored at 0
    this._tickDurMs   = 0;   // observed: how long the last tick body took to run
    this._tickStalls  = 0;   // ticks whose lag exceeded HELLO_DEADLINE_MS — i.e. long enough to be kicked
    // ROLLING lag window (v4.49.0). _tickLagMax is the max over the last
    // TICK_LAG_WINDOW ticks — the value helloPressure and therefore admission
    // read. It must be able to FALL; as an all-time mark it was a ratchet that
    // permanently saturated any node after one browser-tab suspension.
    this._tickLagRing = new Array(TICK_LAG_WINDOW).fill(0);
    this._tickLagIdx  = 0;
    this._tickLagMax  = 0;   // DERIVED: max(_tickLagRing) — recomputed each tick
    // Kept for diagnosis only, never for control: the worst lag ever observed.
    // Losing this on the window would have thrown away the evidence that made
    // the latch findable in the first place.
    this._tickLagPeak = 0;
    this._logSink = (typeof emitLog === 'function') ? emitLog : null;

    // DURABILITY — the second state machine (Aster, council 2026-08-01). Kept in
    // its own module with its own vocabulary because the defect it replaces was
    // one flag carrying two facts: _deliverToApp confirmed the pending entry, so
    // observing DELIVERY discharged DURABILITY. Nothing on the delivery path can
    // reach 'verified' — there is deliberately no function here for it to call.
    this._durability = new DurabilityLedger({ now });

    // REF-1.1 S2.0c Phase 3 — DEFAULT-OFF shadow wiring of the accepted tombstone
    // authorization core (src/pubsub/tombstoneAuth.js). When the flag is set, ONE
    // per-node TombstoneAuthority is built and the _ta* observers (mixed in below)
    // feed it the inbound body/kill/evict stream at the existing funnels WITHOUT
    // changing any behavior — the legacy tombstone path stays authoritative. Flag
    // OFF (default): this is null, every observer is a guarded no-op, and the node
    // is byte-identical to today. Enforcement (making this the source of truth) is
    // a SEPARATE gate that also needs the signed exp from the envelope flag day.
    this._tombAuthority = tombstoneAuth ? makeTombstoneAuthority() : null;

    // REF-1.1 E2.1: the Boundary-1 registry is now the SOLE registration door for
    // the 19 routed handlers — _registerHandlers() calls registerFrame() at each
    // site (no raw this.dht.onRoutedMessage). The DISPATCH registry `_frameDoor` is
    // therefore built UNCONDITIONALLY: the door needs its wiring map, its wrap, and
    // its registry-owned mintLive certifier. OBSERVATION is still gated by the
    // runtime shadow flag (shadowEnabled, DEFAULT-OFF): flag-off the wrap runs each
    // handler verbatim, so the migrated path is byte-identical to the pre-E2.1 raw
    // registration. Its sink feeds _ingestFrameTrace, a guarded no-op unless the M1
    // canary is armed. Must be constructed BEFORE _registerHandlers().
    this._frameDoor = buildBoundary1Registry({
      enabled: shadowEnabled,
      sink: (rec) => this._ingestFrameTrace(rec),
    });
    // REF-1.1 S2/M1 CANARY INSTRUMENTATION — unchanged, still gated by the per-
    // manager `frameRegistry` flag (DEFAULT-OFF). `_frameRegistry` is the canary
    // HANDLE the inspectors (frameRegistryShadow/Summary) read: the SAME registry
    // object as the door when armed, null when not — so built===false / rows===0
    // flag-off exactly as the accepted S2/S3/M1 smokes assert. The dispatch door is
    // a separate always-present concern the canary surface does not report. The
    // trace ring + MONOTONIC lifetime counters exist only when armed (a fault that
    // scrolls out of the 1024-entry ring is still counted; `dropped` = evictions);
    // _ingestFrameTrace guards on their absence, so a stray observation while
    // unarmed is a safe no-op.
    this._frameTraces = frameRegistry ? [] : null;
    this._frameLifetime = frameRegistry
      ? { total: 0, faults: 0, unobserved: 0, dropped: 0,
          faultKinds: Object.create(null), verdicts: Object.create(null), byType: Object.create(null) }
      : null;
    this._frameRegistry = frameRegistry ? this._frameDoor : null;

    this.renewMs     = renewMs;          // adaptive ceiling
    this.renewFastMs = renewFastMs;      // adaptive floor
    this.dropMs    = dropMs;
    this.maxDirect = maxDirect || MAX_DIRECT;
    this.refreshIntervalMs = refreshIntervalMs;
    this._cacheMax   = replayCacheSize || CACHE_MAX;
    this._cacheBytes = replayCacheBytes || CACHE_BYTES;
    this._rootReplicas = Number.isFinite(rootReplicas) ? Math.max(0, rootReplicas) : ROOT_REPLICAS;

    // Public/inspectable state (contract surface).
    this.axonRoles       = new Map();   // topicIdBig -> Role  (topics I host: root or relay)
    this.mySubscriptions = new Map();   // topicIdBig -> { since, lastRenewSent }
    this._hostedTopics   = new Set();   // topicIdBig hosted without app consumption
    this._backupTopics   = new Set();   // topicIdBig I hold a warm replica for → subscribe like a child relay (single-root election)
    this._lastSeenTsByTopic = new Map();// topicIdBig -> ts  (AxonaPeer seeds `since` here)

    // Internal.
    this._upstream        = new Map();  // topicIdBig -> [hex]  the relay we renew toward
    this._rootHint        = new Map();  // topicIdBig -> { via:hex|null, at }  cached iterative-lookup root
    this._unattachedSince = new Map();  // topicIdBig -> ts  first tick seen subscribed-but-unpinned (reachable-root fallback)
    this._rootBeacons     = new Map();  // topicIdBig -> { root:hex, at, exp }  inbound root advert (soft state)
    this._beaconSeen      = new Map();  // beaconId -> exp  (flood dedup)
    this._lastBeaconAt    = 0;
    this._beaconSeq       = 0;
    this._beaconFanout    = beaconFanout;   // tunable root-announce reach (see _emitRootBeacons)
    this._beaconLayers    = beaconLayers;
    this._appDelivered    = new Map();  // "topicHex:msgId" -> true (exactly-once LRU)
    this._deliveryCallback = null;
    this._hostKeyspace    = false;
    this._pending         = new Map();  // pull corrId -> { resolve, timer }
    this._pullSeq         = 0;
    this._timer           = null;
    this._burstTimers     = new Set();  // cold-publish burst + first-publish setTimeout handles (cleared on stop)
    this._publishedTopics = new Set();  // topics this node has published to (for the first-publish re-send)
    this.myMetricsRequests = new Map(); // dataTopicBig -> { lastSent }  topics THIS node wants metrics for (renewed like subscriptions)
    this._metricsWanted   = new Map();  // dataTopicBig -> exp   soft flag on a path node (short-circuit duplicate METRICSON)
    this._metricsFwdAt    = new Map();  // dataTopicBig -> ts    last upstream METRICSON forward (fan-in coalesce)
    this._metricsPublisher = null;      // (dataTopicIdHex, snapshot) => Promise  set by the peer; publishes to metricTopic(T)

    // The root-claim state machine: every isRoot transition + its guards
    // (claim / defer / demote / handoff decision table) live in rootClaim.js.
    this._rootClaim = new RootClaim(this, { beaconMs: BEACON_MS });

    this._registerHandlers();
  }

  // ── XOR-distance helper (264-bit ids as bigints) ────────────────────────
  _cmpXor(a, b, target) { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }

  // ── routing core ────────────────────────────────────────────────────
  // Route toward via[0] if present, else toward the topic id. The topic id is
  // authoritative; a dead waypoint is popped and routing continues. Never
  // orphaned by a stale via.
  _send(type, payload) {
    const via = Array.isArray(payload.via) ? payload.via : [];
    const target = via.length ? idBig(via[0]) : idBig(payload.topicId);
    // Delegates to _route rather than calling the transport directly, so it
    // inherits the never-rejects containment documented there. This was a second,
    // separate crash surface: _send had its own copy of the routeMessage call and
    // so was untouched when _route was hardened. Two copies of an emission path
    // means two places to remember, and I had already forgotten one.
    return this._route(target, type, payload);
  }
  // RETURNS the routeMessage result (Q2/C4), as a promise that NEVER REJECTS.
  //
  // Production routing reports failure by RESOLVING {consumed:false, exhausted:true}
  // — it does not throw — so a caller that discards the promise cannot distinguish
  // delivery from silence, and no try/catch around it ever will. That is why this
  // returns a value at all.
  //
  // THE NON-REJECTING PART IS LOAD-BEARING AND WAS LEARNED THE HARD WAY. Before
  // v4.57.0 this returned undefined, so the 13 fire-and-forget callers had nothing
  // to drop. Making it return the promise turned EVERY ONE of them into a potential
  // process kill, because Node >=15 terminates on an unhandled rejection. Aster
  // spotted two of those sites; the regression he asked for (fence_syncpush_rejection)
  // then failed with SEVEN unhandled rejections, proving the exposure was the whole
  // call graph rather than those two.
  //
  // Patching 13 call sites would leave the 14th to be written next month. So the
  // containment is here, once: a transport error becomes a FAILURE VERDICT of the
  // same shape routing already uses, which dispatchVerdict() classifies as 'failed'.
  // A caller that ignores the result is safe; a caller that reads it gets the truth.
  _route(targetBig, type, payload) {
    const fail = (e) => ({ consumed: false, error: String((e && e.message) || e), transportError: true });
    try {
      return Promise.resolve(
        this.dht.routeMessage(targetBig, type, payload, { fromId: idHex(this.nodeId), viaHopBudget: VIA_HOP_BUDGET }),
      ).catch(fail);
    } catch (e) {
      return Promise.resolve(fail(e));   // synchronous throw out of routeMessage
    }
  }
  // Pop a dead waypoint and keep routing. When the via chain empties, _send
  // falls through to the TOPIC ID — that is deliberate and load-bearing: it is
  // how a subscriber pinned to a dead root re-homes onto a fresh one
  // (smoke_pubsub_core "via dead-waypoint fall-through"). Do NOT add a
  // terminal guard here; the decline path needs one, this path must not have
  // one. See _rerouteDeclined.
  _reroute(type, payload) {
    payload.via = (Array.isArray(payload.via) ? payload.via : []).slice(1);
    this._send(type, payload);
  }

  /**
   * Forward a message this node REFUSED to seat, one via-hop onward.
   * Returns TRUE only if it was actually handed to a DIFFERENT node.
   *
   * Why this cannot just call _reroute: a decline site is only reached when
   * this node is TERMINAL for the topic — nobody is closer. So the topic-id
   * fall-through that makes _reroute correct for a dead waypoint is exactly
   * wrong here: the DHT hands the message straight back to us, and
   *   _onPub -> _becomeRoot -> admitRole -> refuse -> reroute -> _onPub -> ...
   * spins synchronously and unbounded. No timers, no sockets, no health check,
   * no logs — a hard process wedge. It took the east production bridge down for
   * ~50 min on 2026-07-27 (bridge fence + directory publish, empty synaptome).
   *
   * So: an explicit surviving via hop is a real forward. Anything else is the
   * end of the line, and the caller must say so rather than retry.
   *
   * Today only the bridge fence refuses at the HARD tier, so only a bridge can
   * reach this. The moment a second HARD reason exists, every node can.
   */
  _rerouteDeclined(type, payload) {
    const via = (Array.isArray(payload.via) ? payload.via : []).slice(1);
    payload.via = via;
    if (!via.length) return false;                     // topic-id fall-through returns here
    if (idBig(via[0]) === this.nodeId) return false;   // via points at us: same trap
    this._send(type, payload);
    return true;
  }

  /**
   * A refused message with nowhere left to go. Terminal and undeliverable.
   * Silent loss is what the reroute was added to prevent, so this is LOUD:
   * a real failure of placement, not routine.
   */
  _undeliverable(type, topicBig, why) {
    this._log('warn', 'undeliverable', { topic: idHex(topicBig).slice(0, 12), type, why });
  }

  // True iff a topic (or any id) shares this node's region byte (S2 prefix). The
  // region byte is the high byte of every 264-bit id; only same-region nodes may
  // form a topic's axon-tree infrastructure (root + child relays).
  _sameRegion(idBigVal) {
    try { return extractS2Prefix(idBigVal) === extractS2Prefix(this.nodeId); }
    catch { return false; }
  }

  // The region GATE used by every enforcement site. When the region lock is off
  // (default, pre-critical-mass) this is always true → an out-of-region node may
  // root/relay/host any topic (nearest node wins, pre-4.13.0 behavior). When on,
  // it collapses to the strict same-region check.
  _regionOk(idBigVal) {
    return !_regionLock() || this._sameRegion(idBigVal);
  }

  // ── Axonic admission control (v4.46.0) ─────────────────────────────────
  // ONE gate, THREE reasons, TWO tiers. The neuromorphic layer has had the
  // equivalent for a long time (MAX_SYNAPTOME budget + a refusal in
  // NeuronNode.addIncomingSynapse + breadth-before-depth fill in
  // buildXorRoutingTable); the axonic layer had none of it.
  //
  // NOTE ON THE ADDRESS RULE: this is NOT an exception to it. Hosting is still
  // decided by ADDRESS. Every reason below is a property of the node's OWN
  // state, self-declared and self-limiting: it can only ever cause a node to
  // hold LESS, never to acquire a role it is not closest to. Skipping a
  // refusing node is the chooser respecting an honest "cannot", not a
  // privilege granted to anybody.

  /** Has this node been in the mesh long enough, and is it actually meshed? */
  seated() {
    if ((this._now() - this._joinedAt) < this._roleGraceMs) return false;
    // Deliberately not a bare timer: a node whose clock ran out but which has
    // no routable non-bridge neighbour is exactly the node that must not take
    // roles. Generalises the existing meshBare() guard (wireHandlers.js:99)
    // from "bare" to "seated".
    return !this._rootClaim.meshBare();
  }

  /**
   * CAPACITY AS A MEASUREMENT (v4.47.0).
   *
   * Every number here is observed — a wall-clock delta over real state — and
   * every pressure has a denominator that is an actual protocol deadline, so
   * "how close am I to failing" has a literal answer rather than a vibe.
   *
   *   servicePressure = age of my least-recently-serviced role / DROP_MS
   *     DROP_MS is when a cohort gives up on an unserviced role. At 1.0 a role
   *     HAS silently rotted. This catches every cause at once — skipped ticks,
   *     event-loop stalls, budget starvation, GC pauses — because it measures
   *     the outcome (staleness) rather than any single mechanism.
   *
   *   helloPressure = observed tick lag / HELLO_DEADLINE_MS
   *     The bridge closes a client that misses its hello window. At 1.0 this
   *     node is being kicked off the network. This is the #332 join-storm
   *     spiral expressed as a number the node can read about itself.
   *
   * NOT included: ceil(roles / BUDGET) * tick. That is a linear function of the
   * role count — MAX_ROLES in different units — and would be arithmetic wearing
   * a telemetry costume.
   */
  inspectCapacity() {
    const now = this._now();
    // D0 / M4: pressure is the MAX over per-obligation (age / that obligation's
    // OWN deadline), so 1.0 means "this obligation has failed" for every row and
    // the rows are comparable. Previously one DROP_MS denominator served every
    // nature, which made 1.0 meaningful for renewal and arbitrary for the rest.
    let worstRatio = 0, worstAgeMs = 0, worstKind = null, overdue = 0, unserviced = 0;
    let obligations = 0;                             // the DENOMINATOR: rows evaluated, not roles held
    // `since` is the stamp when the obligation has been discharged at least once,
    // and the role's BIRTH when it never has. A MISSING stamp (null) is innocent only while
    // the role is younger than its own deadline: past that, it has never been
    // serviced at all, which is the worst case rather than an exempt one. Treating
    // "never discharged" as unconditionally not-debt was a false negative of the
    // same shape as the bug D0 exists to fix — caught by smoke_role_admission.mjs,
    // which builds 96 never-serviced roles and rightly expects saturation.
    //
    // C2: null is the SENTINEL; 0 is a REAL instant. `pubsubPeerDied` writes
    // lastRenewSent = null to force an immediate re-emit, and that must not be
    // readable as a time — `at || bornAt` could not tell "renew now" from
    // "unknown", so a subscription whose first re-emit never landed stayed
    // permanently exempt from pressure, the same false negative D0 exists to
    // remove. Presence is asked explicitly, of each field separately, and no
    // timestamp is tested for truthiness anywhere in this path.
    //
    // Zero WAS the sentinel until v4.52.0, and `t > 0` made an injected clock
    // starting at 0 permanently unknown — production Date.now never yields 0,
    // but `now` is a public injection point and simulations routinely start at 0,
    // so the metric was blind under exactly the harness we use to test it.
    // (Aster, 8fbb1a9 P2.)
    const stamped = (t) => typeof t === 'number' && Number.isFinite(t);
    const consider = (kind, at, deadline, bornAt = 0) => {
      obligations++;                                 // counted even when unmeasurable — see overdueFrac
      // Presence is a SEPARATE question from value. Collapsing them into `!since`
      // reintroduced the same defect one layer down: a legitimate stamp of 0 read
      // as absence, so a clock starting at 0 was exempt from pressure forever.
      const hasAt = stamped(at), hasBorn = stamped(bornAt);
      if (!hasAt && !hasBorn) { unserviced++; return; }   // never stamped AND no birth time — unknown
      const since = hasAt ? at : bornAt;
      const age = now - since;
      const ratio = age / deadline;
      if (!stamped(at)) unserviced++;                // still counted as never-discharged, but no longer exempt
      if (ratio >= 1) overdue++;                     // past ITS OWN deadline, not a shared one
      if (ratio > worstRatio) { worstRatio = ratio; worstAgeMs = age; worstKind = kind; }
    };
    for (const role of this.axonRoles.values()) {
      // A role with no sync ledger is UNMEASURABLE, not absent. Skipping it here
      // would hide it from `unserviced` and leave the 8x MAX_ROLES backstop —
      // which exists precisely for telemetry-dead roles — with nothing to report.
      if (!role.sync) { unserviced++; continue; }
      const nature = roleNature(role);               // 'root' | 'backup' | 'child' — derived, never stored
      if (nature === 'root') {
        // Only a root that HOLDS something owes a full push; _replicateRole returns
        // early on an empty cache, so an empty root is not in debt for never pushing.
        if (role.cache.length || role.tombstones.size) {
          consider('ROOT', role.sync.lastFullAt, OBLIGATIONS.ROOT.deadline, role.createdAt);
        }
      } else {
        const kind = nature === 'backup' ? 'BACKUP' : 'CHILD';
        consider(kind, role.sync.lastRenewAt, OBLIGATIONS[kind].deadline, role.createdAt);
      }
      // HOLDER is an ORTHOGONAL flag, not a primary nature: a hosted root owes
      // both. Same stamp, same deadline, so it cannot double-count into a worse
      // ratio than the renewal row already produced.
      if (this._hostedTopics?.has(role.topicId)) {
        consider('HOLDER', role.sync.lastRenewAt, OBLIGATIONS.HOLDER.deadline, role.createdAt);
      }
    }
    // APP_SUB — the coverage hole. mySubscriptions is a separate map, so before
    // D0 the node's own subscriptions were unmeasurable rather than mismeasured.
    //
    // C9: a LOCALLY ROOTED subscription owes nothing here, and charging it was an
    // availability regression introduced by D0 itself. refreshTick renews app
    // subscriptions by walking mySubscriptions, but skips the topic outright at
    // `if (role && role.isRoot) continue` (repairPlane.js) — correctly, since a
    // root has no upstream to renew toward and serves itself from local cache.
    // sub.lastRenewSent is therefore written once at subscribe and never again,
    // so measuring it against DROP_MS made an ordinary node — one that subscribed
    // to a topic and then became its root, which is topology-random and common on
    // a small mesh — falsely saturate ~110s later and start refusing pushed roles.
    // The topic is not unmeasured by skipping it: the ROOT row above already
    // carries this node's real obligation for it.
    for (const [topicBig, sub] of this.mySubscriptions) {
      if (this.axonRoles.get(topicBig)?.isRoot) continue;
      consider('APP_SUB', sub.lastRenewSent, OBLIGATIONS.APP_SUB.deadline, sub.createdAt);
    }
    const roles = this.axonRoles.size;
    return {
      roles,
      subscriptions: this.mySubscriptions.size,      // now measured, not just held
      overdue,                                       // obligations past their OWN deadline
      obligations,                                   // rows evaluated this pass
      // C7: the denominator is obligations EVALUATED, not roles held. Dividing by
      // roles let a hosted root contribute two rows against one role (>1.0), and
      // made app subscriptions on a role-less node divide by zero.
      overdueFrac: obligations ? +(overdue / obligations).toFixed(3) : 0,
      unserviced,                                    // born but not yet discharged once
      worstAgeMs,
      worstObligation: worstKind,                    // WHICH obligation is worst — the old number could not say
      servicePressure: +worstRatio.toFixed(3),
      tickLagMs: this._tickLagMs,
      tickLagMaxMs: this._tickLagMax,      // ROLLING max over the last TICK_LAG_WINDOW ticks (v4.49.0)
      tickLagWindow: this._tickLagRing.length,
      tickLagPeakMs: this._tickLagPeak,    // all-time worst — DIAGNOSIS ONLY, drives nothing
      tickDurMs: this._tickDurMs,
      tickStalls: this._tickStalls,
      helloPressure: +(this._tickLagMax / HELLO_DEADLINE_MS).toFixed(3),
    };
  }

  /**
   * Is this node failing to service what it holds?
   *
   * Replaces the old `axonRoles.size >= MAX_ROLES`. That asked about inventory;
   * this asks about capability, which is the question that actually predicts
   * failure. MAX_ROLES survives only as a far-off absolute backstop for the
   * pathological case where telemetry itself is broken (no tick has ever run,
   * so every pressure reads 0) — it must never be the primary signal again.
   */
  saturated() {
    const c = this.inspectCapacity();
    if (c.servicePressure >= SATURATION_PRESSURE) return true;   // rotting roles
    if (c.helloPressure   >= SATURATION_PRESSURE) return true;   // about to be kicked
    return c.roles >= this._maxRoles * 8;                        // telemetry-dead backstop
  }

  /**
   * May this node take a NEW role right now?
   * @returns {{ok:boolean, why?:string, hard?:boolean}}
   *   hard:true  — categorical; the floor must NEVER override it.
   *   hard:false — situational; the floor may override to avoid a partition.
   */
  canAcceptRole() {
    // HARD — a bridge is a bridge: transport and introduction, never a root.
    // Deliberately not soft: the floor would otherwise seat a root on the one
    // node whose failure is least tolerable, precisely under the load where it
    // can least afford it. host() was removed 2026-07-25 for this reason and
    // sub() kept rooting anyway; a soft tier reopens that door on a timer.
    if (this._neverRoot) return { ok: false, why: 'bridge', hard: true };

    // SOFT — situational, self-declared, floor-overridable.
    if (!this.seated())   return { ok: false, why: 'not-seated', hard: false };
    if (this.saturated()) return { ok: false, why: 'saturated',  hard: false };

    // Paced admission — the axonic analogue of Phase-1-breadth. Absorb a
    // backlog over ticks instead of one event-loop-blocking burst (#332).
    const now = this._now();
    if (now - this._admitTickAt >= this.refreshIntervalMs) {
      this._admitTickAt = now; this._admitTickCount = 0;
    }
    if (this._admitTickCount >= this._roleAdmitPerTick) {
      return { ok: false, why: 'paced', hard: false };
    }
    return { ok: true };
  }

  /**
   * Admission decision for one role, with the mandatory floor.
   *
   * THE FLOOR IS NOT OPTIONAL. If every candidate refuses, nobody roots and the
   * topic has no root — and both soft reasons can cause that fleet-wide (a
   * simultaneous restart puts everyone in grace; a loaded backbone makes
   * everyone saturated). Both happened on prod 2026-07-26. A grace period that
   * can partition the network is worse than none, so a SOFT refusal with no
   * alternative is overridden and logged loudly. A HARD refusal never is.
   *
   * @param {bigint} topicBig
   * @param {boolean} hasAlternative  caller knows another node can take this
   * @returns {boolean} true ⇒ proceed to _becomeRoot
   */
  admitRole(topicBig, hasAlternative = false) {
    const v = this.canAcceptRole();
    if (v.ok) { this._admitTickCount++; return true; }

    this._admitRefusals[v.why] = (this._admitRefusals[v.why] || 0) + 1;
    const topic = idHex(topicBig).slice(0, 12);

    if (v.hard) {
      this._log('info', 'role-refused', { topic, why: v.why, hard: true, roles: this.axonRoles.size });
      return false;                      // never floored
    }
    if (hasAlternative) {
      this._log('info', 'role-refused', { topic, why: v.why, roles: this.axonRoles.size });
      return false;
    }
    // Floor: no alternative exists. Accept and say so.
    this._admitRefusals.floored++;
    this._log('warn', 'admitted-despite', { topic, why: v.why, roles: this.axonRoles.size });
    this._admitTickCount++;
    return true;
  }

  /**
   * Admission for a PUSHED role (HANDOFF heir). Deliberately stricter about what
   * may refuse than admitRole():
   *
   *   refuses on  — 'bridge' (hard), 'saturated' (a genuine "I cannot")
   *   ACCEPTS on  — 'not-seated', 'paced'
   *
   * Why grace must NOT refuse a handoff: grace is 90s, the leaver's ack window is
   * ≤5s (HANDOFF_ACK_MAX_MS). A refusal the leaver cannot outwait is not a
   * deferral, it is data loss — and in a fleet-wide restart EVERY candidate is in
   * grace at once, so every handoff would be refused and every last copy dropped.
   * A departing node's history has to land somewhere; "I am new" is not a reason
   * to drop it, "I am full" is.
   */
  admitPushedRole(topicBig) {
    const topic = idHex(topicBig).slice(0, 12);
    if (this._neverRoot) {
      this._admitRefusals.bridge++;
      this._log('info', 'role-refused', { topic, why: 'bridge', hard: true, pushed: true });
      return false;
    }
    if (this.saturated()) {
      this._admitRefusals.saturated++;
      this._log('warn', 'role-refused', { topic, why: 'saturated', pushed: true, roles: this.axonRoles.size });
      return false;                     // leaver's unacked path re-homes it (4.45.0 honest ack)
    }
    return true;
  }

  /** Admission counters for health()/observability. */
  inspectAdmission() {
    return {
      roles: this.axonRoles.size,
      maxRoles: this._maxRoles,
      seated: this.seated(),
      saturated: this.saturated(),
      neverRoot: this._neverRoot,
      graceRemainingMs: Math.max(0, this._roleGraceMs - (this._now() - this._joinedAt)),
      refusals: { ...this._admitRefusals },
      capacity: this.inspectCapacity(),
    };
  }

  // I am the root for a topic iff I am the routing terminus for its bare id.
  // A non-root relay that becomes the closest node (e.g. after the old root dies)
  // is promoted here — without this it would reroute bare-topic publishes to
  // itself forever. Rules + defer gate live in the state machine.
  _maybePromoteRoot(role, payload, meta) {
    this._rootClaim.promote(role, payload, meta);
  }

  // Strictly-closer live-root defer gate — the decision table in rootClaim.js.
  _liveCloserRoot(topicBig, opts) {
    return this._rootClaim.liveCloserRoot(topicBig, opts);
  }

  // Defer a stranded terminal message to the beaconed root: demote any spurious
  // root claim I hold (and re-home under the true root so my subtree keeps
  // receiving), then forward the payload via-pinned to it.
  //
  // REMAINING CALLER: the SUB path only. Its gate (_liveCloserRoot with
  // requireReachable defaulting true) admits only channel-verified neighbours
  // or fresh verified records, so the demote here is evidence-based at defer
  // time. PUB and KILL moved to _forwardToRoot (v4.59.0) — their looser gate
  // means the named root may be a guess, and a guess must not move state.
  _deferToRoot(topicBig, type, payload, rootHex) {
    this._rootClaim.demote(topicBig, rootHex, 'defer-terminal');
    this._send(type, { ...payload, via: [rootHex] });
  }

  // Forward a one-shot message (PUB/KILL) to the beaconed root and let the
  // VERDICT drive state — the C+D unified transition (council 2026-08-02, seq
  // 146/147, + the atNode amendment). Until v4.59.0 this path was _deferToRoot,
  // which demoted our role and re-pinned _upstream to the named root BEFORE the
  // send: one publish handed to a dead relay both vanished ("consumed") and
  // converted a working read path into a starved one (fence_pub_defers_to_corpse
  // §1/§2/§4; the 2026-08-02 prod write outage). The contract now:
  //
  //   consumed AND attributed to the named root → the one piece of evidence
  //       that justifies demote + re-home (the multi-hop live root that made a
  //       strict-reachability gate wrong). Demote does the re-home. Attribution
  //       is decided by dispatchAttributedTo — the shared predicate, not a
  //       call-site comparison (Aster, council seq 149): atNode arrives as a
  //       bigint from production adapters and hex from doubles, and a padding
  //       or type mismatch must never masquerade as "different node".
  //   consumed at ANOTHER node / atNode absent → NO mutation. The message is
  //       safe with whoever took it, and OUR state is coherent on its own
  //       terms: we keep whatever role we hold and keep serving it. We do NOT
  //       presume a DELIVER will arrive to re-home us (seq 149 — a
  //       non-subscribing root may never receive one); if a genuinely closer
  //       live root exists, its own beacons demote us at receipt, which is the
  //       standing evidence-of-life path. Pinning toward a root that never
  //       touched the message would re-create the corpse-pin this replaces.
  //   failed → invalidate ONLY the matching beacon record (same root, same
  //       `at` stamp — a late verdict must not erase a newer beacon). No
  //       demote, no pin. The invalidation lands sub-second, inside the
  //       publisher's early-resend pump window, so the SAME message's retries
  //       arrive at a node that will now root and ingest properly. "First
  //       lost, second saved" is the floor, not the expectation.
  //   unsupported / violation → NO state transition (violation logs loudly).
  //       Silence is never evidence, in either direction — the same fail-closed
  //       rule as _unpinIfWaypointDead and the replica ledger.
  // D0 (Write-Flight Ack Routing): pick the topic-closest ADJACENT peer that has
  // attested `write-flight-ack-v1` capability (R13), so an API-origin publisher
  // can delegate flight ownership to a 4.62.2-capable relay instead of owning a
  // carried flight itself. Returns the peer's hex id, or null → the D0
  // observation-only fallback (R14).
  //
  // Capability is read from the transport adapter's per-channel flag
  // (`dht.isCapable(peerHex)`), which the transport sets ONLY from a verified
  // CAP_ATTEST (capAttest.js). FAIL-CLOSED: an adapter with no `isCapable`
  // (sim doubles, pre-4.62.2 transports) yields null for everyone, so nothing is
  // treated as capable and the publisher takes the fallback — never a false
  // delegate. A capability-attested bridge is a valid delegate: it is a
  // known-capable ingress (R13/R14), so it is NOT excluded here.
  pickCapableAdjacent(topicBig) {
    if (typeof this.dht?.isCapable !== 'function' || typeof this.dht?.neighbors !== 'function') return null;
    let best = null, bestD = null;
    for (const n of (this.dht.neighbors() || [])) {
      let nb; try { nb = idBig(n); } catch { continue; }
      if (nb === this.nodeId) continue;
      const hex = lc(idHex(nb));
      let capable = false;
      try { capable = !!this.dht.isCapable(hex); } catch { capable = false; }
      if (!capable) continue;
      const d = nb ^ topicBig;
      if (bestD === null || d < bestD) { bestD = d; best = nb; }
    }
    return best === null ? null : lc(idHex(best));
  }

  _forwardToRoot(topicBig, type, payload, rootHex) {
    const declares = this.dht?.verdictsSupported;
    const rec = this._rootBeacons.get(topicBig);           // capture identity + at BEFORE the send
    // E3 (v0.3): a WRITE completes only on its INGEST-ack. The flight opens at
    // dispatch; every routing verdict below stays hop-local evidence — consumed
    // (anywhere, including at the named root) no longer ends the write's story.
    // D1 (Write-Flight Ack Routing): opening the flight returns its ack-routing
    // tuple {ackTo, flightNonce, attemptId}; stamp it on the forwarded write so
    // the root can route a SIGNED ingest proof straight back to the flight owner
    // (ackTo) across any number of hops — the deaf-flight fix. If the payload
    // already carries an attemptId (D2 mints it at the API boundary), it is
    // threaded through _flightOpen so retries/promotions stay one attempt.
    let ackRoute = null;
    if (type === T.PUB || type === T.KILL) {
      ackRoute = this._flightOpen(topicBig, rootHex, type, payload, payload?.attemptId ?? null);
    }
    const outPayload = ackRoute
      ? { ...payload, via: [rootHex], ackTo: ackRoute.ackTo, flightNonce: ackRoute.flightNonce, attemptId: ackRoute.attemptId }
      : { ...payload, via: [rootHex] };
    const sent = this._send(type, outPayload);
    Promise.resolve(sent).then((r) => {
      const v = dispatchVerdict(r, declares);
      if (v === 'violation') {
        this._log('error', 'pubsub:dispatch-contract-violation', {
          topic: idHex(topicBig).slice(0, 12), peer: String(rootHex).slice(0, 12),
          detail: `adapter declares verdictsSupported but returned no verdict on ${type}`,
        });
        return;
      }
      if (v === 'consumed') {
        if (!dispatchAttributedTo(r, rootHex)) return;     // consumed elsewhere / unattributed → fail closed
        this._rootClaim.demote(topicBig, rootHex, 'defer-confirmed');
        return;
      }
      if (v !== 'failed') return;                          // unsupported → no evidence → nothing moves
      // STRICT generation guard (v4.59.2, both reviewers on fb77b70): deletion
      // requires the CAPTURED record — same root, same `at` — with no fallback.
      // The first draft allowed `!rec ||`, meaning a send with nothing captured
      // could delete whatever matching-root beacon existed by verdict time —
      // exactly the newer-generation erasure the guard exists to prevent, one
      // conditional away. No captured record = no deletion authority: this
      // verdict describes a probe of a pointer we never held.
      const cur = this._rootBeacons.get(topicBig);
      if (rec && cur && cur.root === rec.root && cur.at === rec.at) {
        this._rootBeacons.delete(topicBig);
        this._log('info', 'pubsub:beacon-invalidated', {
          topic: idHex(topicBig).slice(0, 12), was: String(rootHex).slice(0, 12),
          detail: `${type} forwarded toward the beaconed root reached nobody — pointer dropped`,
        });
      }
    }).catch(() => {});   // _route cannot reject (v4.57.1); belt and braces
  }

  _becomeRoot(topicBig, why = 'terminal') {
    return this._rootClaim.become(topicBig, why);
  }

  // ── introspection (consumed by AxonaPeer.health()) ───────────────────
  // These were dropped in the v3.12 clean break, which left health().axonRoles
  // permanently empty — every relay reported roles=0 while actually rooting
  // topics, which masked the prod root-split for a full diagnosis cycle.
  // Observability surfaces must fail loudly or exist; these exist again.
  inspectRoles() {
    const out = [];
    for (const r of this.axonRoles.values()) {
      out.push({
        topicId: idHex(r.topicId),
        isRoot: !!r.isRoot,
        nature: roleNature(r),                           // ROOT | BACKUP | CHILD (Phase 7; I-6)
        holder: this._hostedTopics.has(r.topicId) || this.mySubscriptions.has(r.topicId),
        children: [...r.children],
        subscribers: r.subscribers.size,
        replayCacheSize: r.cache.length,
      });
    }
    return out;
  }

  inspectHosting() {
    return {
      topics: [...this._hostedTopics].map((t) => idHex(t)),
      subscriptions: this.mySubscriptions.size,
      backups: this._backupTopics.size,
      // Cache-bearing roots with NO replica anywhere — this node holds the
      // network's only copy of each (#362). Apps can check before leaving.
      singletonRoots: [...this.axonRoles.values()]
        .filter((r) => r.isRoot && (r.replicas?.size ?? 0) === 0 && r.cache.length > 0).length,
    };
  }

  // Subscribe — always sent SYNCHRONOUSLY and immediately (fast path, never blocked
  // on the network). Pinned (steady state) → via the relay. Unpinned → greedy ([])
  // toward the bare topic id, every hop routing by its own synaptome.
  //
  // NO root-hint via on the unpinned path (v4.64.0). A cached hint pins a waypoint
  // that was the closest root at ELECTION time; the neuromorphic layer restructures
  // the mesh continuously, so on resubscribe that waypoint can go from the optimal
  // path to a poor one — the SUB forced through a node the synaptome has already
  // routed around. Greedy + synaptome finds the current-best terminal on its own;
  // trust that. (_rootHint_ still runs its background lookup to warm the WRITE-path
  // cache — pub/kill/pull/metrics/repair — which is unchanged.)
  _sendSubscribe(topicBig) {
    const pinned = this._upstream.get(topicBig) || [];
    const via = pinned;   // [] when unpinned → greedy toward the topic id
    const sent = this._emitSubscribe(topicBig, via.slice(0, MAX_VIA));
    // Only a PINNED renewal can teach us the pin is dead. An unpinned SUB routes
    // toward the topic id itself, and its failure says the mesh is unreachable,
    // not that a waypoint is stale — there is nothing to drop.
    if (pinned.length) this._unpinIfWaypointDead(topicBig, pinned[0], sent);
  }

  // A subscriber must not renew forever toward a corpse.
  //
  // _upstream is the pin — the relay we renew toward, written by _onDeliver from
  // the DELIVER `from`. Until v4.58.0 exactly two things dropped it:
  // pubsubPeerDied (fires only for a peer we hold a CHANNEL to) and role
  // teardown. A relay reached through ROUTING can die with neither firing: no
  // channel closes, so pubsubPeerDied is silent, and _emitSubscribe stamps the
  // renewal obligation discharged the moment the send is on the wire. The pin
  // then outlives its target and nothing in the process can learn otherwise —
  // worse, `attached` stays true, so the adaptive interval BACKS OFF toward
  // RENEW_MS while reaching nobody. Only a reload recovered it, because a fresh
  // peer starts with an empty _upstream (David, 2026-08-01, wedged axona.chat
  // window; fence_subscribe_unpin).
  //
  // The assumption this corrects is stated in pubsubPeerDied's own header: "the
  // next renewal routed toward it is popped at the live terminal ('reroute') and
  // re-seats at the true root". True only when the via chain REACHES a live
  // node. When the pinned relay is simply gone the SUB exhausts in the mesh, no
  // terminal is reached, and nobody pops anything.
  //
  // Recovery is deliberately IDENTICAL to pubsubPeerDied's — drop the pin, snap
  // the interval to the floor, null the stamp — so remote death heals exactly
  // the way local death already does, on a path that has been in production
  // since 2026-07-13 rather than a second one invented here.
  //
  // This is only expressible now: _route discarded routeMessage's promise until
  // v4.57.0 and did not classify it until v4.58.0. Before this week a failed
  // renewal returned undefined and there was nothing to check.
  _unpinIfWaypointDead(topicBig, deadHex, sent) {
    const declares = this.dht?.verdictsSupported;
    Promise.resolve(sent).then((r) => {
      const v = dispatchVerdict(r, declares);
      if (v === 'violation') {
        this._log('error', 'pubsub:dispatch-contract-violation', {
          topic: idHex(topicBig).slice(0, 12), peer: deadHex.slice(0, 12),
          detail: 'adapter declares verdictsSupported but returned no verdict on SUB',
        });
      }
      // ONLY an explicit routing verdict of failure unpins. 'unsupported' and
      // 'violation' mean "no evidence", and unpinning on no evidence would
      // re-home every healthy subscriber on every non-reporting adapter on its
      // very first renewal. See dispatch.js on why that is the same fail-closed
      // rule that makes 'consumed' the only thing which credits a replica.
      if (v !== 'failed') return;
      const up = this._upstream.get(topicBig);
      if (!up || up[0] !== deadHex) return;   // a DELIVER re-homed us mid-flight
      this._upstream.delete(topicBig);
      const s = this.mySubscriptions.get(topicBig);
      if (s) { s.interval = this.renewFastMs; s.lastRenewSent = null; }  // null = 'renew now', NOT a time (C2)
      // THE ROLE STAMP TOO. _emitSubscribe stamps role.sync.lastRenewAt for every
      // role, and OBLIGATIONS reads that stamp for CHILD, BACKUP and HOLDER — so
      // resetting only the app subscription left every RELAY role reading
      // DISCHARGED after a renewal that reached nobody. That is this file's own
      // stated defect, left standing in the other half of the same funnel because
      // I scoped it out as "keep it minimal" (Aster, council seq 110). Same
      // sentinel, same meaning: null is 'renew now', never a time (C2).
      const role = this.axonRoles.get(topicBig);
      if (role?.sync) role.sync.lastRenewAt = null;
      this._log('info', 'pubsub:upstream-unpinned', {
        topic: idHex(topicBig).slice(0, 12), was: deadHex.slice(0, 12),
        detail: 'renewal did not reach its pinned waypoint — re-homing unpinned',
      });
    }).catch(() => {});   // _route cannot reject (v4.57.1); belt and braces
  }
  _emitSubscribe(topicBig, via) {
    const role = this.axonRoles.get(topicBig);
    const sub  = this.mySubscriptions.get(topicBig);
    const latest = !!(sub && sub.replayLatest);   // since:'latest' — newest entry rides this DELIVER, regardless of age
    // The dispatch outcome is RETURNED, not discarded: _sendSubscribe is the only
    // caller that knows whether the via it handed us came from the _upstream pin,
    // so it is the only one that can act on a failure. See _unpinIfWaypointDead.
    const sent = this._send(T.SUB, {
      topicId: idHex(topicBig), via, subscriberId: idHex(this.nodeId),
      since: this._sinceFor(topicBig),
      hw: role ? this._highWater(role) : 0,   // a cache-bearing relay advertises its history (§6)
      lw: role ? this._lowWater(role) : 0,    // …and its OLDEST stamp, so a root missing the pre-transition half pulls it
      latest,
    });
    // D0 / M4 COMPLETION STAMP. The renewal obligation (CHILD / BACKUP / HOLDER /
    // APP_SUB in OBLIGATIONS) is discharged HERE — after the SUB is on the wire,
    // not when the tick decided to try. Placed in _emitSubscribe rather than at
    // the three refreshTick call sites because this is the single funnel they all
    // pass through, so a future caller cannot forget to stamp.
    const nowAt = this._now();
    if (role) role.sync.lastRenewAt = nowAt;
    if (sub)  sub.lastRenewSent     = nowAt;
    // One-shot: 'latest' delivers the current value once at subscribe, not on
    // every renewal — clear the flag after this first emit.
    if (latest) sub.replayLatest = false;
    return sent;
  }

  // ── public API (contract surface) ────────────────────────────────────

  // Outstanding durability obligations: messages this node stamped whose cohort
  // dispatch has not yet been verified. leave() drains on THIS, never on local
  // delivery — seeing your own message says the root has it, not that anyone
  // else does.
  durabilityPending() { return this._durability.pending(); }

  // FINISHED and not durable: the attempt budget ran out, or there was no cohort
  // at all (a singleton root holding the only copy). Terminal, so leave() does
  // not wait on it — but an operator should see it, because it is the honest
  // count of history this node alone is carrying.
  durabilityUndurable() { return this._durability.undurable(); }

  // Route the UN-stamped publish toward the topic's root; root stamps it. Sent
  // SYNCHRONOUSLY and immediately: via the warm true-root hint if we have one (so
  // publisher + subscribers converge on the same root), else greedy ([]) toward the
  // bare topic id. _rootHint_ refreshes the hint in the background — never blocking
  // the publish on a slow live-mesh lookup.
  pubsubPublish(topicId, json, meta = {}) {
    const hint = this._rootHint_(topicId);
    // Retain briefly so a publish that stranded on the greedy walk (hint not yet
    // warm) is re-sent toward the true root the moment the background lookup
    // resolves — a one-shot publish never re-routes on its own, so a cold-hint
    // strand = a lost message. Idempotent: the root dedups by msgId.
    if (!this._pendingPub) this._pendingPub = new Map();
    // Keyed by msgId (not topic) so two quick publishes to the SAME topic don't
    // overwrite each other's pending retry — each message is independently retried.
    let pmsgId = null; try { pmsgId = JSON.parse(json)?.msgId ?? null; } catch { /* opaque body */ }
    if (pmsgId) this._pendingPub.set(pmsgId, { topicBig: topicId, json, at: this._now(), tries: 0 });
    this._send(T.PUB, { topicId: idHex(topicId), via: hint ? [hint] : [], json });
    // Early re-sends — ONE plan, ONE pump (v4.25.0, Phase 6): a cold publisher
    // (not yet integrated) front-loads burst waves while its table warms; a WARM
    // first publish to a topic gets one quick re-send so a just-formed tree still
    // catches it; subsequent warm publishes go back to a single send. Same quench
    // as the tick retry: the pending entry vanishing on observation (I-9).
    if (pmsgId) {
      const gaps = this._earlyResendPlan(this._isColdPublisher(), !this._publishedTopics.has(topicId));
      if (gaps.length) this._earlyResendPump(topicId, pmsgId, gaps);
      this._publishedTopics.add(topicId);
    }
    return meta.postHash || '';
  }

  pubsubSubscribe(topicId, opts = {}) {
    const seeded = this._lastSeenTsByTopic.get(topicId);
    const since  = Number.isFinite(seeded) ? seeded : this._now();
    // since:'latest' → carry a replayLatest flag so the root replays its newest
    // cache entry regardless of age (the ts-floor can't express "newest"). Sticky
    // across renewals (re-delivery is deduped); cleared by a later non-latest sub.
    this.mySubscriptions.set(topicId, {
      // C1: createdAt is the ACTIVATION time and is never rewritten. lastRenewSent
      // is reset to null by pubsubPeerDied to force an immediate re-emit, so it
      // cannot double as the birth time — without this, a subscription whose first
      // re-emit after upstream death never lands reads as pressure 0 forever.
      createdAt: this._now(),
      since, lastRenewSent: this._now(), interval: this.renewFastMs,
      replayLatest: !!opts.replayLatest,
    });
    // If this node already HOLDS the topic's cache (it is the root, or a
    // cache-bearing relay), no wire replay can serve it: the outgoing SUB
    // carries since=high-water (§6 — a holder never re-pulls history it already
    // stores), and a root's own SUB self-loops without seating. Replay the local
    // cache straight to the app against the app-level floor — without this, a
    // since:'all' subscriber that happens to be the topic's root receives zero
    // of the history it is itself storing. Idempotent (exactly-once app dedup).
    const role = this.axonRoles.get(topicId);
    if (role) this._replayLocal(role, since, !!opts.replayLatest);
    this._sendSubscribe(topicId);
  }

  pubsubUnsubscribe(topicId) {
    this.mySubscriptions.delete(topicId);
    const via = this._upstream.get(topicId) || [];
    this._send(T.UNSUB, { topicId: idHex(topicId), via, subscriberId: idHex(this.nodeId) });
    this.pubsubResetTopicConsumption(topicId);
  }

  // ── Demand-driven metrics ────────────────────────────────────────────
  // The peer registers a publisher that turns (dataTopicIdHex, snapshot) into a
  // publish to metricTopic(dataTopicId). Kept out of the kernel so the kernel
  // never needs an author key — the snapshot is published like any other message.
  setMetricsPublisher(fn) { this._metricsPublisher = (typeof fn === 'function') ? fn : null; }

  // Request metrics for a DATA topic: start a renewable lease toward its root.
  // Idempotent; renewed on the refresh tick. (The peer calls this when the app
  // subscribes to metricTopic(dataTopicId).)
  pubsubMetricsOn(dataTopicBig) {
    if (!this.myMetricsRequests.has(dataTopicBig)) this.myMetricsRequests.set(dataTopicBig, { lastSent: 0 });
    this._sendMetricsOn(dataTopicBig);
    this.myMetricsRequests.get(dataTopicBig).lastSent = this._now();
  }
  // Stop wanting metrics for a topic (lease lapses at the root → it stops publishing).
  pubsubMetricsOff(dataTopicBig) { this.myMetricsRequests.delete(dataTopicBig); }

  // Route a METRICSON toward the data topic's root (lookup-assisted, like SUB).
  _sendMetricsOn(dataTopicBig) {
    const hint = this._rootHint_(dataTopicBig);
    this._send(T.METRICSON, { topicId: idHex(dataTopicBig), via: hint ? [hint] : [], requesterId: idHex(this.nodeId) });
  }

  // Publish one metric snapshot for a rooted topic, throttled to METRICS_PUB_MS
  // (callers may fire on any trigger — the tick, or a just-armed lease — and the
  // throttle keeps the net cadence). Advisory: never throws.
  _publishMetricSnapshot(topicBig, role, now) {
    if (!this._metricsPublisher || !role.isRoot) return;
    if (now - role.metricsLastPub < METRICS_PUB_MS) return;
    role.metricsLastPub = now;
    const snap = { v: 1, topic: idHex(topicBig), ts: now, by: idHex(this.nodeId),
      current_count: role.cache.length, seq: role.seq,
      subscribers: role.subscribers.size, bytes: role.cacheBytes,
      publishes: role.publishes ?? 0 };
    try { const p = this._metricsPublisher(idHex(topicBig), snap); if (p && typeof p.catch === 'function') p.catch(() => {}); }
    catch { /* advisory — never let a metrics publish break the caller */ }
  }

  pubsubResetTopicConsumption(topicId) {
    // "Consumed nothing" → seed the since-floor to 0 so a following subscribe
    // replays the FULL history (since:'all'). MUST NOT delete the entry: a
    // missing _lastSeenTsByTopic makes pubsubSubscribe fall back to since=now()
    // (live tail), which silently defeats since:'all' (the live backlog/gap
    // recover-0% bug — the root then filters out everything before now).
    this._lastSeenTsByTopic.set(topicId, 0);
    this._upstream.delete(topicId);
    const prefix = topicId.toString(16) + ':';
    for (const k of this._appDelivered.keys()) if (k.startsWith(prefix)) this._appDelivered.delete(k);
  }

  pubsubHost(topicId) {
    // REGION RULE backstop (when enforced): a node hosts/roots only topics in its region.
    if (!this._regionOk(topicId)) {
      this._log('warn', 'host-refused-foreign-region', { topic: idHex(topicId).slice(0, 12) });
      return;
    }
    this._hostedTopics.add(topicId);
    // Participate so the node won't be torn down and can root the topic if closest.
    // Route the announce through _sendSubscribe (lookup-assisted → the true root, and
    // advertises our high-water for §6 PULLUP) rather than a bare greedy via:[] — the
    // bare send stranded the initial host announce until the next refreshTick healed it
    // (the tick already renews hosts via _sendSubscribe). v4.10.1.
    this._sendSubscribe(topicId);
  }
  pubsubUnhost(topicId) {
    this._hostedTopics.delete(topicId);
    const role = this.axonRoles.get(topicId);
    if (role) { const me = lc(idHex(this.nodeId)); role.subscribers.delete(me); role.children.delete(me); }
  }
  pubsubHostKeyspace(on = true) { this._hostKeyspace = !!on; }

  // Route the kill (tombstone) toward the topic's root EXACTLY like a publish:
  // via the warm true-root hint if we have one, else greedy. A kill is a one-shot
  // routed message — without the hint it strands on the greedy walk just as a cold
  // publish does, and (unlike a renewed subscribe) never re-routes on its own, so a
  // stranded kill = a tombstone that never reaches subscribers (the ~30% "kill not
  // received" flake). Retain it briefly so the background lookup re-sends it toward
  // the true root once resolved. Idempotent — the root dedups the tombstone by msgId.
  pubsubKill(topicId, kill) {
    const hint = this._rootHint_(topicId);
    if (!this._pendingKill) this._pendingKill = new Map();
    if (kill?.msgId) this._pendingKill.set(kill.msgId, { topicBig: topicId, kill, at: this._now(), tries: 0 });
    this._send(T.KILL, { topicId: idHex(topicId), via: hint ? [hint] : [], kill });
  }
  // pubsubUnpub() — REMOVED v4.3.0 (decision 2026-06-25: keep kill, drop unpub)
  pubsubTouch(topicId, touch) { this._send(T.TOUCH, { topicId: idHex(topicId), via: [], touch }); }

  requestPull(topicId, postHash = null, { timeoutMs = 1000 } = {}) {
    const corrId = idHex(this.nodeId).slice(0, 8) + ':' + (++this._pullSeq);
    // Route toward the true root via the warm lookup-assist hint (like publish/kill),
    // not a bare greedy via:[] — a pull that strands on a local minimum reaches a
    // non-cohort node and returns null (a false "no message") even though the cohort
    // holds it. The hint seeds the walk at the topic-closest node it can serve. v4.10.1.
    const hint = this._rootHint_(topicId);
    return new Promise((resolve) => {
      // Q1: a read must say WHICH kind of nothing it got. Resolving null here made
      // a timeout indistinguishable from a responder that holds nothing and from an
      // unparseable reply — three facts, one value — and every consumer above then
      // manufactured a confident negative from it. See test/fence_pull_outcome.mjs.
      const timer = setTimeout(() => { this._pending.delete(corrId); resolve({ kind: 'timeout', timeoutMs }); }, timeoutMs);
      if (typeof timer.unref === 'function') timer.unref();
      // Store the requester alongside corrId: the PULL/PULLRESP pair is (corrId,
      // requesterId), not corrId alone. _onPullResp requires the response's
      // requesterId to fold to this so a locally-routed response carrying a
      // FOREIGN requesterId cannot settle our read (Aster, council d17ece0b).
      this._pending.set(corrId, { resolve, timer, requesterId: this.nodeId });
      this._send(T.PULL, { topicId: idHex(topicId), via: hint ? [hint] : [], corrId, postHash: postHash || null, requesterId: idHex(this.nodeId) });
    });
  }
  // Enumerate the topics THIS node currently roots, each with a locally-computed
  // metric snapshot. The producer side of the derived-metric-topic convention: an
  // infrastructure root walks this on a timer and republishes each to metricTopic(T).
  // (Re-added in v4.10.1 — the routing-only clean break (v3.12) dropped it, silently
  // killing all metrics on the 4.x line.) Under the v4.10.0 cohort model EVERY
  // co-hosting root publishes its own snapshot; `seq` and `current_count` converge
  // across the cohort (anti-entropy), while `subscribers` is this member's local
  // subset — the reader (peer.metrics) aggregates across the cohort.
  rootedTopics() {
    const out = [];
    const now = this._now();
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot) continue;
      // Recover the signed topic descriptor from the newest cached envelope (the role
      // holds only the topic id as a bigint). No cache → nothing to describe → skip.
      let descriptor = null;
      for (let i = role.cache.length - 1; i >= 0; i--) {
        try { const env = JSON.parse(role.cache[i].json); if (env && env.topic) { descriptor = env.topic; break; } } catch { /* */ }
      }
      out.push({
        topicId:       idHex(t),
        descriptor,                          // { region, owner, name, write } | null
        current_count: role.cache.length,    // messages currently in cache (swept of expired/killed)
        seq:           role.seq,             // message counter — dense per-topic high-water (monotonic)
        subscribers:   role.subscribers.size,// this cohort member's local subscriber subset
        bytes:         role.cacheBytes,      // live cached envelope bytes
        publishes:     role.publishes ?? 0,  // advisory throughput: distinct messages ever cached here
      });
    }
    return out;
  }

  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  setLogSink(fn) { this._logSink = (typeof fn === 'function') ? fn : null; }

  resetState() {
    this.axonRoles.clear();
    if (this._tombAuthority) this._taReset();   // Phase 3 shadow: no role holds a body now — rebuild (no-op flag-off)
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
    this._backupTopics.clear();
    this._lastSeenTsByTopic.clear();
    this._upstream.clear();
    this._rootHint.clear();
    this._pendingPub?.clear();
    this._lookupInflight?.clear();
    this._rootBeacons.clear();
    this._beaconSeen.clear();
    this._lastAnnounce?.clear();
    this._lastBeaconAt = 0;
    this._appDelivered.clear();
    for (const p of this._pending.values()) clearTimeout(p.timer);
    this._pending.clear();
  }

  _log(level, event, ctx) {
    if (this._logSink) { try { this._logSink(level, 'pubsub:' + event, ctx); } catch { /* sink threw */ } }
  }
}

// ── Phase 2 assembly ────────────────────────────────────────────────────
// The four concern modules contribute their methods to the prototype; `this`
// is the manager everywhere, and all state stays on the manager façade (the
// same pattern as rootClaim.js, which owns the isRoot transitions).
Object.assign(
  AxonaManager.prototype,
  topicStoreMethods,     // cache, tombstones, exactly-once app delivery
  rootElectionMethods,   // beacons, hints, self-verification, liveness
  repairPlaneMethods,    // the tick scheduler, retries, replication, departure
  wireHandlersMethods,   // routed handlers + axon-tree mechanics
  syncEngineMethods,     // Phase 8: the ONE repair/durability sync operation + policy table
  writeFlightMethods,    // E3 (Dead-Root Eviction v0.3): ingest-ack write completion, receipt probe, evict + retry-promote
  tombstoneAuthWiringMethods,  // REF-1.1 S2.0c Phase 3: DEFAULT-OFF shadow observers (no-op unless the tombstoneAuth flag is set)
);

export default AxonaManager;
