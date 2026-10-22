// =====================================================================
// constants.js — the pub/sub kernel's tunables, wire types, and gated
// policy switches, in one place (refactor Phase 2; audited in
// INVARIANTS.md Appendix A — every constant there is live with a test or
// incident behind it). Grouped by concern; Phase 4 turns these groups
// into documented policy objects.
// =====================================================================

// ── Inbound caps (D-1: bound attacker-controlled payloads) ──────────────
// Re-exported via AxonaManager.js unchanged — AxonaPeer and std/chunk import
// these as the publish-size contract; independent of the pub/sub mechanism.
export const MAX_PUBLISH_BYTES = 256 * 1024;         // absolute hard ceiling (chars)
export const MAX_RELIABLE_PUBLISH_BYTES = 15 * 1024; // WebRTC-interop reliable floor (O-5)

// Region is a PLACEMENT HINT ONLY (the S2 high byte folded into every id for
// routing locality). It is never an eligibility predicate: no code compares
// regions to gate admission, rooting, seating, storage, repair, or delivery. The
// closest reachable node roots a topic whatever its region. (The region-occupancy
// wall and its configureRegionLock/isRegionLockEnforced flag were removed in
// v4.75.0 — they were never part of the protocol's intent.)

// ── Renewal / attachment (the adaptive-renewal policy) ──────────────────
export const RENEW_MS        = 60_000;          // re-subscribe cadence CEILING (stable state)
// Adaptive renewal (churn re-home fix): a subscriber re-homes only when it renews,
// so the renewal interval IS the orphan window after its relay/root churns. A flat
// 60s is too slow for mobile churn; a flat 5s is 12× the steady-state traffic. So
// renew FAST right after subscribing or after a relay change (re-pin), backing off
// ×1.5 toward the ceiling while stable. Sustained churn keeps re-pinning → stays
// fast (measured ~82% delivery @30%/round vs 43% at 60s); calm → backs off → cheap.
export const RENEW_FAST_MS   = 5_000;           // adaptive floor: initial + post-re-home interval
export const RENEW_BACKOFF   = 1.5;             // multiply the interval each stable renewal, up to RENEW_MS
export const DROP_MS         = 180_000;         // evict a subscriber after missed renewals (≥ 3× ceiling)
// IDLE-ROLE REAP (David 2026-09-23). A role whose cache is EMPTY and whose last
// message is older than this is torn down, whatever else would retain it — root
// or child, with subscribers or without. Without it a node that wins ROOT for a
// topic keeps the seat for ever: prod west held 141 roles over 141 distinct
// topics, 45 of them rooted, with ZERO children and ZERO cached messages, and
// re-accrued 193 within three minutes of a restart, which is what drove its
// saturation flag and ~39% of one core on a single connection.
// A subscriber that still wants the topic re-subscribes on its own renewal
// cadence and the role is rebuilt, so this is recoverable by construction.
export const ROLE_IDLE_TTL_MS = 24 * 60 * 60 * 1000;   // 24 h
// EMPTY-ROLE GRACE (David 2026-10-19). A keyspace-hosting node retained every
// root it won for the whole idle TTL even when the role held NOTHING — no
// bodies, no active tombstone, no seated subscriber, no backup seat, no lease,
// no local intent. Aster's 2026-10-18 census counted 1,011 such roots across
// the fleet; sfo3 grizzly1's seven-day journal showed the reapers firing exactly
// at the TTL and the relay forming ~200 roots a day for single-use test topics,
// so the inventory was one day of churn held by the keyspace pin. The pin is
// now a bounded grace measured from the last tick on which the role held
// anything; a later SUB or PUB re-forms the role at the same routing terminus.
// Sized about an order of magnitude above everything an empty root may still
// be doing: 6.7× the 45 s root verify, 10× a publisher's 30 s re-send window,
// ~9× the birth probe (≤ 3 tries 5 s apart, re-armed once after 20 s). A
// policy choice, not a bound on scheduler suspension (Aster edfdd869). 0
// restores the eternal pin.
export const ROLE_EMPTY_GRACE_MS = 5 * 60 * 1000;   // 5 min

// ── TOPIC LIFETIME (axona-docs architecture/Axona-Topic-Lifetime-v0.1.md, title
// v0.9 @ 11288d7; David "Build" 2026-10-20) ──────────────────────────────────
// The 2026-10-19 census: 2,997 roles over 802 topics, 1,367 of them BACKUPS and
// 1,147 of those seating nobody — a root pushes to two backups because it has
// bodies, the backups seat at the root every tick because they exist, the root
// is never dead because it has seats, the backups are never dead because they
// are backups. David: "If there is also nothing within the cache the entire
// topic framework must be dismantled. This must also be done quickly and
// regularly." The whole machine sits behind TOPIC_LIFETIME (kernel option
// `topicLifetime`, env TOPIC_LIFETIME=1). ON by default since 4.118.0 (David,
// 2026-10-22: "a full arm of both"): every node arms unless TOPIC_LIFETIME=0 or
// the option says otherwise. 4.117.0 shipped it OFF; the one day armed by env
// (2026-10-21) showed that a holder that never honours the notice regenerates
// the root without bound, which is why the bridges now floor the kernel at
// 4.118.0: an unarmed holder cannot reach the door.
export const TOPIC_LIFETIME_DEFAULT = 1;
// A standby appointment is a LEASE on one thing: the root's REPLICATE. Renewed
// only by a REPLICATE (lastReplicaAt's one writer is becomeBackup); nothing the
// backup sends moves it. L = DROP_MS, the seat lease, so a backup lapses on the
// same clock a reader does (packet 4.4).
export const STANDBY_LEASE_MS = DROP_MS;                 // 180 s
// The two assumptions the grant rule rests on, stated apart from it (4.4):
// D = the longest a frame is in flight; S = the clock skew between any two
// relays. A conforming grant reads at most D + S old by the receiver's clock;
// one reading older is dropped, whatever it names. S is SET by David
// (2026-10-20, "That is fine"), not measured; a real skew beyond S drops
// conforming grants and admits nothing, so a later measurement can only widen it.
export const APPOINTMENT_DELAY_MS = 60_000;              // D
export const APPOINTMENT_SKEW_MS  = 15_000;              // S
// A holder that leaves its part in a LIVE generation keeps a LEFT record for
// L + S + D (255 s) on the monotonic clock (4.2); the map is bounded and
// pressure evicts only an EXPIRED record. Termination writes NO record (4.5).
export const LEFT_RECORD_MAX = 4096;
// TOPIC_TERMINATED is sent once per tick for up to this many ticks, or until
// every seat has left, whichever is first; the role goes on the tick after its
// last send (4.5).
export const TOPIC_TERMINATE_TRIES = 3;
// A thin branch steps down (4.7): a child whose declared demand has read below
// CHILD_MIN_READERS for CHILD_STEP_DOWN_MS rehomes its readers to its upstream
// and retires. Hysteresis: a new child is not judged before CHILD_MIN_STAY_MS;
// a parent does not re-promote a node that stepped down within
// CHILD_COOLDOWN_MS. David 2026-10-20: "Let's use 2 subscribers and 90 seconds."
export const CHILD_MIN_READERS   = 2;
export const CHILD_STEP_DOWN_MS  = 90_000;
export const CHILD_MIN_STAY_MS   = 90_000;
export const CHILD_COOLDOWN_MS   = 300_000;

// ── Root election / convergence ─────────────────────────────────────────
// Reachable-root fallback (cold-convergence fix). An unpinned subscriber whose
// iterative root hint names a node CLOSER in XOR but that never adopts it back
// (unreachable on the greedy data path / "broken-but-authentic") would otherwise
// defer forever and the topic never roots → total strand. After this confirmation
// window with no upstream pin, if we are the closest node among our REACHABLE
// neighbours we claim root locally — preferring a reachable root over a
// closer-but-unconfirmed one. Long enough (≥2 ticks) that a genuinely reachable
// multi-hop closer root has time to adopt us first; a wrongly-claimed farther root
// is self-corrected by the strictly-closer beacon demotion (_onRootBeacon).
export const ROOT_CLAIM_MS   = 6_000;           // unconfirmed-deferral window before self-claiming root
// Singleton-root durability: a root with no sub-axon tree (the small-topic common
// case) has no cache-holding backup, so an abrupt root churn loses all history for
// future since:'all' joiners. The root proactively replicates its full cache to its
// REPLICAS nearest reachable neighbours (the natural successors); on churn the
// now-closest backup already holds everything and the reachable-root fallback
// promotes it with no gap. Backups are re-evaluated each tick (closer newcomers
// recruited, farther ones retired). 0 disables.
export const ROOT_REPLICAS   = 2;
// A backup is a subscribing CHILD RELAY that also prefetches the root's full cache
// (see _onReplicate + the _backupTopics subscribe loop in refreshTick). Promotion is
// NOT a bespoke, local-only decision anymore (that split roots when two backups
// couldn't see each other): a backup renews its subscribe every tick, so on root
// churn the SAME probe-protected machinery every subscriber/host uses — _rootHint_'s
// iterative lookup() → single globally-closest terminus — elects exactly ONE new root
// (the closest self-roots, the rest re-home under it), gap-free from the warm cache.
// This constant only bounds set growth: a backup whose root stopped replicating to it
// this long (root departed & we've since re-homed/promoted, or the root retired us) is
// dropped from _backupTopics as cleanup — never used to trigger promotion.
export const BACKUP_EVICT_MS = 60_000;
// Delta-aware replication (v4.24.1, task #333): the periodic _replicateRoots sweep
// pushes a root's FULL cache+tombstones only when its state changed since the last
// push, a new cohort member was recruited, or this anti-entropy backstop elapsed —
// otherwise it sends an empty REPLICATE as a liveness KEEPALIVE (the recipient
// refreshes lastReplicaAt; ingest of an empty batch is a no-op). Before this, every
// root re-sent its entire cache to its whole cohort EVERY tick; during the 4.24.0
// soak's root-transition storms that per-tick × per-role full-state traffic was the
// bandwidth fuel of the role-bloat collapse (#332 feedback loop: 2.7 GB relay RSS,
// >1M log lines/night). Convergence is unaffected: any divergence changes the
// diverged root's own state signature and triggers one full push (union-ingest at
// the receiving root), then quenches back to keepalives.
export const ROOT_REPLICATE_FULL_MS = 60_000;
// ── Join-storm pacing (task #332, invariant I-11: bulk work never starves ──
// liveness). Two failure facets, both live-reproduced on testnet 2026-07-17:
// a node holding N roles that gains a new cohort member (a joining relay)
// fires N full-state pushes at it within ONE tick, and the recipient's event
// loop drowns in per-message signature verifies — its mesh keepalives miss,
// peers evict it, and the mesh dissolves without self-healing.
//
// Sender leg: at most this many FULL replications per tick (round-robin cursor
// across roles, so deferred roles are next tick's first served). Keepalives are
// unbudgeted (empty, cheap). Seeding a newcomer into a 1,000-role region takes
// ~1000/32 ≈ 31 ticks ≈ 2.6 min — degraded convergence instead of a dead relay.
// Under sustained load the 60s anti-entropy backstop stretches proportionally;
// that is I-11 working as intended, not a bug.
export const REPLICATE_FULL_BUDGET = 32;
// Receiver leg (defense in depth — also bounds a malicious flood): REPLICATE /
// REPLAYUP ingest is queued and drained in time-boxed slices so verification
// CPU can never monopolize the loop. Queue overflow drops the NEWEST payload
// (logged); anti-entropy re-delivers within ROOT_REPLICATE_FULL_MS.
export const INGEST_QUEUE_MAX = 4096;           // queued ingest payloads before overflow-drop
export const INGEST_SLICE_MS  = 8;              // ingest CPU per slice; keeps ping RTT << mesh eviction timeout
// Mesh re-warm (facet 2: relays never re-initiated their inter-mesh after a
// mass departure — peers=1..2 forever, systemd-green but backbone-dead). If the
// mesh stays below MIN for TICKS consecutive ticks, re-run self-integration
// (findKClosest(self) + open channels — idempotent, never throws), at most once
// per cooldown.
export const MESH_REWARM_MIN         = 3;       // mesh peers below this = starved
export const MESH_REWARM_TICKS       = 3;       // consecutive starved ticks before acting (~15s: outlasts a blip)
export const MESH_REWARM_COOLDOWN_MS = 60_000;  // min gap between re-warm attempts (integration takes seconds)
// R5 (Routed-Walk-Terminals v0.6): the gap DOUBLES after every ask that found
// the mesh still starved — 1, 2, 4, 8 min … up to this cap — and resets to the
// cooldown the moment a bound channel appears. A node whose offers never
// complete asked every minute without bound before this (one Safari tab:
// 272 asks and 5699 relayed signals in an hour, 2026-10-11).
export const MESH_REWARM_BACKOFF_MAX_MS = 600_000;

// ── Cache / tree shape / wire sanity ────────────────────────────────────
export const CACHE_MAX       = 1024;            // messages cached per relay
export const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
export const MAX_DIRECT      = 20;              // direct subscribers before a relay delegates

// ── Axonic admission control (v4.46.0) ─────────────────────────────────────
// The NEUROMORPHIC layer has had all three of these for a long time: a declared
// degree budget (AxonaDomain.MAX_SYNAPTOME = 50), a refusal at the budget
// (NeuronNode.addIncomingSynapse just returns), and — under a cap — a stratified
// breadth-before-depth fill so a capped node spreads coverage instead of
// exhausting itself on whatever it happened to see first
// (utils/geo.js buildXorRoutingTable). The AXONIC layer had none of them: roles
// accrued without limit, a node could never say no, and a joining node was
// handed a bulk backlog while still mid-handshake. Prod, 2026-07-26: relays at
// 325/431/523/720 roles on 961 MB boxes, five of nine locked out of the bridge.
//
// MAX_ROLES is the axonic MAX_SYNAPTOME. It is a SELF-DECLARED, SELF-LIMITING
// ceiling: it can only ever cause a node to hold LESS, never to acquire. It is
// NOT an address-rule exception — hosting is still decided by address; this only
// lets an address-eligible node answer "full" instead of degrading silently.
export const MAX_ROLES = 96;   // roles (root+backup+child) before a node declares saturated

// A joining node must TRANSPORT immediately — reachability to a newcomer lives
// in its neighbours' tables and is healed by inbound traffic, so carrying
// traffic is what integrates it. It must NOT manage roles immediately: bulk role
// ingest blocks the event loop, the client-hello misses the bridge's 5s deadline,
// the node is rejected, and it accrues more roles while locked out (#332/#338).
export const ROLE_GRACE_MS = 90_000;   // refuse root/sub MANAGEMENT this long after join

// Paced role admission — the axonic analogue of Phase-1-breadth. Even past
// grace, a node accepts at most this many NEW roles per refresh tick, so a
// backlog is absorbed over seconds instead of in one event-loop-blocking burst.
export const ROLE_ADMIT_PER_TICK = 4;

// ── Capacity as a MEASUREMENT, not an inventory (v4.47.0) ──────────────────
// MAX_ROLES (above) counts roles. A count is the wrong instrument: 96 idle
// roles is nothing, 96 hot ones may be fatal, and the same count means
// different things on different hardware. What matters is whether a node can
// SERVICE what it holds inside the deadlines the protocol already imposes.
//
// Two deadlines already exist and are the honest denominators:
//
//   DROP_MS (180s) — a role/subscriber unserviced this long is treated as DEAD
//     by its cohort. So (age of my least-recently-serviced role) / DROP_MS is
//     literally "fraction of the way to silently rotting a role". At 1.0 a role
//     HAS rotted and nobody was told.
//
//   HELLO_DEADLINE_MS (5s) — the bridge closes a client that fails to complete
//     its hello in this window (the misleadingly-named state=upgrade-required).
//     Event-loop lag / HELLO_DEADLINE_MS is "fraction of the way to being
//     kicked off the network". This is the #332 spiral, made observable.
//
// Both are OBSERVED (wall-clock deltas over real state), not derived from the
// role count. That distinction is the whole point: ceil(roles/BUDGET)*tick is
// a linear function of the count — MAX_ROLES in different units — and would
// have dressed arithmetic up as telemetry.
export const HELLO_DEADLINE_MS = 5_000;   // bridge client-hello budget (mirror of the bridge's own)

// ── The tick-lag observation WINDOW (v4.49.0) ──────────────────────────────
// 4.47.0 recorded tick lag as an ALL-TIME high-water mark, which made
// helloPressure a ratchet: it could rise and could never fall. One 60s browser
// tab suspension — what every phone does on screen lock — put helloPressure at
// 11.0 (18x the 0.6 threshold) and left it there. Measured: still saturated
// after 2,050 healthy ticks (~2.8h). A saturated node refuses HANDOFF, so every
// backgrounded browser peer silently stopped being somewhere a departing node's
// last copy of a topic's history could land.
//
// The replacement is a rolling maximum over the last N ticks. A WINDOW, not a
// decay constant, deliberately: a decay factor has no natural unit here (why
// 0.95 and not 0.9?) and its recovery time is an emergent property you have to
// derive. A tick count states the recovery bound directly — "a stall stops
// counting against you N ticks after it stops happening" — which is the
// property the fence asserts and the property an operator needs to reason about.
//
// 12 ticks x the 5s refresh interval = ~60s of wall clock. (The window is in
// TICKS, so its wall-clock length follows refreshIntervalMs; every consumer in
// the fleet uses the 5s default — verified, no overrides in kernel, relay or
// bridge.) Long enough that a genuinely struggling node stays
// saturated across its bad patch; short enough that a recovered one is trusted
// again within a minute. The window only advances when a tick RUNS: a frozen
// node writes no samples and keeps its old reading, which is correct — we have
// no evidence of health from a node that isn't running.
export const TICK_LAG_WINDOW = 12;        // ticks retained for the rolling lag maximum

// Declare saturated at this fraction of a deadline. 0.6 leaves room to shed
// pressure before anything is actually lost — a node that waits until 1.0 has
// already dropped history.
export const SATURATION_PRESSURE = 0.6;

// ── SERVICE OBLIGATIONS (D0 / M4, 2026-07-30) ───────────────────────────
// What the refresh tick OWES each role, where that debt is discharged, and by
// when. Normative and machine-readable so it can be fenced the way SYNC_POLICIES
// is — a rule that is not a test drifts.
//
// WHY THIS TABLE EXISTS. Before D0, capacity was measured from a single
// `lastServicedAt` stamped on EVERY role at the TOP of refreshTick, before any
// work. The stamp therefore meant "a tick began while this role existed", not
// "this role's obligation was discharged", and servicePressure was measured
// against one denominator (DROP_MS) for every nature. Three consequences, all
// measured (test/d0_probe.mjs, commit 89c0798):
//   1. DOMINATED — reaching SATURATION_PRESSURE needed a 108s tick gap while
//      helloPressure needed 8s, so servicePressure could never be the deciding
//      signal in the only scenario it could detect.
//   2. FALSE NEGATIVE under the exact cause its own docstring named. 640 roles /
//      40 ticks: worst un-refreshed full push 95_000ms against its own 60_000ms
//      deadline, while servicePressure read 0, overdue read 0, saturated() was
//      false and admitPushedRole() still returned true.
//   3. COVERAGE — mySubscriptions was outside the walk entirely, so the node's
//      own app subscriptions were unmeasurable rather than mismeasured.
//
// THE FIX IS THE COMPLETION POINT, NOT THE FIELD. Each obligation is stamped
// where the work actually finishes. Two of these were already in the right place
// and simply were not being read: `sync.lastFullAt` is written inside the
// `if (full)` branch of _replicateRole, AFTER the pushes.
//
// DEADLINES ARE PER-OBLIGATION, and that is the second half of the fix. One
// DROP_MS denominator made 1.0 mean "failed" for renewal and something arbitrary
// for everything else. Now age/deadline == 1.0 means THIS obligation has failed,
// for every row, so the max across rows is comparable.
//
// ROOT carries replication only, deliberately. Beacons (BEACON_MS) and root
// self-verify are both throttled/batched, and a missed one costs convergence
// LATENCY; a missed replication costs DURABILITY. Measuring latency work as debt
// would re-introduce the false-positive risk this table exists to remove. That is
// a judgement, not a measurement — flagged as such to the council 2026-07-30.
// THE ROOT DEADLINE IS THREE PERIODS, NOT ONE (4.113.0; council 11a8c5d5,
// 2026-10-13). Until 4.112.0 ROOT's deadline was ROOT_REPLICATE_FULL_MS itself,
// the same 60 s at which the push re-arms (repairPlane: `age >= ROOT_REPLICATE_
// FULL_MS` fires the push). So on every node holding a root the ROOT age cycled
// 0 → 60 s each minute, the pressure 0 → 1.0, crossing SATURATION_PRESSURE 0.6
// at 36 s: a root serviced ON SCHEDULE read saturated for the last 24 s of
// every minute, refused new roles and HANDOFF for that window, and its bridge
// reported "degraded" (east 0.874 and west 0.589 in one read, two phases of one
// cycle). Every other row measures a 60 s renewal against DROP_MS 180 s, three
// to one; ROOT measured 60 against 60. Now ROOT uses the same ratio: a push on
// schedule (60 s, plus one 5 s tick) peaks at 65/180 = 0.36; saturation (0.6)
// means a push 48 s late, most of a period; overdue (1.0) means two pushes
// missed. The push cadence itself is unchanged.
export const ROOT_FULL_DEADLINE_MS = 3 * ROOT_REPLICATE_FULL_MS;   // 180 s

export const OBLIGATIONS = Object.freeze({
  ROOT: Object.freeze({
    what:     'replicate full state to the K-closest cohort',
    stamp:    'lastFullAt',              // role.sync.lastFullAt
    deadline: ROOT_FULL_DEADLINE_MS,     // 180s — three push periods (4.113.0); was the period itself
    why:      'a missed full push costs DURABILITY: the cohort holds a stale copy',
  }),
  CHILD: Object.freeze({
    what:     'renew the subscribe toward our upstream',
    stamp:    'lastRenewAt',             // role.sync.lastRenewAt
    deadline: DROP_MS,                   // 180s — when the upstream evicts us
    why:      'past DROP_MS the upstream has dropped us and delivery stops',
  }),
  BACKUP: Object.freeze({
    what:     'renew the subscribe that keeps us election-eligible',
    stamp:    'lastRenewAt',
    deadline: DROP_MS,
    why:      'a lapsed backup cannot win the election its principal is relying on',
  }),
  HOLDER: Object.freeze({
    what:     're-announce the hosted topic and advertise our high-water',
    stamp:    'lastRenewAt',
    deadline: DROP_MS,
    why:      'a silent holder is invisible to a freshly promoted root, so its cache strands',
  }),
  APP_SUB: Object.freeze({
    what:     'renew the local application subscription',
    stamp:    'lastRenewSent',           // mySubscriptions entry, NOT a role
    deadline: DROP_MS,
    why:      'the node stops receiving its own subscriptions; invisible before D0',
  }),
});
export const DELEGATE_BATCH  = 8;               // subscribers handed off when promoting a child
export const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
export const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side, Phase 2+)
export const TTL_MS          = 24 * 60 * 60 * 1000;   // 24h message hold, keyed on the ROOT timestamp
export const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
export const REPLAY_CHUNK_BYTES = 96 * 1024;    // byte budget per replay deliver batch
export const FUTURE_TOLERANCE_MS = 5 * 60 * 1000;   // §5 bad-clock rule: drop replayed stamps this far ahead

// ── Publish/kill reliability (observation-confirmed retry, I-9) ─────────
export const PENDING_PUB_TTL_MS = 30_000;       // retain a recent publish/kill this long so refreshTick can RE-SEND it toward the true root until the publisher observes its own msgId (implicit ack) — covers a strand on the greedy walk AND packet loss; idempotent (root dedups by msgId)
export const PENDING_PUB_MAX_TRIES = 6;         // cap re-sends so a never-confirmed publish (e.g. a non-subscribed publisher under loss) can't re-send unboundedly; loss^7 is negligible even at 30%
// Cold-publish burst (v4.11.0): a publish from a freshly-joined, not-yet-integrated
// node is the worst case for the one-shot greedy PUB — its routing table lacks the
// keyspace breadth to route to the true root, so the message strands and (unlike a
// renewing subscribe) never re-homes. Waiting for the table to warm is harmful: it's
// OUTBOUND traffic that integrates a newcomer (reachability to a node lives in its
// NEIGHBOURS' tables, healed by directed sends — same lesson as churn-in warmup). So
// while cold, re-send the SAME signed envelope a few times over the first second:
// idempotent (root dedups by msgId), each send both integrates us further AND gets a
// fresh shot at the true root as tables converge. Naturally disabled once warm (the
// gate is low neighbour count), and the slow refreshTick retry still backstops after.
export const COLD_BURST_TRIES     = 5;          // extra fast re-sends on a cold publish
export const COLD_BURST_INTERVAL_MS = 200;      // spacing of the fast burst (≈1s total)
export const COLD_BURST_SLOW_TRIES    = 5;      // then a second, slower wave as tables keep warming
export const COLD_BURST_SLOW_INTERVAL_MS = 400; // 5 × 400ms ≈ 2s more coverage past the first second
export const COLD_PEER_THRESHOLD  = 8;          // "cold" = fewer than this many neighbours (not yet integrated)
// First publish to a topic — even when WARM — is re-sent once this long after, so a
// tree that formed microseconds before the send (a just-arrived subscriber, a root
// that just rooted) still catches it. Cold publishers already re-send via the burst.
export const FIRST_PUBLISH_RESEND_MS = 200;

// ── Root beacon (soft-state root advertisement; Pubsub-Root-Beacon-v0.1) ────
// A root periodically announces "root for T was last at X" to its K XOR-closest
// neighbors (the topic's convergence basin), recursive 2 layers. Receivers cache
// it (verify-don't-trust) and consult it before greedy/lookup, fixing the
// last-mile divergence (publisher/subscriber resolving different roots).
export const BEACON_MS       = 20_000;          // emission cadence (faster than RENEW_MS so churn heals quickly)
export const BEACON_TTL_MS   = 50_000;          // inbound pointer validity (~2.5×BEACON_MS)
export const BEACON_FANOUT   = 6;               // K closest neighbors per layer (fan-out ≤ K+K²)
export const BEACON_LAYERS   = 2;               // recursive forward depth
export const BEACON_SEEN_MS  = 60_000;          // flood-dedup retention

// Root self-verification (v4.19.1). Beacon-gated reconciliation is reach-limited:
// a spurious root minted by a stranded SUB on a FRESH topic (no beacon anywhere
// yet) can sit outside the true root's beacon basin (fanout^layers) and never
// hear the demotion — its via-pinned subscribers are then permanently orphaned
// (observed on prod: binary 0-of-N subscribers inside an otherwise-perfect
// tree). So every root verifies its own claim with the SAME iterative
// closest-node lookup subscribers use: once shortly after forming (the
// fresh-topic race window), then periodically. Finding a strictly-closer live
// node ⇒ seed a VERIFIED root pointer + demote + re-home + push cache up.
// Batched per tick so a many-rooted relay doesn't storm lookups; each lookup is
// fired non-blocking (NEVER awaited in the tick — the 4.18.1 lesson).
// Routed-outcome observability (#58 D3). routeMessage reports failure by
// resolving {consumed:false} and logs nothing, so routed reachability was
// unmeasurable in production. Counts are tallied at the single _route
// containment point and reported in bulk, never per failure — a node that
// cannot reach a nominee retries every tick, so per-failure lines would flood.
export const ROUTE_FAIL_TRACK_MAX = 16;         // distinct failing targets retained (bounded map)
export const ROUTE_REPORT_TOP     = 3;          // worst offenders named in each summary

export const ROOT_VERIFY_FIRST_MS = 6_000;      // first verify after root-formed
export const ROOT_VERIFY_MS       = 45_000;     // steady-state re-verify cadence
export const ROOT_VERIFY_BATCH    = 3;          // max verify lookups launched per tick

// ── Demand-driven metrics (any root, no special nodes) ───────────────────
// Metrics are NOT a relay feature — any node that is root for a topic publishes
// its snapshots WHEN and ONLY WHEN a metrics lease is active. A subscriber to
// metricTopic(T) emits a METRICSON toward T (routed like a SUB); the root (and
// every inheriting root) sets a renewable lease and, while fresh, publishes a
// snapshot to metricTopic(T) each METRICS_PUB_MS. Path nodes cache the flag so a
// second requester's METRICSON short-circuits (the root already knows). The lease
// self-expires when the last metric subscriber stops renewing → metrics turn off,
// no orphan load.
export const METRICS_LEASE_MS = 70_000;   // root keeps publishing this long after the last METRICSON (~= dropMs; renewed by the subscriber's refresh tick)
export const METRICS_PUB_MS   = 20_000;   // snapshot cadence at the root while the lease is fresh
export const METRICS_COALESCE_MS = 8_000; // a path node re-forwards METRICSON upstream at most this often (fan-in dedup; still keeps the root's lease alive)

// ── Wire message types (all ROUTED) ─────────────────────────────────────
export const T = {
  SUB:      'pubsub:sub',       // subscribe — routed toward topic id (or a via waypoint)
  UNSUB:    'pubsub:unsub',     // explicit unsubscribe (renewal lapse also drops)
  PUB:      'pubsub:pub',       // publish — routed toward topic id; NO timestamp (root stamps)
  DELIVER:  'pubsub:deliver',   // stamped messages — routed toward a subscriber id
  ADOPT:    'pubsub:adopt',     // delegate: "become my child relay + take these subscribers"
  PULLUP:   'pubsub:pullup',    // "I'm behind you — replay your stamped history up to me" (§6)
  REPLAYUP: 'pubsub:replayup',  // a relay's stamped cache delta, routed UP to a behind parent
  HANDOFF:  'pubsub:handoff',   // graceful-leave: a departing root pushes its cache to its heir
  KILL:     'pubsub:kill',      // retract a message (thin; TODO Phase 4)
  RECEIPTPROBE: 'pubsub:receiptprobe', // flight -> suspect root: prove the correlated receipt for
                                //   {topicId, msgId, op} or state the inability. Reachability is not
                                //   an answer (Aster seq 427) — only INGESTACK or RECEIPTNACK is.
  RECEIPTNACK: 'pubsub:receiptnack', // root -> flight: explicit not-held. Honest once (earns one direct
                                //   retry of the write); a nack after the retry convicts.
  INGESTACK: 'pubsub:ingestack', // root -> forwarder: correlated proof of INGEST (Dead-Root Eviction v0.3).
                                //   {topicId, msgId, epoch, op:'pub'|'kill'} emitted AFTER topic-store
                                //   ingest, never at routing. One hop back to meta.fromId — the
                                //   FORWARDER, deliberately not the origin publisher (no publish-ack:
                                //   acking the publisher would disclose its location). The E3 write
                                //   flight treats this as the ONLY terminal success; a routing
                                //   'consumed' verdict is hop-local evidence and never completes a write.
  UNPUB:    'pubsub:unpub',     // RESERVED — removed v4.3.0 (no handler/sender); wire string kept so legacy frames are ignored, not misrouted
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
  ROOTBEACON: 'pubsub:rootbeacon', // soft-state root advertisement to the topic's neighborhood
  REPLICATE: 'pubsub:replicate', // singleton-root durability: root pushes cache+tombstones to its N nearest neighbours (warm backup roots)
  METRICSON: 'pubsub:metricson', // demand-driven metrics: routed toward the topic id like SUB; marks the path + root so ANY root publishes snapshots while the lease is fresh
  HANDOFFACK: 'pubsub:handoffack', // heir confirms a HANDOFF landed (v4.24.0) — the leaver retries / cohort-sprays unacked topics instead of silently dropping the last copy
  MERGE_OFFER:  'pubsub:mergeoffer',  // dual-root 4(d) step 1: the LOSER offers its claim to the winner (corr, loserEpoch, hw, lw, count, timing)
  REPLAYACK:    'pubsub:replayack',   // dual-root 4(d) step 4: the WINNER's terminal ack of the whole export under corr (retained, delsActive, hash) or a refusal
  MERGE_CLOSED: 'pubsub:mergeclosed', // dual-root 4(d) completion: the winner closes the handover on the loser's merge-bearing SUB
  ADOPTACK: 'pubsub:adoptack',   // dual-root packet 4(d) (axona-docs 14a6bd8): the child answers an ADOPT — accepted ids or a refusal — so the parent releases delegated subscribers only on confirmed adoption
  TOPIC_TERMINATED: 'pubsub:topicterminated', // Topic Lifetime 4.5 (axona-docs 11288d7): a root ends its generation; routed to every seat and replica, forwarded by a child to its seats; a reader's subscription ENDS
  REHOME: 'pubsub:rehome',       // Topic Lifetime 4.7: a thin child steps down and sends its readers to its upstream; kernel-internal, the app's subscription persists
};

// ── Dual-root reconciliation (axona-docs 14a6bd8; David "build it" 2026-10-17) ──
// The whole machine sits behind MERGE_CAPABLE (kernel option `mergeCapable`,
// env MERGE_CAPABLE=1). ON by default since 4.118.0 (David, 2026-10-22: "a full
// arm of both"); MERGE_CAPABLE=0 disarms a node. 4.115.0 to 4.117.0 shipped it OFF.
export const MERGE_CAPABLE_DEFAULT = 1;
export const ADOPT_ACK_MS          = 5_000;   // a delegation not acknowledged within this is rolled back: the parent keeps serving its subscribers
// The gate's forward ledger (the shape of the step-down hold's holdFwd ledger).
export const MERGE_GATE_FWD_MAX     = 256;
export const MERGE_GATE_REENTRY_MS  = 10_000;

// ── Empty-self-root cohort pull (v4.24.0 — the alert-bot read-miss fix) ──────
// A cold subscriber whose SUB terminates at itself becomes the topic's root with
// an EMPTY cache while a live holder (ex-root / backup / host) still has the
// history — and nothing tells the holder about the new closer root, so the
// state is STICKY (field: 82% of misses, unrecovered at 600s). The new root
// therefore PULLS: probe the K-closest cohort + the nodes on its own iterative
// lookup path (the runner-up closest is usually the prior root) with a
// PULLUP(sinceHw:0); any holder replays via the existing REPLAYUP → verified
// union-ingest. Delayed so a pub-terminal root (whose own publish fills the
// cache immediately) never probes; bounded tries so a genuinely-fresh topic
// quenches after finding nobody.
export const EMPTY_ROOT_PROBE_DELAY_MS    = 800;    // wait before first probe (skip if cache filled)
export const EMPTY_ROOT_PROBE_MAX         = 3;      // total probes per role while empty
export const EMPTY_ROOT_PROBE_INTERVAL_MS = 5_000;  // min gap between probes (via refreshTick)
export const EMPTY_ROOT_PROBE_FANOUT      = 4;      // candidates contacted per probe

// ── Leave-handoff confirmation (v4.24.0) ─────────────────────────────────────
// The graceful-leave HANDOFF was fire-and-forget: a failed heir delivery
// silently transferred nothing (diagnosis: gone=40/40 with leaveMs≈11ms — a
// CONFIRMATION failure, not a window failure). The leaver now waits briefly for
// HANDOFFACK, retries once, and finally sprays the cache to the K-closest
// cohort as REPLICATE (idempotent; backups store it, roots union-ingest it).
export const HANDOFF_ACK_MS  = 700;   // per-round ack window BASE (also the stall-exit horizon)
export const HANDOFF_TRIES   = 2;     // sends before falling back to cohort spray
// Review 2026-07-25 (mass-leaver scaling): the round ack window was a FLAT
// HANDOFF_ACK_MS while heir-side ingest is O(topics received) — a mass leaver's
// few heirs absorb dozens of simultaneous cache ingests (time-sliced), so acks
// arrive in O(K) and "unacked" overwhelmingly meant ACKED LATE; every late
// topic fell through to a single unconfirmed Phase C fallback (68/68 warns on
// a ~68-root prod leaver). The window now scales with the round's batch size.
export const HANDOFF_ACK_PER_TOPIC_MS = 25;    // per-unacked-topic margin added to the round window
export const HANDOFF_ACK_MAX_MS       = 5000;  // per-round window cap (leave()'s outer bound already scales with role count)

// ── Timing version (packet 4(d), "MERGE_TIMEOUT_MS bounds ONE gate interval") ──
// The three terms are frozen with the version and published with it; a peer
// whose OFFER carries another version or an A outside limits is refused
// before the cut (d3). EXPORT_BYTES_MAX is the SERIALIZED bound the export is
// measured against at the cut: bodies at CACHE_BYTES, tombstones at their
// count cap times the proof's maximum size, the manifest and header at their
// fixed sizes, one frame of slack for packing. F_MAX = ⌈EXPORT_BYTES_MAX /
// REPLAY_CHUNK_BYTES⌉ + 1 (the header). S_MAX is an ASSUMED per-frame service
// allowance (the 2026-10-08 RTT matrix read 39 ms p50 one hop; a 96 KiB frame
// of ~100 signed bodies verifies in ~100 ms on a relay); it informs, it does
// not prove. A_ack = HANDOFF_ACK_MAX_MS. The constant is the universal worst
// case 2 × ((2 + 2·F_MAX) × S_MAX + A_ack), never below HANDOFF_ACK_MAX_MS.
export const MERGE_TIMING_VERSION   = 1;
export const MERGE_TOMB_PROOF_MAX_BYTES = 512;                       // one signed kill proof, serialized, upper bound
export const EXPORT_BYTES_MAX       = CACHE_BYTES                       // bodies
                                    + CACHE_MAX * MERGE_TOMB_PROOF_MAX_BYTES   // tombstones with proofs (count bounded by the body cap)
                                    + CACHE_MAX * 2 * 80                      // manifest: two msgId lists
                                    + 1024                                    // header frame
                                    + REPLAY_CHUNK_BYTES;                     // one frame of packing slack
export const MERGE_F_MAX            = Math.ceil(EXPORT_BYTES_MAX / REPLAY_CHUNK_BYTES) + 1;
export const MERGE_S_MAX_MS         = 250;
export const MERGE_TIMEOUT_MS       = Math.max(HANDOFF_ACK_MAX_MS, 2 * ((2 + 2 * MERGE_F_MAX) * MERGE_S_MAX_MS + HANDOFF_ACK_MAX_MS));
// The remaining round constants (packet 4(d), "Shared parameters, bound to the round").
export const MERGE_A_MIN_MS         = HANDOFF_ACK_MAX_MS;       // an offered A below this is refused-timing
export const MERGE_A_MAX_MS         = 4 * MERGE_TIMEOUT_MS;     // …or above this
export const MERGE_E_MAX_MS         = FUTURE_TOLERANCE_MS;      // E = the kernel's clock allowance; a larger E is refused
export const MERGE_TICK_MS          = 5_000;                    // P: one scheduler tick (refreshIntervalMs default)
export const MERGE_RETENTION_MS     = MERGE_TIMEOUT_MS + 2 * FUTURE_TOLERANCE_MS + MERGE_TICK_MS;   // R = A + E_L + E_W + P at equal tolerances
export const MERGE_OFFER_TIMEOUT_MS = 2 * HANDOFF_ACK_MAX_MS;   // an OFFER unanswered for this long: the winner is legacy or gone (fallback, d4)
export const MERGE_WRITE_RESERVE_ENTRIES = 64;                  // W's own writes during a round draw on this reserve, fail closed past it
export const MERGE_WRITE_RESERVE_BYTES   = 1024 * 1024;
