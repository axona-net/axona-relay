# Kernel Invariants

The behavioral contract the pub/sub kernel must uphold, distilled from the
incidents that taught us each rule. **Every entry names the regression test
that enforces it** — a change that breaks one of these must fail a test, not a
production network. This file is the Phase 0 output of the refactor program
(`axona-docs/architecture/Kernel-Refactor-Analysis-v0.1.md`); Phases 1–4 must
preserve every invariant below with the suite passing identically.

How to read an entry: **the rule**, the incident that paid for it, and the
enforcing test(s) in `test/`.

---

## I-1. A topic has exactly one root, and a wrong claim converges without flapping

The root is emergent (the live node XOR-closest to the topic id), never
elected by ballot. Any node that wrongly claims root-ness — a stranded
terminal SUB, a near-miss relay after churn, a backup that promoted — must
converge to the true root and must not re-take the claim on the next stranded
message (the ~20s prod root-flap of 2026-07-07).

- `smoke_root_reconcile.mjs` — divergent-view fabric: migration, strand-no-reroot,
  churn safety, shadowed-corner self-verify, alone-in-the-dark (18 checks)
- `smoke_replica_fast_promote.mjs` — single-root election among warm backups
- `smoke_pubsub_root_election.mjs` (legacy chain) — election through the
  subscribe machinery
- `smoke_reachable_root.mjs` — prefer a reachable root over a
  closer-but-unconfirmed one; claim after the confirmation window

## I-2. Never defer a root claim to a farther node, a ghost, or the node handing off

Every defer/demote gate is one-directional (strictly closer only) and must be
backed by liveness evidence: a channel-verified neighbour, a network-verified
pointer, or a recent beacon. A departed peer's beacons are purged on death and
on handoff; the heir of a handoff never defers back to the leaver (the
4.19.5 "heir un-adopts itself" own-goal).

- `smoke_root_reconcile.mjs` — corpse-beacon freshness cut (phase 4);
  strictly-closer beacon demotion
- `smoke_leave_handoff_burst.mjs` — heir keeps the claim, leaver's ghost
  beacon purged, `pubsubPeerDied` sweep (9 checks)
- `smoke_pubsub_beacon.mjs` — verify-don't-trust acceptance (a liar cannot
  divert traffic to a farther node)

## I-3. A recovery path never waits unboundedly on — or dies on — the failure it handles

The reconnect path must survive the very errors reconnection produces (the
502-during-bridge-boot reconnect-death, 4.19.3); drains and handoffs are
bounded by the caller's timeout, and nothing in a periodic tick ever awaits a
network lookup inline (the 4.18.1 lesson).

- `smoke_transport_web_reconnect.js` — throw-on-unlistened-error FakeWS,
  failed-upgrade (502) storm survived (25 checks)
- `smoke_leave_teardown.mjs` — evidence-based drain bounded by `timeoutMs`;
  stuck-pending drain still returns

## I-4. A peer that has left is silent — and its rooted history departs before it does

`leave()` drains pending publishes on evidence, hands off every rooted topic's
cache+tombstones to live heirs (in parallel, with an iterative-lookup fallback
for thin tables), then stops the tick and clears retry state. Zero routed
sends after departure (the 100%-CPU-for-40s alert-bot incident, 4.19.4); no
topic's history dies with a graceful leaver (4.19.5).

- `smoke_leave_teardown.mjs` — tick cleared, pendings cleared, zero post-leave
  sends, `stop()` parity (12 checks)
- `smoke_leave_handoff_burst.mjs` — 24-topic parallel handoff inside the
  bound; thin-table lookup fallback
- `smoke_pubsub_leave_handoff.mjs` — end-to-end handoff through the sim stack

## I-5. A client is never judged by time the server wasn't listening

Idle/liveness verdicts must exclude windows where the judging process was
stalled or not yet consuming (the bridge event-loop-stall admission drops,
bridge 2.67.0: stall sampler + client-hello grace re-arm + idle-sweep skip).

- Enforced in `axona-bridge` `src/server.js` (`stallTaintedSince()` +
  `stallPending()`, grace re-arm). Covered by the bridge's
  `scripts/smoke-loop-stall.js` (bridge 2.72.1, wired into its `npm test`):
  reproduces the stall with a test-gated synchronous busy-wait
  (`BRIDGE_TEST_STALL=on` → `/__test/stall`) and asserts hello-grace
  admission, idle-sweep skip, eventual kick of a genuinely-idle client once
  the taint ages out, and the healthz stall counters. Writing the test
  exposed a timer-ordering hazard — on loop resumption Node fires expired
  timers in due-time order, so a judgment timer could run *before* the
  sampler's tick recorded the stall — closed by `stallPending()` (judgment
  sites also check raw heartbeat drift for a stall the sampler hasn't seen
  yet).

## I-6. Observability surfaces exist or fail loudly — never silently zero

An introspection surface that returns empty when the subsystem is active is
worse than none: it masks incidents (roles=0 masked the prod root-split for a
full diagnosis cycle; `rootedTopics()`'s silent drop in the v3.12 clean break
killed the metrics plane for the whole 4.x line until 4.10.1).

- `smoke_health.js` — `health().axonRoles` populated via `inspectRoles`
- `smoke_rooted_topics.mjs` — `rootedTopics()` snapshots (seq, current_count)
- `smoke_metrics_demand.mjs`, `smoke_metric_topic.mjs` — the demand-driven
  metrics plane end-to-end

## I-7. Fixes must hold for 100%-transient peers — no stable-node privilege

Standing project policy (2026-07-08): mechanisms must be correct **under**
churn, never masked by privileging stable nodes (reluctant-root/host-preference
class rejected as a false win). Corollary — sim methodology: single-seed
delivery% is noise; churn verdicts require REPS≥5 mean±sd.

- `churn_sustained.mjs`, `churn_refill.mjs` — kernel-level churn suites
- `dht-sim/harness/relay-churn-experiment.mjs` + `pubsub-real-kernel.mjs` —
  scale/churn harnesses over the shipped kernel
- Process-enforced at review: a churn fix that special-cases stable nodes is
  rejected regardless of its numbers.

## I-8. Migrated cache never resurrects a killed message

Every cache-migration path (replicate, pull-up/replay-up, handoff, replay)
carries tombstones alongside bodies and applies them first; a kill is a
publish-with-a-side-effect and must reach the whole cohort exactly as a
publish must (the kill-leak class, closed 4.8.8→4.10.0).

- `smoke_kill_migration.mjs` — tombstones ride every migration path
- `smoke_pubsub_kill.mjs`, `smoke_pubsub_kill_heal.mjs` — kill replay +
  self-heal; `smoke_kill_resurrection.js` (legacy chain) — forged-kill revoke

## I-9. Publish confirmation is observation, not acknowledgment

A PUB carries no return address (publisher location privacy — twice-reverted
design rule). The only confirmation is observing one's own msgId arrive by
any stamped path; retries are idempotent (root dedups by msgId) and bounded
(TTL + max tries). Nothing may add an ack channel that discloses the
publisher's location.

- `repro_lossy_restart.mjs` — persistent retry under 10–30% loss
- `smoke_cold_burst.mjs` — cold-publish burst confirms + stops on observation
- `smoke_pubsub_durability.mjs` — replay-up/high-water recovery

## I-10. Standing state is bounded by demand, never by churn history

Per-topic standing state anywhere in the network is O(subscribers + cohort).
No mechanism may create state that accumulates with join/leave *events*, and
any mechanism that writes standing state on ANOTHER node must name that
state's eviction path in the same change. Corollary (the principal-liveness
rule, paid for by the 4.24.0 backbone collapses): standing state may only be
planted by a principal alive to maintain it — a departing node transfers
principal-ship (HANDOFF) or does nothing; it never sends REPLICATE.

- `smoke_churn_amplification.mjs` — ack-dropped burst-publisher churn: zero
  departure REPLICATE, bounded handoffs, bounded fleet roles, since:'all'
  durability retained (50/50)
- `smoke_root_replication.mjs` — delta gate: unchanged roots send keepalives,
  not full state; state change / new member / 60s backstop re-arm one full push

## I-11. Bulk work never starves liveness

Any loop that ingests or emits unbounded batches yields to the macrotask
queue at a fixed stride so heartbeats interleave; a node must never be
evicted by its peers *because* it was absorbing history (the #332 join-storm:
bulk ingest → missed heartbeats → mass eviction → `state=stale` mesh death).

- `_ingestStampedBatch` (wireHandlers) — macrotask yield every 16 messages on
  the REPLAYUP/HANDOFF/REPLICATE ingest paths
- Full mesh re-bootstrap after mass eviction remains open (task #332)

---

## Appendix A — Constants audit (AxonaManager.js, 4.19.6)

All 36 module constants audited for the refactor. **Live** = load-bearing with
a test or incident behind it. **Vestigial** = kept only for wire/API
compatibility. Dead flag-gated code was **removed** in 4.19.6 (see Appendix B).

| Constant | Status | Notes |
|---|---|---|
| MAX_PUBLISH_BYTES, MAX_RELIABLE_PUBLISH_BYTES | live (exported) | D-1 caps; std/chunk contract |
| RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS | live | adaptive renewal (`smoke_adaptive_renewal.mjs`) |
| ROOT_CLAIM_MS | live | reachable-root confirmation window (`smoke_reachable_root.mjs`) |
| ROOT_REPLICAS, BACKUP_EVICT_MS | live | cohort replication + backup cleanup (`smoke_root_replication.mjs`, `smoke_replica_fast_promote.mjs`) |
| CACHE_MAX, CACHE_BYTES, TTL_MS, REPLAY_CHUNK_BYTES | live | cache bounds + hold (`smoke_hold_ttl.js`) |
| MAX_DIRECT, DELEGATE_BATCH | live | tree widen-before-deepen |
| MAX_VIA, VIA_HOP_BUDGET | live | wire sanity caps |
| APP_DEDUP_MAX | live | exactly-once LRU |
| PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES | live | bounded publish/kill retry (I-9) |
| COLD_BURST_* (4), COLD_PEER_THRESHOLD, FIRST_PUBLISH_RESEND_MS | live | cold/first-publish re-send (`smoke_cold_burst.mjs`) |
| FUTURE_TOLERANCE_MS | live | §5 bad-clock rule |
| BEACON_MS, BEACON_TTL_MS, BEACON_FANOUT, BEACON_LAYERS, BEACON_SEEN_MS | live | root beacons (`smoke_pubsub_beacon.mjs`); BEACON_MS×1.5 also the corpse-freshness cut (I-2) |
| ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH | live | root self-verification (`smoke_root_reconcile.mjs`) |
| METRICS_LEASE_MS, METRICS_PUB_MS, METRICS_COALESCE_MS | live | demand-driven metrics (`smoke_metrics_demand.mjs`) |

Vestigial (kept deliberately): `T.TOUCH`/`_onTouch`/`pubsubTouch` (deprecated
v4.3.0, wire string kept so legacy frames are ignored, not misrouted);
`T.UNPUB` (reserved wire string, no handler); the constructor's `..._legacy`
swallow (accepted-and-ignored clean-break tunables from pre-3.15 callers).

## Appendix B — Dead code removed in 4.19.6 (behavior-preserving)

Removed with git history as the archive; revive from the named commits if a
future design wants the idea back.

- **`_reannounceCacheRoots` flag + refreshTick block** — the cache-bearing-root
  re-announce A/B, measured *below* the deploy gate (Howard 25/30, regressed
  kill/since-all recovery). Flag was never set outside its own test.
  (+ `test/smoke_backlog_reannounce.mjs`.)
- **`pickRelayPeer` wiring + `AxonaPeer._pickRelayPeer`** — batch-adoption
  recruitment from the pre-3.15 tree; the option has been silently swallowed
  by the manager's `_legacy` sink since the clean break, so the method was
  unreachable. Its bridge-exclusion property is enforced in the live code by
  `_nearestReachable`/`_replicateRole`/`_bestKnownClosest` (bridge never a
  root/relay). (+ `test/smoke_pickrelay_bridge.mjs`.)
- **`AxonaManager.requestMetrics()`** — Phase-4 stub returning
  `{accumulated: []}`; no callers since metrics moved to derived topics.
- **`invalidateKClosestCache()`** — no-op since the routed model removed the
  K-closest cache; deleted with its five optional-chained call sites in
  AxonaPeer.
