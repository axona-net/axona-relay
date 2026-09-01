# RUNBOOK — combined Gate-4 delivery-diagnosis arm

Status: **STAGED, not executed.** Every mutating step below is David's Gate-4
call. Nothing here runs a roll, a cold-start, or an arm on its own.

## What this arm answers

Is the live ~70% delivery a **forward-push transport loss**, or does the miss sit
somewhere else — subscription setup, tree construction, or the app callback? The
first instrumented read (2026-09-01) was VOID: LAT_TRACE was non-uniform, so tx and
rx were different populations. Everything below exists to make the *next* read
valid and to localize every miss to exactly one stratum.

The tooling, all drafted and council-reviewed, none deployed:

| Piece | Repo / SHA |
|---|---|
| Pre-arm coverage gate | axona-relay `400f4aa` (`harness/coverage-check.sh`) |
| Per-hop tx/rx pairs | axona-protocol `a5debcc` |
| Expectation-ledger stamp (kernel) | axona-protocol `73b705d` (`_fanoutLedger`) |
| Reconciliation analyzer | axona-relay `d206e53` (`harness/reconcile-delivery.mjs`) |
| Lifecycle ledger + three-set D_intent | axona-relay `fa87652` |

## Preconditions — do these before arming (each is David's go)

**P0 — bump the kernel version for the armed telemetry build.** `73b705d` added the
stamp without bumping `KERNEL_VERSION` (still `4.69.0`). Bump it (e.g. `4.70.0`) in
axona-protocol so the coverage gate's version check is meaningful — otherwise a
stale 4.69.0 relay without the stamp reads as "expected version". Push
axona-protocol testnet.

**P1 — vendor the stamp into axona-relay.** The vendored kernel is still `4.69.0`
and `_fanoutLedger` is ABSENT (`grep -c _fanoutLedger vendor/axona-protocol/src/pubsub/AxonaManager.js` → 0), so today's relay binary would emit no expectation ledger. Refresh it:

```bash
cd ~/Documents/claude/axona-relay && bash scripts/sync-protocol.sh
```

The script gates on the relay suite. Then verify + commit + push:

```bash
grep -c _fanoutLedger vendor/axona-protocol/src/pubsub/AxonaManager.js   # expect >=1
git add vendor/axona-protocol && git commit -m "vendor: sync kernel with fanout-ledger stamp for the Gate-4 arm" && git push origin testnet
```

**P2 — every fleet host pulls `origin/testnet`.** Gets the vendored stamp (relay
binary) AND the harness lifecycle ledger + analyzers. `ab-coldstart.sh` starts
relays from each host's checkout, so the checkout must be current first. On m4,
m1, axona-linux, and the win box: `git fetch -q origin && git reset -q --hard origin/testnet`.

## Step 1 — cold-start a uniformly-traced fleet (Gate 4)

Do NOT in-place roll: the roll's stop races the ssh watchdog and leaves a mixed
fleet — that mix is what made LAT_TRACE non-uniform and voided the first read. Cold
start stops every relay and starts each fresh with one env (`LAT_TRACE=1` baked in):

```bash
cd ~/Documents/claude/axona-relay && bash harness/ab-coldstart.sh <KNEAR>
```

`<KNEAR>` = the fleet's standing value (confirm the baseline before arming — hold
it constant; this is not a kNear experiment). Cold-start also clears the relay-side
disc so the arm starts fresh.

## Step 2 — coverage gate (dry check before the window)

`launch.sh` runs this automatically for a `LAT_TRACE=1` arm and refuses to open the
window unless coverage is uniform. Run it standalone first to confirm the fleet is
ready and to see the per-host trace coverage:

```bash
EXPECT=<version> REGION=eagle BRIDGE=wss://testnet.axona.net bash harness/coverage-check.sh
```

Expect `VERDICT: ARM ADMITTED`. If it REFUSES, it names the untraced pids — cold-start
those hosts again (do not override). `COVERAGE_OVERRIDE=1` exists only for a
deliberately partial arm and produces a void read.

## Step 3 — launch the traced no-churn arm

No-churn isolates the ~30% gap from migration (established: the gap is
churn-independent). NODES=6 matches the six unix sidecars — win sidecars stay off
(they never carried LAT_TRACE), so `NODES` must be 6 or readers 6-7 are phantom
subscribers that can never receive (~25% structural miss floor).

```bash
SEED=<n> NODES=6 DURATION_MS=<ms> OPEN_N=4 OWNED_N=2 \
  LAT_TRACE=1 NO_CHURN=1 REGION=eagle BRIDGE=wss://testnet.axona.net \
  bash harness/launch.sh
```

DURATION: the per-hop falsifier needs N_ADEQUATE=125 ok-write tx **per hop
stratum** (Aster ec9b4016). A 20-minute arm may not power hop-2/hop-3 — if so the
honest verdict is INCONCLUSIVE for those strata, and the fix is a longer window or
a higher publish rate, not a retire on thin data. Recommend ≥30 min, or raise the
schedule's publish rate to power the deep strata in less wall-clock.

`launch.sh` runs the cross-host clock probe (`clock-probe.mjs`) automatically —
pre (immediately before the window) and post (after collection) — writing
`harness/results/clock-<seed>-{pre,post}.json`. The reconciliation analyzer reads
them to bound its boundary band from measured uncertainty + pre/post drift, and
VOIDs the timing-dependent analysis if a probe failed or drift exceeds the frozen
bound (Aster 2a2778b2). No probe ⇒ the analyzer runs with a conservative band and
reports timing UNVALIDATED.

## Step 4 — collection

`launch.sh` collects automatically (incl. win relay-disc since `400f4aa`, and the
sidecar `sidecar-/disc-/latstage-` families). Confirm win relay-disc landed:
`ls harness/results/relay-disc-win-all.jsonl`.

## Step 5 — analysis (three independent reads on the same data)

```bash
# a) end-to-end delivery baseline (the ~70% figure)
node harness/soak-account.mjs --dir harness/results --seed <n> --nodes 6 ... 

# b) per-hop DELIVER drop, to the frozen rules (e618a19a + power ec9b4016).
#    Its validity guard VOIDs if coverage is still asymmetric — a green run here
#    is itself proof the coverage gate worked.
node harness/analyze-deliver-hop.mjs harness/results

# c) three-set reconciliation: service completeness (D_required denominator),
#    activation failure, belief divergence, path-aware miss localization. Reads the
#    clock probes (needs SEED to find clock-<seed>-{pre,post}.json) and prints the
#    per-host offset/uncertainty/drift + whether timing is validated.
SEED=<n> node harness/reconcile-delivery.mjs harness/results
```

## Step 6 — verdict

- **analyze-deliver-hop**: per route≤3 stratum — Wilson lower > 3% ⇒ transport loss
  STANDS; n≥125 and upper < 3% ⇒ supports RETIRE; else INCONCLUSIVE. VOID ⇒ coverage
  still broke; re-cold-start and re-arm, do not interpret.
- **reconcile-delivery**: reads where the misses actually sit —
  `D_required∖D_active` (subscription/activation failure) ·
  `D_active∖D_belief` (tree-construction / lease-propagation divergence) ·
  `D_belief∖R_app` split path-aware into forwarding-drop vs final-hop/callback.
  Boundary-ambiguous and truncated publishes are censored/VOID, never scored.

If the per-hop read STANDS and the reconciliation puts the mass in
`D_belief∖R_app` forwarding-drop, the forward-push-loss diagnosis is confirmed and
the fix work (fast gap-fill to beat the deadline) is next. If the mass is in
`D_required∖D_active` or `D_active∖D_belief`, the diagnosis is wrong and the lever
is subscription/tree construction, not the transport. Either way, publish the
verdict verbatim to the council.

## Gates

- P0/P1 change what the fleet runs (deploy branch) — David's go.
- Step 1 (cold-start) IS the Gate-4 arm of the fleet — David's explicit go.
- Nothing above promotes to production (Gate 6) — separate, later, David's.
- axona.bot runs no step without that go, and brings the coverage-gate verdict
  back before the window opens.

Diagnosis of forward-push loss stays OPEN until a valid read from this arm.
