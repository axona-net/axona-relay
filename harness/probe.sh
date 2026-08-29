#!/usr/bin/env bash
# =============================================================================
# harness/probe.sh — ONE instrumented pub/sub probe cycle.
#
# A short, churn-free, FIXED-workload run of the instrumented harness, then a
# compact health line appended to harness/results/probe-log.jsonl. Fired every
# ~15 minutes by the cadence; it measures whatever the fleet is currently armed
# to. The seed is fixed so the workload (topic map + schedule) is identical
# every cycle — only the network's health varies, which is the whole point.
#
#   ARM=A bash harness/probe.sh        # A = connection-quality stack OFF
#   ARM=B bash harness/probe.sh        # B = stack ON (after the fleet is armed)
#
# A lock skips a cycle if the previous one is still running (a 15-min cadence
# over an ~12-min probe leaves little overlap margin).
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

SEED="${PROBE_SEED:-700}"                    # FIXED — identical workload each cycle
NODES="${PROBE_NODES:-8}"
DURATION_MS="${PROBE_DURATION_MS:-480000}"   # 8-min workload (+~2min settle, fits 15)
OPEN_N="${PROBE_OPEN_N:-15}"
OWNED_N="${PROBE_OWNED_N:-8}"
ARM="${ARM:-A}"                              # label only; the fleet's arm is what it is
RESULTS=harness/results
LOG="$RESULTS/probe-log.jsonl"
LOCK="$RESULTS/.probe.lock"
mkdir -p "$RESULTS"

# Lock: skip if a probe is still running (stale lock > 20min is reaped).
if [ -f "$LOCK" ]; then
  if [ "$(( $(date +%s) - $(stat -f %m "$LOCK" 2>/dev/null || echo 0) ))" -lt 1200 ]; then
    echo "probe: previous cycle still running (lock held) — skipping"; exit 0
  fi
fi
echo $$ > "$LOCK"
trap 'rm -f "$LOCK"' EXIT

TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "── probe $TS  seed=$SEED arm=$ARM  $((DURATION_MS/60000))min workload, churn-free"

# Clear this seed's ledgers on EVERY host first. The ledger APPENDS, so a re-run
# with the same seed concatenates onto the previous run and corrupts the join
# (the same topic/seq from two runs, observations bleeding across). Fixed-seed
# comparability REQUIRES a clean slate each cycle — local and remote.
rm -f "$RESULTS"/sidecar-"$SEED"-*.jsonl "$RESULTS"/findings-"$SEED".jsonl "$RESULTS"/summary-"$SEED".json 2>/dev/null
ssh -o ConnectTimeout=8 m1 "rm -f ~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" 2>/dev/null || true
ssh -o ConnectTimeout=8 axona-linux "rm -f ~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" 2>/dev/null || true
printf 'rm -f /c/Users/david/github/axona-relay/harness/results/sidecar-%s-*.jsonl\n' "$SEED" \
  | ssh -o ConnectTimeout=10 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' 2>/dev/null || true

# COORD_WAIT_MS must outlast the slowest host's connect+announce, or readers
# fall back to a wrong owner and manufacture #393 strands (the 30s window did
# exactly that — the Windows box announced late). READINESS_MS can be forced to
# 0 (PROBE_READINESS_MS=0) for the barrier-off discriminator run.
NO_CHURN=1 SEED="$SEED" NODES="$NODES" DURATION_MS="$DURATION_MS" OPEN_N="$OPEN_N" OWNED_N="$OWNED_N" \
  HEAD_SWEEP_MS=30000 READINESS_MS="${PROBE_READINESS_MS:-20000}" COORD_WAIT_MS="${PROBE_COORD_WAIT_MS:-90000}" \
  bash harness/launch.sh > "$RESULTS/probe-$SEED.out" 2>&1

if [ ! -f "$RESULTS/summary-$SEED.json" ]; then
  echo "probe: analyzer produced no summary — see $RESULTS/probe-$SEED.out" >&2
  exit 1
fi
node harness/probe-summary.mjs "$RESULTS/summary-$SEED.json" "$TS" "$ARM" "$LOG"
