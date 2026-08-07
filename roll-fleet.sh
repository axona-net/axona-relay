#!/usr/bin/env bash
# =============================================================================
# roll-fleet.sh — THE ONLY SANCTIONED WAY to update a RUNNING relay fleet.
#
# MANDATE (David, 2026-08-02): "provide a deploy script for updating protocols
# that must always be followed. Excuses don't bring data back to life."
#
# WHY THIS SCRIPT EXISTS. On 2026-08-02, during the 4.59.2 testnet roll, a
# free-hand restart shrank the live fleet 26 -> 3: the operator's count read 0
# (wrong pgrep pattern), start-fleet.sh stopped ALL relays and started its
# DEFAULT of 3. Two independent failures compounded — a measurement that lied
# and a script that fills silence with a default. On production that sequence
# destroys the region's held history: a full-stop-then-start marches the fleet
# down to zero, and the LAST relays to leave have no live heirs, so their
# graceful handoff drains into the void. The Ship-of-Theseus measurement is
# unambiguous on this: staged replacement with live heirs lost 0 of 1,890
# topics; removal without heirs is where the 1.96% whole-topic loss lives.
#
# THE THREE RULES, each mapped to a failure this script makes unreachable:
#
#   1. THE COUNT IS AN ARGUMENT, VERIFIED AGAINST MEASUREMENT — NEVER A
#      DEFAULT. You must pass EXPECT=<n>; the script measures the live fleet
#      and REFUSES to run unless they match. A wrong pattern now aborts the
#      deploy instead of silently right-sizing the fleet.
#
#   2. START-THEN-STOP, ONE SLOT AT A TIME. The replacement relay is launched
#      and VERIFIED INTEGRATED (banner version + open mesh) before any old
#      relay is asked to leave. The fleet never drops below EXPECT; every
#      departing relay hands off into a fleet at full strength that already
#      contains its successor. If a replacement fails to integrate, the roll
#      ABORTS with every old relay still running — new code that cannot join
#      must never be the reason old relays die.
#
#   3. EVERY STEP IS VERIFIED BY ARTIFACT, NEVER BY EXIT CODE. The replacement
#      is proven by ITS OWN LOG (banner carrying the expected kernel version,
#      state=open with a bound mesh); the departure is proven by the PROCESS
#      BEING GONE; the roll is proven by a final census. An exit code describes
#      the transport; only the artifact describes the payload.
#
#   Usage:
#     EXPECT=26 EXPECT_KERNEL=4.59.2 bash roll-fleet.sh
#     EXPECT=26 EXPECT_KERNEL=4.59.2 REGION=eagle BRIDGE=wss://testnet.axona.net bash roll-fleet.sh
#
#   Cold start (no fleet running) is start-fleet.sh's job; it now refuses to
#   run when a fleet is live and points here.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
INTEGRATE_TIMEOUT="${INTEGRATE_TIMEOUT:-90}"   # s to wait for a replacement to join
LEAVE_TIMEOUT="${LEAVE_TIMEOUT:-45}"           # s to wait for a graceful leave
SETTLE="${SETTLE:-3}"                          # s between slots, after both checks pass

fail() { echo "✗ ABORT: $*" >&2; exit 1; }

# ── Rule 1: the count is an argument, verified against measurement ──────────
[ -n "${EXPECT:-}" ]        || fail "EXPECT=<live relay count> is REQUIRED. There is no default. Measure first: pgrep -fl 'src/index.js'"
[ -n "${EXPECT_KERNEL:-}" ] || fail "EXPECT_KERNEL=<x.y.z> is REQUIRED — the kernel version you believe you are deploying."

# One measurement function, used everywhere — never two patterns in one deploy.
# Suffix-anchored (absolute and relative launches both match); wrappers
# (caffeinate) excluded by comm.
# PREDICATE FIX (2026-08-07): this compared comm = "node", but macOS ps
# reports comm as the FULL binary path (/usr/local/bin/node), so the equality
# matched NOTHING and live_pids() measured 0 against a healthy fleet — every
# roll would abort on EXPECT mismatch. Never match the node side (its path
# varies by install); EXCLUDE the wrapper instead: caffeinate's comm is the
# bare word "caffeinate" on every macOS. Verified against a live 3-relay
# fleet: old predicate 0, this one 3.
live_pids() {
  for pid in $(pgrep -f "src/index.js" 2>/dev/null || true); do
    [ "$(ps -p "$pid" -o comm= 2>/dev/null)" != "caffeinate" ] && echo "$pid"
  done
  # Explicit success: without this, the loop's LAST iteration testing a
  # caffeinate wrapper leaves the function returning 1, and set -e kills the
  # whole script with no message — a silent abort found by negative-testing
  # this very script before its first real run.
  return 0
}

VENDORED="$(node -p "require('./vendor/axona-protocol/package.json').version" 2>/dev/null || echo MISSING)"
[ "$VENDORED" = "$EXPECT_KERNEL" ] || fail "vendored kernel is $VENDORED, you said EXPECT_KERNEL=$EXPECT_KERNEL. Re-vendor first (scripts/sync-protocol.sh) or fix the argument."

OLD_PIDS=($(live_pids))
MEASURED="${#OLD_PIDS[@]}"
if [ "$MEASURED" -ne "$EXPECT" ]; then
  fail "measured $MEASURED live relay(s), you said EXPECT=$EXPECT. One of the two is wrong and this script will not guess which. (pgrep -fl 'src/index.js' to see them.)"
fi
[ "$MEASURED" -gt 0 ] || fail "no fleet is running — a roll needs something to roll. Cold start is start-fleet.sh."

mkdir -p relay-logs
GEN="$(date +%Y%m%d-%H%M%S)"
echo "→ rolling $MEASURED relay(s) to kernel $EXPECT_KERNEL (generation $GEN)"
echo "  region=$REGION bridge=$BRIDGE  start-then-stop, one slot at a time"

# ── Rule 2 + 3: start-then-stop per slot, artifact-verified both ways ───────
slot=0
for old in "${OLD_PIDS[@]}"; do
  slot=$((slot + 1))
  log="relay-logs/relay-$GEN-$slot.log"

  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    caffeinate -i nohup node src/index.js >> "$log" 2>&1 &
  newpid=$!

  # The replacement is proven by ITS OWN LOG: banner with the kernel we expect,
  # then state=open with at least one bound mesh channel. Until both appear,
  # no old relay dies.
  ok_banner=0; ok_mesh=0
  for _ in $(seq 1 "$INTEGRATE_TIMEOUT"); do
    if [ "$ok_banner" -eq 0 ] && grep -q "kernel v$EXPECT_KERNEL" "$log" 2>/dev/null; then ok_banner=1; fi
    if [ "$ok_mesh" -eq 0 ] && grep -E 'state=open .*mesh\(open/bound\)=[0-9]+/[1-9]' "$log" >/dev/null 2>&1; then ok_mesh=1; fi
    [ "$ok_banner" -eq 1 ] && [ "$ok_mesh" -eq 1 ] && break
    kill -0 "$newpid" 2>/dev/null || break   # replacement died — abort below
    sleep 1
  done
  if [ "$ok_banner" -ne 1 ] || [ "$ok_mesh" -ne 1 ]; then
    kill -TERM "$newpid" 2>/dev/null || true
    fail "slot $slot: replacement (pid $newpid) did not integrate within ${INTEGRATE_TIMEOUT}s (banner=$ok_banner mesh=$ok_mesh — see $log). ALL $((MEASURED - slot + 1)) remaining old relays are STILL RUNNING; the fleet is intact. Fix the cause, rerun."
  fi

  # Only now does one old relay leave — into a fleet at full strength + 1.
  kill -TERM "$old" 2>/dev/null || true
  gone=0
  for _ in $(seq 1 "$LEAVE_TIMEOUT"); do
    kill -0 "$old" 2>/dev/null || { gone=1; break; }
    sleep 1
  done
  if [ "$gone" -ne 1 ]; then
    kill -9 "$old" 2>/dev/null || true
    echo "  ⚠ slot $slot: old pid $old needed SIGKILL after ${LEAVE_TIMEOUT}s (leave wedged — its state relied on the standing replicas)"
  fi

  # Census after every slot: the fleet must be back at exactly EXPECT.
  NOW="$(live_pids | wc -l | tr -d ' ')"
  [ "$NOW" -eq "$EXPECT" ] || fail "slot $slot: census reads $NOW, expected $EXPECT. Stopping so you can look before anything else moves."
  echo "  ✓ slot $slot/$MEASURED: pid $old → $newpid  (census $NOW/$EXPECT)"
  sleep "$SETTLE"
done

# ── Final verification: count + every new banner ────────────────────────────
FINAL="$(live_pids | wc -l | tr -d ' ')"
[ "$FINAL" -eq "$EXPECT" ] || fail "final census reads $FINAL, expected $EXPECT"
BAD=0
for l in relay-logs/relay-"$GEN"-*.log; do
  grep -q "kernel v$EXPECT_KERNEL" "$l" || { echo "  ✗ $l lacks kernel v$EXPECT_KERNEL banner"; BAD=1; }
done
[ "$BAD" -eq 0 ] || fail "one or more replacements report the wrong kernel"
echo "✓ ROLL COMPLETE: $FINAL/$EXPECT relays on kernel v$EXPECT_KERNEL (generation $GEN)."
echo "  Every departure had live heirs; the fleet never dropped below $EXPECT."
