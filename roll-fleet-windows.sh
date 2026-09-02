#!/usr/bin/env bash
# =============================================================================
# roll-fleet-windows.sh — THE sanctioned way to update a RUNNING Windows
# (git-bash) relay fleet. The Windows analog of roll-fleet.sh, written and
# reviewed BEFORE the first Windows roll exactly as windows-fleet.sh demanded.
#
# It holds roll-fleet.sh's THREE RULES on the tools Windows has:
#
#   1. THE COUNT IS AN ARGUMENT, VERIFIED AGAINST MEASUREMENT. EXPECT=<n> is
#      required; the script measures node.exe (tasklist) and REFUSES on mismatch.
#      (Census assumes node.exe == relays, the same assumption windows-fleet.sh
#      makes; this host runs relays only.)
#
#   2. START-THEN-STOP, ONE SLOT AT A TIME. A replacement is launched and proven
#      INTEGRATED (banner kernel version + state=open with a bound mesh channel,
#      read from ITS OWN LOG) before any old relay is stopped. The fleet never
#      drops below EXPECT; every departure leaves into a fleet at full strength+1
#      that already contains its successor. A replacement that fails to integrate
#      ABORTS the roll with every old relay still running.
#
#   3. EVERY STEP IS VERIFIED BY ARTIFACT. The replacement by its log banner +
#      open mesh; the departure by the PID being GONE from tasklist; the roll by
#      a final census + every new banner.
#
# WINDOWS SPECIFICS:
#   · git-bash's `$!` is the BASH job pid, NOT the Windows node.exe pid. The new
#     relay's pid is therefore identified by node.exe SET DIFFERENCE (the one pid
#     present after the launch that was not present before it).
#   · There is no SIGTERM path to a windowless node.exe console process, so a
#     departing relay is FORCE-stopped (taskkill //F) — it cannot run leave()'s
#     graceful handoff. Zero-loss here rests ENTIRELY on live heirs: the
#     start-then-stop order keeps a full-strength fleet, and ROOT_REPLICAS hold
#     warm copies. With kernel >=4.73.0 those replicas also inherit the root's
#     SUBSCRIBER LIST, so a force-killed root's subtree is re-adopted by a live
#     backup with both cache AND subscribers — ungraceful death is safe by design.
#
#   Usage (from git-bash on the Windows host):
#     EXPECT=20 EXPECT_KERNEL=4.73.0 REGION=eagle BRIDGE=wss://testnet.axona.net \
#       bash roll-fleet-windows.sh
#
#   Cold start (no fleet running) is windows-fleet.sh's job.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
INTEGRATE_TIMEOUT="${INTEGRATE_TIMEOUT:-90}"   # s to wait for a replacement to integrate
LEAVE_TIMEOUT="${LEAVE_TIMEOUT:-30}"           # s to wait for a force-stopped pid to disappear
SETTLE="${SETTLE:-3}"                          # s between slots

fail() { echo "✗ ABORT: $*" >&2; exit 1; }

# ── Rule 1: the count is an argument, verified against measurement ──────────
[ -n "${EXPECT:-}" ]        || fail "EXPECT=<live relay count> is REQUIRED. Census first: bash windows-fleet.sh census"
[ -n "${EXPECT_KERNEL:-}" ] || fail "EXPECT_KERNEL=<x.y.z> is REQUIRED — the kernel version you believe you are deploying."

# node.exe pids, one per line. tasklist CSV field 2 is the PID; the memory field
# (field 5+) can contain a comma, but the PID field precedes it, so -F, is safe.
node_pids() {
  tasklist //FI "IMAGENAME eq node.exe" //FO CSV //NH 2>/dev/null \
    | awk -F',' '{ gsub(/"/,"",$2); if ($2 ~ /^[0-9]+$/) print $2 }'
}
alive() { node_pids | grep -qx "$1"; }

# vendored kernel must match what you say you are deploying (git pull first)
VENDORED="$(node -p "require('./vendor/axona-protocol/package.json').version" 2>/dev/null || echo MISSING)"
[ "$VENDORED" = "$EXPECT_KERNEL" ] || fail "vendored kernel is $VENDORED, you said EXPECT_KERNEL=$EXPECT_KERNEL. git pull origin testnet first."

mapfile -t OLD_PIDS < <(node_pids)
MEASURED="${#OLD_PIDS[@]}"
[ "$MEASURED" -eq "$EXPECT" ] || fail "measured $MEASURED node.exe, you said EXPECT=$EXPECT. One of the two is wrong and this script will not guess which. (tasklist //FI \"IMAGENAME eq node.exe\")"
[ "$MEASURED" -gt 0 ] || fail "no fleet running — a roll needs something to roll. Cold start is windows-fleet.sh."

mkdir -p relay-logs
GEN="$(date +%Y%m%d-%H%M%S)"
echo "→ rolling $MEASURED relay(s) to kernel $EXPECT_KERNEL (generation $GEN) [Windows]"
echo "  region=$REGION bridge=$BRIDGE  start-then-stop, one slot at a time"

slot=0
for old in "${OLD_PIDS[@]}"; do
  slot=$((slot + 1))
  log="relay-logs/relay-$GEN-$slot.log"

  # snapshot the node.exe set BEFORE launch so we can name the new pid by diff
  mapfile -t PRE < <(node_pids)
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    nohup node src/index.js >> "$log" 2>&1 &

  # Rule 2/3: the replacement is proven by ITS OWN LOG — banner with the kernel
  # we expect, then state=open with at least one bound mesh channel. No old
  # relay dies until both appear.
  ok_banner=0; ok_mesh=0
  for _ in $(seq 1 "$INTEGRATE_TIMEOUT"); do
    if [ "$ok_banner" -eq 0 ] && grep -q "kernel v$EXPECT_KERNEL" "$log" 2>/dev/null; then ok_banner=1; fi
    if [ "$ok_mesh" -eq 0 ] && grep -E 'state=open .*mesh\(open/bound\)=[0-9]+/[1-9]' "$log" >/dev/null 2>&1; then ok_mesh=1; fi
    [ "$ok_banner" -eq 1 ] && [ "$ok_mesh" -eq 1 ] && break
    sleep 1
  done

  # name the just-launched relay by set difference (the one new node.exe pid)
  mapfile -t POST < <(node_pids)
  newpid="$(comm -13 <(printf '%s\n' "${PRE[@]}" | sort) <(printf '%s\n' "${POST[@]}" | sort) | head -1)"

  if [ "$ok_banner" -ne 1 ] || [ "$ok_mesh" -ne 1 ]; then
    [ -n "$newpid" ] && taskkill //PID "$newpid" //F >/dev/null 2>&1 || true
    fail "slot $slot: replacement (pid ${newpid:-?}) did not integrate within ${INTEGRATE_TIMEOUT}s (banner=$ok_banner mesh=$ok_mesh — see $log). ALL $((MEASURED - slot + 1)) remaining old relays are STILL RUNNING; the fleet is intact. Fix the cause, rerun."
  fi

  # Only now does one OLD relay leave — into a fleet at full strength + 1.
  # Windows has no graceful SIGTERM path (see header): force-stop, heirs cover it.
  taskkill //PID "$old" //F >/dev/null 2>&1 || true
  gone=0
  for _ in $(seq 1 "$LEAVE_TIMEOUT"); do
    alive "$old" || { gone=1; break; }
    sleep 1
  done
  [ "$gone" -eq 1 ] || echo "  ⚠ slot $slot: old pid $old still present after ${LEAVE_TIMEOUT}s (taskkill //F did not clear it — investigate)"

  # Census after every slot: the fleet must be back at exactly EXPECT.
  NOW="$(node_pids | wc -l | tr -d ' ')"
  [ "$NOW" -eq "$EXPECT" ] || fail "slot $slot: census reads $NOW, expected $EXPECT. Stopping so you can look before anything else moves."
  echo "  ✓ slot $slot/$MEASURED: pid $old → ${newpid:-?}  (census $NOW/$EXPECT)"
  sleep "$SETTLE"
done

# ── Final verification: count + every new banner ────────────────────────────
FINAL="$(node_pids | wc -l | tr -d ' ')"
[ "$FINAL" -eq "$EXPECT" ] || fail "final census reads $FINAL, expected $EXPECT"
BAD=0
for l in relay-logs/relay-"$GEN"-*.log; do
  grep -q "kernel v$EXPECT_KERNEL" "$l" || { echo "  ✗ $l lacks kernel v$EXPECT_KERNEL banner"; BAD=1; }
done
[ "$BAD" -eq 0 ] || fail "one or more replacements report the wrong kernel"
echo "✓ ROLL COMPLETE: $FINAL/$EXPECT relays on kernel v$EXPECT_KERNEL (generation $GEN) [Windows]."
echo "  Every departure had live heirs (force-stop + standing replicas); the fleet never dropped below $EXPECT."
