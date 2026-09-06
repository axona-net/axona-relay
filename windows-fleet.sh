#!/usr/bin/env bash
# =============================================================================
# windows-fleet.sh — the Windows (git-bash) analog of start-fleet.sh.
#
# Exists because git-bash cannot run the ritual scripts: it ships no pgrep,
# and its ps has no -o. This script preserves start-fleet's INVARIANTS on
# the tools Windows does have:
#   - the census is MEASURED (tasklist), never assumed;
#   - every launched relay is verified by ITS OWN LOG BANNER carrying the
#     expected kernel version before the script reports success;
#   - background node.exe processes survive the ssh session ending (native
#     Windows children are not tied to the parent's lifetime).
#
# Usage (from git-bash):
#   N=20 EXPECT_KERNEL=4.68.2 REGION=eagle BRIDGE=wss://testnet.axona.net \
#     bash windows-fleet.sh
#
# Census only:   bash windows-fleet.sh census
#
# NO roll support here — a Windows roll analog gets written and reviewed
# BEFORE the first Windows roll, same discipline as roll-fleet.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
EXPECT_KERNEL="${EXPECT_KERNEL:-}"

census() { tasklist //FI "IMAGENAME eq node.exe" 2>/dev/null | grep -c "node.exe" || true; }
fail(){ echo "✗ ABORT: $*" >&2; exit 1; }

# Fleet cadence standard v1 (ops/FLEET-CADENCE.md): staged start gated on
# bridged+bond, then an open backstop. No burst.
source "$(dirname "$0")/fleet-cadence.sh"
read_state(){ grep -oE "state=[a-z]+ peers=[0-9]+ synaptome=[0-9]+ mesh\(open/bound\)=[0-9]+/[0-9]+" "$1" 2>/dev/null | tail -1; }

if [ "${1:-}" = "census" ]; then
  echo "node.exe processes: $(census)"
  exit 0
fi

N="${N:?N=<relay count> is required — there is no default (the start-fleet rule)}"
[ -n "$EXPECT_KERNEL" ] || { echo "EXPECT_KERNEL=<version> is required"; exit 1; }

BEFORE=$(census)
[ "$BEFORE" -eq 0 ] || { echo "ABORT: $BEFORE node.exe already running — this script cold-starts only. A live fleet needs the roll analog (not yet written)."; exit 1; }

mkdir -p relay-logs
echo "→ starting $N relay(s) STAGED: region=$REGION bridge=$BRIDGE (Windows/git-bash)"
GEN="win-$(date +%Y%m%d-%H%M%S)"   # fresh per-run logs so banner/state greps can't hit stale content
declare -a SLOT_LOG
for n in $(seq 1 "$N"); do
  LOG="relay-logs/$GEN-$n.log"
  SLOT_LOG[$n]="$LOG"
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    nohup node src/index.js >> "$LOG" 2>&1 &
  echo "   relay-win-$n launched ($LOG)"
  # STAGED (no burst): gate on this relay bridging+bonding before the next.
  await_advance read_state "$LOG" || fail "relay-win-$n did not bridge+bond within ${ADVANCE_CAP}s ($LOG)"
  grep -q "kernel v$EXPECT_KERNEL" "$LOG" 2>/dev/null || fail "relay-win-$n missing kernel v$EXPECT_KERNEL banner ($LOG)"
  cadence_jitter
done

echo "→ OPEN backstop + census…"
for n in $(seq 1 "$N"); do
  await_open read_state "${SLOT_LOG[$n]}" || fail "relay-win-$n never reached state=open — HALT (${SLOT_LOG[$n]})"
done
AFTER=$(census)
[ "$AFTER" -eq "$N" ] || fail "census $AFTER != $N — read the logs, do not assume"
echo "✓ fleet up — $N/$N bridged, bonded, state=open (gen $GEN)"
