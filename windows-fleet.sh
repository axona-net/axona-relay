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

if [ "${1:-}" = "census" ]; then
  echo "node.exe processes: $(census)"
  exit 0
fi

N="${N:?N=<relay count> is required — there is no default (the start-fleet rule)}"
[ -n "$EXPECT_KERNEL" ] || { echo "EXPECT_KERNEL=<version> is required"; exit 1; }

BEFORE=$(census)
[ "$BEFORE" -eq 0 ] || { echo "ABORT: $BEFORE node.exe already running — this script cold-starts only. A live fleet needs the roll analog (not yet written)."; exit 1; }

mkdir -p relay-logs
echo "→ starting $N relay(s): region=$REGION bridge=$BRIDGE (Windows/git-bash)"
declare -a SLOT_LOG
for n in $(seq 1 "$N"); do
  LOG="relay-logs/relay-win-$n.log"
  SLOT_LOG[$n]="$LOG"
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    nohup node src/index.js >> "$LOG" 2>&1 &
  echo "   relay-win-$n launched"
  sleep 1
done

echo "→ verifying (12s settle)…"
sleep 12
OK=0
for n in $(seq 1 "$N"); do
  if grep -q "kernel v$EXPECT_KERNEL" "${SLOT_LOG[$n]}" 2>/dev/null; then
    OK=$((OK + 1))
  else
    echo "   ✗ relay-win-$n: no banner with kernel v$EXPECT_KERNEL — check ${SLOT_LOG[$n]}"
  fi
done
AFTER=$(census)
echo "banners verified: $OK/$N   node.exe census: $AFTER"
[ "$OK" -eq "$N" ] && [ "$AFTER" -eq "$N" ] && { echo "✓ fleet up — $N/$N verified"; exit 0; }
echo "✗ fleet INCOMPLETE — do not assume; read the logs."
exit 1
