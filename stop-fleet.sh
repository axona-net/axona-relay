#!/usr/bin/env bash
set -euo pipefail
# =============================================================================
# stop-fleet.sh — standardized GRACEFUL shutdown (Fleet Cadence Standard v1,
# ops/FLEET-CADENCE.md). One relay at a time: signal it to leave, wait for it to
# drain/exit, jitter, then the next. NEVER a bulk stop — a herd of simultaneous
# leaves drains topic handoffs into the void (the 1.96% whole-topic loss that
# roll-fleet was built to prevent). Auto-detects the fleet type on-host.
#
# Usage (ON the relay host):
#   bash stop-fleet.sh          # graceful, one at a time
#   DRY=1 bash stop-fleet.sh    # list what it WOULD stop; stop nothing
# =============================================================================
DIR="$(cd "$(dirname "$0")" && pwd)"
source "$DIR/fleet-cadence.sh"
cd "$DIR"
DRY="${DRY:-0}"

# --- platform detection ---
if command -v systemctl >/dev/null 2>&1 && systemctl list-units "axona-relay@*" --no-legend --no-pager 2>/dev/null | grep -q axona-relay; then
  MODE=systemd
elif uname -s 2>/dev/null | grep -qiE "mingw|msys"; then
  MODE=windows
else
  MODE=process
fi
echo "→ stop-fleet: mode=$MODE (graceful, one at a time)"

case "$MODE" in
  systemd)
    # systemctl stop sends SIGTERM (runs the kernel leave()/drain) and blocks.
    stop_one(){ systemctl stop "$1"; }
    alive(){ systemctl is-active --quiet "$1"; }
    targets="$(systemctl list-units "axona-relay@*" --state=running --no-legend --no-pager 2>/dev/null | awk '{print $1}' || true)"
    ;;
  windows)
    # console node cannot take a graceful SIGTERM; taskkill //F one at a time
    # with a settle is the best Windows can do (documented caveat).
    stop_one(){ taskkill //PID "$1" //F >/dev/null 2>&1 || true; }
    alive(){ tasklist //FI "PID eq $1" 2>/dev/null | grep -q "\b$1\b"; }
    targets="$(tasklist //FI "IMAGENAME eq node.exe" //FO CSV //NH 2>/dev/null | sed 's/"//g' | awk -F, '{print $2}' | grep -E '^[0-9]+$' || true)"
    ;;
  process)
    # SIGTERM triggers the kernel's leave() — drain + role handoff — then exit.
    stop_one(){ kill -TERM "$1" 2>/dev/null || true; }
    alive(){ kill -0 "$1" 2>/dev/null; }
    targets=""
    for p in $(pgrep -f "src/index.js" 2>/dev/null || true); do
      [ "$(ps -p "$p" -o comm= 2>/dev/null)" != caffeinate ] && targets="$targets $p"
    done
    ;;
esac

cnt=$(printf '%s\n' $targets | grep -c . || true)
[ "$cnt" -gt 0 ] || { echo "  no live relays found — nothing to stop"; exit 0; }
echo "  $cnt relay(s) to stop: $(printf '%s ' $targets)"
[ "$DRY" = "1" ] && { echo "✓ DRY: would graceful-leave $cnt relay(s) one at a time"; exit 0; }

i=0
for t in $targets; do
  i=$((i+1))
  echo "  → leaving $i/$cnt: $t"
  graceful_leave stop_one alive "$t" || echo "    ⚠ $t did not exit within ${LEAVE_TIMEOUT}s"
  cadence_jitter
done
echo "✓ STOP-FLEET COMPLETE: $i relay(s) left one at a time (mode=$MODE)"
