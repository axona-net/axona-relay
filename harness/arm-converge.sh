#!/usr/bin/env bash
# =============================================================================
# harness/arm-converge.sh — finish arming a Mac host by replacing every relay
# that started BEFORE the arm-roll (i.e. the stack-off survivors the pid-ordered
# roll missed after macOS pid-wrap) with a fresh ARMED heir. Runs ON the target
# host (locally on m4, or shipped over ssh to m1).
#
# Age-based on purpose: it needs neither process-env readability (macOS `ps eww`
# does not expose a caffeinate-launched relay's env across an ssh session) nor
# correct pid ordering (pids wrapped, so lowest-pid != oldest). A relay whose
# lstart epoch is < CUTOFF_EPOCH is a pre-arm stack-off relay; replace it.
# Heir-first (start, settle, then kill the specific old pid) so census holds.
#
#   NODE_BIN=/path/to/node CUTOFF_EPOCH=<epoch> EXPECT=<n> bash harness/arm-converge.sh
# =============================================================================
set -uo pipefail
NODE_BIN="${NODE_BIN:-$(command -v node)}"
CUTOFF_EPOCH="${CUTOFF_EPOCH:?CUTOFF_EPOCH (arm-roll start) required}"
EXPECT="${EXPECT:?EXPECT required}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
export RELAY_SYNAPTOME_MAINTAIN=1 RELAY_ADMISSION_GATE=1 RELAY_ATTEMPT_GUARD=1 RELAY_PRESENCE=1
# Arm C: routing fix flags (composes with the stack above). ARM_FIX=1 (default on
# here — arm-converge is only invoked for the fix roll) sets both fix env vars so
# every replacement relay comes up new-kernel + stack + fix.
if [ "${ARM_FIX:-1}" = 1 ]; then export FINDK_SKIP_DEAD=1 SUB_TERMINAL_VERIFY=1; fi
# Density sweep + relay-side disc (2026-08-31): the successor quota and LAT_TRACE are
# inherited by start_armed's node from this process env. Export them so a re-soak
# roll arms at the chosen kNear and the Mac relays emit disc-relay-<pid>.jsonl.
export RELAY_SYNAPTOME_KNEAR="${RELAY_SYNAPTOME_KNEAR:-5}" LAT_TRACE="${LAT_TRACE:-0}"

node_relays() {  # pids whose comm is exactly node
  for p in $(pgrep -f "src/index.js" 2>/dev/null); do
    [ "$(basename "$(ps -p "$p" -o comm= 2>/dev/null)" 2>/dev/null)" = node ] && echo "$p"
  done
}
start_epoch() {  # epoch of a pid's start time (macOS lstart → epoch)
  local ls; ls="$(ps -p "$1" -o lstart= 2>/dev/null)"; [ -z "$ls" ] && { echo 0; return; }
  date -j -f "%a %b %e %T %Y" "$ls" +%s 2>/dev/null || echo 0
}
start_armed() {
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    caffeinate -i nohup "$NODE_BIN" src/index.js \
      >> "relay-logs/relay-armfix-$(date +%s)-$1.log" 2>&1 </dev/null &
}

echo "arm-converge: CUTOFF_EPOCH=$CUTOFF_EPOCH EXPECT=$EXPECT node=$NODE_BIN"
old=""
for p in $(node_relays); do
  se="$(start_epoch "$p")"
  [ "$se" -gt 0 ] && [ "$se" -lt "$CUTOFF_EPOCH" ] && old="$old $p"
done
n="$(echo $old | wc -w | tr -d ' ')"
echo "pre-arm (stack-off) relays: $n →$old"

for p in $old; do
  start_armed "$p"; sleep 6
  kill "$p" 2>/dev/null && echo "  replaced old $p"
done
sleep 4

# Trim any overshoot down to EXPECT by removing the NEWEST relays (all armed now).
cur="$(node_relays | wc -l | tr -d ' ')"
while [ "$cur" -gt "$EXPECT" ]; do
  newest="$(for p in $(node_relays); do echo "$(start_epoch "$p") $p"; done | sort -n | tail -1 | awk '{print $2}')"
  [ -z "$newest" ] && break
  kill "$newest" 2>/dev/null && echo "  trimmed newest $newest (overshoot)"
  sleep 3; cur="$(node_relays | wc -l | tr -d ' ')"
done

fin="$(node_relays | wc -l | tr -d ' ')"
still_old=0
for p in $(node_relays); do se="$(start_epoch "$p")"; [ "$se" -gt 0 ] && [ "$se" -lt "$CUTOFF_EPOCH" ] && still_old=$((still_old+1)); done
echo "arm-converge DONE: census=$fin (want $EXPECT), remaining pre-arm relays=$still_old (want 0)"
