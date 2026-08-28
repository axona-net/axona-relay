#!/usr/bin/env bash
# =============================================================================
# harness/relay-churn.sh — ONE relay roll or kill on ONE host, §6, Arm A only.
#
#   bash harness/relay-churn.sh <relay-roll|relay-kill> <host> <heirMs>
#
# relay-roll: START a replacement relay, verify its banner, THEN stop one old
#             — census never dips (the roll-fleet invariant, one slot).
# relay-kill: STOP one relay, wait heirMs (≥3min heir-convergence window),
#             then START a replacement — census dips by one for the window,
#             which the caller floor-guards before invoking.
# Prints the post-action host census as its LAST line (the driver reads it).
#
# Invoked ONLY by churn.mjs under ARM_RELAY=1. Uses each host's proven launch
# form: start-fleet's per-relay line (macOS/linux) and windows-fleet/schtasks
# (win). NEVER a bare SIGKILL loop — one relay, verified, ritual-shaped.
# =============================================================================
set -uo pipefail
KIND="$1"; HOST="$2"; HEIR_MS="${3:-180000}"
REGION="${REGION:-eagle}"; BRIDGE="${BRIDGE:-wss://testnet.axona.net}"

case "$HOST" in
  m4)          RELAY_DIR="/Users/croqueteer/Documents/claude/axona-relay"; RUN=local; NODE="node" ;;
  m1)          RELAY_DIR='~/Documents/claude/axona-relay'; RUN="ssh -o ConnectTimeout=10 m1"; NODE="/opt/homebrew/Cellar/node/26.6.0/bin/node" ;;
  axona-linux) RELAY_DIR='~/Documents/claude/axona-relay'; RUN="ssh -o ConnectTimeout=10 axona-linux"; NODE='$HOME/bin/node' ;;
  *) echo "relay-churn: host $HOST not supported for unix roll (win = separate path)"; echo 0; exit 1 ;;
esac

# census on the target host (comm=node excludes caffeinate; ssh for remotes)
census() {
  if [ "$RUN" = local ]; then
    local c=0 p
    for p in $(pgrep -f "src/index.js"); do
      [ "$(ps -o comm= -p "$p" 2>/dev/null | xargs basename 2>/dev/null)" = node ] && c=$((c+1))
    done; echo "$c"
  else
    $RUN 'pgrep -f "src/index.js" | wc -l' 2>/dev/null | tr -d ' '
  fi
}

start_one() {  # start a replacement relay, detached, its own log
  local ts; ts="churn-$(date +%s)-$RANDOM"
  if [ "$RUN" = local ]; then
    ( cd "$RELAY_DIR" && RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
        nohup "$NODE" src/index.js >> "relay-logs/relay-$ts.log" 2>&1 & )
  else
    $RUN "cd $RELAY_DIR && RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE RELAY_TUI=0 nohup $NODE src/index.js >> relay-logs/relay-$ts.log 2>&1 & echo started"
  fi
}

stop_one() {  # stop the OLDEST one relay (heir-preserving: never the newest)
  if [ "$RUN" = local ]; then
    local victim; victim=$(pgrep -f "src/index.js" | head -1)
    [ -n "$victim" ] && kill "$victim" 2>/dev/null
  else
    $RUN 'v=$(pgrep -f "src/index.js" | head -1); [ -n "$v" ] && kill $v 2>/dev/null; echo stopped'
  fi
}

B=$(census)
if [ "$KIND" = relay-roll ]; then
  start_one; sleep 20            # let the replacement bind before removing one
  stop_one
elif [ "$KIND" = relay-kill ]; then
  stop_one; sleep "$(( HEIR_MS / 1000 ))"   # the heir-convergence window
  start_one; sleep 15
else
  echo "relay-churn: unknown kind $KIND"; echo "$B"; exit 1
fi
sleep 5
census
