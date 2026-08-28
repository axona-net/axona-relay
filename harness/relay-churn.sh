#!/usr/bin/env bash
# =============================================================================
# harness/relay-churn.sh — ONE relay roll or kill on ONE host, §6, Arm A/B only.
#
#   bash harness/relay-churn.sh <relay-roll|relay-kill> <host> <heirMs>
#
# relay-roll: START a replacement, wait for it to bind, THEN stop one old —
#             census never dips. relay-kill: STOP one, wait heirMs (≥3min
#             heir window), then START a replacement.
# Prints the post-action host census as its LAST line (a bare integer).
#
# Invoked by churn.mjs under ARM_RELAY=1, but also runnable standalone for the
# per-host validation this file was rebuilt to survive:
#
#   REGION=eagle BRIDGE=wss://testnet.axona.net \
#     bash harness/relay-churn.sh relay-roll m4 20000
#
# DETACHMENT (the v2 wall): a started daemon must not hold the invoking
# parent's stdout/stderr, or churn.mjs's execSync (and the Bash tool) blocks on
# EOF until timeout. The fix is the EXACT start-fleet incantation:
#   * absolute node path (command -v node) — `nohup node` under a node parent
#     loses PATH and dies "No such file or directory" (start-fleet.sh:16);
#   * bare `&` with all THREE std fds redirected: >>LOG 2>&1 </dev/null;
#   * caffeinate -i so display-sleep can't suspend the relay.
# Remote hosts add a hard local ssh watchdog: a launch ssh can hang holding a
# backgrounded child's channel, so every ssh is time-boxed and always returns.
# Windows uses a one-shot schtasks task — the only relay start proven to
# survive ssh disconnect on that box (own logon session).
# =============================================================================
set -uo pipefail
KIND="${1:?usage: relay-churn.sh <relay-roll|relay-kill> <host> [heirMs]}"
HOST="${2:?host required}"
HEIR_MS="${3:-180000}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"

# ssh with a hard local watchdog: run in the background, kill it if it outlives
# the budget, always return. Guarantees this script cannot wedge the driver.
sshw() {  # <timeoutSec> <host> <remote-command-string>
  local t="$1" host="$2" cmd="$3" sp wp rc
  ssh -o ConnectTimeout=10 "$host" "$cmd" & sp=$!
  ( sleep "$t"; kill "$sp" 2>/dev/null ) & wp=$!
  wait "$sp" 2>/dev/null; rc=$?
  kill "$wp" 2>/dev/null; wait "$wp" 2>/dev/null
  return $rc
}
# git-bash over ssh, payload on stdin (cmd.exe never sees the quotes).
winw() {  # <timeoutSec> <stdin-script>
  local t="$1" script="$2" sp wp
  printf '%s\n' "$script" | ssh -o ConnectTimeout=10 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' & sp=$!
  ( sleep "$t"; kill "$sp" 2>/dev/null ) & wp=$!
  wait "$sp" 2>/dev/null
  kill "$wp" 2>/dev/null; wait "$wp" 2>/dev/null
}

case "$HOST" in
  m4)
    RELAY_DIR="/Users/croqueteer/Documents/claude/axona-relay"
    NODE_BIN="$(command -v node)"
    census() {   # live relay nodes = src/index.js procs whose comm is NOT caffeinate
      local c=0 p
      for p in $(pgrep -f "src/index.js" 2>/dev/null); do
        [ "$(ps -p "$p" -o comm= 2>/dev/null)" != caffeinate ] && c=$((c+1))
      done
      echo "$c"
    }
    start_one() {
      ( cd "$RELAY_DIR" && mkdir -p relay-logs
        RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
          caffeinate -i nohup "$NODE_BIN" src/index.js \
            >> "relay-logs/relay-churn-$(date +%s)-$$.log" 2>&1 </dev/null &
        disown )
    }
    stop_one() {   # kill the OLDEST relay node (lowest-pid src/index.js, comm=node)
      local p pick=""
      for p in $(pgrep -f "src/index.js" 2>/dev/null | sort -n); do
        [ "$(ps -p "$p" -o comm= 2>/dev/null)" = caffeinate ] && continue
        pick="$p"; break
      done
      [ -n "$pick" ] && kill "$pick" 2>/dev/null
      return 0
    }
    ;;

  m1|axona-linux)
    if [ "$HOST" = m1 ]; then
      NODE_BIN="/opt/homebrew/Cellar/node/26.6.0/bin/node"
    else
      NODE_BIN='$HOME/bin/node'   # expanded remote-side, single-quoted here
    fi
    census() { sshw 15 "$HOST" 'pgrep -f "src/index.js" | wc -l' | tr -d '[:space:]'; }
    start_one() {
      sshw 25 "$HOST" "cd ~/Documents/claude/axona-relay && mkdir -p relay-logs && RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE RELAY_TUI=0 nohup $NODE_BIN src/index.js >> relay-logs/relay-churn-\$(date +%s).log 2>&1 </dev/null & echo started" >/dev/null
    }
    stop_one() {
      sshw 15 "$HOST" 'p=$(pgrep -f "src/index.js" | sort -n | head -1); [ -n "$p" ] && kill $p 2>/dev/null; echo stopped' >/dev/null
    }
    ;;

  axona-win)
    # Delegate to the committed relay-aware helper on the box (filters the
    # harness sidecars out of census/stop; schtasks start survives ssh).
    WR='/c/Users/david/github/axona-relay/harness/win-relay.sh'
    census()   { winw 15 "RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE $WR census" | tr -d '[:space:]'; }
    start_one(){ winw 20 "RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE $WR start" >/dev/null; }
    stop_one() { winw 15 "RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE $WR stop"  >/dev/null; }
    ;;

  *) echo "relay-churn: host $HOST unknown" >&2; echo 0; exit 1 ;;
esac

B=$(census)
if [ "$KIND" = relay-roll ]; then
  start_one; sleep 25            # replacement binds before we remove one old
  stop_one
elif [ "$KIND" = relay-kill ]; then
  stop_one; sleep "$(( HEIR_MS / 1000 ))"
  start_one; sleep 20
else
  echo "relay-churn: unknown kind $KIND" >&2; echo "${B:-0}"; exit 1
fi
sleep 5
census
