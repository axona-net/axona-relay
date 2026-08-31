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

# Arm B (ARM_STACK=1): every replacement relay comes up with the four
# connection-quality env vars set (relay.js ARM_ENVS). Empty in Arm A, so a
# stack-off replacement is byte-identical to a cold-started stack-off relay.
# assertArmedModules() makes this self-proving: a relay whose requested module
# fails to land CRASHES on boot rather than joining half-armed, so a mis-arm
# aborts the roll instead of silently confounding the run. The unix start_one
# forms leave ARM_ENV unquoted on purpose (word-splitting into env assignments);
# Windows carries ARM_STACK through to win-relay.sh, which bakes the `set` lines.
# ARM_ENV = literal assignments spliced into the remote ssh command strings
# (m1/linux), where they are source text the remote shell parses as prefix
# assignments — no local word-splitting involved. For the LOCAL m4 path we
# EXPORT the vars instead, so node inherits them regardless of whether this
# script's interpreter word-splits unquoted expansions (bash does, zsh does
# not — relying on that split silently produced a stack-OFF relay on the first
# attempt). Windows carries ARM_STACK through to win-relay.sh.
ARM_ENV=''
if [ "${ARM_STACK:-0}" = 1 ]; then
  ARM_ENV='RELAY_SYNAPTOME_MAINTAIN=1 RELAY_ADMISSION_GATE=1 RELAY_ATTEMPT_GUARD=1 RELAY_PRESENCE=1'
  export RELAY_SYNAPTOME_MAINTAIN=1 RELAY_ADMISSION_GATE=1 RELAY_ATTEMPT_GUARD=1 RELAY_PRESENCE=1
fi
# Arm C (ARM_FIX=1): the routing fix flags — FINDK_SKIP_DEAD (findKClosest skips
# dead/unconnected probe peers) + SUB_TERMINAL_VERIFY (synchronous origin-
# independent verification before a SUB self-roots). Appended to ARM_ENV for the
# ssh command strings; exported for the local m4 path. Windows carries ARM_FIX
# through to win-relay.sh. Composes with ARM_STACK (go-with-B keeps the stack on).
if [ "${ARM_FIX:-0}" = 1 ]; then
  ARM_ENV="${ARM_ENV:+$ARM_ENV }FINDK_SKIP_DEAD=1 SUB_TERMINAL_VERIFY=1"
  export FINDK_SKIP_DEAD=1 SUB_TERMINAL_VERIFY=1
fi
# Density sweep + relay-side disc (2026-08-31): thread the successor quota and the
# LAT_TRACE flag through so a re-soak roll arms at the chosen kNear AND makes the
# relays emit disc-relay-<pid>.jsonl. Appended to ARM_ENV for the m1/linux ssh
# strings; exported for the local m4 path; win carries them to win-relay.sh below.
if [ -n "${RELAY_SYNAPTOME_KNEAR:-}" ]; then
  ARM_ENV="${ARM_ENV:+$ARM_ENV }RELAY_SYNAPTOME_KNEAR=$RELAY_SYNAPTOME_KNEAR"
  export RELAY_SYNAPTOME_KNEAR
fi
if [ "${LAT_TRACE:-0}" = 1 ]; then
  ARM_ENV="${ARM_ENV:+$ARM_ENV }LAT_TRACE=1"
  export LAT_TRACE=1
fi

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
    census() {   # live relays = src/index.js procs whose comm is exactly `node`.
      # NOT "!= caffeinate": that also counts the caffeinate WRAPPER's twin and
      # transient ssh/bash procs whose args contain src/index.js (the v3 +1 bug).
      local c=0 p
      for p in $(pgrep -f "src/index.js" 2>/dev/null); do
        [ "$(basename "$(ps -p "$p" -o comm= 2>/dev/null)" 2>/dev/null)" = node ] && c=$((c+1))
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
    stop_one() {   # kill the OLDEST relay node (lowest-pid comm=node src/index.js)
      local p pick=""
      for p in $(pgrep -f "src/index.js" 2>/dev/null | sort -n); do
        [ "$(basename "$(ps -p "$p" -o comm= 2>/dev/null)" 2>/dev/null)" = node ] && { pick="$p"; break; }
      done
      [ -n "$pick" ] && kill "$pick" 2>/dev/null
      return 0
    }
    ;;

  m1)
    # Apple-silicon Mac → relays run under caffeinate, so a naive
    # `pgrep|wc` DOUBLE-counts (node + its caffeinate twin). Count comm=node.
    NODE_BIN="/opt/homebrew/Cellar/node/26.6.0/bin/node"
    census() { sshw 15 m1 'c=0; for p in $(pgrep -f "src/index.js"); do [ "$(basename "$(ps -p $p -o comm=)")" = node ] && c=$((c+1)); done; echo $c' | tr -d '[:space:]'; }
    start_one() {
      sshw 25 m1 "cd ~/Documents/claude/axona-relay && mkdir -p relay-logs && $ARM_ENV RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE RELAY_TUI=0 caffeinate -i nohup $NODE_BIN src/index.js >> relay-logs/relay-churn-\$(date +%s).log 2>&1 </dev/null & echo ok" >/dev/null
    }
    stop_one() {
      sshw 15 m1 'for p in $(pgrep -f "src/index.js" | sort -n); do [ "$(basename "$(ps -p $p -o comm=)")" = node ] && { kill $p 2>/dev/null; break; }; done; echo ok' >/dev/null
    }
    ;;

  axona-linux)
    # Linux Mint → no caffeinate; identify relays by their exe (comm reads as
    # the node thread name "MainThread", so match on /proc/PID/exe → node).
    NODE_BIN='$HOME/bin/node'   # expanded remote-side, single-quoted here
    census() { sshw 15 axona-linux 'n=0; for p in $(pgrep -f "src/index.js"); do case "$(readlink /proc/$p/exe 2>/dev/null)" in *node*) n=$((n+1));; esac; done; echo $n' | tr -d '[:space:]'; }
    start_one() {
      sshw 25 axona-linux "cd ~/Documents/claude/axona-relay && mkdir -p relay-logs && $ARM_ENV RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE RELAY_TUI=0 nohup $NODE_BIN src/index.js >> relay-logs/relay-churn-\$(date +%s).log 2>&1 </dev/null & echo ok" >/dev/null
    }
    stop_one() {
      sshw 15 axona-linux 'for p in $(pgrep -f "src/index.js" | sort -n); do case "$(readlink /proc/$p/exe 2>/dev/null)" in *node*) kill $p 2>/dev/null; break;; esac; done; echo ok' >/dev/null
    }
    ;;

  axona-win)
    # Delegate to the committed relay-aware helper on the box (filters the
    # harness sidecars out of census/stop; schtasks start survives ssh).
    WR='/c/Users/david/github/axona-relay/harness/win-relay.sh'
    census()   { winw 15 "RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE $WR census" | tr -d '[:space:]'; }
    start_one(){ winw 20 "RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE ARM_STACK=${ARM_STACK:-0} ARM_FIX=${ARM_FIX:-0} RELAY_SYNAPTOME_KNEAR=${RELAY_SYNAPTOME_KNEAR:-5} LAT_TRACE=${LAT_TRACE:-0} $WR start" >/dev/null; }
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
