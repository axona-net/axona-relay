#!/usr/bin/env bash
# =============================================================================
# harness/ab-coldstart.sh <kNear> — COLD-start the full 62-relay fleet at ONE
# uniform kNear, including the windows relays (David 2026-08-31). The in-place
# roll (arm-roll-host) leaves win in a mixed kNear because the stop races the ssh
# watchdog; a cold start avoids the roll entirely — stop every relay, then start
# each fresh with the same env, so there is no old/new mix. START is the reliable
# windows op; only the mid-roll STOP was flaky.
#
#   bash harness/ab-coldstart.sh 10     # arms every relay at kNear=10 + LAT_TRACE
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
K="${1:?usage: ab-coldstart.sh <kNear>}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
ENVV="RELAY_SYNAPTOME_MAINTAIN=1 RELAY_ADMISSION_GATE=1 RELAY_ATTEMPT_GUARD=1 RELAY_PRESENCE=1 FINDK_SKIP_DEAD=1 SUB_TERMINAL_VERIFY=1 RELAY_SYNAPTOME_KNEAR=$K LAT_TRACE=1 RELAY_REGION=$REGION BRIDGE_URL=$BRIDGE RELAY_TUI=0"

echo "[coldstart] kNear=$K  $(date -u +%H:%M:%SZ)"

# ── stop everything ──────────────────────────────────────────────────
echo "[coldstart] stopping all relays"
for p in $(pgrep -f "src/index.js" 2>/dev/null); do [ "$(basename "$(ps -p "$p" -o comm= 2>/dev/null)" 2>/dev/null)" = node ] && kill "$p" 2>/dev/null; done
ssh -o ConnectTimeout=10 m1 'for p in $(pgrep -f "src/index.js"); do [ "$(basename "$(ps -p $p -o comm=)")" = node ] && kill $p 2>/dev/null; done' 2>/dev/null
ssh -o ConnectTimeout=10 axona-linux 'for p in $(pgrep -f "src/index.js"); do case "$(readlink /proc/$p/exe 2>/dev/null)" in *node*) kill $p 2>/dev/null;; esac; done' 2>/dev/null
printf '%s\n' 'powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name=\x27node.exe\x27\" | Where-Object { $_.CommandLine -match \x27src.index.js\x27 } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"' \
  | ssh -o ConnectTimeout=15 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' 2>/dev/null
# also clear the relay-side disc so the coming arm starts fresh
rm -f relay-logs/disc-relay-*.jsonl
ssh -o ConnectTimeout=10 m1 'rm -f ~/Documents/claude/axona-relay/relay-logs/disc-relay-*.jsonl' 2>/dev/null
ssh -o ConnectTimeout=10 axona-linux 'rm -f ~/Documents/claude/axona-relay/relay-logs/disc-relay-*.jsonl' 2>/dev/null
sleep 5

# ── start fresh (uniform env) ────────────────────────────────────────
NODE_M4="$(command -v node)"; NODE_M1="/opt/homebrew/Cellar/node/26.6.0/bin/node"
ts=$(date +%s)
echo "[coldstart] starting m4 x25"
( cd "$PWD" && mkdir -p relay-logs
  for i in $(seq 1 25); do
    env $ENVV caffeinate -i nohup "$NODE_M4" src/index.js >> "relay-logs/relay-cold-$ts-m4-$i.log" 2>&1 </dev/null &
    disown; sleep 0.25
  done )
echo "[coldstart] starting m1 x12"
ssh -o ConnectTimeout=15 m1 "cd ~/Documents/claude/axona-relay && mkdir -p relay-logs && for i in \$(seq 1 12); do $ENVV caffeinate -i nohup $NODE_M1 src/index.js >> relay-logs/relay-cold-$ts-m1-\$i.log 2>&1 </dev/null & done; echo m1-started" 2>&1 | grep -v Warning
echo "[coldstart] starting linux x5"
ssh -o ConnectTimeout=15 axona-linux "cd ~/Documents/claude/axona-relay && mkdir -p relay-logs && for i in \$(seq 1 5); do $ENVV nohup \$HOME/bin/node src/index.js >> relay-logs/relay-cold-$ts-lx-\$i.log 2>&1 </dev/null & done; echo linux-started" 2>&1 | grep -v Warning
echo "[coldstart] starting win x20 (schtasks, fresh)"
for i in $(seq 1 20); do
  printf 'RELAY_REGION=%s BRIDGE_URL=%s ARM_STACK=1 ARM_FIX=1 RELAY_SYNAPTOME_KNEAR=%s LAT_TRACE=1 /c/Users/david/github/axona-relay/harness/win-relay.sh start\n' "$REGION" "$BRIDGE" "$K" \
    | ssh -o ConnectTimeout=15 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' >/dev/null 2>&1
  sleep 0.3
done
echo "[coldstart] all start commands issued $(date -u +%H:%M:%SZ); relays bind over the next ~30-60s"
