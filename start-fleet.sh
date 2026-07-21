#!/usr/bin/env bash
# start-fleet.sh — (re)launch the local useast relay fleet against testnet.
# Hosts keyspace 0x89 + runs the metric-publish loop (RELAY_METRICS default on).
# Kills any existing fleet first so you don't end up with TWO relays per slot
# (that happened once: a second launch 10 min after the first → 6 procs, 2/slot).
#
#   bash start-fleet.sh           # 3 relays, region useast, bridge wss://testnet.axona.net
#   N=3 REGION=useast BRIDGE=wss://testnet.axona.net bash start-fleet.sh
#
# Logs: relay-logs/relay-<n>.log  (one per slot). Survives the app closing
# (caffeinate + nohup); launchd is TCC-blocked from ~/Documents.
set -u
cd "$(dirname "$0")"
N="${N:-3}"
REGION="${REGION:-useast}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
mkdir -p relay-logs

echo "→ stopping any existing relay fleet (ROLLING — one at a time)…"
# Mass-simultaneous SIGTERM makes every relay's graceful-leave heirs the OTHER
# dying relays (total-cohort teardown) and shreds the region's held history on
# every deploy. Stop one, wait for its leave() to complete (process exit) so
# its roles land on still-alive heirs, then move to the next.
for pid in $(pgrep -f "node src/index.js"); do
  kill -TERM "$pid" 2>/dev/null || continue
  for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$pid" 2>/dev/null || true   # 40s cap, then hard-stop a wedged leaver
  sleep 2                              # let heirs settle before the next departure
done

echo "→ starting $N relay(s): region=$REGION bridge=$BRIDGE"
for n in $(seq 1 "$N"); do
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    caffeinate -i nohup node src/index.js >> "relay-logs/relay-$n.log" 2>&1 &
  echo "   relay-$n pid $!"
  sleep 1
done
echo "✓ fleet up. tail -f relay-logs/relay-1.log"
