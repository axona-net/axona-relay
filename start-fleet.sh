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

echo "→ stopping any existing relay fleet…"
pkill -f "node src/index.js" 2>/dev/null && sleep 2 || true

echo "→ starting $N relay(s): region=$REGION bridge=$BRIDGE"
for n in $(seq 1 "$N"); do
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
    caffeinate -i nohup node src/index.js >> "relay-logs/relay-$n.log" 2>&1 &
  echo "   relay-$n pid $!"
  sleep 1
done
echo "✓ fleet up. tail -f relay-logs/relay-1.log"
