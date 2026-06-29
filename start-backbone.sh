#!/usr/bin/env bash
# start-backbone.sh — (re)launch the local 3-region testnet relay backbone
# (3 relays each for uswest / useast / uscentlw) against wss://testnet.axona.net.
# This is the documented laptop backbone fleet (temporary bootstrap protection
# until the user base scales). Each relay hosts its region keyspace + runs the
# metric-publish loop. Logs: relay-logs/tn-<region>-<n>.log (one per slot).
#
# Kills any existing `node src/index.js` fleet first so you never get 2/slot.
# Survives terminal close + sleep (caffeinate + nohup); launchd is TCC-blocked
# from ~/Documents.
#
#   bash start-backbone.sh
#   REGIONS="uswest useast uscentlw" PER=3 BRIDGE=wss://testnet.axona.net bash start-backbone.sh
#
# Stop: pkill -f "node src/index.js"
set -u
cd "$(dirname "$0")"
REGIONS="${REGIONS:-uswest useast uscentlw}"
PER="${PER:-3}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
mkdir -p relay-logs

echo "→ stopping any existing relay fleet…"
pkill -f "node src/index.js" 2>/dev/null && sleep 2 || true

echo "→ starting backbone: regions=[$REGIONS] per-region=$PER bridge=$BRIDGE"
for region in $REGIONS; do
  for n in $(seq 1 "$PER"); do
    RELAY_REGION="$region" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
      caffeinate -i nohup node src/index.js >> "relay-logs/tn-$region-$n.log" 2>&1 &
    echo "   tn-$region-$n pid $!"
    sleep 1
  done
done
echo "✓ backbone up ($(echo $REGIONS | wc -w | tr -d ' ') regions × $PER). tail -f relay-logs/tn-useast-1.log"
