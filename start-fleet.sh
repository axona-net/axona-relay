#!/usr/bin/env bash
# start-fleet.sh — (re)launch the local eagle relay fleet against testnet.
# Hosts keyspace 0x89 + runs the metric-publish loop (RELAY_METRICS default on).
# Kills any existing fleet first so you don't end up with TWO relays per slot
# (that happened once: a second launch 10 min after the first → 6 procs, 2/slot).
#
#   bash start-fleet.sh           # 3 relays, region eagle, bridge wss://testnet.axona.net
#   N=3 REGION=eagle BRIDGE=wss://testnet.axona.net bash start-fleet.sh
#
# Logs: relay-logs/relay-<n>.log  (one per slot). Survives the app closing
# (caffeinate + nohup); launchd is TCC-blocked from ~/Documents.
set -u
cd "$(dirname "$0")"
N="${N:-3}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
mkdir -p relay-logs

# COLD START ONLY (David, 2026-08-02). This script stops EVERYTHING it finds
# and starts N — which on a live fleet is a march to zero, and the LAST relays
# to leave have no heirs: their handoff drains into the void. That is how a
# free-hand restart shrank 26 -> 3 mid-deploy on 2026-08-02, and on production
# it destroys the region's held history. If a fleet is running, the only
# sanctioned path is roll-fleet.sh (start-then-stop, one slot at a time, count
# verified, never below strength). Excuses don't bring data back to life.
RUNNING=0
for pid in $(pgrep -f "src/index.js" 2>/dev/null || true); do
  [ "$(ps -p "$pid" -o comm= 2>/dev/null)" = "node" ] && RUNNING=$((RUNNING + 1))
done
if [ "$RUNNING" -gt 0 ]; then
  echo "✗ REFUSING: $RUNNING relay(s) are LIVE. This script is for cold start only." >&2
  echo "  To update a running fleet:  EXPECT=$RUNNING EXPECT_KERNEL=<x.y.z> bash roll-fleet.sh" >&2
  echo "  To genuinely tear down first (destroys held state): stop them yourself, deliberately." >&2
  exit 1
fi

echo "→ stopping any existing relay fleet (ROLLING — one at a time)…"
# Mass-simultaneous SIGTERM makes every relay's graceful-leave heirs the OTHER
# dying relays (total-cohort teardown) and shreds the region's held history on
# every deploy. Stop one, wait for its leave() to complete (process exit) so
# its roles land on still-alive heirs, then move to the next.
# PATTERN FIX (2026-08-01). This read `pgrep -f "node src/index.js"`, which does
# NOT match the running processes: they launch as
#   /usr/local/bin/node /Users/.../axona-relay/src/index.js
# with ABSOLUTE paths, so the relative-form pattern matched nothing and the stop
# step silently did nothing. Every launch then ADDED a fleet instead of replacing
# it — the exact "2/slot" hazard this block's own comment warns about, found at
# 26 live eagle relays. A stop that matches nothing looks identical to a stop
# that had nothing to do; that is the same confident-false-negative as the rest
# of this week. Anchored on the path suffix so both forms match.
for pid in $(pgrep -f "src/index.js"); do
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
