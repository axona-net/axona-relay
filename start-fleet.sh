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
#
# THIS SCRIPT VERIFIES ITS OWN ARTIFACT (2026-08-05). It used to print
# "✓ fleet up" unconditionally — the loop backgrounds each launch, so $? is the
# fork's, never node's. Over SSH (non-interactive shell, no Homebrew PATH) all
# 26 relays died instantly on `nohup: node: No such file or directory` and the
# script still reported success; the soak then ran against an empty fleet until
# a process count caught it. A launcher whose success message is independent of
# whether anything launched is not a launcher. Now: node is resolved BEFORE any
# slot starts, and after a settle every slot must be alive AND have written its
# startup banner to the log region THIS launch produced. Exit 1 otherwise.
set -u
cd "$(dirname "$0")"
N="${N:-3}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
SETTLE="${SETTLE:-10}"     # seconds to wait before verifying; raise on a slow host
mkdir -p relay-logs

# Resolve node ONCE, up front, and fail loud. Launching N processes that each
# die on a missing interpreter produces N identical errors buried in N logs;
# one check produces one message naming the fix.
if ! NODE_BIN="$(command -v node 2>/dev/null)"; then
  echo "✗ REFUSING: 'node' is not on PATH — nothing launched." >&2
  echo "  A non-interactive shell (ssh host 'bash start-fleet.sh') does not read" >&2
  echo "  the profile that puts Homebrew node on PATH. Export it explicitly:" >&2
  echo "    export PATH=/opt/homebrew/opt/node@24/bin:\$PATH" >&2
  exit 1
fi
echo "→ node $("$NODE_BIN" -v) at $NODE_BIN"

# COLD START ONLY (David, 2026-08-02). This script stops EVERYTHING it finds
# and starts N — which on a live fleet is a march to zero, and the LAST relays
# to leave have no heirs: their handoff drains into the void. That is how a
# free-hand restart shrank 26 -> 3 mid-deploy on 2026-08-02, and on production
# it destroys the region's held history. If a fleet is running, the only
# sanctioned path is roll-fleet.sh (start-then-stop, one slot at a time, count
# verified, never below strength). Excuses don't bring data back to life.
# PREDICATE FIX (2026-08-07, second site): this compared comm = "node", but
# macOS ps reports comm as the FULL binary path, so the guard measured 0
# against a live fleet and the refusal below never fired — the cold-start
# fence was dead. Same predicate as the verify count and roll-fleet's
# live_pids(): exclude the caffeinate wrapper, never match the node side.
RUNNING=0
for pid in $(pgrep -f "src/index.js" 2>/dev/null || true); do
  [ "$(ps -p "$pid" -o comm= 2>/dev/null)" != "caffeinate" ] && RUNNING=$((RUNNING + 1))
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

# CAFFEINATE=0 drops the per-relay wakefulness wrapper. Use it on a host already
# held awake some other way (Amphetamine, pmset), and ALWAYS on a soak runner:
# the soak SIGKILLs relays, `caffeinate -i node …` makes caffeinate the PARENT,
# and killing the child orphans the parent — one live, assertion-holding process
# per kill, accumulating for the length of the run. Thirty had collected on the
# dev laptop before anyone looked. Default stays 1; existing behaviour unchanged.
CAFFEINATE="${CAFFEINATE:-1}"
echo "→ starting $N relay(s): region=$REGION bridge=$BRIDGE caffeinate=$CAFFEINATE"
declare -a SLOT_PID SLOT_OFF
# STAGED START (2026-09-04, David). The stop loop above is one-at-a-time, but this
# loop USED to fire all N launches with only a 1s gap — effectively a burst. A
# simultaneous cold-join of N relays is a storm trigger: each join makes the bridge
# notify every other peer, so N joins at once spike its event loop into loop-stall
# (the 2026-09-04 M1 incident — 12 relays bursting prod collapsed the browser mesh).
# So gate each launch on the PREVIOUS relay actually MESHING — its log reaching
# "state=open" past this launch's offset — bounded by SEAT_WAIT so a slow/broken
# relay can't hang the script. Set SEAT_GATE=0 to restore the old fast burst (only
# for a throwaway fleet where a transient bridge spike doesn't matter).
SEAT_GATE="${SEAT_GATE:-1}"      # 1 = wait for each relay to mesh before launching the next
SEAT_WAIT="${SEAT_WAIT:-30}"     # max seconds to wait for "state=open"; then proceed anyway
for n in $(seq 1 "$N"); do
  LOG="relay-logs/relay-$n.log"
  # Logs are APPENDED across launches, so a banner from a previous run would
  # satisfy the check forever. Record the byte offset first and read only past
  # it — the same offset-bounding the soak census uses on these files.
  SLOT_OFF[$n]=$( [ -f "$LOG" ] && wc -c < "$LOG" | tr -d ' ' || echo 0 )
  if [ "$CAFFEINATE" = "1" ]; then
    RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
      caffeinate -i nohup "$NODE_BIN" src/index.js >> "$LOG" 2>&1 &
  else
    RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
      nohup "$NODE_BIN" src/index.js >> "$LOG" 2>&1 &
  fi
  SLOT_PID[$n]=$!
  echo "   relay-$n pid ${SLOT_PID[$n]}"
  # Stage: hold until this relay has meshed before launching the next. The final
  # slot needs no gate — the verify step below already waits SETTLE seconds.
  if [ "$SEAT_GATE" = "1" ] && [ "$n" -lt "$N" ]; then
    seated=0
    for _ in $(seq 1 "$SEAT_WAIT"); do
      kill -0 "${SLOT_PID[$n]}" 2>/dev/null || break   # died → stop waiting; verify reports it
      NEW=$(tail -c "+$(( ${SLOT_OFF[$n]} + 1 ))" "$LOG" 2>/dev/null || true)
      case "$NEW" in *"state=open"*) seated=1; break ;; esac
      sleep 1
    done
    if [ "$seated" = "1" ]; then
      echo "     ↳ relay-$n meshed (state=open) — launching next"
    else
      echo "     ↳ relay-$n no state=open within ${SEAT_WAIT}s — launching next anyway (verify will judge)" >&2
    fi
  else
    sleep 1
  fi
done

# ---- VERIFY. Nothing below trusts the launch loop's exit status. ----
echo "→ verifying (${SETTLE}s settle)…"
sleep "$SETTLE"

FAILED=0
for n in $(seq 1 "$N"); do
  LOG="relay-logs/relay-$n.log"
  WHY=""
  # 1. The launched process is still there. Under CAFFEINATE=1 this pid is
  #    caffeinate, which exits when its child does — either way, gone is gone.
  kill -0 "${SLOT_PID[$n]}" 2>/dev/null || WHY="process exited"
  # 2. It got far enough to announce itself. Alive-but-wedged before startup
  #    is not started, and only THIS launch's log region counts.
  NEW=$(tail -c "+$(( ${SLOT_OFF[$n]} + 1 ))" "$LOG" 2>/dev/null || true)
  case "$NEW" in
    *"axona-relay v"*) : ;;
    *) WHY="${WHY:+$WHY; }no startup banner" ;;
  esac
  if [ -n "$WHY" ]; then
    FAILED=$((FAILED + 1))
    echo "✗ relay-$n FAILED ($WHY)" >&2
    echo "$NEW" | tail -3 | sed 's/^/    /' >&2
  fi
done

# 3. The artifact itself: how many relay processes are actually live. This is
#    the number that matters, and it is counted independently of the pids we
#    think we started.
#    COUNT FIX (2026-08-07): a raw pgrep count also matches each relay's
#    caffeinate wrapper (its command line contains "src/index.js" as args), so
#    a healthy CAFFEINATE=1 launch of N read as 2N and this script exit-1'd on
#    success — the inverse of the 2026-08-05 lie, a confident FALSE ALARM.
#    Exclude wrappers by comm ("caffeinate" is bare and stable; node's comm is
#    a full install-dependent path, so never match the node side). Same
#    predicate as roll-fleet.sh's live_pids() — one pattern per deploy, both
#    scripts.
LIVE=0
for pid in $(pgrep -f "src/index.js" 2>/dev/null || true); do
  [ "$(ps -p "$pid" -o comm= 2>/dev/null)" != "caffeinate" ] && LIVE=$((LIVE+1))
done

if [ "$FAILED" -gt 0 ] || [ "$LIVE" -ne "$N" ]; then
  echo "✗ FLEET NOT UP: $LIVE/$N relay(s) live, $FAILED slot(s) failed." >&2
  echo "  The survivors were left running — stop them deliberately if you want a" >&2
  echo "  clean retry (they are seconds old and hold nothing worth preserving)." >&2
  exit 1
fi

echo "✓ fleet up — verified $LIVE/$N live with startup banners. tail -f relay-logs/relay-1.log"
