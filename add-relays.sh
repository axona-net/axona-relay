#!/usr/bin/env bash
# =============================================================================
# add-relays.sh — GROW a live fleet to a target count.
#
# roll-fleet.sh REPLACES; it never adds. That gap is why Air sat at 3 relays for
# two days after one died: the roll faithfully rolled 3, and nothing restored the
# 4th. start-fleet.sh refuses to run against a live fleet (correctly — it is a
# cold-start tool). So there was no sanctioned path between "roll what exists"
# and "stop everything and cold start". This is that path.
#
# It only ever ADDS. It never kills, never restarts, never touches a running
# relay. If the fleet is already at or above TARGET it does nothing and says so.
# The worst case for a failed add is one dead newcomer and a log to read; every
# existing relay is untouched throughout.
#
# Each newcomer is proven by ITS OWN LOG before the next one starts — the same
# two-part proof roll-fleet.sh uses: the kernel banner we expect, then state=open
# with at least one BOUND mesh channel. SETTLE caps that wait (default 20s, David
# 2026-09-08: "20 seconds is enough time for each to settle via the bridge"). A
# newcomer that misses the gate is left running and reported, not killed — it may
# simply be slow, and a relay that integrates a second late is still a relay.
#
#   TARGET=6 EXPECT_KERNEL=4.78.0 REGION=eagle BRIDGE=wss://bridge.axona.net \
#     [SETTLE=20] [CAFFEINATE=0] bash add-relays.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://bridge.axona.net}"     # PROD default: this script grows the live fleet
SETTLE="${SETTLE:-20}"
LOGDIR="${LOGDIR:-relay-logs}"

fail() { echo "✗ ABORT: $*" >&2; exit 1; }

[ -n "${TARGET:-}" ]        || fail "TARGET=<n> is REQUIRED (the count you want AFTER this run)."
[ -n "${EXPECT_KERNEL:-}" ] || fail "EXPECT_KERNEL=<x.y.z> is REQUIRED — the kernel you believe you are launching."

# Census delegated to relay-census.sh — the single definition. It has been got
# wrong twice by hand (caffeinate wrappers double-counting on mac; the pattern
# matching the shell running it on linux), so nothing re-implements it.
live_pids() { bash "$(dirname "$0")/relay-census.sh" --pids; }

VENDORED="$(node -p "require('./vendor/axona-protocol/package.json').version" 2>/dev/null || echo MISSING)"
[ "$VENDORED" = "$EXPECT_KERNEL" ] || fail "vendored kernel is $VENDORED, you said EXPECT_KERNEL=$EXPECT_KERNEL. Pull/re-vendor first."

HAVE="$(live_pids | grep -c . || true)"
NEED=$(( TARGET - HAVE ))
echo "→ fleet has $HAVE, target $TARGET, region=$REGION bridge=$BRIDGE settle=${SETTLE}s"
if [ "$NEED" -le 0 ]; then echo "✓ already at or above target ($HAVE ≥ $TARGET) — nothing added"; exit 0; fi

mkdir -p "$LOGDIR"
GEN="add-$(date +%Y%m%d-%H%M%S)"
added=0
for i in $(seq 1 "$NEED"); do
  log="$LOGDIR/$GEN-$i.log"
  if [ "${CAFFEINATE:-$(command -v caffeinate >/dev/null 2>&1 && echo 1 || echo 0)}" = "1" ]; then
    RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
      caffeinate -i nohup node src/index.js >> "$log" 2>&1 &
  else
    RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 \
      nohup node src/index.js >> "$log" 2>&1 &
  fi
  newpid=$!

  ok_banner=0; ok_mesh=0
  for _ in $(seq 1 "$SETTLE"); do
    if [ "$ok_banner" -eq 0 ] && grep -q "kernel v$EXPECT_KERNEL" "$log" 2>/dev/null; then ok_banner=1; fi
    if [ "$ok_mesh" -eq 0 ] && grep -E 'state=open .*mesh\(open/bound\)=[0-9]+/[1-9]' "$log" >/dev/null 2>&1; then ok_mesh=1; fi
    [ "$ok_banner" -eq 1 ] && [ "$ok_mesh" -eq 1 ] && break
    kill -0 "$newpid" 2>/dev/null || break
    sleep 1
  done

  if ! kill -0 "$newpid" 2>/dev/null; then
    echo "  ✗ add $i/$NEED: pid $newpid DIED — see $log. Existing fleet untouched; stopping."
    fail "newcomer died on launch — fix the cause before adding more"
  fi
  if [ "$ok_banner" -eq 1 ] && [ "$ok_mesh" -eq 1 ]; then
    echo "  ✓ add $i/$NEED: pid $newpid open+bound on kernel v$EXPECT_KERNEL"
  else
    echo "  ⚠ add $i/$NEED: pid $newpid alive but not open+bound within ${SETTLE}s (banner=$ok_banner mesh=$ok_mesh, $log) — left running"
  fi
  added=$(( added + 1 ))
done

FINAL="$(live_pids | grep -c . || true)"
echo "✓ ADD COMPLETE: added $added, fleet now $FINAL (target $TARGET) on kernel v$EXPECT_KERNEL"
[ "$FINAL" -ge "$TARGET" ] || echo "  ⚠ below target — $(( TARGET - FINAL )) short; check the logs above"
