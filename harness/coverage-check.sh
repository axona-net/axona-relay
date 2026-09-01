#!/usr/bin/env bash
# =====================================================================
# harness/coverage-check.sh — PRE-ARM trace-coverage gate.
#
# Refuses to admit a traced arm unless EVERY live relay across EVERY host is
# emitting LAT_TRACE telemetry under the expected kernel. This is the direct
# remedy for the 2026-09-01 VOID read: senders were traced (21 relays stamped
# tx) but most receivers were not (6 stamped rx) because LAT_TRACE was not
# uniform across the fleet. tx and rx were then different event populations and
# the per-hop DELIVER drop was unmeasurable — the analyzer had to VOID it. A
# uniformly-traced fleet is the precondition Aster required ("verify coverage
# before admitting the arm", a87ad414). No arm is started here; no kernel change.
#
# Signal: a relay that is BOTH kernel>=EXPECT AND LAT_TRACE=1 writes
# relay-logs/disc-relay-<pid>.jsonl (src/index.js:236). So per host:
#   live      = relay pids (src/index.js node procs), per-host census
#   covered   = live pids that have a NON-EMPTY relay-logs/disc-relay-<pid>.jsonl
#   uncovered = live pids with no such file — untraced OR a pre-telemetry kernel
# And each host's CHECKOUT KERNEL_VERSION must equal EXPECT (catches an un-rolled
# host whose relays would come up untraceable on the next roll).
#
# A freshly-rolled relay only creates its disc file on its first pubsub:disc /
# pubsub:lat-stage event, so a quiet relay can read as uncovered for a few
# seconds. COVERAGE_WARM_S (default 90) re-polls until uniform or timeout.
#
# Exit 0 => ARM ADMITTED (coverage uniform). Exit 1 => ARM REFUSED (gaps printed).
# COVERAGE_OVERRIDE=1 forces admit and says so LOUDLY — for a deliberate
# partial-coverage arm only, never as a way past a real gap.
#
#   EXPECT=4.69.0 HOSTS="m4 m1 axona-linux axona-win" bash harness/coverage-check.sh
# =====================================================================
set -u

EXPECT="${EXPECT:-$(grep -hoE "KERNEL_VERSION[[:space:]]*=[[:space:]]*'[^']+'" \
  vendor/axona-protocol/src/transport/handshake.js 2>/dev/null | grep -oE "[0-9]+\.[0-9]+\.[0-9]+" | head -1)}"
HOSTS="${HOSTS:-m4 m1 axona-linux axona-win}"
WARM_S="${COVERAGE_WARM_S:-90}"
POLL_S=10
M4_DIR="/Users/croqueteer/Documents/claude/axona-relay"
REMOTE_DIR='~/Documents/claude/axona-relay'
WIN_DIR='/c/Users/david/github/axona-relay'
WR='/c/Users/david/github/axona-relay/harness/win-relay.sh'

[ -n "$EXPECT" ] || { echo "coverage-check: could not read EXPECT kernel version" >&2; exit 2; }

sshw() {  # <timeoutSec> <host> <remote-command>
  local t="$1" host="$2" cmd="$3" sp wp rc
  ssh -o ConnectTimeout=10 "$host" "$cmd" & sp=$!
  ( sleep "$t"; kill "$sp" 2>/dev/null ) & wp=$!
  wait "$sp" 2>/dev/null; rc=$?
  kill "$wp" 2>/dev/null; wait "$wp" 2>/dev/null
  return $rc
}
winw() {  # <timeoutSec> <stdin-script>
  local t="$1" script="$2" sp wp
  printf '%s\n' "$script" | ssh -o ConnectTimeout=10 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' & sp=$!
  ( sleep "$t"; kill "$sp" 2>/dev/null ) & wp=$!
  wait "$sp" 2>/dev/null
  kill "$wp" 2>/dev/null; wait "$wp" 2>/dev/null
}

# Per-host CHECKOUT kernel version.
host_version() {
  local h="$1"
  case "$h" in
    m4)         grep -hoE "KERNEL_VERSION[[:space:]]*=[[:space:]]*'[^']+'" "$M4_DIR/vendor/axona-protocol/src/transport/handshake.js" 2>/dev/null | grep -oE "[0-9]+\.[0-9]+\.[0-9]+" | head -1 ;;
    axona-win)  winw 15 "grep -hoE \"KERNEL_VERSION[[:space:]]*=[[:space:]]*'[^']+'\" $WIN_DIR/vendor/axona-protocol/src/transport/handshake.js 2>/dev/null | grep -oE '[0-9]+\\.[0-9]+\\.[0-9]+' | head -1" | tr -d '[:space:]' ;;
    *)          sshw 15 "$h" "grep -hoE \"KERNEL_VERSION[[:space:]]*=[[:space:]]*'[^']+'\" $REMOTE_DIR/vendor/axona-protocol/src/transport/handshake.js 2>/dev/null | grep -oE '[0-9]+\\.[0-9]+\\.[0-9]+' | head -1" | tr -d '[:space:]' ;;
  esac
}

# Per-host live relay pids, one per line.
host_pids() {
  local h="$1"
  case "$h" in
    m4)
      local p
      for p in $(pgrep -f "src/index.js" 2>/dev/null); do
        [ "$(basename "$(ps -p "$p" -o comm= 2>/dev/null)" 2>/dev/null)" = node ] && echo "$p"
      done ;;
    m1)         sshw 15 m1 'for p in $(pgrep -f "src/index.js"); do [ "$(basename "$(ps -p $p -o comm=)")" = node ] && echo $p; done' | tr -d '\r' ;;
    axona-linux) sshw 15 axona-linux 'for p in $(pgrep -f "src/index.js"); do case "$(readlink /proc/$p/exe 2>/dev/null)" in *node*) echo $p;; esac; done' | tr -d '\r' ;;
    axona-win)  winw 15 "RELAY_REGION=${REGION:-eagle} BRIDGE_URL=${BRIDGE:-} $WR pids" | tr -d '\r' ;;
  esac
}

# ARMED pids: the relay's UNCONDITIONAL startup-ready attestation (Aster 017bae90
# rule 5) — src/index.js writes relay-logs/disc-relay-<pid>.jsonl with an {ev:'armed',
# kv, latTrace:1, self, pid, startNonce} record BEFORE any traffic. Activity-independent
# (kills the quiet-relay false-refusal) and CROSS-PLATFORM including Windows (kills the
# structural-assumption loophole — no launch-arg trust, no env-read). A live pid is
# ARMED iff its disc file carries an armed record with kv==EXPECT and latTrace==1.
# Requires the fleet rolled to the attesting relay (src/index.js) — a fleet on the
# pre-attestation relay shows 0 armed and the gate refuses, correctly demanding the roll.
host_armed_pids() {
  local h="$1"
  case "$h" in
    m4)          for f in "$M4_DIR"/relay-logs/disc-relay-*.jsonl; do [ -f "$f" ] || continue; grep -q '"ev":"armed"' "$f" 2>/dev/null && grep -q "\"kv\":\"$EXPECT\"" "$f" 2>/dev/null && grep -q '"latTrace":1' "$f" 2>/dev/null && basename "$f" | grep -oE '[0-9]+'; done 2>/dev/null ;;
    axona-win)   winw 20 "for f in $WIN_DIR/relay-logs/disc-relay-*.jsonl; do [ -f \"\$f\" ] || continue; grep -q '\"ev\":\"armed\"' \"\$f\" && grep -q '\"kv\":\"$EXPECT\"' \"\$f\" && grep -q '\"latTrace\":1' \"\$f\" && basename \"\$f\" | grep -oE '[0-9]+'; done 2>/dev/null" | tr -d '\r' ;;
    *)           sshw 20 "$h" "for f in $REMOTE_DIR/relay-logs/disc-relay-*.jsonl; do [ -f \"\$f\" ] || continue; grep -q '\"ev\":\"armed\"' \"\$f\" && grep -q '\"kv\":\"$EXPECT\"' \"\$f\" && grep -q '\"latTrace\":1' \"\$f\" && basename \"\$f\" | grep -oE '[0-9]+'; done 2>/dev/null" | tr -d '\r' ;;
  esac
}

echo "== pre-arm coverage gate — expect kernel $EXPECT, hosts: $HOSTS =="

deadline=$(( $(date +%s) + WARM_S ))
attempt=0
while :; do
  attempt=$((attempt+1))
  total_live=0 total_unc=0 bad_ver=0
  report=""
  for h in $HOSTS; do
    ver="$(host_version "$h")"
    vok="ok"; [ "$ver" = "$EXPECT" ] || { vok="MISMATCH($ver)"; bad_ver=$((bad_ver+1)); }

    # live pids (unique)
    live="$(host_pids "$h" | grep -E '^[0-9]+$' | sort -u)"
    lc=$(printf '%s\n' "$live" | grep -cE '^[0-9]+$')
    # armed pids (startup-ready attestation record; uniform cross-platform)
    method="attest"
    traced="$(host_armed_pids "$h" | grep -E '^[0-9]+$' | sort -u)"
    # uncovered = live not armed
    unc="$(comm -23 <(printf '%s\n' "$live" | grep -E '^[0-9]+$' | sort -u) \
                    <(printf '%s\n' "$traced" | grep -E '^[0-9]+$' | sort -u))"
    uc=$(printf '%s\n' "$unc" | grep -cE '^[0-9]+$')

    total_live=$((total_live+lc)); total_unc=$((total_unc+uc))
    covd=$((lc-uc))
    line="  $h: kernel=$vok  live=$lc armed=$covd unarmed=$uc  ($method)"
    [ "$uc" -gt 0 ] && line="$line  [UNARMED pids: $(printf '%s ' $unc)]"
    report="$report$line"$'\n'
  done

  printf '%s' "$report"
  if [ "$total_unc" -eq 0 ] && [ "$bad_ver" -eq 0 ] && [ "$total_live" -gt 0 ]; then
    echo "== VERDICT: ARM ADMITTED — $total_live live relays, all traced under $EXPECT =="
    exit 0
  fi
  now=$(date +%s)
  if [ "$now" -ge "$deadline" ]; then break; fi
  echo "   … not uniform yet (live=$total_live uncovered=$total_unc ver-mismatch=$bad_ver); re-polling in ${POLL_S}s (warm until $((deadline-now))s left)"
  sleep "$POLL_S"
done

echo "== VERDICT: ARM REFUSED — coverage is not uniform after ${WARM_S}s warm =="
echo "   live=$total_live uncovered=$total_unc kernel-mismatch-hosts=$bad_ver"
echo "   An untraced or stale relay makes tx/rx asymmetric and voids the per-hop read (2026-09-01)."
echo "   Fix: roll the offending host(s) to $EXPECT with LAT_TRACE=1 and restart every relay, then re-run."
if [ "${COVERAGE_OVERRIDE:-0}" = 1 ]; then
  echo "== COVERAGE_OVERRIDE=1 — admitting a KNOWN partial-coverage arm anyway. The per-hop read WILL be scoped/void; this is on the operator. =="
  exit 0
fi
exit 1
