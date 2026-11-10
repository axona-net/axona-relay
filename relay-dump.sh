#!/usr/bin/env bash
# =============================================================================
# relay-dump.sh — ASK EVERY RELAY ON THIS HOST FOR ITS DUMP LINES, PRINT THEM.
#
# READ-ONLY. It restarts nothing and changes no configuration. It sends each
# relay SIGUSR1, the relay's own diagnostic signal (handled since 0.161.0; since
# 0.198.0 the relay answers with two log lines, `health-dump` and `peer-dump`),
# then prints what each relay wrote to its own log AFTER the signal:
#
#   relay-20261109-1538-3 health-dump {...}
#   relay-20261109-1538-3 peer-dump {...}
#   relay-20261109-1538-4 no-dump            (a relay that wrote neither line)
#   SUMMARY dumps health=8/8 peer=8/8 waited=3s
#
# Same line shapes as `windows/relayctl.ps1 dump`, so one reader serves both.
#
# WHAT IT IS NOT.
#   - Not a process census of its own: the pid list is `relay-census.sh --pids`,
#     the single definition. A hand-written pgrep here would be the fourth copy.
#   - Not a log tail. It notes each log's length BEFORE signalling and reads
#     only the bytes after it, so an old dump line is never printed as current.
#   - It writes NO file. A peer-dump line carries node ids; they go to stdout
#     and nowhere else.
#   - Not for Windows (no SIGUSR1 there; use relayctl.ps1 dump) and not for the
#     droplets (systemd units log to journald, which has no byte offset).
#
#   bash relay-dump.sh            # every relay on this host
#   DUMP_WAIT_SEC=5 bash relay-dump.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")" || exit 2
WAIT="${DUMP_WAIT_SEC:-3}"
case "$WAIT" in ''|*[!0-9]*) echo "relay-dump: DUMP_WAIT_SEC must be whole seconds" >&2; exit 2 ;; esac
case "$(uname -s 2>/dev/null)" in MINGW*|MSYS*|CYGWIN*) echo "relay-dump: not for Windows; run windows/relayctl.ps1 dump" >&2; exit 2 ;; esac

# The log a relay holds open as stdout. Same resolution as relay-census.sh
# --kernels: /proc on Linux, lsof on macOS. Empty when it cannot be resolved.
log_of() {
  if [ -r "/proc/$1/fd/1" ]; then readlink "/proc/$1/fd/1" 2>/dev/null
  else lsof -p "$1" -a -d 1 -Fn 2>/dev/null | sed -n 's/^n//p' | head -1; fi
}

PIDS=(); LOGS=(); OFFS=()
for pid in $(bash relay-census.sh --pids); do
  log=$(log_of "$pid")
  off=0
  if [ -n "$log" ] && [ -f "$log" ] && [ -r "$log" ]; then off=$(wc -c < "$log" | tr -d ' '); else log=""; fi
  PIDS+=("$pid"); LOGS+=("$log"); OFFS+=("$off")
done
N=${#PIDS[@]}
if [ "$N" -eq 0 ]; then echo "SUMMARY dumps health=0/0 peer=0/0 waited=0s"; exit 0; fi

# Signal only a relay whose log this script can read back. A dump nobody reads
# is a log line written for nothing.
i=0
while [ "$i" -lt "$N" ]; do
  [ -n "${LOGS[$i]}" ] && kill -USR1 "${PIDS[$i]}" 2>/dev/null
  i=$((i + 1))
done
sleep "$WAIT"

health=0; peers=0; i=0
while [ "$i" -lt "$N" ]; do
  log="${LOGS[$i]}"
  if [ -z "$log" ]; then echo "pid-${PIDS[$i]} no-dump (log unresolved, not signalled)"; i=$((i + 1)); continue; fi
  label=$(basename "$log"); label=${label%.log}
  new=$(tail -c "+$(( ${OFFS[$i]} + 1 ))" "$log" 2>/dev/null || true)
  h=$(printf '%s\n' "$new" | grep -o '\] health-dump {.*}' | tail -1); h=${h#\] }
  p=$(printf '%s\n' "$new" | grep -o '\] peer-dump {.*}'   | tail -1); p=${p#\] }
  [ -n "$h" ] && { health=$((health + 1)); echo "$label $h"; }
  [ -n "$p" ] && { peers=$((peers + 1));  echo "$label $p"; }
  [ -z "$h" ] && [ -z "$p" ] && echo "$label no-dump"
  i=$((i + 1))
done
echo "SUMMARY dumps health=$health/$N peer=$peers/$N waited=${WAIT}s"
exit 0
