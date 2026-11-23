#!/usr/bin/env bash
# =============================================================================
# relay-dump.sh — ASK EVERY RELAY OF THIS CHECKOUT FOR ITS DUMP LINES, PRINT THEM.
#
# READ-ONLY for the relays. It restarts nothing, changes no configuration and
# SIGNALS NO PROCESS. It asks the way `windows/relayctl.ps1 dump` asks: it
# rewrites ONE file, dump-requests/request, in this checkout. Every relay
# started from this checkout (0.205.0 and later) polls that file's modification
# time every 2 s and answers with two log lines, `health-dump` and `peer-dump`
# (src/dumprequest.js). Then it prints what each relay wrote to its own log
# AFTER the request:
#
#   relay-20261109-1538-3 health-dump {...}
#   relay-20261109-1538-3 peer-dump {...}
#   relay-20261109-1538-4 no-dump            (a relay that wrote neither line)
#   pid-4711 skipped (not running from this checkout)
#   SUMMARY dumps health=8/8 peer=8/8 waited=5s            (+ ` skipped=1`)
#
# WHY A FILE AND NOT SIGUSR1 (Aster cd4d626b, f3b80b9a; 2026-11-23). The first
# cut sent SIGUSR1 to the pids relay-census.sh names. That census is host-wide
# and matches a command line, a pid is a number that can be reused between the
# look and the signal, and SIGUSR1 to a node process with no handler for it
# starts that process's inspector. No test on a pid closes all three. The
# request file is addressed to nobody: only a process that armed the watcher on
# THIS checkout's directory reads it, so the asker needs no process identity.
# The census is still used, for ONE thing: finding which log files to read.
#
# WHAT IT IS NOT.
#   - Not a log tail. It notes each log's length BEFORE the request and reads
#     only the bytes after it, so an old dump line is never printed as current.
#   - It writes ONE file, dump-requests/request, holding a UTC time and nothing
#     else. A peer-dump line carries node ids; they go to stdout and nowhere else.
#   - A relay older than 0.205.0 on mac or linux does not watch the file and
#     prints `no-dump`.
#   - Not for Windows (use relayctl.ps1 dump) and not for the droplets (systemd
#     units log to journald, which has no byte offset).
#
#   bash relay-dump.sh            # every relay of this checkout
#   DUMP_WAIT_SEC=8 bash relay-dump.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")" || exit 2
WAIT="${DUMP_WAIT_SEC:-5}"
case "$WAIT" in ''|*[!0-9]*) echo "relay-dump: DUMP_WAIT_SEC must be whole seconds" >&2; exit 2 ;; esac
case "$(uname -s 2>/dev/null)" in MINGW*|MSYS*|CYGWIN*) echo "relay-dump: not for Windows; run windows/relayctl.ps1 dump" >&2; exit 2 ;; esac

# The log a process holds open as stdout. Same resolution as relay-census.sh
# --kernels: /proc on Linux, lsof on macOS. Empty when it cannot be resolved.
log_of() {
  if [ -r "/proc/$1/fd/1" ]; then readlink "/proc/$1/fd/1" 2>/dev/null
  else lsof -p "$1" -a -d 1 -Fn 2>/dev/null | sed -n 's/^n//p' | head -1; fi
}
# Which census pids run FROM THIS CHECKOUT. This decides whose log is READ; it
# decides nothing about who is asked, because nobody is addressed. A pid that
# has exited, or runs anywhere else, is named and skipped.
HERE=$(pwd -P)
cwd_of() {
  if [ -e "/proc/$1/cwd" ]; then readlink "/proc/$1/cwd" 2>/dev/null
  else lsof -a -p "$1" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -1; fi
}
is_ours() { local c; c=$(cwd_of "$1"); [ -n "$c" ] && [ "$c" = "$HERE" ]; }
mtime_of() { stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null || echo 0; }

PIDS=(); LOGS=(); OFFS=(); FOREIGN=()
for pid in $(bash relay-census.sh --pids); do
  case "$pid" in ''|*[!0-9]*) continue ;; esac
  if ! is_ours "$pid"; then FOREIGN+=("$pid"); continue; fi
  log=$(log_of "$pid")
  off=0
  if [ -n "$log" ] && [ -f "$log" ] && [ -r "$log" ]; then off=$(wc -c < "$log" | tr -d ' '); else log=""; fi
  PIDS+=("$pid"); LOGS+=("$log"); OFFS+=("$off")
done
N=${#PIDS[@]}
SKIP=${#FOREIGN[@]}
i=0
while [ "$i" -lt "$SKIP" ]; do echo "pid-${FOREIGN[$i]} skipped (not running from this checkout)"; i=$((i + 1)); done
tail_of() { if [ "$SKIP" -gt 0 ]; then echo " skipped=$SKIP"; fi; }
if [ "$N" -eq 0 ]; then echo "SUMMARY dumps health=0/0 peer=0/0 waited=0s$(tail_of)"; exit 0; fi

# THE REQUEST. A relay answers a CHANGED modification time. Where the filesystem
# keeps whole seconds, a second request inside one second would change nothing,
# so the time is read back and the file written again a second later if needed.
REQ_DIR="dump-requests"; REQ="$REQ_DIR/request"
mkdir -p "$REQ_DIR" 2>/dev/null || { echo "relay-dump: cannot create $REQ_DIR" >&2; exit 2; }
before=$(mtime_of "$REQ")
date -u +%Y-%m-%dT%H:%M:%SZ > "$REQ" || { echo "relay-dump: cannot write $REQ" >&2; exit 2; }
if [ "$(mtime_of "$REQ")" = "$before" ]; then sleep 1; date -u +%Y-%m-%dT%H:%M:%SZ > "$REQ"; fi
sleep "$WAIT"

health=0; peers=0; i=0
while [ "$i" -lt "$N" ]; do
  log="${LOGS[$i]}"
  if [ -z "$log" ]; then echo "pid-${PIDS[$i]} no-dump (log unresolved)"; i=$((i + 1)); continue; fi
  label=$(basename "$log"); label=${label%.log}
  new=$(tail -c "+$(( ${OFFS[$i]} + 1 ))" "$log" 2>/dev/null || true)
  h=$(printf '%s\n' "$new" | grep -o '\] health-dump {.*}' | tail -1); h=${h#\] }
  p=$(printf '%s\n' "$new" | grep -o '\] peer-dump {.*}'   | tail -1); p=${p#\] }
  [ -n "$h" ] && { health=$((health + 1)); echo "$label $h"; }
  [ -n "$p" ] && { peers=$((peers + 1));  echo "$label $p"; }
  [ -z "$h" ] && [ -z "$p" ] && echo "$label no-dump"
  i=$((i + 1))
done
echo "SUMMARY dumps health=$health/$N peer=$peers/$N waited=${WAIT}s$(tail_of)"
exit 0
