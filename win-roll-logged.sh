#!/usr/bin/env bash
# =============================================================================
# win-roll-logged.sh — run windows-roll.sh with its output VISIBLE.
#
# The Windows roll was diagnosed as "died", then "stalled", then "hung at slot 3"
# across three attempts on 2026-09-08 — and in none of them had anyone read what
# the roll actually said. Its stdout goes back through ssh, which buffers it
# until the process exits, so a roll that is working looks identical to one that
# is dead. All three diagnoses were guesses about a process whose own words were
# sitting unread.
#
# The redirect has to live INSIDE a script because the axona-win ssh shell is
# cmd.exe, which consumes `>` and `|` before bash ever sees them. That is why
# this file exists rather than a `> log` on the command line.
#
# Prints the log path FIRST, so a caller can tail it from a second connection
# while the roll runs.
#
#   N=23 EXPECT_KERNEL=4.78.0 REGION=eagle BRIDGE=wss://bridge.axona.net \
#     ADVANCE_CAP=20 READY_TIMEOUT=60 JITTER=0 bash win-roll-logged.sh
# =============================================================================
cd "$(dirname "$0")"
mkdir -p relay-logs
LOG="relay-logs/winroll-$(date +%Y%m%d-%H%M%S).out"
echo "LOGGING_TO=$LOG"
bash windows-roll.sh > "$LOG" 2>&1
rc=$?
echo "EXIT=$rc"
tail -5 "$LOG"
exit $rc
