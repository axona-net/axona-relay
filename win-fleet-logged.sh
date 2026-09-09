#!/usr/bin/env bash
# =============================================================================
# win-fleet-logged.sh — COLD START the Windows fleet with its output VISIBLE.
#
# The exact counterpart of win-roll-logged.sh, and it exists for the same
# reason: a Windows fleet operation's stdout goes back through ssh, which
# buffers it until the process exits, so a working start and a dead one look
# identical from the laptop. Three Windows ROLLS were misdiagnosed that way
# before win-roll-logged.sh existed; the cold-start path had no equivalent, so
# the same blindness was one command away.
#
# The redirect has to live INSIDE a script because the axona-win ssh shell is
# cmd.exe, which consumes `>` and `|` before bash ever sees them.
#
# Prints the log path FIRST so a second connection can tail it while the start
# runs — a 20-relay cold start takes many minutes and must be observable
# throughout, not only at the end.
#
#   N=20 EXPECT_KERNEL=4.79.0 REGION=eagle BRIDGE=wss://bridge.axona.net \
#     ADVANCE_CAP=90 JITTER=0 bash win-fleet-logged.sh
#
# BRIDGE IS NOT OPTIONAL IN PRACTICE: windows-fleet.sh defaults it to TESTNET.
# A prod cold start that forgets it silently builds a fleet on the wrong
# network — every relay healthy, every relay useless.
# =============================================================================
cd "$(dirname "$0")"
mkdir -p relay-logs
LOG="relay-logs/winfleet-$(date +%Y%m%d-%H%M%S).out"
echo "LOGGING_TO=$LOG"
echo "BRIDGE=${BRIDGE:-<unset — windows-fleet.sh will default to TESTNET>}"
bash windows-fleet.sh > "$LOG" 2>&1
rc=$?
echo "EXIT=$rc"
tail -8 "$LOG"
exit $rc
