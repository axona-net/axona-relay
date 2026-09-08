#!/usr/bin/env bash
# =============================================================================
# win-roll-detached.sh — run windows-roll.sh so it OUTLIVES the ssh session.
#
# Twice on 2026-09-08 a Windows roll driven straight from `ssh … bash
# windows-roll.sh` died part way: once after 3 slots, once after ~13. Each time
# no bash.exe remained on the host while the local ssh sat there hung, so it
# looked like a slow roll and was a dead one — and half a fleet was left on the
# old kernel with heirs already started, over target.
#
# A foreground remote process is hostage to the connection. This detaches the
# roll with nohup, hands back the log path immediately, and lets the caller poll.
# The roll then finishes whether or not anything is still watching.
#
# It also keeps every redirection INSIDE the script. The axona-win ssh shell is
# cmd.exe, which eats `>`, `|` and `&` before bash ever sees them; that is why
# the redirect belongs here and not in the command line that invokes this.
#
#   N=20 EXPECT_KERNEL=4.78.0 REGION=eagle BRIDGE=wss://bridge.axona.net \
#     [INTEGRATE_TIMEOUT=20] bash win-roll-detached.sh
#
# Prints:  STARTED pid=<pid> log=<path>
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"

mkdir -p relay-logs
LOG="relay-logs/winroll-$(date +%Y%m%d-%H%M%S).out"

# Env is already exported into this process by the caller's `VAR=… bash …`
# prefix; nohup inherits it. Detached from the terminal AND from stdin so a
# closing ssh channel cannot take it down.
nohup bash windows-roll.sh > "$LOG" 2>&1 < /dev/null &
pid=$!
disown "$pid" 2>/dev/null || true

echo "STARTED pid=$pid log=$LOG"
