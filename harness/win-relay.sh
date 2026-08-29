#!/usr/bin/env bash
# =============================================================================
# harness/win-relay.sh — RELAY-AWARE census/start/stop for the Windows fleet.
# Runs NATIVELY on axona-win (git-bash), invoked as:
#     ssh axona-win '"C:\Program Files\Git\bin\bash.exe" harness/win-relay.sh <cmd>'
#
# Why a file and not an inline ssh command: the WMI filter needs
# Name='node.exe' with its quotes intact, and passing that through
# ssh→cmd→git-bash strips a quoting layer every hop (the v2 "Invalid query").
# On disk, bash reads it verbatim — one layer, done.
#
# RELAY-AWARE: filters node.exe by command line (src\index.js) so the harness
# sidecars that also run as node.exe are NEVER counted or killed. start uses a
# one-shot schtasks task — the only start that survives an ssh-initiated launch
# on this box (own logon session; a plain nohup dies with the ssh channel).
# =============================================================================
set -uo pipefail
cd /c/Users/david/github/axona-relay || exit 1
REGION="${RELAY_REGION:-eagle}"
BRIDGE="${BRIDGE_URL:-wss://testnet.axona.net}"

# node.exe processes whose command line is the relay entrypoint, oldest first.
RELAY_FILTER="Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" | Where-Object { \$_.CommandLine -match 'src.index.js' }"

relay_pids() {   # ProcessIds, oldest (earliest CreationDate) first, one per line
  powershell -NoProfile -Command "$RELAY_FILTER | Sort-Object CreationDate | ForEach-Object { \$_.ProcessId }" | tr -d '\r'
}

case "${1:-}" in
  census)
    powershell -NoProfile -Command "@($RELAY_FILTER).Count" | tr -d '[:space:]'
    ;;
  start)
    mkdir -p relay-logs
    ts=$(date +%s)
    # Arm B (ARM_STACK=1): the four connection-quality env vars live in the
    # committed launcher .cmd, NOT inlined here — schtasks /TR caps at 261 chars
    # and the four RELAY_* names overflow it (the v1 "/TR cannot be more than
    # 261 characters" abort that stranded a win slot). /TR now just calls the
    # launcher with a short arg list: region, bridge, arm flag, logfile.
    arm="${ARM_STACK:-0}"
    launch='C:\Users\david\github\axona-relay\harness\win-relay-launch.cmd'
    tr="cmd /c $launch $REGION $BRIDGE $arm relay-logs\\relay-churn-$ts.log"
    powershell -NoProfile -Command "schtasks /Create /F /TN axona-relay-churn-$ts /SC ONCE /ST 00:00 /TR '$tr' | Out-Null; schtasks /Run /TN axona-relay-churn-$ts | Out-Null"
    echo started
    ;;
  stop)   # kill the OLDEST relay (first by CreationDate)
    p=$(relay_pids | head -1 | tr -cd '0-9')
    [ -n "$p" ] && powershell -NoProfile -Command "Stop-Process -Id $p -Force"
    echo stopped
    ;;
  *) echo "usage: win-relay.sh census|start|stop" >&2; exit 1 ;;
esac
