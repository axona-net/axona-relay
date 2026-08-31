#!/usr/bin/env bash
# =============================================================================
# harness/capacity-sample.sh — per-host capacity sampler for the kNear re-soak
# (Aster condition 3, 2026-08-31): a density change must be judged with its cost,
# not on completeness/latency alone. Every INTERVAL_S this appends one JSONL row
# per host with the relay process count, summed RSS (MB), and summed %CPU for the
# relay processes (node src/index.js), so kNear 5/10/20 can be compared at equal
# capacity — does more successor maintenance cost materially more memory/CPU?
#
# SCOPE: OS-observable capacity only. Kernel-internal counters Aster also named —
# lookup RPCs, fanout bytes, synaptome-maintenance traffic — are NOT here; they
# need kernel-side counters emitted to the relay-side disc sink (src/index.js
# LAT_TRACE), a flagged follow-up. This samples what the OS can see today.
#
#   SEED=31 INTERVAL_S=30 nohup bash harness/capacity-sample.sh > /dev/null 2>&1 &
# Writes harness/results/capacity-<seed>.jsonl until killed.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
SEED="${SEED:?SEED required}"
INTERVAL_S="${INTERVAL_S:-30}"
OUT="harness/results/capacity-$SEED.jsonl"
mkdir -p harness/results

# unix host: reuse the proven census (pgrep src/index.js, comm=node), summing
# RSS(kB)→MB and %CPU per relay pid. Robust across macOS/Linux ps quirks.
unix_sample() {  # <host|local>
  local host="$1"
  # match node procs on macOS (comm=…/node) AND Linux (comm=MainThread, node's
  # main-thread name); the mac caffeinate twin (comm=caffeinate) is excluded.
  local script='for p in $(pgrep -f "src/index.js" 2>/dev/null); do ps -p $p -o rss=,pcpu=,comm=; done | awk '\''$3 ~ /node|MainThread/ {rss+=$1; cpu+=$2; n++} END {printf "%d %.1f %.1f", n+0, rss/1024, cpu+0}'\'''
  if [ "$host" = local ]; then bash -c "$script"; else ssh -o ConnectTimeout=10 "$host" "$script" 2>/dev/null; fi
}
# windows: count + working-set MB (CPU% is not cheaply summable over ssh; omit).
win_sample() {
  printf '%s\n' 'powershell -NoProfile -Command "$p=Get-CimInstance Win32_Process -Filter \"Name=\x27node.exe\x27\" | Where-Object { $_.CommandLine -match \x27src.index.js\x27 }; $n=@($p).Count; $m=([math]::Round((($p | Measure-Object WorkingSetSize -Sum).Sum)/1MB,1)); Write-Output \"$n $m -1\""' \
    | ssh -o ConnectTimeout=12 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' 2>/dev/null | tr -d '\r'
}

row() {  # <host> "<n rssMB cpu>"
  local host="$1"; set -- $2
  local n="${1:-0}" rss="${2:-0}" cpu="${3:-0}"
  printf '{"wall":%s,"host":"%s","relays":%s,"rssMB":%s,"cpuPct":%s}\n' \
    "$(node -e 'console.log(Date.now())')" "$host" "${n:-0}" "${rss:-0}" "${cpu:--1}" >> "$OUT"
}

echo "capacity-sample START seed=$SEED interval=${INTERVAL_S}s → $OUT" >&2
while :; do
  row m4          "$(unix_sample local)"
  row m1          "$(unix_sample m1)"
  row axona-linux "$(unix_sample axona-linux)"
  row axona-win   "$(win_sample)"
  sleep "$INTERVAL_S"
done
