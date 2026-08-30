#!/usr/bin/env bash
# =============================================================================
# harness/launch.sh — the multi-host sidecar launcher.
#
# Starts K sidecars per host across the four fleet hosts, waits out the
# window, collects every ledger back to the M4, and runs the analyzer.
# Encodes each host's hard-won quirks in ONE place:
#   m1           node only at /opt/homebrew/Cellar/node/*/bin (no login PATH)
#   axona-linux  node in ~/bin (tarball install)
#   axona-win    git-bash; launch ssh may hang holding children's channel —
#                every remote launch runs under a local watchdog kill
#
#   SEED=7 NODES=8 DURATION_MS=600000 OPEN_N=4 OWNED_N=2 bash harness/launch.sh
#
# Peer allocation (NODES=8): m4 0-1 · m1 2-3 · axona-linux 4-5 · axona-win 6-7.
# The build validation runs NO relay churn; the churn driver's validation plan
# restarts one LOCAL sidecar mid-window to exercise replay.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."      # repo root

SEED="${SEED:?SEED required}"
NODES="${NODES:-8}"
DURATION_MS="${DURATION_MS:-600000}"
OPEN_N="${OPEN_N:-4}"
OWNED_N="${OWNED_N:-2}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"
RESULTS=harness/results
mkdir -p "$RESULTS"

COMMON="NODES=$NODES SEED=$SEED DURATION_MS=$DURATION_MS OPEN_N=$OPEN_N OWNED_N=$OWNED_N REGION=$REGION BRIDGE=$BRIDGE LEDGER_DIR=harness/results HEAD_SWEEP_MS=${HEAD_SWEEP_MS:-60000} READINESS_MS=${READINESS_MS:-15000} COORD_WAIT_MS=${COORD_WAIT_MS:-120000} TRACE=${TRACE:-0} LAT_TRACE=${LAT_TRACE:-0} SUB_TERMINAL_VERIFY=${SUB_TERMINAL_VERIFY:-1} FINDK_SKIP_DEAD=${FINDK_SKIP_DEAD:-1}"

launch_local() {  # peerIdx...
  for i in "$@"; do
    env HOST=m4 OS=darwin PEER_IDX="$i" $COMMON node harness/sidecar.mjs --peer "$i" \
      > "$RESULTS/sidecar-$SEED-$i.out" 2>&1 &
    echo "  m4 peer $i pid $!"
  done
}

remote() {  # host cmd — with a local watchdog so a held channel can't wedge us
  local host="$1"; shift
  ( ssh -o ConnectTimeout=15 "$host" "$@" & local sp=$!
    ( sleep 40; kill "$sp" 2>/dev/null ) & wait "$sp" 2>/dev/null )
}

launch_ssh_unix() {  # host node_prefix os peerIdx...
  local host="$1" prefix="$2" os="$3"; shift 3
  for i in "$@"; do
    remote "$host" "cd ~/Documents/claude/axona-relay && nohup env PATH=\"$prefix:\$PATH\" HOST=$host OS=$os PEER_IDX=$i $COMMON node harness/sidecar.mjs --peer $i > harness/results/sidecar-$SEED-$i.out 2>&1 & echo \"  $host peer $i launched\""
  done
}

launch_win() {  # peerIdx...
  # Windows quoting is the graveyard: ssh hands the command to cmd.exe FIRST,
  # which eats nested quotes before git-bash sees them (the seed-7 run's
  # "system cannot find the path specified"). Ship the payload as a SCRIPT
  # over stdin instead — bash -s reads it verbatim, no cmd parsing at all.
  # FOREGROUND ssh: it reads the script from stdin, and a backgrounded job's
  # stdin is /dev/null — the seed-8 launches read EOF and did nothing. The
  # remote bash exits after nohup-backgrounding node (all fds redirected to
  # files), so the channel closes promptly; ConnectTimeout guards the dial.
  # One-shot scheduled task per peer — the ONLY launch proven to survive ssh
  # disconnect on this box (schtasks runs in its own logon session; every
  # direct shape, bash and Start-Process alike, died with the session).
  for i in "$@"; do
    remote axona-win "powershell -ExecutionPolicy Bypass -File C:\\Users\\david\\github\\axona-relay\\harness\\win-spawn.ps1 -Peer $i -Seed $SEED -Nodes $NODES -DurationMs $DURATION_MS -OpenN $OPEN_N -OwnedN $OWNED_N -Region $REGION -Bridge $BRIDGE"
  done
}

echo "── updating remote checkouts"
remote m1 'cd ~/Documents/claude/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1'
remote axona-linux 'cd ~/Documents/claude/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1'
echo 'cd /c/Users/david/github/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1' \
  | ssh -o ConnectTimeout=15 axona-win '"C:\Program Files\Git\bin\bash.exe" -s'

echo "── launching $NODES sidecars (seed $SEED, $(( DURATION_MS / 60000 ))min window)"
launch_local 0 1
launch_ssh_unix m1 "/opt/homebrew/Cellar/node/26.6.0/bin:/opt/homebrew/bin" darwin 2 3
launch_ssh_unix axona-linux "\$HOME/bin" linux 4 5
# Windows sidecars carry neither LAT_TRACE nor the fix flags (win-spawn.ps1 forwards
# a fixed arg list), so they cannot join an armed+traced measurement. Default them
# OFF: the 20 win RELAYS stay in the armed fleet under test, only the client
# workload is unix (uniform armed+traced). WIN_SIDECARS=1 restores peers 6-7.
if [ "${WIN_SIDECARS:-0}" = 1 ]; then launch_win 6 7; fi

# Churn: ARM_RELAY=1 runs the §6 relay-churn schedule (Arm A/B); otherwise the
# validation plan (one local sidecar restart). The relay actions are gated
# inside churn.mjs on ARM_RELAY too — belt and braces.
if [ "${NO_CHURN:-0}" = "1" ]; then
  echo "── no churn (probe mode: pure pub/sub on the stable fleet)"
elif [ "${ARM_RELAY:-0}" = "1" ]; then
  echo "── churn driver: §6 RELAY CHURN SCHEDULE (Arm run, ARM_RELAY=1)"
  ARM_PLAN=$(node harness/gen-arm-plan.mjs "$DURATION_MS")
  # ARM_STACK flows through churn.mjs → relay-churn.sh so every churn-replacement
  # relay comes up stack-ON (Arm B). In Arm A it is 0 and replacements are
  # byte-identical stack-off relays. execSync inherits this env into relay-churn.sh.
  ARM_RELAY=1 ARM_STACK=${ARM_STACK:-0} ARM_FIX=${ARM_FIX:-1} SEED=$SEED HOST=m4 REGION=$REGION BRIDGE=$BRIDGE LEDGER_DIR=$RESULTS \
    PLAN="$ARM_PLAN" node harness/churn.mjs > "$RESULTS/churn-$SEED.out" 2>&1 &
  echo "  churn driver pid $!"
else
  echo "── churn driver (validation plan: restart local peer 0 mid-window)"
  SEED=$SEED HOST=m4 LEDGER_DIR=$RESULTS \
    PLAN="[{\"atMs\":$(( DURATION_MS / 2 )),\"kind\":\"sidecar-restart\",\"peerIdx\":0,\"env\":{\"HOST\":\"m4\",\"OS\":\"darwin\",\"NODES\":\"$NODES\",\"SEED\":\"$SEED\",\"DURATION_MS\":\"$(( DURATION_MS / 2 - 20000 ))\",\"OPEN_N\":\"$OPEN_N\",\"OWNED_N\":\"$OWNED_N\",\"REGION\":\"$REGION\",\"BRIDGE\":\"$BRIDGE\",\"LEDGER_DIR\":\"harness/results\"}}]" \
    node harness/churn.mjs > "$RESULTS/churn-$SEED.out" 2>&1 &
  echo "  churn driver pid $!"
fi

SETTLE=$(( DURATION_MS / 1000 + 120 ))
echo "── waiting ${SETTLE}s (window + settle)"
sleep "$SETTLE"

echo "── collecting remote ledgers"
scp -q "m1:~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" "$RESULTS/" 2>/dev/null || echo "  m1: none collected"
scp -q "axona-linux:~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" "$RESULTS/" 2>/dev/null || echo "  axona-linux: none collected"
# Windows collection by ssh-cat — scp chokes on the drive-letter colon.
for i in $(seq 0 $(( NODES - 1 ))); do
  f="sidecar-$SEED-$i.jsonl"
  [ -f "$RESULTS/$f" ] && continue
  printf 'cat /c/Users/david/github/axona-relay/harness/results/%s 2>/dev/null\n' "$f" \
    | ssh -o ConnectTimeout=15 axona-win '"C:\Program Files\Git\bin\bash.exe" -s' > "$RESULTS/$f" 2>/dev/null
  [ -s "$RESULTS/$f" ] || rm -f "$RESULTS/$f"
done
ls "$RESULTS"/sidecar-$SEED-*.jsonl 2>/dev/null | sed 's/^/  /'

echo "── analyzer"
node harness/analyze.mjs --dir "$RESULTS" --seed "$SEED" --nodes "$NODES" \
  --open-n "$OPEN_N" --owned-n "$OWNED_N" --duration-ms "$DURATION_MS" \
  --offsets '{"m4":0,"m1":136,"axona-linux":182,"axona-win":89}' \
  --out "$RESULTS/findings-$SEED.jsonl" \
  --summary-out "$RESULTS/summary-$SEED.json"
echo "── done (analyzer exit $?)"
