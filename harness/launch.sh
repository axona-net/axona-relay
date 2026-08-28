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

COMMON="NODES=$NODES SEED=$SEED DURATION_MS=$DURATION_MS OPEN_N=$OPEN_N OWNED_N=$OWNED_N REGION=$REGION BRIDGE=$BRIDGE LEDGER_DIR=harness/results"

launch_local() {  # peerIdx...
  for i in "$@"; do
    env HOST=m4 OS=darwin PEER_IDX="$i" $COMMON node harness/sidecar.mjs \
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
    remote "$host" "cd ~/Documents/claude/axona-relay && nohup env PATH=\"$prefix:\$PATH\" HOST=$host OS=$os PEER_IDX=$i $COMMON node harness/sidecar.mjs > harness/results/sidecar-$SEED-$i.out 2>&1 & echo \"  $host peer $i launched\""
  done
}

launch_win() {  # peerIdx...
  for i in "$@"; do
    remote axona-win "\"C:\\Program Files\\Git\\bin\\bash.exe\" -lc 'cd /c/Users/david/github/axona-relay && nohup env HOST=axona-win OS=win32 PEER_IDX=$i $COMMON node harness/sidecar.mjs > harness/results/sidecar-$SEED-$i.out 2>&1 & echo \"  axona-win peer $i launched\"'"
  done
}

echo "── updating remote checkouts"
remote m1 'cd ~/Documents/claude/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1'
remote axona-linux 'cd ~/Documents/claude/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1'
remote axona-win "\"C:\\Program Files\\Git\\bin\\bash.exe\" -lc 'cd /c/Users/david/github/axona-relay && git fetch -q origin && git reset -q --hard origin/testnet && git log --oneline -1'"

echo "── launching $NODES sidecars (seed $SEED, $(( DURATION_MS / 60000 ))min window)"
launch_local 0 1
launch_ssh_unix m1 "/opt/homebrew/Cellar/node/26.6.0/bin:/opt/homebrew/bin" darwin 2 3
launch_ssh_unix axona-linux "\$HOME/bin" linux 4 5
launch_win 6 7

echo "── churn driver (validation plan: restart local peer 0 mid-window)"
SEED=$SEED HOST=m4 LEDGER_DIR=$RESULTS \
  PLAN="[{\"atMs\":$(( DURATION_MS / 2 )),\"kind\":\"sidecar-restart\",\"peerIdx\":0,\"env\":{\"HOST\":\"m4\",\"OS\":\"darwin\",\"NODES\":\"$NODES\",\"SEED\":\"$SEED\",\"DURATION_MS\":\"$(( DURATION_MS / 2 - 20000 ))\",\"OPEN_N\":\"$OPEN_N\",\"OWNED_N\":\"$OWNED_N\",\"REGION\":\"$REGION\",\"BRIDGE\":\"$BRIDGE\",\"LEDGER_DIR\":\"harness/results\"}}]" \
  node harness/churn.mjs > "$RESULTS/churn-$SEED.out" 2>&1 &
echo "  churn driver pid $!"

SETTLE=$(( DURATION_MS / 1000 + 120 ))
echo "── waiting ${SETTLE}s (window + settle)"
sleep "$SETTLE"

echo "── collecting remote ledgers"
scp -q "m1:~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" "$RESULTS/" 2>/dev/null || echo "  m1: none collected"
scp -q "axona-linux:~/Documents/claude/axona-relay/harness/results/sidecar-$SEED-*.jsonl" "$RESULTS/" 2>/dev/null || echo "  axona-linux: none collected"
scp -q "axona-win:C:/Users/david/github/axona-relay/harness/results/sidecar-$SEED-*.jsonl" "$RESULTS/" 2>/dev/null || echo "  axona-win: none collected"
ls "$RESULTS"/sidecar-$SEED-*.jsonl 2>/dev/null | sed 's/^/  /'

echo "── analyzer"
node harness/analyze.mjs --dir "$RESULTS" --seed "$SEED" --nodes "$NODES" \
  --open-n "$OPEN_N" --owned-n "$OWNED_N" --duration-ms "$DURATION_MS" \
  --offsets '{"m4":0,"m1":108,"axona-linux":102,"axona-win":104}' \
  --out "$RESULTS/findings-$SEED.jsonl"
echo "── done (analyzer exit $?)"
