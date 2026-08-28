#!/usr/bin/env bash
# =============================================================================
# harness/win-spawn.sh — spawn one sidecar on the Windows box, detached.
#
# Exists because process survival on Windows OpenSSH is shape-sensitive: the
# only launch form PROVEN to outlive its ssh session on this box is the
# windows-fleet.sh shape — an ssh -lc invoking a REPO SCRIPT that nohups
# node and exits (its relays are alive a day later). Direct stdin-script
# launches died with their session twice (seed-8, seed-9). This file IS that
# extra layer for the harness.
#
#   bash harness/win-spawn.sh <peerIdx> <seed> <nodes> <durationMs> <openN> <ownedN> <region> <bridge>
# =============================================================================
set -u
cd "$(dirname "$0")/.."
I="$1"; SEED="$2"; NODES="$3"; DURATION_MS="$4"; OPEN_N="$5"; OWNED_N="$6"; REGION="$7"; BRIDGE="$8"
mkdir -p harness/results
RELAY_UNUSED=1 nohup env HOST=axona-win OS=win32 PEER_IDX="$I" NODES="$NODES" SEED="$SEED" \
  DURATION_MS="$DURATION_MS" OPEN_N="$OPEN_N" OWNED_N="$OWNED_N" REGION="$REGION" BRIDGE="$BRIDGE" \
  LEDGER_DIR=harness/results node harness/sidecar.mjs --peer "$I" \
  > "harness/results/sidecar-$SEED-$I.out" 2>&1 &
echo "win-spawn: peer $I pid $!"
