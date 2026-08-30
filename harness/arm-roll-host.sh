#!/usr/bin/env bash
# =============================================================================
# harness/arm-roll-host.sh — roll ONE host's whole relay set to stack-ON by
# looping the proven, per-host-census-correct relay-churn armed roll. Each
# relay-churn relay-roll starts an ARMED replacement (ARM_STACK=1) and stops
# the OLDEST relay (a stack-off one, until none remain) — heir-preserving, so
# the host census never dips below EXPECT. roll-fleet.sh is NOT used here: its
# `pgrep -f | minus caffeinate` census double-counts on Linux (sees 10 for 5
# node relays), while relay-churn.sh carries the correct node-aware census per
# host. Run DETACHED (nohup) so a foreground timeout can never interrupt a roll
# mid-flight and strand an unpaired start (the 5→6 drift seen when a live loop
# was killed between start_one and stop_one).
#
#   nohup bash harness/arm-roll-host.sh <host> <expect> > arm-<host>.log 2>&1 &
#
# Verification is separate (each live relay's process environ / boot banner).
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
HOST="${1:?usage: arm-roll-host.sh <host> <expect>}"
EXPECT="${2:?expect count required}"
REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://testnet.axona.net}"

echo "[$HOST] arm-roll START target=$EXPECT $(date -u +%H:%M:%SZ)"
for i in $(seq 1 "$EXPECT"); do
  echo "[$HOST] --- armed roll $i/$EXPECT $(date -u +%H:%M:%SZ) ---"
  c=$(ARM_STACK=1 ARM_FIX="${ARM_FIX:-1}" REGION="$REGION" BRIDGE="$BRIDGE" bash harness/relay-churn.sh relay-roll "$HOST" 20000)
  echo "[$HOST]   post-roll census=$c (want $EXPECT)"
done
echo "[$HOST] arm-roll DONE $(date -u +%H:%M:%SZ)"
