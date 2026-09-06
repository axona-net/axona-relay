#!/usr/bin/env bash
set -euo pipefail
# =============================================================================
# windows-roll.sh — the Windows (git-bash) analog of roll-fleet.sh.
#
# windows-fleet.sh cold-starts only and refuses a live fleet. This rolls a LIVE
# Windows fleet onto a new build with the same discipline as roll-fleet.sh:
# START-THEN-STOP, one slot at a time. A NEW relay is launched and VERIFIED by
# its OWN fresh log banner (expected kernel + bridge socket open) BEFORE one OLD
# relay is retired, so the fleet never drops below N and every departure has a
# live heir already in the mesh.
#
# Windows caveats, stated plainly:
#   - node.exe carries no per-process relay identity; like windows-fleet.sh this
#     treats every node.exe as a fleet relay. Run ONLY on a dedicated relay host.
#   - the old relay is retired with `taskkill //F` (TerminateProcess) — a Windows
#     console node cannot receive a graceful SIGTERM. The start-then-stop ORDER
#     is what protects topics: the heir is integrated before the kill, so the
#     departing relay's roles are already covered.
#
# Usage (from git-bash, ON the Windows relay host):
#   N=20 EXPECT_KERNEL=4.75.2 REGION=eagle BRIDGE=wss://bridge.axona.net bash windows-roll.sh
#   DRY=1 N=20 EXPECT_KERNEL=4.75.2 ... bash windows-roll.sh   # pull+install+load-test, start/kill NOTHING
# =============================================================================
cd "$(dirname "$0")"

REGION="${REGION:-eagle}"
BRIDGE="${BRIDGE:-wss://bridge.axona.net}"    # prod default — a roll acts on a LIVE fleet
BRANCH="${BRANCH:-testnet}"
GRACE="${GRACE:-15}"                          # seconds to wait for a new relay's banner+socket
DRY="${DRY:-0}"

fail(){ echo "✗ ABORT: $*" >&2; exit 1; }
census(){ tasklist //FI "IMAGENAME eq node.exe" 2>/dev/null | grep -c "node.exe" || true; }
pids(){ tasklist //FI "IMAGENAME eq node.exe" //FO CSV //NH 2>/dev/null | sed 's/"//g' | awk -F, '{print $2}' | grep -E '^[0-9]+$' || true; }

N="${N:?N=<live relay count> is REQUIRED — no default (the roll-fleet rule)}"
[ -n "${EXPECT_KERNEL:-}" ] || fail "EXPECT_KERNEL=<x.y.z> is REQUIRED"

# 1. measure + refuse on miscount
BEFORE="$(census)"
[ "$BEFORE" -eq "$N" ] || fail "measured $BEFORE node.exe, you said N=$N — one is wrong, not guessing"
mapfile -t OLD_PIDS < <(pids)
[ "${#OLD_PIDS[@]}" -eq "$N" ] || fail "captured ${#OLD_PIDS[@]} pid(s), expected $N"
echo "→ live fleet $BEFORE node.exe; rolling to kernel $EXPECT_KERNEL  region=$REGION bridge=$BRIDGE"

# 2. vendored-kernel gate + pull (ff-only) + install + LOAD-TEST — no start/kill yet
vend="$(node -p "require('./vendor/axona-protocol/package.json').version")"
[ "$vend" = "$EXPECT_KERNEL" ] || fail "vendored kernel $vend != EXPECT_KERNEL $EXPECT_KERNEL — re-vendor or fix the arg"
git fetch origin "$BRANCH" -q && git pull --ff-only origin "$BRANCH" -q || fail "git pull --ff-only failed (diverged) — resolve by hand"
echo "  head=$(git rev-parse --short HEAD)"
npm install --no-audit --no-fund >/dev/null 2>&1
ndc="$(node -p "require('./node_modules/node-datachannel/package.json').version" 2>/dev/null || echo unknown)"
echo "  node-datachannel=$ndc"
node -e 'const n=require("node-datachannel");const p=new n.PeerConnection("t",{iceServers:[]});p.createDataChannel("x");p.close();console.log("  load-test: 0.33.1 OK on",process.platform,process.arch);setTimeout(()=>process.exit(0),300);' \
  || fail "node-datachannel failed to load — fleet left on the old binary, nothing started or killed"

if [ "$DRY" = "1" ]; then echo "✓ DRY: prepped + load-tested, started/killed nothing"; exit 0; fi

# 3. staged start-then-stop — one slot at a time, heir verified before the kill
GEN="roll-$(date +%Y%m%d-%H%M%S)"
mkdir -p relay-logs
rolled=0
for i in $(seq 1 "$N"); do
  LOG="relay-logs/$GEN-$i.log"
  RELAY_REGION="$REGION" BRIDGE_URL="$BRIDGE" RELAY_TUI=0 nohup node src/index.js >> "$LOG" 2>&1 &
  ok=0
  for _ in $(seq 1 "$GRACE"); do
    if grep -q "kernel v$EXPECT_KERNEL" "$LOG" 2>/dev/null && grep -q "bridge-socket-open" "$LOG" 2>/dev/null; then ok=1; break; fi
    sleep 1
  done
  [ "$ok" -eq 1 ] || fail "slot $i: new relay ($LOG) showed no 'kernel v$EXPECT_KERNEL' + 'bridge-socket-open' within ${GRACE}s — NOT retiring any old relay"
  old="${OLD_PIDS[$((i-1))]}"
  taskkill //PID "$old" //F >/dev/null 2>&1 || echo "  (warn: taskkill pid $old nonzero — end census will catch a discrepancy)"
  rolled=$((rolled+1))
  echo "  ✓ slot $i/$N: heir up (kernel v$EXPECT_KERNEL, bridge open) → retired old pid $old"
done

# 4. verify: exactly N live, and N NEW-generation relays writing their logs.
#    We do NOT re-check the retired old PIDs directly: Windows recycles PIDs, so a
#    fresh relay can be handed a just-freed old PID and a raw-PID check would false-
#    abort. census==N (only reachable if all N kills landed) plus N live new-gen
#    logs is the sound, PID-reuse-immune proof.
[ "$rolled" -eq "$N" ] || fail "rolled $rolled of $N"
sleep 3
AFTER="$(census)"
[ "$AFTER" -eq "$N" ] || fail "post-roll census $AFTER != $N — read the logs, do not assume"
live=0; now=$(date +%s)
for f in "relay-logs/$GEN"-*.log; do m=$(stat -c %Y "$f" 2>/dev/null || echo 0); [ $((now-m)) -lt 120 ] && live=$((live+1)); done
[ "$live" -eq "$N" ] || fail "only $live/$N new-generation relays writing logs — roll incomplete"
echo "✓ WINDOWS ROLL COMPLETE: $N/$N on kernel v$EXPECT_KERNEL (new gen $GEN); node-datachannel $ndc"
