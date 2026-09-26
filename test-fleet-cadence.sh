#!/usr/bin/env bash
# =============================================================================
# test-fleet-cadence.sh — the gate predicates in fleet-cadence.sh, on real
# state lines.
#
# WHY THIS EXISTS. fleet-cadence.sh decides whether a fleet roll advances or
# HALTS, and it had no test. When the bridge degree cap shipped, relays started
# reaching `state=graduated` — healthy, released by the bridge, mesh bound —
# and the OPEN backstop, which tested for `state=open` alone, halted every
# axona-win roll AFTER all 23 slots had already completed. The roll was fine;
# the criterion was stale. Nothing failed loudly enough to be read as a bug,
# so it was carried on an ops menu for days as "fleet.sh hangs".
#
# A gate that decides HALT-or-ADVANCE has to be testable without a fleet.
# Run: bash test-fleet-cadence.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
. ./fleet-cadence.sh

n=0; fail=0
ok(){ if [ "$2" = 0 ]; then n=$((n+1)); echo "  ok $n - $1"; else echo "  ✗  $1"; fail=$((fail+1)); fi; }
yes(){ "$1" "$2" && echo 0 || echo 1; }
no(){  "$1" "$2" && echo 1 || echo 0; }

# The line that halted the 2026-09-25 roll, verbatim from relay-logs.
GRAD="state=graduated peers=9 synaptome=9 mesh(open/bound)=9/9"
OPEN="state=open peers=65 synaptome=65 mesh(open/bound)=64/64"
BRIDGING="state=bridging peers=4 synaptome=4 mesh(open/bound)=4/4"
THIN="state=bridging peers=2 synaptome=2 mesh(open/bound)=1/2"
NONE=""

echo "── OPEN backstop (await_open's predicate) ──"
ok "state=open passes"                          "$(yes _fc_open "$OPEN")"
ok "state=graduated PASSES — the 2026-09-25 regression" "$(yes _fc_open "$GRAD")"
ok "state=bridging does NOT pass"               "$(no  _fc_open "$BRIDGING")"
ok "an empty line does NOT pass"                "$(no  _fc_open "$NONE")"
ok "a never-bridged relay does NOT pass"        "$(no  _fc_open "state=connecting")"

echo "── ADVANCE gate (loose-ready) ──"
ok "state=open advances"                        "$(yes _fc_loose_ready "$OPEN")"
ok "state=graduated advances"                   "$(yes _fc_loose_ready "$GRAD")"
ok "bridging with a bound mesh advances"        "$(yes _fc_loose_ready "$BRIDGING")"
ok "a mesh below the floor does NOT advance"    "$(no  _fc_loose_ready "$THIN")"
ok "an empty line does NOT advance"             "$(no  _fc_loose_ready "$NONE")"

echo "── the states must not be confused with each other ──"
# `graduated` is accepted because it PROVES MORE than open, not because the
# word is tolerated anywhere: a substring of it must not pass.
ok "\"graduate\" alone does NOT pass"           "$(no  _fc_open "state=graduate")"
ok "\"reopened\" does NOT pass"                 "$(no  _fc_open "state=reopened")"
ok "the word in free text does NOT pass"        "$(no  _fc_open "note: was graduated earlier")"

echo "── the WHOLE state vocabulary, pinned ──"
# The predicate is a substring match, so a future state that merely STARTS with
# an accepted one (`state=opening`) would pass silently. The kernel's vocabulary
# is exactly these (setBridgeState in transport/web/index.js, plus tui.js's
# 'down' fallback). Every state that is NOT terminal-healthy must be rejected
# here; adding one to the kernel without adding it here leaves it untested, and
# adding a prefix-colliding one fails this block loudly.
for s in connecting disconnected stale upgrade-required down; do
  ok "state=$s does NOT satisfy the backstop"   "$(no  _fc_open "state=$s peers=0 mesh(open/bound)=0/0")"
done
for s in open graduated; do
  ok "state=$s DOES satisfy the backstop"       "$(yes _fc_open "state=$s peers=9 mesh(open/bound)=9/9")"
done
KNOWN="connecting disconnected graduated open stale upgrade-required"
ACTUAL=$(grep -rhoE "setBridgeState\('[a-z-]+'" \
           vendor/axona-protocol/src/transport/web/index.js \
         | sed -E "s/.*'([a-z-]+)'/\1/" | sort -u | tr '\n' ' ' | sed 's/ $//')
ok "the kernel's state vocabulary is unchanged" "$([ "$ACTUAL" = "$KNOWN" ] && echo 0 || echo 1)"
[ "$ACTUAL" = "$KNOWN" ] || echo "     expected: $KNOWN
     actual:   $ACTUAL   ← a new state needs a line in this test" >&2

echo
[ "$fail" = 0 ] && echo "  all $n checks passed" || echo "  $fail FAILED"
exit "$((fail ? 1 : 0))"
