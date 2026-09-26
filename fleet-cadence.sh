#!/usr/bin/env bash
# =============================================================================
# fleet-cadence.sh — shared implementation of the Fleet Cadence Standard v1.
# Source this from every fleet script so the join/leave cadence is identical
# everywhere. See ops/FLEET-CADENCE.md for the rules.
#
# Contract: the CALLER supplies a reader function `read_state <target>` that
# echoes the target relay's most-recent state line, of the shape
#   state=<word> ... mesh(open/bound)=<open>/<bound>
# (empty if none yet). Everything else — the two-tier gate, timeouts, jitter,
# graceful leave — lives here.
# =============================================================================

READY_TIMEOUT="${READY_TIMEOUT:-180}"   # OPEN backstop: full state=open, seconds
ADVANCE_CAP="${ADVANCE_CAP:-45}"        # ADVANCE gate: bridged+bound, seconds
LEAVE_TIMEOUT="${LEAVE_TIMEOUT:-30}"    # graceful leave drain, seconds
JITTER="${JITTER:-3}"                   # max random gap between gated steps, seconds
POLL="${POLL:-3}"                       # poll interval, seconds

_fc_now(){ date +%s; }

# small randomized gap so relays don't fall into synchronized periodic beats
cadence_jitter(){ local j=$(( RANDOM % (JITTER + 1) )); [ "$j" -gt 0 ] && sleep "$j"; return 0; }

# GRADUATED IS A TERMINAL HEALTHY STATE, NOT A FAILURE TO OPEN.
#
# 2026-09-26. Every axona-win roll since the bridge degree cap shipped ended
# with "✗ OPEN backstop: … did not reach state=open within 270s (last:
# state=graduated peers=9 synaptome=9 mesh(open/bound)=9/9)" and HALTED a roll
# that had already completed all 23 slots. The relay was healthy. The criterion
# predated the feature: a graduated relay CAN NEVER REACH state=open, because
# graduating is precisely the bridge releasing its socket.
#
# WHY ACCEPTING IT IS SAFE — graduated is STRICTLY STRONGER than open, not a
# weaker substitute. The kernel sets it in exactly one place
# (transport/web/index.js:338) and only when BOTH hold:
#   · the close code is 4200, which the bridge sends only to an ADMITTED
#     connection that has held its slot past BRIDGE_GRADUATION_MIN_UPTIME_MS
#     (30s) — so the relay demonstrably bridged; and
#   · meshBoundCount() >= graduationMeshFloor (3) AT THAT MOMENT — so it has a
#     live bound mesh.
# A relay that never bridged cannot reach it, and one that was not really
# meshed falls through that branch and reconnects instead. `open` proves a
# socket; `graduated` proves a socket that existed AND a mesh that is bound.
_fc_healthy_state(){ printf '%s' "$1" | grep -qE "state=(open|graduated)"; }

# loose-ready: already open or graduated, OR bridged with a mostly-bound mesh
_fc_loose_ready(){
  local l="$1" o b
  _fc_healthy_state "$l" && return 0
  o=$(printf '%s' "$l" | sed -nE 's/.*mesh\(open\/bound\)=([0-9]+)\/([0-9]+).*/\1/p')
  b=$(printf '%s' "$l" | sed -nE 's/.*mesh\(open\/bound\)=([0-9]+)\/([0-9]+).*/\2/p')
  [ -n "$o" ] && [ -n "$b" ] && [ "$b" -ge 3 ] && [ "$o" -ge $(( b - 1 )) ]
}
_fc_open(){ _fc_healthy_state "$1"; }

# await_advance <reader_fn> <target> : poll to loose-ready. 0 = advance, 1 = HALT.
await_advance(){
  local reader="$1" target="$2" deadline=$(( $(_fc_now) + ADVANCE_CAP )) l=""
  while [ "$(_fc_now)" -lt "$deadline" ]; do
    l="$("$reader" "$target")"
    _fc_loose_ready "$l" && return 0
    sleep "$POLL"
  done
  echo "  ✗ ADVANCE gate: $target did not bridge+bond within ${ADVANCE_CAP}s (last: ${l:-none})" >&2
  return 1
}

# await_open <reader_fn> <target> : poll to state=open. 0 = open, 1 = HALT (backstop).
await_open(){
  local reader="$1" target="$2" deadline=$(( $(_fc_now) + READY_TIMEOUT )) l=""
  while [ "$(_fc_now)" -lt "$deadline" ]; do
    l="$("$reader" "$target")"
    _fc_open "$l" && return 0
    sleep "$POLL"
  done
  echo "  ✗ OPEN backstop: $target reached neither state=open nor state=graduated within ${READY_TIMEOUT}s (last: ${l:-none})" >&2
  return 1
}

# graceful_leave <stopper_fn> <alive_fn> <target> : run the caller's graceful
# stop (SIGTERM/systemctl stop), then poll the caller's alive-check until the
# target is gone or LEAVE_TIMEOUT. 0 = left cleanly, 1 = still alive at timeout.
# The caller decides whether a timeout escalates (e.g. Windows taskkill //F).
graceful_leave(){
  local stopper="$1" alive="$2" target="$3" deadline=$(( $(_fc_now) + LEAVE_TIMEOUT ))
  "$stopper" "$target"
  while [ "$(_fc_now)" -lt "$deadline" ]; do
    "$alive" "$target" || return 0
    sleep "$POLL"
  done
  echo "  ⚠ graceful_leave: $target still alive after ${LEAVE_TIMEOUT}s" >&2
  return 1
}
