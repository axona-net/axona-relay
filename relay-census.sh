#!/usr/bin/env bash
# =============================================================================
# relay-census.sh — HOW MANY RELAYS ARE RUNNING HERE. The single answer.
#
# This one question has been answered wrong twice in a day, each time by a
# freshly hand-written pgrep, and each time roll-fleet.sh's count gate caught it
# and refused a roll:
#
#   1. `pgrep -f "src/index.js"` also matches the CAFFEINATE WRAPPER that macOS
#      relays launch under (`caffeinate -i nohup node src/index.js`), so every
#      mac relay counts twice. That put Air at 6 and M1 at 8 in the fleet census
#      when they were 3 and 4, and the wrong numbers were carried into STATE.md
#      and a roll argument.
#
#   2. The same pattern, run inline over ssh, matches THE SHELL RUNNING THE
#      CENSUS — its own command line contains the string. On Linux that reported
#      7 relays where there were 5. Bracketing the dot (src/index[.]js) makes the
#      pattern unable to match itself: the searching process's command line
#      contains the brackets, which the regex does not accept. Same trick as
#      writing `[n]ode` in a grep.
#
# Both are one-line mistakes that look right. So the count lives HERE, once, and
# every caller asks this file instead of rewriting the question.
#
#   bash relay-census.sh            # → the count, nothing else
#   bash relay-census.sh --pids     # → one relay pid per line
#   bash relay-census.sh --verbose  # → count plus what was seen and skipped
#
# Windows relays run as bare node.exe with no pgrep available in git-bash, so
# that platform is counted with tasklist. Same script, same answer, one call.
# =============================================================================
set -uo pipefail

MODE="${1:-count}"
PATTERN='src/index[.]js'          # bracketed: cannot match the process asking

is_windows() { case "$(uname -s 2>/dev/null)" in MINGW*|MSYS*|CYGWIN*) return 0 ;; *) return 1 ;; esac; }

relay_pids() {
  if is_windows; then
    # No pgrep in git-bash. tasklist gives one line per node.exe; the relays are
    # the only node processes on these hosts.
    tasklist //FI "IMAGENAME eq node.exe" //NH 2>/dev/null \
      | awk '/node.exe/ {print $2}'
    return 0
  fi
  for pid in $(pgrep -f "$PATTERN" 2>/dev/null || true); do
    # The wrapper's comm is `caffeinate`; the relay's is `node`. Skip wrappers.
    [ "$(ps -p "$pid" -o comm= 2>/dev/null)" != "caffeinate" ] && echo "$pid"
  done
  # Explicit success: without it, a final iteration that skipped a wrapper leaves
  # a non-zero status and `set -e` in a CALLER kills it with no message. That
  # exact silent abort was found by negative-testing roll-fleet.sh before its
  # first run; the same trap applies to anything sourcing this.
  return 0
}

case "$MODE" in
  count)   relay_pids | grep -c . ;;
  --pids|pids) relay_pids ;;
  --verbose|verbose)
    n=$(relay_pids | grep -c .)
    echo "relays: $n"
    if ! is_windows; then
      for pid in $(pgrep -f "$PATTERN" 2>/dev/null || true); do
        c=$(ps -p "$pid" -o comm= 2>/dev/null)
        if [ "$c" = "caffeinate" ]; then echo "  skip pid $pid (caffeinate wrapper)"
        else echo "  relay pid $pid ($c)"; fi
      done
    else
      relay_pids | while read -r p; do echo "  relay pid $p (node.exe)"; done
    fi ;;
  *) echo "usage: relay-census.sh [count|--pids|--verbose]" >&2; exit 2 ;;
esac
