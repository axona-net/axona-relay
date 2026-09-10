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
#   3. BRACKETING WAS NOT ENOUGH, and 2026-09-10 proved it: axona-linux reported
#      7 relays against 5 again. Bracketing only stops the pattern matching the
#      process that carries THE PATTERN. It does nothing about a caller whose
#      command line carries the PLAIN string — a diagnostic ssh running
#      `pgrep -af "node src/index.js"` alongside the census is matched by
#      `src/index[.]js`, because that regex matches the literal text `src/index.js`
#      wherever it appears. Two shells, two phantom relays.
#
#      The fix is structural, not another exclusion. A relay IS a node process.
#      The comm check used to be a blocklist of ONE — "not caffeinate" — which
#      only ever excludes the wrapper somebody already tripped over. Inverted to
#      an ACCEPT-LIST: comm must be `node`. That admits relays and rejects every
#      shell, wrapper, editor and grep at once, including the ones nobody has met
#      yet. Same reasoning as logctx.js: decide on what the thing IS, never on a
#      list of names that bit us before.
#
# All three are one-line mistakes that look right. So the count lives HERE, once,
# and every caller asks this file instead of rewriting the question.
#
#   bash relay-census.sh --selftest # → prove a decoy command line is NOT counted
#
#   bash relay-census.sh            # → the count, nothing else
#   bash relay-census.sh --pids     # → one relay pid per line
#   bash relay-census.sh --verbose  # → count plus what was seen and skipped
#   bash relay-census.sh --kernels  # → what the RUNNING relays actually loaded
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
    # ACCEPT-LIST, not a blocklist. A relay is a process whose EXECUTABLE is
    # node; anything else that merely mentions src/index.js on its command line
    # — the caffeinate wrapper, an ssh shell running a diagnostic, a grep, an
    # editor — is rejected by construction rather than by name.
    #
    # READ THE EXECUTABLE FROM args, NEVER FROM comm. `ps -o comm=` is the
    # THREAD name on Linux, and node renames its main thread, so all five
    # axona-linux relays report comm=MainThread. An accept-list on comm counts
    # ZERO relays there — strictly worse than the over-count it replaced, and it
    # was caught only because a restart script reused the same test and found
    # nothing to restart. args[0] is the command as invoked on both platforms:
    #   relay              → "node src/index.js"                 → node   ACCEPT
    #   caffeinate wrapper → "caffeinate -i nohup node src/..."   → caffeinate REJECT
    #   diagnostic shell   → "bash -c ... node src/index.js"      → bash   REJECT
    exe=$(ps -p "$pid" -o args= 2>/dev/null); exe=${exe%% *}; exe=${exe##*/}
    case "$exe" in node|node.exe|nodejs) echo "$pid" ;; esac
  done
  # Explicit success: without it, a final iteration that skipped a wrapper leaves
  # a non-zero status and `set -e` in a CALLER kills it with no message. That
  # exact silent abort was found by negative-testing roll-fleet.sh before its
  # first run; the same trap applies to anything sourcing this.
  return 0
}

# What each RUNNING relay actually loaded — NOT what the checkout would launch.
# fleet.sh status reported the repo's vendored version and so showed axona-win as
# 4.78.0 while 8 of its 22 relays were still on the old kernel, because a roll had
# died half way. A version column that cannot show a half-rolled host is worse
# than none: it reads as confirmation. Resolved per-pid from the log the process
# still holds open, so it describes the process and not the directory.
running_kernels() {
  if is_windows; then
    # git-bash cannot resolve an open fd to a path. Fall back to the honest
    # proxy: how many relays were started by the newest roll generation.
    local newest
    newest=$(ls -t relay-logs/roll-*.log 2>/dev/null | head -1 | sed -E 's/.*roll-([0-9]+-[0-9]+)-.*/\1/')
    [ -z "$newest" ] && { echo "unknown (no roll logs)"; return 0; }
    echo "$(ls relay-logs/roll-$newest-*.log 2>/dev/null | grep -c .)/$(relay_pids | grep -c .) started by generation $newest"
    return 0
  fi
  local pid log k
  for pid in $(relay_pids); do
    if [ -r "/proc/$pid/fd/1" ]; then log=$(readlink "/proc/$pid/fd/1" 2>/dev/null)
    else log=$(lsof -p "$pid" -a -d 1 -Fn 2>/dev/null | sed -n 's/^n//p' | head -1); fi
    k=""
    [ -n "$log" ] && [ -r "$log" ] && k=$(grep -m1 -o "kernel v[0-9][0-9.]*" "$log" 2>/dev/null)
    echo "${k:-kernel unknown}"
  done | sort | uniq -c | sed 's/^ */  /'
}

case "$MODE" in
  count)   relay_pids | grep -c . ;;
  --kernels|kernels) running_kernels ;;
  --pids|pids) relay_pids ;;
  --verbose|verbose)
    n=$(relay_pids | grep -c .)
    echo "relays: $n"
    if ! is_windows; then
      for pid in $(pgrep -f "$PATTERN" 2>/dev/null || true); do
        e=$(ps -p "$pid" -o args= 2>/dev/null); e=${e%% *}; e=${e##*/}
        case "$e" in
          node|node.exe|nodejs) echo "  relay pid $pid ($e)" ;;
          *) echo "  skip  pid $pid ($e) — matched the pattern, not a node executable" ;;
        esac
      done
    else
      relay_pids | while read -r p; do echo "  relay pid $p (node.exe)"; done
    fi ;;
  # The regression, executable. Spawns a decoy whose COMMAND LINE carries the
  # plain string `node src/index.js` — exactly the shape of the diagnostic ssh
  # that inflated axona-linux to 7 — and asserts the census does not count it.
  # Bracketing alone passes the old trap and FAILS this one.
  --selftest|selftest)
    if is_windows; then echo "selftest: skipped (no pgrep on this platform)"; exit 0; fi
    before=$(relay_pids | grep -c .)
    # The decoy must be a MULTI-command -c string. bash exec-optimises a lone
    # command (`bash -c 'sleep 6'` becomes plain `sleep 6`) and the pattern
    # disappears from argv, which silently makes this test prove nothing. The
    # semicolon keeps bash resident with the literal text on its command line —
    # the exact shape of the diagnostic ssh that inflated axona-linux to 7.
    bash -c 'sleep 6; true # node src/index.js' &
    decoy=$!
    sleep 1
    seen=$(pgrep -f "$PATTERN" 2>/dev/null | grep -c "^$decoy$" || true)
    after=$(relay_pids | grep -c .)
    kill "$decoy" 2>/dev/null; wait "$decoy" 2>/dev/null
    echo "  decoy pid $decoy — matched by the pattern: $([ "$seen" -ge 1 ] && echo YES || echo no)"
    echo "  relays before=$before after=$after"
    if [ "$seen" -lt 1 ]; then
      echo "  ~ INCONCLUSIVE: the decoy was not matched by the pattern at all, so"
      echo "    this run did not exercise the filter. Not a pass."
      exit 2
    fi
    if [ "$after" -eq "$before" ]; then
      echo "  ✓ PASS — the pattern matched the decoy and the census rejected it"
      exit 0
    fi
    echo "  ✗ FAIL — census rose $before -> $after; a non-node process was counted"
    exit 1 ;;
  *) echo "usage: relay-census.sh [count|--pids|--verbose|--kernels|--selftest]" >&2; exit 2 ;;
esac
