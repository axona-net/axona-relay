# scm-guard.sh — sourced at the top of every legacy git-bash fleet script.
# On a Windows host whose relays are SCM services (axona-relay-NN), those
# scripts must not run: they count and kill node.exe directly, which fights the
# services (the SCM restarts what they kill). The one controller there is
# windows/relayctl.ps1. No effect on macOS or Linux.
case "$(uname -s 2>/dev/null)" in
  MINGW*|MSYS*|CYGWIN*)
    if sc.exe query axona-relay-01 >/dev/null 2>&1; then
      echo "REFUSED: this host runs its relays as Windows services (axona-relay-NN)." >&2
      echo "Use windows/relayctl.ps1 (status | roll -Kernel X | start/stop -Slot N); see hosts/<host>.json." >&2
      exit 1
    fi ;;
esac
