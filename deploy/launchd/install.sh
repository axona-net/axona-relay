#!/usr/bin/env bash
# =====================================================================
# install.sh — install the axona-relay + pow-collector as supervised
# macOS LaunchAgents (auto-start at login, auto-restart on exit).
#
#   ./deploy/launchd/install.sh            # install + load both
#   ./deploy/launchd/install.sh relay      # just the relay
#   ./deploy/launchd/install.sh collector  # just the collector
#
# Renders the .plist.template files with this machine's node path + repo
# location into ~/Library/LaunchAgents and bootstraps them. Idempotent:
# re-run to update (bootout + bootstrap). Stop a service with:
#   launchctl bootout gui/$(id -u)/net.axona.relay
# Logs: ~/Library/Logs/axona-relay/{relay,pow-collector}.log
# =====================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd -P)"
WORKDIR="$(cd "$HERE/../.." && pwd -P)"      # repo root (axona-relay)
NODE="$(command -v node || true)"
[ -n "$NODE" ] || { echo "error: node not found on PATH" >&2; exit 1; }
AGENTS="$HOME/Library/LaunchAgents"
LOGDIR="$HOME/Library/Logs/axona-relay"
DOMAIN="gui/$(id -u)"
mkdir -p "$AGENTS" "$LOGDIR"

want="${1:-both}"
svcs=()
case "$want" in
  both)      svcs=(net.axona.pow-collector net.axona.relay) ;;
  relay)     svcs=(net.axona.relay) ;;
  collector) svcs=(net.axona.pow-collector) ;;
  *) echo "usage: $0 [both|relay|collector]" >&2; exit 2 ;;
esac

echo "node=$NODE  workdir=$WORKDIR  domain=$DOMAIN"
for svc in "${svcs[@]}"; do
  out="$AGENTS/$svc.plist"
  sed -e "s#@NODE@#$NODE#g" -e "s#@WORKDIR@#$WORKDIR#g" -e "s#@HOME@#$HOME#g" \
      "$HERE/$svc.plist.template" > "$out"
  # Stop any prior copy (launchd-managed OR a stray nohup/manual process) so we
  # don't end up with duplicates, then (re)bootstrap from the rendered plist.
  launchctl bootout "$DOMAIN/$svc" 2>/dev/null || true
  if [ "$svc" = "net.axona.pow-collector" ]; then pkill -f 'pow-collector.js' 2>/dev/null || true; fi
  sleep 1
  launchctl bootstrap "$DOMAIN" "$out"
  launchctl enable "$DOMAIN/$svc" 2>/dev/null || true
  echo "  ✓ loaded $svc  →  $out"
done
echo "status: launchctl list | grep axona     logs: tail -f $LOGDIR/*.log"
