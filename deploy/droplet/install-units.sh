#!/usr/bin/env bash
set -euo pipefail
# =============================================================================
# install-units.sh — put THIS repo's systemd config on a droplet, then prove it.
#
# WHY THIS EXISTS. Until 2026-09-11 the droplet region topology lived nowhere
# but /etc/systemd/system on three hosts. Rebuild a droplet and it came back
# with `uswest` in grizzly, because the template derives region from the
# INSTANCE NAME and the instance names are geography. That mapping cost a
# night: `useast` was the only eagle relay per box while every topic we use is
# eagle, so it drowned and its siblings idled.
#
# Two other things this fixes by being written down:
#
#   • ENABLEMENT. On all three droplets only `grizzly1` was enabled. `useast`
#     and `uswest` were RUNNING BUT DISABLED — started by hand at some point and
#     never wired to boot. A reboot would have returned one relay, in the region
#     with no traffic, and the loss would have been silent.
#
#   • A dry run. `DRY=1` shows the diff against what is installed and changes
#     nothing, because a systemd unit edit on a live backbone should be readable
#     before it is applied.
#
#   DRY=1 bash deploy/droplet/install-units.sh    # diff only
#         bash deploy/droplet/install-units.sh    # install + enable + verify
#
# It does NOT restart anything. A region change only takes effect on restart,
# and restarting the backbone is a fleet operation with its own gate — do it
# with ops/droplet-roll.sh, one host at a time.
# =============================================================================
SRC="$(cd "$(dirname "$0")/systemd" && pwd)"
DST=/etc/systemd/system
DRY="${DRY:-0}"
INSTANCES="useast uswest grizzly1"

[ -d "$SRC" ] || { echo "✗ no source tree at $SRC" >&2; exit 1; }

changed=0
show() {  # show(src, dst)
  if [ ! -f "$2" ]; then echo "  + $2 (new)"; changed=1; return; fi
  if ! diff -q "$1" "$2" >/dev/null 2>&1; then
    echo "  ~ $2 differs:"
    # `diff` exits 1 when files differ and `head` closes the pipe early, so under
    # `set -euo pipefail` this pipeline ABORTS THE SCRIPT — silently, after the
    # first differing file. It did exactly that on the first run: the template
    # diff printed and the drop-in checks never happened, which reads as "the
    # drop-ins are fine" rather than "the check never ran".
    { diff -u "$2" "$1" 2>/dev/null | sed 's/^/      /' | head -30; } || true
    changed=1
  else echo "  = $2 (identical)"; fi
}

echo "── template ──"
show "$SRC/axona-relay@.service" "$DST/axona-relay@.service"
echo "── region drop-ins ──"
for i in $INSTANCES; do
  show "$SRC/axona-relay@$i.service.d/region.conf" "$DST/axona-relay@$i.service.d/region.conf"
done

if [ "$DRY" = "1" ]; then
  echo "DRY: nothing written. changed=$changed"; exit 0
fi

install -m 0644 "$SRC/axona-relay@.service" "$DST/axona-relay@.service"
for i in $INSTANCES; do
  install -d -m 0755 "$DST/axona-relay@$i.service.d"
  install -m 0644 "$SRC/axona-relay@$i.service.d/region.conf" "$DST/axona-relay@$i.service.d/region.conf"
done
systemctl daemon-reload

# ENABLE every instance we ship. A running-but-disabled relay is a relay that
# vanishes at the next reboot with nothing to say about it.
for i in $INSTANCES; do systemctl enable "axona-relay@$i.service" >/dev/null 2>&1 || true; done

echo "── verify ──"
fail=0
for i in $INSTANCES; do
  want=$(grep -oE 'RELAY_REGION=[a-z0-9]+' "$SRC/axona-relay@$i.service.d/region.conf" | tail -1 | cut -d= -f2)
  got=$(systemctl show "axona-relay@$i" -p Environment --value | tr ' ' '\n' | grep '^RELAY_REGION=' | tail -1 | cut -d= -f2)
  en=$(systemctl is-enabled "axona-relay@$i" 2>&1)
  ok="✓"; [ "$want" = "$got" ] || { ok="✗"; fail=1; }
  [ "$en" = "enabled" ] || { ok="✗"; fail=1; }
  printf "  %s %-10s region want=%-8s got=%-8s enabled=%s\n" "$ok" "$i" "$want" "${got:-<unset>}" "$en"
done
# Regions only take effect on restart; say so rather than implying it is live.
echo "── running now (may lag the config until restarted) ──"
for i in $INSTANCES; do
  r=$(journalctl -u "axona-relay@$i" --no-pager 2>/dev/null | grep -a -oE 'region [a-z]+' | tail -1)
  printf "  %-10s %s\n" "$i" "${r:-<no banner>}"
done
[ "$fail" = "0" ] || { echo "✗ verification failed" >&2; exit 1; }
echo "✓ units installed, enabled and verified"
