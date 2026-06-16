# Supervised relay + collector (macOS launchd)

Runs the relay and the PoW-bench collector as **LaunchAgents**: they start at
login and **auto-restart on any exit** — defense-in-depth on top of the relay's
in-process resilience (the `uncaughtException` guard + connect-retry loop) and
the collector's reconnect watchdog. A truly fatal exit (or a Mac reboot) now
brings the process straight back instead of leaving it dead.

## Install

```bash
./deploy/launchd/install.sh            # both relay + collector
./deploy/launchd/install.sh relay      # just one
./deploy/launchd/install.sh collector
```

The installer renders the `.plist.template` files with this machine's `node`
path and repo location into `~/Library/LaunchAgents/`, stops any prior copy
(including a stray manually-launched process), and bootstraps them. Re-run any
time to update (it's idempotent).

| Service | Label | Command | Notes |
|---|---|---|---|
| Relay | `net.axona.relay` | `node src/index.js` | `RELAY_REGION=auto`, `RELAY_TUI=0` (plain log) |
| Collector | `net.axona.pow-collector` | `node pow-collector.js` | prod bridge; appends `pow-results.jsonl` |

## Operate

```bash
launchctl list | grep axona                       # is it running? (PID, last exit)
tail -f ~/Library/Logs/axona-relay/relay.log
tail -f ~/Library/Logs/axona-relay/pow-collector.log

launchctl kickstart -k gui/$(id -u)/net.axona.relay   # force a restart
launchctl bootout    gui/$(id -u)/net.axona.relay      # STOP (and disable)
```

> `KeepAlive=true` means launchd respawns the process on **any** exit, so you
> can't stop it with Ctrl-C / `kill` — it just comes back. To actually stop it,
> `launchctl bootout` the label (above). `ThrottleInterval=10` caps respawns to
> avoid a tight crash loop.

## Uninstall

```bash
for s in net.axona.relay net.axona.pow-collector; do
  launchctl bootout gui/$(id -u)/$s 2>/dev/null || true
  rm -f ~/Library/LaunchAgents/$s.plist
done
```

The templates carry no machine-specific paths (they're filled in at install
time), so they're safe to commit; the rendered plists in `~/Library/LaunchAgents`
are per-machine and not tracked.
