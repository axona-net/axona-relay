# Droplet systemd config

The region topology of the prod backbone. Until 2026-09-11 this lived nowhere
but `/etc/systemd/system` on three hosts, so rebuilding a droplet silently
changed which region it served.

## The fleet's regions

Every topic this project uses is **eagle** (0x89): council `898be2a9`, jokes
`894226`, general `89f58f`. A relay in any other region is idle by construction.

    eagle    45 relays   air 6 · m1 8 · axona-linux 5 · axona-win 20
                         · droplet useast x3 · droplet uswest x3
    grizzly   3 relays   droplet grizzly1 x3

One relay per droplet stays in grizzly on purpose. Region is a placement hint,
not a wall, but a region with zero nodes has nowhere to hint to.

## The trap this exists to disarm

The unit template sets `Environment=RELAY_REGION=%i` — **the systemd instance
name is the region**. The instance names here are geography, and the kernel
resolves them anyway:

    useast   -> 0x89 eagle          uswest   -> 0x80 grizzly
    uscentlw -> 0x87                grizzly1 -> does not resolve

Nobody chose that. It fell out of the naming, and it meant `useast` was the only
eagle relay on each droplet. It was the sick relay on all three hosts every time
we looked — 143 roles on 4 peers at its worst — while its two siblings sat at
`roles=0` with near-perfect channels, serving regions with no traffic. It reads
as a load problem and it is a configuration problem.

`%i` is kept as a fallback so an un-configured instance still starts, but every
instance ships an explicit `region.conf` that overrides it. **Never add an
instance without one.**

## Enablement

`install-units.sh` enables every instance it ships. It has to: on 2026-09-11 all
three droplets had `useast` and `uswest` **running but disabled** — started by
hand and never wired to boot. A reboot would have returned a single relay, in
the region with no traffic, and nothing would have reported the loss.

## Use

    DRY=1 bash deploy/droplet/install-units.sh   # diff against what is installed
          bash deploy/droplet/install-units.sh   # install, enable, verify

It deliberately does NOT restart anything. A region change takes effect only on
restart, and restarting the backbone is a fleet operation with its own gate:

    ONLY=<ip> EXPECT_PER_DROPLET=3 EXPECT_KERNEL=<ver> bash ops/droplet-roll.sh

## What is still not versioned

The droplet host build itself — node, the checkout at `/opt/axona-relay`, the
ssh keys. This directory covers the units and the region topology only.
