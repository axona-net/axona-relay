# axona-relay

A headless **Axona relay / supernode** for Node.js — a full mesh peer that
joins an Axona network over **real WebRTC**, helps route lookups, roots pub/sub
topics, and relays signaling for other peers, while showing its live state in a
console dashboard.

It runs the *same* kernel stack a browser peer runs (`webTransport` +
`NeuronNode` + `AxonaDomain` + `AxonaPeer`) — the only difference is Node has no
DOM, so we install a spec `RTCPeerConnection` from
[`node-datachannel`](https://github.com/murat-dogan/node-datachannel) and a
`WebSocket` from [`ws`](https://github.com/websockets/ws). **No kernel changes.**

## A relay is a *subset* of the bridge

| Capability | Bridge | axona-relay |
|---|:--:|:--:|
| Authenticated WebRTC mesh links | ✓ | ✓ |
| DHT routing / lookups | ✓ | ✓ |
| Pub/sub **root axon** (cache · fan-out · replay) | ✓ | ✓ |
| Relay WebRTC signaling for others (bridgeless help) | ✓ | ✓ |
| Public WebSocket **server** | ✓ | ✗ (it's a WS *client*) |
| Mint TURN credentials | ✓ | ✗ |
| Admission gate / version floors | ✓ | ✗ |
| Must be publicly reachable | ✓ | ✗ |

It dials a bridge **once**, only for bootstrap + signaling; after that it's a
first-class mesh peer. Run several relays and they strengthen routing and
pub/sub durability across the network — without any of them being an inbound
server.

## Requirements

- **Node ≥ 20** (global WebCrypto / `crypto.subtle`).
- `node-datachannel` ships prebuilt binaries for common platforms; a build
  toolchain is only needed if a prebuilt isn't available for yours.

## Install

```bash
npm install
npm run sync:protocol   # vendor the kernel from ../axona-protocol (already vendored in the repo)
```

## Run

```bash
npm start          # live blessed dashboard (interactive TTY)
npm run probe      # plain timestamped log lines (RELAY_TUI=0) — good for piping / services
```

Quit the dashboard with **q** or **Ctrl-C**.

### Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `BRIDGE_URL` | `wss://testnet.axona.net` | Bridge for bootstrap + signaling |
| `RELAY_REGION` | — | `auto` (detect), a region **name** (`useast`), or a code (`0x89`) — sets the nodeId's geo prefix |
| `RELAY_LAT` / `RELAY_LNG` | `37.77` / `-122.42` | Geo prefix by coordinate (used if `RELAY_REGION` is unset). Default = SF (`uswest`) |
| `RELAY_IDENTITY_PATH` | `./identity.<region>.json` | Persisted keypair (stable nodeId). Default name is region-keyed |
| `RELAY_TUI` | auto (`stdout.isTTY`) | `1` force dashboard, `0` force plain log |

```bash
RELAY_REGION=auto   npm start                 # detect location (IP-geo → timezone)
RELAY_REGION=useast npm start                 # nodeId anchored at us-east (0x89)
BRIDGE_URL=wss://bridge.axona.net RELAY_LAT=40.71 RELAY_LNG=-74.0 npm start
```

Region precedence: `RELAY_REGION` › `RELAY_LAT`/`RELAY_LNG` › default SF. The
region resolves to the cell **center** coordinate, so `RELAY_REGION=useast`
reliably mints a `0x89`-prefixed id. Region names are the protocol's 192
canonical names (`regionName`/`resolveRegion`).

**Auto-detection** (`RELAY_REGION=auto`, opt-in) discovers the relay's location:
first an **IP-geolocation** HTTPS call (city-level — one outbound request to a
public geo API, which sees your IP), falling back to the **OS timezone**
(fully local, no network), then the default. It's only as precise as it needs
to be — the 192 region cells are ~2000 km across. Auto identities persist to a
fixed `identity.auto.json` (so re-detection variance never orphans the nodeId).
The default (no `RELAY_REGION`) makes **no** network call.

> The relay sends its **kernel** version (2.x) in the bridge handshake, which
> clears the kernel-namespace floor (`MIN_KERNEL_VERSION`). It does **not** send
> its 0.x app version on the wire (that would be classified kernel-namespace and
> rejected); the app version is display-only.

### Running more than one relay

Each relay needs its **own** identity. The default identity filename is
region-keyed (`identity.uswest.json`, `identity.useast.json`, …), so relays in
**different** regions just work:

```bash
RELAY_REGION=useast npm start    # terminal 1 → identity.useast.json
RELAY_REGION=uknorth npm start   # terminal 2 → identity.uknorth.json   (britain)
```

Two relays sharing one identity file would collide on the same nodeId, so the
relay takes an **exclusive lock** (`<identity>.lock`) on startup and refuses to
start a second instance on the same identity — give it a different
`RELAY_REGION` or `RELAY_IDENTITY_PATH`. (The region is baked into the persisted
identity; once the file exists, changing `RELAY_REGION`/`RELAY_LAT` is ignored
for that file and the relay warns — delete the file or point at a new path to
re-mint.)

## Identity

On first run the relay derives an Ed25519 keypair and writes the envelope to
`RELAY_IDENTITY_PATH`; later runs reload it, so the relay keeps a **stable
264-bit nodeId** across restarts (what makes a relay a dependable, well-known
node). The file holds a **private key** — it is git-ignored; `chmod 600` it in
production.

## What the dashboard shows

Everything is read from `peer.health()` plus the `onPeerJoin` / `onPeerLeave` /
`onLog` / `onError` event hooks:

- **Header** — relay + kernel version, nodeId, region + region code, bridge URL
  and connection state, uptime, a `MESH DEGRADED` flag if open channels exceed
  authenticated binds.
- **Mesh peers** — the authenticated peers in the synaptome.
- **Status** — synaptome size, mesh channels (open), mesh bound, total bound,
  subscriptions, wire version.
- **Pub/sub roles** — topics this node roots, with subscriber and cache counts.
- **Log** — bridge/mesh/relay/error events as they happen.

## Run as a service (systemd)

```ini
# /etc/systemd/system/axona-relay.service
[Unit]
Description=Axona relay
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/bin/node /opt/axona-relay/src/index.js
Environment=RELAY_TUI=0
Environment=BRIDGE_URL=wss://testnet.axona.net
Environment=RELAY_IDENTITY_PATH=/var/lib/axona-relay/identity.relay.json
StateDirectory=axona-relay
Restart=always
User=axona

[Install]
WantedBy=multi-user.target
```

In plain (`RELAY_TUI=0`) mode it emits one status line per second plus event
lines, which land cleanly in `journalctl`.

## Layout

```
src/
  polyfill.js   install RTCPeerConnection (node-datachannel) + WebSocket (ws)
  identity.js   load-or-create the persisted relay keypair
  relay.js      assemble + start the AxonaPeer mesh relay
  tui.js        blessed dashboard  +  plain-log presenter
  index.js      entrypoint: env config, wiring, render loop, graceful shutdown
vendor/axona-protocol/   pinned kernel copy (refresh via npm run sync:protocol)
scripts/sync-protocol.sh
```

## Roadmap ideas

- Topic **pinning** (`RELAY_PIN=us-east/hello-world,…`) so a relay proactively
  subscribes/roots chosen topics for durability.
- Prometheus `/metrics` (peers, binds, roles, replay-cache sizes).
- Multiple relays + a small chaos harness to measure pub/sub durability vs
  churn.
- Bridgeless bootstrap: seed from a known peer list instead of a bridge URL.
