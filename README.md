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
| `RELAY_IDENTITY_PATH` | `./identity.relay.json` | Persisted keypair (stable nodeId) |
| `RELAY_LAT` / `RELAY_LNG` | `37.77` / `-122.42` | Geo prefix for the nodeId (SF) |
| `RELAY_TUI` | auto (`stdout.isTTY`) | `1` force dashboard, `0` force plain log |

```bash
BRIDGE_URL=wss://bridge.axona.net RELAY_LAT=40.71 RELAY_LNG=-74.0 npm start
```

> The relay sends its **kernel** version (2.x) in the bridge handshake, which
> clears the kernel-namespace floor (`MIN_KERNEL_VERSION`). It does **not** send
> a 0.x app version on the wire (that would be classified kernel-namespace and
> rejected); the `0.1.0` app version is display-only.

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
