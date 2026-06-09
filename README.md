# axona-relay

A headless **Axona relay / supernode** for Node.js — a full mesh peer that
joins an Axona network over **real WebRTC**, helps route lookups, roots pub/sub
topics, and relays signaling for other peers, while showing its live state in a
console dashboard.

**v0.7.2** on kernel **v2.32.0** (`axona/5` wire epoch). Defaults to the
**production** network ([bridge.axona.net](https://bridge.axona.net)); set
`RELAY_NETWORK=testnet` to target the SF staging line instead.

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
| `RELAY_NETWORK` | `prod` | Which network to bootstrap from: `prod` (`bridge.axona.net`) or `testnet` (`testnet.axona.net`) |
| `BRIDGE_URL` | — | Explicit bridge URL; **overrides** `RELAY_NETWORK` |
| `RELAY_REGION` | — | `auto` (detect), a region **name** (`useast`), or a code (`0x89`) — sets the nodeId's geo prefix |
| `RELAY_LAT` / `RELAY_LNG` | `37.77` / `-122.42` | Geo prefix by coordinate (used if `RELAY_REGION` is unset). Default = SF (`uswest`) |
| `RELAY_IDENTITY_PATH` | `./identity.<region>.json` | Persisted keypair (stable nodeId). Default name is region-keyed |
| `RELAY_TUI` | auto (`stdout.isTTY`) | `1` force dashboard, `0` force plain log |

Bridge selection precedence: `BRIDGE_URL` › `RELAY_NETWORK` › default (`prod`).

```bash
npm start                                     # production network (default)
RELAY_NETWORK=testnet npm start               # the SF staging line
RELAY_REGION=auto   npm start                 # detect location (IP-geo → timezone)
RELAY_REGION=useast npm start                 # nodeId anchored at us-east (0x89)
BRIDGE_URL=wss://my-bridge:8080 RELAY_LAT=40.71 RELAY_LNG=-74.0 npm start   # explicit bridge
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

Just run it again — **no extra input needed**:

```bash
npm start    # terminal 1 → PRIMARY: the known, persistent node
npm start    # terminal 2 → ADDITIONAL: a fresh ephemeral node, same region
npm start    # terminal 3 → ADDITIONAL: another one …
```

The **first** instance claims the known persistent identity (stable nodeId
across restarts; `identity.<region>.json`, guarded by `<identity>.lock`). Any
**additional** instance sees that identity is in use and, instead of refusing,
mints a **fresh ephemeral identity** in the same region — a unique nodeId, not
written to disk. So you always have one well-known node plus as many throwaway
nodes as you like for testing and extra capacity; the header/log marks each
**PRIMARY** or **ADDITIONAL**.

You can still pin a *second persistent, known* node by giving it its own region
or path: `RELAY_REGION=uknorth npm start` (→ `identity.uknorth.json`) or
`RELAY_IDENTITY_PATH=./identity.b.json npm start`.

(The region is baked into a persisted identity; once its file exists, changing
`RELAY_REGION`/`RELAY_LAT` for that file is ignored and the relay warns — delete
the file or use a new path to re-mint.)

## Publish / subscribe from the command line (scripts & agents)

`src/cli.js` is a one-shot, headless pub/sub client built on the same connect
machinery as the relay. It connects with an **ephemeral** identity (no lock, no
persisted key — so it never collides with a running relay), does one job, prints
**JSON to stdout** (logs go to stderr), and exits. That makes it drivable from a
shell, a script, or an AI agent like Claude Code.

```bash
# publish (prints {ok, msgId, …})
node src/cli.js pub  "claude/test" "hello from a script"

# subscribe — stream matching messages as JSON lines for N seconds
node src/cli.js sub  "claude/test" --for 25 --since all

# fetch just the latest message on a topic
node src/cli.js pull "claude/test"
```

Also wired as npm scripts (`npm run pub -- "<topic>" "<msg>"`) and a `bin`
(`axona-cli` after `npm link`).

**Topic + region.** The topic string is used verbatim and anchored at a
**synthetic region publisher** (`<s2-prefix>‖0²⁵⁶`), exactly as `axona-peer`
and the kernel demo do. Both sides must use the same `--region` (default
`useast` / `0x89`) or they derive different topic IDs and never meet. Because
of this, the CLI **interoperates with the live apps**: publishing to
`us-east/hello-world` shows up in the [axona.net](https://axona.net) /
`demo.axona.net` feed (or `demo-testnet.axona.net` with `--network testnet`),
and vice-versa.

| Option | Default | Meaning |
|---|---|---|
| `--region <name\|code>` | `useast` | synthetic-publisher region (e.g. `uknorth`, `0x44`) |
| `--for <seconds>` | `25` | `sub`: how long to listen |
| `--since <all\|new>` | `all` | `sub`: replay backlog, or live-only |
| `--network <prod\|testnet>` | `prod` | which network to bootstrap from (or `RELAY_NETWORK`) |
| `--bridge <wss-url>` | — | explicit bridge URL; overrides `--network` / `BRIDGE_URL` |
| `--ready-timeout <s>` | `30` | max wait for mesh readiness before giving up |

A `pub` round-trips through a real production root: a separate `sub --since all`
on the same topic+region replays it back (verified end-to-end). Add
`--network testnet` to both sides to exercise the staging line instead.

## Native MCP tool (Claude Code & other agents)

`src/mcp.js` is a [Model Context Protocol](https://modelcontextprotocol.io)
server (stdio) that exposes the same operations as **first-class agent tools**
— so Claude Code gets `axona_publish` / `axona_subscribe` / `axona_pull`
directly, instead of shelling out to the CLI. Each call connects a fresh
ephemeral peer to the network — **production by default** — (via `src/ops.js`,
the shared core behind both the CLI and this server), does one job, tears down,
and returns JSON.

| Tool | Args | Returns |
|---|---|---|
| `axona_publish` | `topic`, `message`, `region?` | `{ ok, msgId, … }` |
| `axona_subscribe` | `topic`, `region?`, `seconds?` (1–120), `since?` (`all`\|`new`) | `{ received, messages[] }` |
| `axona_pull` | `topic`, `region?` | `{ found, message, msgId }` |

Register it as a project-scoped MCP server — a `.mcp.json` at your project root:

```json
{
  "mcpServers": {
    "axona": { "command": "node", "args": ["/abs/path/to/axona-relay/src/mcp.js"] }
  }
}
```

**Targeting a different network.** The tools default to production. To point the
MCP server at the testnet (or an explicit bridge), add an `env` block — the same
`RELAY_NETWORK` / `BRIDGE_URL` precedence as the CLI applies:

```json
{
  "mcpServers": {
    "axona": {
      "command": "node",
      "args": ["/abs/path/to/axona-relay/src/mcp.js"],
      "env": { "RELAY_NETWORK": "testnet" }
    }
  }
}
```

or `claude mcp add axona -- node /abs/path/to/axona-relay/src/mcp.js`. Claude
Code loads MCP servers at startup and prompts once to approve a new
project-scoped server, so **restart / reconnect** after adding it; the tools
then appear as `mcp__axona__axona_publish`, etc. (Also exposed as the
`axona-mcp` bin.) Topic/region semantics and live-app interop are identical to
the CLI above.

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
Environment=RELAY_NETWORK=prod
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
  ops.js        shared connect + publish/subscribe/pull core (used by cli + mcp)
  cli.js        one-shot headless pub/sub/pull (JSON out) for scripts & agents
  mcp.js        MCP stdio server — axona_publish/subscribe/pull as native tools
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
