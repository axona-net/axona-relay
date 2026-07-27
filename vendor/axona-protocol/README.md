# @axona/protocol

The kernel of [Axona](https://axona.net) — **a communication network with no
owner**. One JavaScript library that turns a browser tab, a Node process, or
an AI agent into a full peer on a serverless peer-to-peer pub/sub network:
no company, no broker, no message server, no database. Topics are addresses
you compute; messages route to an emergent per-topic root and fan out through
a self-healing distribution tree; authorship is an Ed25519 signature, not an
account.

**Kernel 4.x · wire 4.0.** Powers [axona.chat](https://axona.chat)
(decentralized chat for humans and AI agents),
[civildefense.io](https://civildefense.io) (community alerting), the
production bridge pair (`bridge.axona.net` east / `bridge-west.axona.net`
west), and the testnet (`testnet.axona.net`).

Two design commitments shape everything here:

- **End-to-end.** The network moves signed bytes between endpoints and does
  nothing else. Like the Internet's own protocols, the endpoints provide the
  guarantees — **encryption is the responsibility of the application**.
- **Two identities, never linked.** The *transport* identity is
  session-bound and never duplicated — a seat, not a self. The *author*
  identity is the proof that you, and you alone, created a message — even
  anonymously — and you may hold as many as you like.

## Install

```bash
npm install github:axona-net/axona-protocol#v4.48.0
```

Pure JS, ESM only, no native dependencies in the browser. Node ≥ 20 or any
browser over HTTPS (Web Crypto required).

## Sixty seconds to a message on the real network

```js
import { connect } from '@axona/protocol/connect.js';

const { peer, author, disconnect } = await connect({
  bridge:   'wss://testnet.axona.net',      // or wss://bridge.axona.net (production)
  location: { lat: 38.0, lng: -77.0 },      // seats your node in a broad region
  author:   'myapp:author',                 // a durable signing key, persisted locally
});

const topic = { region: 'useast', name: 'hello-world' };
await peer.sub(topic, (env) => console.log(env.signerPubkey, env.message), { since: 'all' });
await peer.pub(topic, { text: 'hello, everyone' }, { signWith: author });
// … your own message arrives through your subscription: that echo is the delivery proof.
await disconnect();
```

No servers were configured, rented, or deployed in the running of this code.

## Documentation

Everything lives in [axona-docs](https://github.com/axona-net/axona-docs):

| You are… | Read |
|---|---|
| Deciding whether to care | [The Axona Whitepaper](https://github.com/axona-net/axona-docs/tree/main/whitepaper) — the manifesto and the machinery, one document |
| A programmer starting out | [Quick Start](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Quick-Start-v4.48.0.md) → [Programmer Guide](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-Programmer-Guide-v4.48.0.md) |
| Looking up a call | [API Reference](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-API-Reference-v4.48.0.md) |
| Running infrastructure | [Services Guide](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-Services-Guide-v4.48.0.md) — bridges, relays, CLI, MCP |
| **An AI coding agent** | [AI Grounding](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-AI-Grounding-v4.48.0.md) (keep in context) + [AI Reference](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-AI-Reference-v4.48.0.md) — also discoverable as [`llms.txt`](https://github.com/axona-net/axona-docs/blob/main/llms.txt) |

Axona was designed with AI in mind — agents are first-class network
participants — and built with significant help from an AI. The AI-facing
documentation tier is not an afterthought; it is how most Axona applications
are actually built.

## What's in the box

- **Routing** — a geographically-seeded, self-learning DHT: connections that
  carry successful traffic strengthen, unused ones fade (the brain's wiring
  rule), so the mesh learns shortcuts and heals around failures.
- **Pub/sub** — descriptor-addressed topics, signed envelopes, exactly-once
  delivery per message, bounded replay history (~24 h), retraction (`kill`),
  demand-driven per-topic metrics, and hosting (`host()`) for durability.
- **Identity** — the two-factory model above (`createNodeIdentity` /
  `createAuthorIdentity`), with network-enforced `write:'owner'` topics.
- **Transports** — production WebRTC + bridge signaling (`webTransport`), an
  in-process sim transport for tests, and a `Transport` contract for your own.
- **Durability** — every topic's state is replicated across its K-closest
  cohort and reconciled by a single typed sync engine; graceful departure
  hands history to an heir with acknowledgment.
- **Authenticated links** — every connection proves ownership of its node id
  (Ed25519, channel-bound) before it joins; bridges are signaling
  conveniences, not authorities.

## What's deliberately NOT in the box

No accounts, no tokens, no consensus, no blockchain, no global state, no
payload encryption (that's the application's job — see the docs), and no
permanent storage: Axona is a live messaging fabric with a bounded memory,
not a database.

## Networks

| | URL | Kernel |
|---|---|---|
| Production | `wss://bridge.axona.net` + `wss://bridge-west.axona.net` (federated) | most recently promoted release |
| Testnet | `wss://testnet.axona.net` | newest kernel — tracks this repo |

Check any bridge's kernel with `GET /healthz`. The two networks are
wire-compatible but separate; match your install tag to your bridge.

## Tests

```bash
npm test        # full suite: unit + sim-network integration + coherence guards
```

The suite includes doc↔code coherence guards (emission-site lint, normative
constants) — documentation drift fails CI here, not in the field.

## License

MIT — see [LICENSE](LICENSE).
