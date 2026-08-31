# Proposal: a subscriber's pull should not route to the root

*Draft for council, 2026-08-31. Prompted by David: "the axon tree has the same
information." Kernel change to `peer.pull` / `requestPull`.*

## The question

What does it cost a subscriber to read the head of a topic it already subscribes
to, and does that read need to leave the node at all?

Today it leaves the node and travels to the root. `peer.pull(msgId, {topic})`
([AxonaPeer.js](../vendor/axona-protocol/src/dht/AxonaPeer.js) `pull`) calls
`requestPull` ([AxonaManager.js](../vendor/axona-protocol/src/pubsub/AxonaManager.js)
`requestPull`), which sends a `T.PULL` message `via: [rootHint]` — the warm
lookup-assist toward the topic's true root — and waits for a `PULLRESP`. There is
no check for "do I subscribe to this topic" and no consultation of the local
delivery cache. Every pull is a routed read to the root.

For a topic the caller does **not** subscribe to, that is correct: the caller has
no position on the tree, so it must route to find the data. For a topic the caller
**does** subscribe to, it is wrong twice over.

## Why it is wrong for a subscriber

A subscriber sits on the topic's axon tree. It receives every message through its
subscription callback — the root's live fanout push, and any renewal replay — and
its upstream parent holds the same messages one hop away. The tree already
delivered the data. Routing a head-read to the root asks a distant node for
something the local node, or its parent, already has.

Two costs follow.

**It contends where it must not.** The pull walks to the root and competes there
with the live delivery push and with every other node's routing. That contention
is not CPU — it is root and lookup occupancy. Measured: a head-sweep that pulled
every subscribed topic every 10s instead of every 60s dropped watch-delivery from
97.5% to 67.5% at fixed successor density, with flat CPU and only +8% memory. A
modest pull rate is enough when it lands on the one node the whole tree depends on.

**It reads the wrong node.** The pull returns the root's head, not the reader's.
The root holding message N is not the reader having received N. Any consumer that
treats a routed-pull head as "what I have" is reading the root's memory and calling
it its own delivery. The kernel's own comment on the pull path notes the failure's
other face: a pull that strands on a local minimum reaches a non-cohort node and
returns a false null "even though the cohort holds it."

## The change

When the caller subscribes to the topic (`this.mySubscriptions.has(topicBig)`),
serve the pull without routing to the root:

- **Local** — return from the subscription's own ingested cache: the highest
  message it has delivered (for `pull(null)` = latest) or a specific `msgId` it
  holds. Zero network. This answers "what have I received?", which is the question
  a subscriber's pull is actually asking.
- **Upstream** — for the authoritative tree head rather than the local view, query
  `this._upstream.get(topicBig)` — the parent, one hop — instead of the root. This
  answers "is there anything above me I have not yet been pushed?" without touching
  the root and without a multi-hop walk that can strand.

A non-subscribed pull is unchanged: it still routes to the root, because a
non-subscriber has no tree position to read from.

## Semantics and compatibility

- `pull(msgId)` for a message the subscription holds → local hit, no network.
- `pull(null)` "latest" → local head; or, if the caller wants the authoritative
  latest (to detect a message pushed to the tree but not yet to this leaf), the
  one-hop upstream read.
- Non-subscriber pulls: byte-identical to today.
- Gate it OFF by default (opt-in, version-gated) so the change lands as a canary,
  not a silent behavior swap — the same discipline as the arming stack.

## Why it is worth doing

This is not only a harness fix. Every application that pulls after subscribing —
a "did I miss this one?" check, a resync on reconnect — pays a full root route
today for data the tree already delivered to it, and inherits the local-minimum
false-null on a churned mesh. A subscriber's pull served locally or one hop up is
faster, cheaper, cannot false-null, and removes a source of root contention from
the live system, not just from measurement.

## Test

- A subscribed `pull` issues no root-routed `T.PULL` (assert on the wire).
- It returns the locally-held head and matches the subscription's delivered set.
- A non-subscribed `pull` is unchanged (still routes; same result as today).
- Under churn, a subscribed `pull` never returns a false null while the local
  cache holds the message.

## Open questions for council

1. Local read vs one-hop upstream as the default for `pull(null)` on a subscribed
   topic — the local view is free but lags the tree head by the push latency; the
   upstream read is one hop but authoritative.
2. Opt-in gate vs default-on once proven — this changes a read path many apps use.
