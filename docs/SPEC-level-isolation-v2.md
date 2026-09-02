# Level-Isolation Diagnosis — Build Spec v2

- Draft: `AXONABOT-COUNCIL-20260901-LEVEL-ISOLATION-01`, rev 2 (council-corrected)
- Kernel deployed on testnet: **4.71.0** (wire 4.0)
- Scope: **testnet only**. No prod change. Diagnostic build; nothing here ships to prod.
- Council: proposal `65756eed` → Vega `0e7a8988` → Orion `aac74797` → Aster review `5802a22e` → adoption `ab170a96` → Vega `cbf3899a`. All five Aster corrections folded.
- Gates: the fleet-admission deploy, Gate 4, and Gate 6 remain David's.

## Why this exists

We measured the live delivery gap four ways. Each time the loss landed in one
bucket we could not open: forwarding-path non-arrival on a structurally sound
tree, mechanism unresolved. Two facts keep it closed. We do not see every hop,
so a missing receipt is ambiguous. And the mesh may hold nodes we neither
control nor instrument, so some hops cannot be seen at all.

Adding telemetry to the fanout layer has not moved this. The pivot: make the
fleet fully ours and fully logged, then test each layer from the bottom up. The
first level that is not sound is where the defect lives, and where the fix goes.
No level above it is instrumented further until the one below reads clean.

## A — Close the fleet (authenticated, not version-gated)

A kernel-version gate is necessary and not sufficient. A foreign peer can
advertise 4.71.0, and bridge admission does not prove that no peer entered
through another discovery or reconnect path. Host and IP resolution are
supporting evidence, not identity.

**Mechanism.** A run-scoped authenticated allowlist of exact node identities and
process nonces:

- The allowlist is the set of our processes for the run, keyed by node identity + `processNonce`.
- Enforced at **every** testnet ingress and peer handshake, not the bridge alone.
- Alternate discovery paths are disabled for the arm.
- Minimum kernel 4.71.0 still required, as a floor beneath the allowlist.
- **Two-way census, both directions proven:** every connected peer is on the allowlist, and every allowlisted live process is present and connected. A gap either way voids the arm.

**Scope fence.** The testnet bridge (`testnet.axona.net` droplet) only. The prod
bridge (`bridge.axona.net`) stays open and untouched — a version or allowlist
gate there would lock out the apps and the council peers.

**Privacy fence.** Transport-id and node-identity logging is a testnet diagnostic,
gated, and never enabled on prod. It does not change the standing protocol
invariant that a transport id is never persisted into a protocol frame.

## B — Per-node message-transition ledger

Every node is ours, so every node records what it did with each message. The
ledger replaces the per-hop tx/rx pairing that keeps voiding: a transport send
resolves on its reply, so "sent" and "round-tripped" are one event. Instead we
write two independent one-way records and join them on the edge — the sender's
forward-with-outcome, and the receiver's arrival. That yields, per hop, both
whether the frame was sent and whether it was received.

**Every row carries the run/epoch frame:** `runId`, `membershipEpoch`,
`processNonce`, and the immutable `membershipDigest` for that epoch. A membership
change, channel replacement, or duplicate identity closes the epoch and opens a
new one; rows never cross epochs.

**Sender record — one per intended downstream edge:**

| field | meaning |
|---|---|
| `msgId` / `publishNonce` | message and publish-instance identity |
| `edgeAttemptId` | unique id for this send attempt on this edge |
| `attemptOrdinal` | 1st, 2nd… attempt for the same intended edge |
| `fromTransportId` / `toTransportId` | this node and the intended downstream |
| `enqueueT` | when the send entered the scheduler/queue |
| `sendAttemptT` | when it was dequeued and handed to transport |
| `syncReturn` | the synchronous return of the send call |
| `asyncOutcome` | `accepted` / `delivered` / `failed`, with time and reason |
| `connId` / `sessionId` | the channel it went out on |
| `disposition` | includes an explicit **`not-attempted`** when an intended downstream never leaves the scheduler |

**Receiver record — one per arrival:**

| field | meaning |
|---|---|
| `msgId` / `publishNonce` | message identity |
| `edgeAttemptId` | the sender's attempt id, echoed if carried, for exact-edge join |
| `fromTransportId` / `selfTransportId` | upstream sender and this receiver |
| `arrivalT` | wall and monotonic arrival time |
| `role` | root / relay / leaf |
| `topicId` | topic anchor |

**Integrity.** Append-only files, one per process, with a monotonic per-process
sequence number on every row and an end-of-run **completeness manifest** (first
and last sequence numbers, row count). A missing tail record is then
distinguishable from a lost frame. Harvested per run; joined by `msgId` and
`edgeAttemptId` across processes to reconstruct each message's full path as a
graph.

**What the queue boundary buys.** `enqueueT` vs `sendAttemptT` vs `asyncOutcome`
separates the three states that used to collapse into one residual:

1. **not-attempted** — the scheduler never dequeued it (omission before send).
2. **attempted-then-failed** — dequeued, transport returned or called back failure.
3. **accepted-not-received** — transport accepted it, no matching receiver arrival.

State 3, and only state 3, is a candidate for physical in-transit loss. States 1
and 2 are local and were invisible before.

## C — Bottom-up level tests

Each level is gated on the one below it clearing. Clearing means the Wilson 95%
upper bound on loss sits **below a pre-declared, layer-specific tolerance** — not
merely "zero observed in N≥125." The test matrix randomizes direction, host pair,
path length, payload size, connection age, and time block, so a run of easy cases
cannot clear a leaking layer.

| level | what it exercises | a miss means |
|---|---|---|
| **L0** membership | after A, the mesh is exactly our allowlisted nodes | the fleet is not closed; do not proceed |
| **L1** transport, one hop | an **already-open** direct channel, no overlay lookup | transport / framing fault |
| **L2** routing to a known node | the **exact** multi-hop routing primitive beneath placement, topic and root selection bypassed (API path named explicitly) | overlay pathing / next-hop table fault |
| **L3** topic placement (the engine) | (a) root-set **selection**, scored against the frozen eligible snapshot using the protocol's real distance metric, tie-break, replication factor, and eligibility filters; (b) **delivery** to that selected set | keyspace-distance / root-election divergence (a), or delivery fault to a correctly selected set (b) |
| **L4** fanout | full tree from a **verified** selected root | tree-branch maintenance / forwarding-scheduler queueing |

L3 is the neuromorphic engine under test. It is scored only after L1 and L2 read
clean, so a failure there cannot be blamed on the layers beneath it. Selection
and delivery are scored apart: a correct selection that then fails to deliver is
a different defect from a wrong selection.

## The caveat that governs interpretation

A closed fleet measures **mechanism, not production reliability.** Excluding
foreign peers deliberately changes the topology, and it may remove the very
condition that triggers the defect. If the loss falls toward zero once the fleet
is closed, that is not a clean bill of health — it is evidence the trigger
involves the peers we removed (an un-instrumented forwarder, a throttled
background tab, a version-skewed client in the path). A disappearance relocates
the question to a level we have not been examining: the mesh's behaviour among
heterogeneous, uncontrolled participants. Every result from this experiment is
reported as isolation evidence, with that boundary stated.

## Fault-localization map (Orion)

- L1 leaks → physical transport / framing.
- L2 leaks → overlay pathing / next-hop table.
- L3 leaks → keyspace-distance calculation or root-election divergence.
- L1–L3 clean and L4 leaks → tree-branch maintenance or forwarding-scheduler queueing.

## Governance

Fleet-admission deploy (A), the ledger build (B), and any arming are David's to
sanction. Gate 4 and Gate 6 remain strictly held. The forward-push-loss
hypothesis stays open, now structured to resolve across L1–L4 instead of sitting
in one bucket.
