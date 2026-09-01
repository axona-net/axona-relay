# Proposal: recover a missed message on the renewal that already runs

*Draft for council, 2026-09-01. Prompted by David: the renewal already carries a
most-recent time; add the last-received sequence number and a list of the earlier
messages still missing, and the renewal becomes the gap-fill. Kernel change to the
subscription renewal payload and the root's replay.*

## The question

What does it take to get a dropped message to a subscriber before its deadline,
without re-sending the messages the subscriber already has?

## The gap it closes

On a static live fleet, pub/sub delivered ~70% of (message, reader) pairs inside
the 120s completeness deadline. The sim delivered 100% until forward-push loss was
injected, and ~15-20% effective push loss reproduced the 70%, with the same
signature: eventual delivery stays ~100%, delivered-late p95 in seconds. The
messages are NOT lost. A push that drops falls to the renewal replay, and the
renewal runs every 60s, so recovery can take longer than the 120s deadline allows.
The defect is recovery latency, not loss.

This is not a routing problem and not a mesh problem. A realistically-formed mesh
with a measured 2-10% greedy-strand rate still delivers 100% with no churn in the
sim, because the root-set, the backups, and the renewal absorb the strands. The
loss sits on the transport, after the tree is built.

## The mechanism: three fields on the renewal

The renewal already carries one field: a since-timestamp, the time of the
most-recent message the subscriber holds. Add two more.

1. **missing-list** — the earlier messages the subscriber has already detected as
   gaps. A subscriber knows message 6 is missing the moment message 7 arrives
   above it.
2. **high-water sequence** — the highest sequence number the subscriber has
   received.

The root holds the topic's head sequence. On the renewal it replays exactly the
named missing-list plus everything above the high-water. A subscriber holding
1-5, 7, 8 sends `missing=[6], high-water=8`, and the root sends 6 and 9..head.
Nothing the subscriber already has.

## Why it takes both fields

Detection splits between the two ends, and each end sees only half.

- The **subscriber** reports the mid-stream gaps it can see. A message above a
  hole proves the hole exists, so a scattered miss is detectable at once.
- The **root** reports the tail only it can see. A subscriber caught up to 5
  cannot tell a dropped 6 from an unpublished 6; no arrival ever proves that gap.
  The root, at head 9, knows the instant the renewal says "I am at 5."

Plain sequence numbers catch the mid-stream case and leave the tail open. A plain
timestamp watermark catches the tail as a coarse window and re-sends everything
newer than the oldest hole. The two fields together catch both, exactly: the
subscriber names its holes, the root fills its tail.

## Watermark semantics

A scalar "largest sequence seen" is unsafe when delivery is sparse or reordered:
it hides every hole below itself. The renewal carries a **contiguous
acknowledgement frontier** (the high-water: the highest sequence below which the
subscriber holds everything) plus **explicit missing ranges** (the missing-list).
The pair is exact where the scalar is not.

Pinned semantics:

- **Epoch scope.** The frontier and the missing-list are scoped to one
  subscription epoch and one topic ordering domain. A resubscribe that opens a new
  epoch resets both; a root migration that changes the ordering domain is a new
  epoch, so a frontier from the old root is never applied against the new root's
  head.
- **Bounds.** The frontier is inclusive (the subscriber holds the frontier
  sequence). The root replays strictly above it, and the named ranges exactly. A
  range `[3-4]` means 3 and 4 inclusive.
- **Holes below the frontier.** By construction there are none: the frontier is
  the highest sequence with no gap beneath it. A hole moves the frontier down to
  just below itself and is named in the missing-list until filled.
- **Out-of-order arrival.** A message that arrives above a still-open hole does
  not advance the frontier; it is held, and the hole stays named until the gap
  fills, at which point the frontier jumps to the new contiguous top.
- **Migration and reconnect.** On reconnect the subscriber renews with its last
  frontier and missing-list under the prior epoch; if the root is new, the new
  epoch starts from the subscriber's frontier and the root fills forward from
  there, so a reconnect never silently drops the gap it was carrying.
- **Tombstones.** A killed message advances the frontier like any delivered
  sequence — the subscriber has resolved that position — and is never re-requested.
- **Cache truncation.** If the root has aged a named range past its 24h hold, it
  answers with a truncation marker for that range instead of the body, so the
  subscriber records the position resolved and stops asking. A gap the root can no
  longer serve is closed, not retried forever.

## Delivery path

The replayed messages arrive through the DELIVER→watch path, the same path as a
forward push, with subscription epoch and ordering preserved, deduplicated by
(epoch, msgId). A gap-fill receipt is credited when the message reaches the
subscriber's callback, never because an upstream value was inspectable. So the
renewal is the recovery channel. There is no new message type and no separate
request protocol.

## Fire it faster than 60s

The renewal runs on a fixed 60s cycle today. Recovery needs it to fire when there
is a gap to fill, not on a clock. A subscriber that detects a mid-stream gap can
renew at once. A head-sequence carried in each push, or a small periodic head
beacon, lets a subscriber detect a tail gap and renew without waiting the full
cycle. The head-sequence is the one extra signal the tail case needs; without it
the tail is invisible until the next scheduled renewal.

## The false-gap grace

A message that is merely slow, not lost, must not be re-requested. Hold a detected
gap for a grace interval before naming it missing. The grace trades recovery
latency against duplicate re-fetches: a short grace recovers faster and re-fetches
more slow-but-present messages, a long grace does the reverse.

## Payload bound

The missing-list is small in the common case, a handful of recent gaps on a
message already being sent. A subscriber that falls far behind caps the list and
range-encodes it (`[3-4, 9-12]`), or falls back to the since-timestamp floor for
the deep history. The payload stays bounded no matter how far behind a reader gets.

## Evidence

Measured in dht-sim (testnet `a14be7a`, `harness/pubsub-gapfill.mjs`), 20% flat
forward-push loss, live cadence (renewal 60s, deadline 120s), realistically-formed
mesh, no churn. Three recovery mechanisms, each at three recovery delays D:

| D | mechanism | completeness @120s | p99 | duplicate trials |
|---|---|---|---|---|
| 30s | since:'all' | 100% | 31s | 45% |
| | since:watermark | 99.0% | 39s | 28% |
| | three-field | 100% | 61s | 1.4% |
| 10s | since:'all' | 99.9% | 17s | 69% |
| | since:watermark | 100% | 18s | 46% |
| | three-field | 100% | 22s | 9.9% |
| 5s | since:'all' | 100% | 12s | 79% |
| | since:watermark | 100% | 11s | 50% |
| | three-field | 100% | 12s | 11% |

Completeness holds at 100% under the three-field renewal at every delay tested.
Duplicate trials fall from 79% (full `since:'all'` resub at a 5s grace) to 11%,
and from 45% to 1.4% at a 30s grace. The residual duplicates are false-gap: at a
5s grace the subscriber re-fetches messages that were slow, not lost, and the slow
original then also lands; the rate falls to 1.4% at a 30s grace. p99 recovery
tracks the grace, with a tail near twice the grace when a replayed message itself
drops and waits the next renewal, so 61s at a 30s grace, inside the 120s deadline.

## What would make this wrong

- The 20% is a flat effective loss, not the live joint per-hop-by-route-length
  distribution. That distribution is unmeasured until the paired
  `deliver:hop_tx` / `deliver:hop_rx` telemetry ships and one instrumented fleet
  run records it. The acceptance run replays the measured distribution, not a flat
  rate.
- The three-field mode is modeled, not run on the kernel. The kernel `since:` is
  timestamp-only and carries no seq-list renewal yet, so the harness models the
  root replaying exactly the named set under the same per-message loss. The numbers
  show what the proposed kernel behavior would achieve, not a kernel measurement.
- If the live per-hop DELIVER drop measures below 3% across routes of 3 hops or
  fewer, the push-loss diagnosis is retired and this recovery solves a problem the
  fleet does not have. The pivot is callback accounting, publish-confirm/root-set
  divergence, replay eligibility, and subscription-state discontinuity.

## Governance

Sim exploration is done and council-reviewed. The kernel change is David's gate:
renewal payload fields, the root-side set arithmetic, the head-sequence signal,
and the false-gap grace. Any fleet arming, and Gates 4 and 6, are his too.
