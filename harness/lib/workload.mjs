// =============================================================================
// harness/lib/workload.mjs — the seeded workload generator.
//
// Everything the two arms share comes from ONE seed: the topic map, the
// publisher rotation, the per-topic cadence schedule, and the payload
// manifest. The §7 attribution gate requires the arms to be identical here —
// same seed in, byte-identical plan out — so this module is deterministic by
// construction: no Date.now(), no Math.random(), no host identity. Hosts and
// wall-clock enter later, in the ledger, never in the plan.
//
// Spec: architecture/Axona-PubSub-Workload-Harness-v0.3.md (axona-docs
// dfd7fe8, §1). Open topics 10–20 chat-shape; owned topics 5–10 feed-shape;
// cadence 30–120s with occasional bursts.
// =============================================================================

// mulberry32 — small, seedable, good enough for plan generation (NOT crypto;
// identities and nonces are minted at runtime by the kernel, never from this).
export function rng(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const pick = (r, lo, hi) => lo + Math.floor(r() * (hi - lo + 1));

/**
 * Generate the full workload plan from a seed.
 *
 * @param {object} o
 * @param {number} o.seed        integer seed — THE identity of the plan
 * @param {number} o.nodes       harness-peer count (one sidecar per host slot)
 * @param {number} [o.openN]     open topic count (default seeded 10..20)
 * @param {number} [o.ownedN]    owned topic count (default seeded 5..10)
 * @param {number} o.durationMs  arm window length
 * @returns plan — topics, schedule (sorted by atMs), and per-topic
 *   required-reader sets (§7 denominator contract: predeclared, never
 *   inferred from who happened to answer).
 */
export function generatePlan({ seed, nodes, openN, ownedN, durationMs }) {
  if (!Number.isInteger(seed)) throw new Error('generatePlan: integer seed required');
  if (!Number.isInteger(nodes) || nodes < 3) throw new Error('generatePlan: nodes >= 3 required');
  if (!Number.isInteger(durationMs) || durationMs <= 0) throw new Error('generatePlan: durationMs required');
  const r = rng(seed);

  const nOpen  = openN  ?? pick(r, 10, 20);
  const nOwned = ownedN ?? pick(r, 5, 10);

  const topics = [];
  // Open topics: every harness node is a required reader; publishers rotate.
  for (let i = 0; i < nOpen; i++) {
    topics.push({
      kind: 'open',
      name: `harness/open-${seed}-${i}`,
      cadenceMs: pick(r, 30_000, 120_000),
      requiredReaders: Array.from({ length: nodes }, (_, n) => n),
      publishers: 'rotate',                    // resolved per-event below
    });
  }
  // Owned topics: one publisher, a seeded subscriber group of >= 3.
  for (let i = 0; i < nOwned; i++) {
    const publisher = pick(r, 0, nodes - 1);
    // Group size is bounded by the universe: at most nodes-1 subscribers
    // exist besides the publisher. Unbounded, nodes=3 demanded 4 distinct
    // indices from 3 and the builder spun forever — caught by a probe whose
    // phase marker showed 'plan' and never 'connecting'.
    const maxGroup = Math.min(Math.max(3, Math.floor(nodes / 2)), nodes - 1);
    const minGroup = Math.min(3, nodes - 1);
    const groupSize = pick(r, minGroup, Math.max(minGroup, maxGroup));
    const group = new Set([publisher]);
    while (group.size < groupSize + 1) group.add(pick(r, 0, nodes - 1));
    group.delete(publisher);
    topics.push({
      kind: 'owned',
      name: `harness/owned-${seed}-${i}`,
      cadenceMs: pick(r, 30_000, 90_000),
      requiredReaders: [...group].sort((a, b) => a - b),
      publishers: publisher,
    });
  }

  // Schedule: per topic, events at cadence with ±20% seeded jitter, plus a
  // burst (3 rapid publishes) roughly every 10th event. Publisher for open
  // topics rotates through the node set in seeded order.
  const schedule = [];
  topics.forEach((t, ti) => {
    let at = pick(r, 1_000, t.cadenceMs);     // stagger topic starts
    let seq = 0;
    while (at < durationMs) {
      const publisher = t.publishers === 'rotate' ? pick(r, 0, nodes - 1) : t.publishers;
      schedule.push({ atMs: at, topic: ti, seq: seq++, publisher, burst: false });
      if (seq % 10 === 0) {
        for (let b = 0; b < 3 && at + (b + 1) * 500 < durationMs; b++) {
          schedule.push({ atMs: at + (b + 1) * 500, topic: ti, seq: seq++, publisher, burst: true });
        }
      }
      const jitter = 1 + (r() - 0.5) * 0.4;
      at += Math.floor(t.cadenceMs * jitter);
    }
  });
  schedule.sort((a, b) => a.atMs - b.atMs || a.topic - b.topic || a.seq - b.seq);

  return { seed, nodes, durationMs, topics, schedule };
}

/** Canonical JSON for hashing/diffing a plan. Plain stringify IS canonical
 *  here: the generator builds every object with a fixed insertion order, so
 *  same seed → byte-identical string. (An array replacer would FILTER keys,
 *  not order them — caught before it shipped.) */
export function planCanonical(plan) {
  return JSON.stringify(plan);
}
