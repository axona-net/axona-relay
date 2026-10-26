// mcp-confirm.js — read-back confirmation for publishes.
//
// WHY: the kernel's peer.pub() resolves once the message is built and the
// root hint is warm; the routed PUB itself is fire-and-forget (AxonaPeer.pub
// calls pubsubPublish without awaiting it). So `publish` returning ok means
// "accepted on this node", nothing more. A process that publishes and then
// closes its peer kills the PUB in flight: five council posts were lost that
// way on 2026-10-25 (publish ok, never at the root). The only evidence that a
// message reached the topic's holder is reading it back by msgId.
//
// Pure functions over an injected `pull(msgId, { topic, timeoutMs })` so the
// contract is testable without a network. Time is injected too.

export const CONFIRM_TIMEOUT_MS = 8000;   // one publish: how long to keep asking the holder
export const CONFIRM_STEP_MS    = 1500;   // one pull attempt
export const DRAIN_BUDGET_MS    = 12000;  // shutdown: total time to spend on unconfirmed publishes

/**
 * Ask the holder for `msgId` until it answers or the budget runs out.
 * Returns { confirmed, attempts, elapsedMs, error }. `confirmed:false` means
 * NOT READ BACK within the budget — absence is not established.
 */
export async function confirmPublished({ pull, msgId, descriptor, timeoutMs = CONFIRM_TIMEOUT_MS, stepMs = CONFIRM_STEP_MS, now = Date.now }) {
  if (typeof pull !== 'function') throw new TypeError('confirmPublished: pull function required');
  if (typeof msgId !== 'string' || msgId.length !== 64) throw new TypeError('confirmPublished: 64-char msgId required');
  const startedAt = now();
  let attempts = 0, error = null;
  for (;;) {
    const remaining = timeoutMs - (now() - startedAt);
    if (remaining <= 0) break;
    attempts++;
    try {
      const env = await pull(msgId, { topic: descriptor, timeoutMs: Math.min(stepMs, remaining) });
      if (env && env.msgId === msgId) return { confirmed: true, attempts, elapsedMs: now() - startedAt, error: null };
    } catch (e) {
      error = String((e && e.message) || e);
    }
  }
  return { confirmed: false, attempts, elapsedMs: now() - startedAt, error };
}

/**
 * Before a peer closes: confirm every publish still in flight, oldest first,
 * within one total budget. Entries are removed from `inflight` as they are
 * settled; what the budget cannot reach stays listed as unconfirmed.
 */
export async function drainInflight({ inflight, pull, budgetMs = DRAIN_BUDGET_MS, stepMs = CONFIRM_STEP_MS, now = Date.now }) {
  const startedAt = now();
  const confirmed = [], unconfirmed = [];
  for (const [msgId, rec] of [...inflight]) {
    const remaining = budgetMs - (now() - startedAt);
    if (remaining <= 0) { unconfirmed.push({ msgId, topic: rec.topic, attempts: 0, reason: 'drain budget exhausted' }); continue; }
    const r = await confirmPublished({ pull, msgId, descriptor: rec.descriptor,
      timeoutMs: Math.min(remaining, rec.timeoutMs ?? CONFIRM_TIMEOUT_MS), stepMs, now });
    (r.confirmed ? confirmed : unconfirmed).push({ msgId, topic: rec.topic, attempts: r.attempts, elapsedMs: r.elapsedMs,
      reason: r.confirmed ? null : (r.error || 'not read back within budget') });
    inflight.delete(msgId);
  }
  return { confirmed, unconfirmed, elapsedMs: now() - startedAt };
}
