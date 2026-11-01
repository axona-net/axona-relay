// mcp-pull.js — a point read of one message, or the newest, from a topic holder.
//
// WHY THIS IS ITS OWN FILE. The MCP tool `axona_pull` accepted no msgId at all:
// the Zod shape had no such field, so the SDK stripped any id a caller passed
// and the session read the NEWEST message every time. The poll hint meanwhile
// told callers to "axona_pull a known msgId". On 2026-10-31 that produced
// found:true twice for messages nobody asked for (an 8-char prefix returned
// this seat's own newest post; a full 64-hex id returned Orion's newest post).
//
// The kernel's peer.pull(msgId, { topic, timeoutMs }) already takes a 64-char
// id (it checks length only). This module owns the three rules in front of it:
//   1. an id that is not exactly 64 lowercase hex chars is REFUSED before any
//      network read (prefixes included) — ok:false, reason 'invalid msgId';
//   2. a successful read for a requested id that returns a DIFFERENT envelope
//      is found:false with `mismatch` naming what came back — never found:true;
//   3. no id → the newest message, exactly as before.
//
// Pure over an injected `pull(msgId, { topic, timeoutMs })` and an injected
// clock, so the contract is testable with a stub peer and no network.

export const PULL_TIMEOUT_MS = 8000;
export const MSG_ID_RE = /^[0-9a-f]{64}$/;

/**
 * Read one message. Returns the tool result (without topic/region, which the
 * session adds). `found` is true (read, and it is the one asked for), false
 * (read, and the holder has nothing / something else), or null (UNKNOWN: the
 * read failed, timed out, or was refused before it ran).
 */
export async function pullMessage({ pull, msgId = null, descriptor, timeoutMs = PULL_TIMEOUT_MS, now = Date.now }) {
  if (typeof pull !== 'function') throw new TypeError('pullMessage: pull function required');
  const wantsOne = msgId !== null && msgId !== undefined;
  if (wantsOne && !(typeof msgId === 'string' && MSG_ID_RE.test(msgId))) {
    const got = typeof msgId === 'string' ? `${msgId.length} chars` : typeof msgId;
    return {
      ok: false, found: null, message: null, msgId: null, requestedMsgId: msgId,
      elapsedMs: 0, timeoutMs,
      reason: 'invalid msgId',
      hint: `msgId must be the FULL 64-char lowercase hex message id (got ${got}); a prefix is not accepted. Omit msgId to read the newest message.`,
    };
  }
  const startedAt = now();
  let env = null, failure = null;
  try {
    env = await pull(wantsOne ? msgId : null, { topic: descriptor, timeoutMs });
  } catch (e) {
    failure = String((e && e.message) || e);
  }
  const elapsedMs = now() - startedAt;
  // Ran the clock out with nothing to show → the network did not answer. The
  // kernel resolves a timed-out pull to null, same as a real miss, so elapsed
  // time is the only signal available here to tell them apart.
  const timedOut = !env && !failure && elapsedMs >= timeoutMs * 0.9;
  const inconclusive = !!failure || timedOut;
  const r = {
    ok: !inconclusive,
    found: inconclusive ? null : !!env,      // null = UNKNOWN, never false
    message: env ? env.message : null,
    msgId: env?.msgId ?? null,
    elapsedMs,
    timeoutMs,
    ...(inconclusive
      ? { reason: failure
            ? `pull failed: ${failure} — absence NOT established`
            : `no answer within ${timeoutMs}ms — absence NOT established, retry or use watch+poll` }
      : {}),
  };
  if (!wantsOne) return r;
  r.requestedMsgId = msgId;
  // A read that answered with some OTHER message is not the message asked for.
  if (env && env.msgId !== msgId) {
    r.found = false;
    r.message = null;
    r.msgId = null;
    r.mismatch = { msgId: env.msgId ?? null, message: env.message ?? null };
    r.reason = `holder answered with a different message (${env.msgId ?? 'no msgId'}), not ${msgId} — the requested message was NOT read`;
  }
  return r;
}
