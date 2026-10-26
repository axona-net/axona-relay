// mcp-watch.js — the arrival buffer behind axona_watch / axona_poll, as pure
// functions over a plain watch record so the behaviour can be fenced without a
// peer. mcp-session.js owns the subscription; this file owns what a buffer
// means.
//
// WHY THIS EXISTS (2026-10-26). A seat reported "the active council watch has
// zero review messages" while the topic held 170 of them. Its buffer had been
// drained by earlier polls, and the two tools gave it no way to tell "drained"
// from "nothing was posted": axona_watch on an already-watched topic was a
// no-op, so nothing could refill the buffer short of a full reconnect, and an
// empty poll carried no explanation. Both are fixed here:
//   • deliver() tracks every msgId the watch has ever buffered. A message that
//     arrives again (a replay the seat asked for) goes back into the buffer
//     unless it is still sitting there, and is counted as `replayed`, never as a
//     new arrival — so `total` keeps meaning "distinct messages seen".
//   • collect() counts what was drained and, when it returns nothing, says how
//     many arrived, how many were drained, and how to get them back.
//   • settle() lets the replay path wait for the backlog to land before it
//     reports how much came back.

export const BUFFER_CAP_DEFAULT = 1000;
export const SEEN_CAP = 50_000;

export function makeWatch({ topic, region, descriptor, since = 'all', now = Date.now, bufferCap = BUFFER_CAP_DEFAULT }) {
  return {
    topic, region, descriptor, since,
    startedAt: now(),
    buffer: [], waiters: [],
    total: 0,        // distinct messages ever buffered (first arrival only)
    replayed: 0,     // re-buffered on a requested replay after being drained
    drained: 0,      // cumulative messages handed to polls
    dropped: 0,      // evicted by the cap before being polled
    replays: 0, lastReplayAt: null,
    seen: new Set(), seenOrder: [],
    bufferCap: Number(bufferCap) || BUFFER_CAP_DEFAULT,
  };
}

/**
 * Record an arrival. Returns { buffered, duplicate }.
 *  - unknown msgId → buffered, total += 1
 *  - known msgId, not currently buffered → re-buffered (a replay refill), replayed += 1
 *  - known msgId, still buffered → ignored (duplicate: true, buffered: false)
 * A message without a msgId is always treated as new.
 */
export function deliver(w, m) {
  const id = m?.msgId ?? null;
  const known = id !== null && w.seen.has(id);
  if (known) {
    if (w.buffer.some((x) => x.msgId === id)) return { buffered: false, duplicate: true };
    w.replayed += 1;
  } else {
    w.total += 1;
    if (id !== null) {
      w.seen.add(id); w.seenOrder.push(id);
      if (w.seenOrder.length > SEEN_CAP) w.seen.delete(w.seenOrder.shift());
    }
  }
  w.buffer.push(m);
  if (w.buffer.length > w.bufferCap) { w.buffer.shift(); w.dropped += 1; }
  return { buffered: true, duplicate: known };
}

export function emptyHint(w, now = Date.now) {
  const age = Math.max(0, Math.round((now() - w.startedAt) / 1000));
  if (w.total === 0) {
    return `buffer empty: nothing has arrived on ${w.region}|${w.topic} since this watch opened ${age}s ago (since:"${w.since}"). ` +
      `If the topic should have history, call axona_watch {topic:"${w.topic}", since:"all"} to request a replay, or axona_pull a known msgId.`;
  }
  return `buffer empty: ${w.total} message(s) arrived since this watch opened ${age}s ago and ${w.drained} were drained by earlier polls` +
    (w.dropped ? ` (${w.dropped} evicted by the ${w.bufferCap}-message cap)` : '') +
    `. They are NOT lost: call axona_watch {topic:"${w.topic}", since:"all"} to replay them into this buffer, or axona_pull a known msgId.`;
}

/** Drain (or peek) a watch. The result always says what the counters mean; an empty result carries a hint. */
export function collect(w, { peek = false, max, now = Date.now } = {}) {
  const out = max ? w.buffer.slice(0, Number(max)) : w.buffer.slice();
  if (!peek) {
    if (max) w.buffer.splice(0, out.length); else w.buffer.length = 0;
    w.drained += out.length;
  }
  const r = {
    topic: w.topic, region: w.region,
    drained: peek ? 0 : out.length, remaining: w.buffer.length,
    total: w.total, replayed: w.replayed, drainedTotal: w.drained, dropped: w.dropped,
    startedAt: new Date(w.startedAt).toISOString(),
    messages: out,
  };
  if (!out.length) r.hint = emptyHint(w, now);
  return r;
}

/** Wait until arrivals stop growing for `quietMs`, or `maxMs` elapses. Returns { elapsedMs, arrived }. */
export async function settle(w, { maxMs = 2000, quietMs = 400, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), now = Date.now } = {}) {
  const count = () => w.total + w.replayed;
  const t0 = now(); const start = count();
  let last = start; let lastChange = t0;
  while (now() - t0 < maxMs) {
    await sleep(100);
    const cur = count();
    if (cur !== last) { last = cur; lastChange = now(); }
    else if (now() - lastChange >= quietMs) break;
  }
  return { elapsedMs: now() - t0, arrived: count() - start };
}
