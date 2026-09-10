// =============================================================================
// logctx.js — how a log line's context object is rendered, and what may be cut.
//
// A flat 120-char slice on every ctx cost four investigations in one day
// (2026-09-07): health-dump lost its `seated` array, routed-outcomes lost
// top[2], replicate-all-failed lost `targets`, and kill-replicate-all-failed
// carries the same shape and would have been the fifth. Each time the cure was
// to add another event NAME to an allowlist — which only ever fixes the surface
// that already bit you, and leaves the next one to be found the same way: by
// reading a production line and noticing it ends mid-token.
//
// So the rule is on the CONTENT, not the name.
//
//   A ctx that carries an ARRAY or a NESTED OBJECT is EVIDENCE. Something in
//   the kernel enumerated WHO or WHICH, and a slice through that list does not
//   shorten the record — it invalidates it, mid-token, while still reading as
//   complete. That last part is what makes it expensive: a severed line looks
//   like a whole one, so it gets believed.
//
//   A flat scalar ctx is a status line. Dropping its tail loses a field but
//   cannot fabricate one, so the cheap cap stays there — the transport events
//   are chatty and that is what the cap was written for.
//
// ONE INVARIANT, whichever branch runs: the string returned is ALWAYS parseable
// JSON. When evidence exceeds EVIDENCE_CAP we drop whole VALUES and say how
// many entries went, rather than cutting the text at a byte offset.
//
// -----------------------------------------------------------------------------
// 0.127.0 — the reduction is LARGEST-FIRST, and health-dump is exempt (GH #63).
//
// The first cut of that reduction replaced EVERY structured value as soon as the
// record went over. Reading the lookahead census off the prod droplets for #62,
// eleven relays came back whole and the twelfth did not:
//
//   health-dump {"peers":54,…,"lookahead":"<object omitted: ctx over 4000c>",
//                "seated":"<27 entries omitted: ctx over 4000c>"}
//
// `sfo3/useast` seats 27 roles. `seated` alone blew the cap; `lookahead` was
// collateral, dropped for being an object in the same record as an array it has
// nothing to do with.
//
// Both values that can overflow grow with how much work the relay is doing —
// `seated` with roles seated, the census `byRank` with routing traffic. So an
// all-or-nothing reduction degrades MONOTONICALLY WITH LOAD and stops reporting
// at exactly the relays worth reading. Every idle relay reported in full; the
// most heavily seated one reported nothing structured. A blind spot in the
// diagnostic that exists to observe load, correlated with load.
//
// So: sort the structured values by rendered size, drop from the largest, and
// STOP the moment the record fits.
//
// MEASURE IT BEFORE BELIEVING IT. I first wrote on #63 that largest-first would
// drop `seated` and keep the census. It does the opposite. On the real shape —
// pinned in the fence at 4,082 characters, 82 over — the census is 2,187 bytes
// and `seated` is 1,628, so the census is the LARGER value and largest-first
// drops exactly the thing I wanted to read. Both were omitted on the observed
// line, so nothing in that output said which was bigger, and I assumed.
//
// Largest-first is still the right reduction: it preserves the most evidence it
// can instead of destroying all of it. It is NOT the fix for this record.
//
// The fix for this record is the second change. health-dump joins armed-* as a
// standing full-fidelity contract, so nothing in it is reduced at any size. The
// cap was written for the chatty transport events; health-dump is emitted once,
// on an explicit SIGUSR1, to an operator already reading the log. It is not a
// volume risk. That is a NAME rule and it is deliberate — the shape rule above
// cannot express "this event is rare because a human asked for it".
//
// So the two changes are not redundant and they are not interchangeable. The
// exemption is what makes the fleet readable. Largest-first is what stops the
// next surface being found the way this one was: `replicate-all-failed` carries
// a `targets` array with the same property, and adding event names one at a
// time is the failure this module was written to end.
// =============================================================================

export const CTX_CAP      = 120;    // flat scalar ctx — the chatty transport events
export const EVIDENCE_CAP = 4_000;  // structured ctx — bounded, but never severed

// Events emitted whole whatever their size or shape. Both entries are here for
// the same reason: they are read by somebody who asked for them, and neither is
// on a hot path. `armed-*` is a prefix because the canary runbook reads its
// thresholds out of a family of them; `health-dump` is exact, because guessing
// at suffixes is how an allowlist quietly grows to cover things nobody checked.
const FULL_FIDELITY_EVENTS = new Set(['health-dump']);

/** True when `event` is emitted at full fidelity regardless of size or shape. */
export function isFullFidelity(event) {
  return typeof event === 'string'
    && (event.startsWith('armed-') || FULL_FIDELITY_EVENTS.has(event));
}

/** True when `ctx` carries an array or nested object — i.e. it enumerates. */
export function isEvidence(ctx) {
  return ctx !== null && typeof ctx === 'object'
    && Object.values(ctx).some((v) => v !== null && typeof v === 'object');
}

/**
 * Render a log context for emission.
 * @param {string} event  event name (armed-* is a standing full-fidelity contract)
 * @param {object} ctx    the context object
 * @returns {string}      always-parseable JSON
 */
export function renderCtx(event, ctx) {
  const json = JSON.stringify(ctx);
  // Standing contracts, independent of shape: the canary soak runbook reads its
  // thresholds out of armed-*, and health-dump is an operator-requested SIGUSR1
  // diagnostic. Neither is ever reduced. See the header note for #63.
  if (isFullFidelity(event)) return json;
  if (json.length <= CTX_CAP) return json;

  // Flat scalar ctx: still capped, but cut the VALUES, not the JSON text.
  // Slicing the rendered string at a byte offset was the original sin — it is
  // what produced `..."kids` and taught a reader that the record ended there.
  // Trimming a value leaves the object well-formed and marks where it stopped.
  if (!isEvidence(ctx)) {
    const trimmed = {};
    for (const [k, v] of Object.entries(ctx)) {
      trimmed[k] = (typeof v === 'string' && v.length > CTX_CAP)
        ? `${v.slice(0, CTX_CAP)}<cut>` : v;
    }
    return JSON.stringify(trimmed);
  }
  if (json.length <= EVIDENCE_CAP) return json;

  // Too large even for evidence. Drop whole values and NAME what went, so the
  // reader learns there was more instead of inferring completeness from a cut.
  //
  // LARGEST-FIRST, and stop as soon as it fits (GH #63). Dropping every
  // structured value because one of them is oversized costs the record every
  // other piece of evidence it was carrying, and the values that overflow are
  // the ones that grow with load — so all-or-nothing goes blind exactly where
  // the reading matters. Sort by rendered size, drop from the top, stop early.
  const reduced = { ...ctx };
  const bySize = Object.entries(ctx)
    .filter(([, v]) => v !== null && typeof v === 'object')
    .map(([k, v]) => [k, JSON.stringify(v).length])
    // Descending by size, then by key: two values of equal size must reduce the
    // same way on every relay, or the same fleet reads differently host to host.
    .sort((a, b) => (b[1] - a[1]) || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

  for (const [k, size] of bySize) {
    if (JSON.stringify(reduced).length <= EVIDENCE_CAP) break;
    const v = ctx[k];
    const note = `<${Array.isArray(v) ? `${v.length} entries` : 'object'} omitted: ctx over ${EVIDENCE_CAP}c>`;
    // The note is itself ~45 characters. Replacing a value SMALLER than its own
    // note grows the record — never do it, even if that leaves us over the cap.
    // Being over by a little beats destroying evidence to get under.
    if (note.length + 2 >= size) continue;
    reduced[k] = note;
  }
  // May still exceed EVIDENCE_CAP when the overflow is in the scalars, or in
  // structured values too small to be worth dropping. That is deliberate: the
  // cap bounds what we CHOOSE to cut, and the invariant is that the line is
  // whole and parseable, never that it is short.
  return JSON.stringify(reduced);
}
