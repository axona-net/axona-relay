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
// =============================================================================

export const CTX_CAP      = 120;    // flat scalar ctx — the chatty transport events
export const EVIDENCE_CAP = 4_000;  // structured ctx — bounded, but never severed

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
  // armed-* is a standing contract independent of shape: the canary soak
  // runbook reads its thresholds out of this JSONL, so it is never reduced.
  if (typeof event === 'string' && event.startsWith('armed-')) return json;
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
  const reduced = {};
  for (const [k, v] of Object.entries(ctx)) {
    reduced[k] = (v !== null && typeof v === 'object')
      ? `<${Array.isArray(v) ? `${v.length} entries` : 'object'} omitted: ctx over ${EVIDENCE_CAP}c>`
      : v;
  }
  return JSON.stringify(reduced);
}
