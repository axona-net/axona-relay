// graduated_sample.js — "The introduction represents the mesh, not the
// socket" v0.1 (axona-docs c7b0230), P1.a, the kernel half.
//
// A bridge peer-list may carry a field of its own, `graduated`: a sample of
// 66-hex ids the bridge has graduated off its door. An id is not a
// connection handle (Orion, MPSH-3): the mesh never dials the field. The
// kernel NOMINATES the sample into the candidate cache as source
// `directory`, the path the row-12 peer-list handler already takes, and the
// fill dials under its budget through whatever path the node holds. A kernel
// without this file ignores the field; a bridge without the flag never sends
// it.
//
// This file holds the sanitiser only: shape, bound, charset, duplicates.
// Self-exclusion and bound-peer exclusion happen where every nomination is
// judged (AxonaPeer._nominateCandidate).

/** The most ids a frame may carry; a bridge's R_sample is 16 by default. */
export const GRADUATED_SAMPLE_MAX = 64;

const HEX66 = /^[0-9a-f]{66}$/;

/**
 * The `graduated` field of a peer-list frame as the kernel will use it:
 * an array, at most GRADUATED_SAMPLE_MAX entries read, each a lowercase
 * 66-hex string, duplicates dropped, order kept. Anything else is empty.
 * @param {unknown} field
 * @returns {string[]}
 */
export function sanitizeGraduatedSample(field) {
  if (!Array.isArray(field)) return [];
  const out = [];
  const seen = new Set();
  for (let i = 0; i < field.length && i < GRADUATED_SAMPLE_MAX; i++) {
    const v = field[i];
    if (typeof v !== 'string' || !HEX66.test(v) || seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}
