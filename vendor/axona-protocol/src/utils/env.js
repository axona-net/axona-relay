// =============================================================================
// env.js — the ONLY way kernel code reads a tuning knob from the environment.
//
// WHY THIS EXISTS. `process` is not merely absent in a browser — reading a bare
// `process.env.X` throws a ReferenceError, and an undeclared identifier throws
// BEFORE the `&&` in `process.env && process.env.X` can short-circuit. That
// guard reads as defensive and is not.
//
// On 2026-09-08 four such reads sat in AxonaManager. One of them was the first
// statement of `_steerColdSubscribe`, on the synchronous path of `sub()`:
//
//     sub() → pubsubSubscribe() → _sendSubscribe() → _steerColdSubscribe()
//                                                    └── ReferenceError
//
// So EVERY browser subscribe threw, in every browser — measured identically in
// Safari and in Chromium, which also does not define `process`. Bundlers hide
// this unevenly: Vite rewrites `process.env.NODE_ENV` and nothing else, so a
// bundled app fails exactly the same way while looking like it should not. It
// surfaced as an unhandled promise rejection rather than a thrown error, which
// is why it read as a peer/connectivity problem for as long as it did.
//
// `typeof process` is the one form that is safe on an undeclared identifier, so
// every read goes through here and no call site repeats the guard.
// =============================================================================

/** The process env bag, or null in any host that has no `process`. */
function bag() {
  // typeof on an undeclared identifier is the ONLY non-throwing test.
  if (typeof process === 'undefined') return null;
  return (process && process.env) || null;
}

/** Raw string value, or `fallback` (default null) when unset or hostless. */
export function envStr(name, fallback = null) {
  const e = bag();
  if (!e) return fallback;
  const v = e[name];
  return (v === undefined || v === '') ? fallback : v;
}

/** Numeric knob. Falls back on unset, hostless, AND unparseable — a typo in an
 *  env var must not silently become NaN and poison a timer. */
export function envNum(name, fallback) {
  const v = envStr(name, null);
  if (v === null) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

/** Comma-separated list knob, trimmed and emptied-out. `null` when unset. */
export function envList(name) {
  const v = envStr(name, null);
  if (v === null) return null;
  const out = v.split(',').map((s) => s.trim()).filter(Boolean);
  return out.length ? out : null;
}

/** True when running somewhere `process.env` exists (Node, not a browser). */
export function hasProcessEnv() {
  return bag() !== null;
}
