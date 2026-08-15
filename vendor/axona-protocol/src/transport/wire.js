// =====================================================================
// wire.js — shared JSON codec for Axona wire frames.
//
// Axona protocol values include `bigint` (XOR distances, low-level
// routing math) and `Set` (per-lookup `queried`) — neither survives a
// vanilla `JSON.stringify`.  We work around with a string-suffix
// convention:
//
//   BigInt 0xabc        →  "2748n"   (decimal digits + "n" sentinel)
//   Set([id1, id2, …])  →  [id1, id2, …]
//
// At the API boundary node IDs are 66-char hex strings — they need no
// special encoding.  The codec is for the internal payload types the
// protocol carries (XOR-distance bigints inside routing-decision
// envelopes, queried-set membership maps).
//
// Used by:
//   - src/transport/web/  (WebRTC data channels + bridge WebSocket)
//   - src/transport/node/ (WebSocket transport in the bridge)
//
// The bridge mirrors these conventions so every WS / DC channel uses
// the same wire format.  The protocol layer is responsible for
// wrapping incoming arrays back into a `Set` where it expects one
// (e.g., the `lookup_step` handler re-coerces `payload.queried`).
// =====================================================================

/** JSON.stringify replacer.  Emits BigInt as "<digits>n", Set as array. */
export function bigintReplacer(_key, value) {
  if (typeof value === 'bigint') return value.toString() + 'n';
  if (value instanceof Set)      return [...value];
  return value;
}

/** JSON.parse reviver.  Inverts the "<digits>n" suffix back to BigInt. */
export function bigintReviver(_key, value) {
  if (typeof value === 'string' && /^-?\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
}

/** Convenience: `JSON.stringify` with the Axona replacer. */
export function encode(msg) {
  return JSON.stringify(msg, bigintReplacer);
}

/** Convenience: `JSON.parse` with the Axona reviver. */
export function decode(text) {
  return JSON.parse(text, bigintReviver);
}

// MAX_FRAME_BYTES — the transport contract's HARD pre-parse ceiling on a single
// inbound wire frame, in UTF-8 BYTES. This is the frame/envelope guard that the
// decoders (node/index.js, web/index.js, web/mesh.js) own; it is NOT the
// registry's per-scalar budget cap (registry/types.js MAX_BYTES_CEILING = 64 KiB),
// which bounds one projected scalar, a different resource.
//
// *** PROVISIONAL VALUE — REF-1.1 S2.0c Finding 2 is OPEN (Aster seq 708). ***
// A single decode-site ceiling must admit the LARGEST legitimate frame across
// EVERY producer sharing the ingress, because the family is unknown before parse.
// peer.pub (256 KiB-char envelope) is NOT the largest: REPLICATE / HANDOFF /
// REPLAYUP carry the whole role cache (topicStore CACHE_BYTES = 16 MiB chars,
// CACHE_MAX = 1024), so a legitimate full-state sync frame reaches ~16 MiB chars
// (up to ~48 MiB UTF-8 with a multibyte body) — far above this 1 MiB. Aster
// reproduced a 1,076,365-byte legitimate REPLICATE from 70 cache entries.
// The final value is a protocol-design FORK (chunk the full-state producers below
// a small documented ceiling, vs. a ~CACHE_BYTES-scale ceiling) recorded in
// axona-docs/architecture/REF-1.1-S2.0c-Frame-Ceiling-Inventory.md and pending a
// council/David scope decision. Until then this constant is NOT wired to any live
// decode path (flag OFF, S2.1 blocked) and MUST NOT be treated as justified.
//
// The invariance and rejection semantics below are settled regardless of value:
// a registered row / decoder variant MAY pass a TIGHTER ceiling, but the hard cap
// can never be raised past — a supplied ceiling above it is rejected, not honored
// (see snapshotMint._certify).
export const MAX_FRAME_BYTES = 1 << 20;   // 1048576 — PROVISIONAL, see Finding 2
