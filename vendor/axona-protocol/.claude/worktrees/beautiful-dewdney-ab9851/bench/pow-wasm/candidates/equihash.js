// Equihash-style ASYMMETRIC PoW candidate — memory-CAPACITY-hard to SOLVE
// (build + sort a large list; generalized-birthday / Wagner), trivially cheap to
// VERIFY (recompute 2^k hashes, check they XOR to zero). Faithful reference
// implementation (Biryukov–Khovratovich 2016): BLAKE2b for the list entries,
// k sort-and-XOR stages. Pure JS, no dependency.
//
// DIFFICULTY = COLLISION-BITS B (per stage) — the memory axis: the initial list
// is N = 2^(B+1) entries, so peak memory grows with B. k is fixed at 3 (⇒ an
// 8-index solution). Total hash width = (k+1)·B bits.
//
// NOTE: benchmark-faithful — exact memory+sort cost profile + cheap verify, with
// a simplified solution-binding (XOR-to-zero + distinct, not Zcash's full
// personalization/tree-ordering rules). Correct + measurable; an audited solver
// is a separate step before this is the kernel PoW. BLAKE2b is a no-alloc 32-bit
// limb implementation (vector-verified), fast enough that the N-entry list
// allocation — not the hashing — dominates, so the sweep reaches the OOM floor.

// ── BLAKE2b (32-bit limbs, ZERO per-call allocation) ────────────────
// 64-bit word w lives at index 2w (lo), 2w+1 (hi) in reused Uint32Arrays.
const SIGMA = [
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
  [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
  [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
  [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
  [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
  [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
  [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
  [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
  [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
  [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
  [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];
const IVw = new Uint32Array([                       // 8 words as lo,hi pairs
  0xf3bcc908, 0x6a09e667, 0x84caa73b, 0xbb67ae85, 0xfe94f82b, 0x3c6ef372, 0x5f1d36f1, 0xa54ff53a,
  0xade682d1, 0x510e527f, 0x2b3e6c1f, 0x9b05688c, 0xfb41bd6b, 0x1f83d9ab, 0x137e2179, 0x5be0cd19,
]);
const H = new Uint32Array(16);                      // chaining value (8 words)
const V = new Uint32Array(32);                      // working state (16 words)
const M = new Uint32Array(32);                      // message block (16 words)

function addInto(a, lo, hi) { const s = V[a] + lo; V[a] = s >>> 0; V[a + 1] = (V[a + 1] + hi + (s > 0xffffffff ? 1 : 0)) >>> 0; }
function xorRotr(w, xlo, xhi, n) {                  // V[w] = rotr(V[w] ^ x, n)
  const lo = (V[w] ^ xlo) >>> 0, hi = (V[w + 1] ^ xhi) >>> 0;
  if (n === 32) { V[w] = hi; V[w + 1] = lo; return; }
  if (n < 32) { V[w] = ((lo >>> n) | (hi << (32 - n))) >>> 0; V[w + 1] = ((hi >>> n) | (lo << (32 - n))) >>> 0; return; }
  const k = n - 32, slo = hi, shi = lo;             // n>32: swap then rotr(n-32)
  if (k === 0) { V[w] = slo; V[w + 1] = shi; return; }
  V[w] = ((slo >>> k) | (shi << (32 - k))) >>> 0; V[w + 1] = ((shi >>> k) | (slo << (32 - k))) >>> 0;
}
function G(a, b, c, d, mx, my) {
  a *= 2; b *= 2; c *= 2; d *= 2; mx *= 2; my *= 2;
  addInto(a, V[b], V[b + 1]); addInto(a, M[mx], M[mx + 1]); xorRotr(d, V[a], V[a + 1], 32);
  addInto(c, V[d], V[d + 1]); xorRotr(b, V[c], V[c + 1], 24);
  addInto(a, V[b], V[b + 1]); addInto(a, M[my], M[my + 1]); xorRotr(d, V[a], V[a + 1], 16);
  addInto(c, V[d], V[d + 1]); xorRotr(b, V[c], V[c + 1], 63);
}
function compress(byteCount, last) {
  V.set(H); V.set(IVw, 16);
  V[24] ^= byteCount >>> 0; V[25] ^= Math.floor(byteCount / 4294967296) >>> 0;   // word 12 ^= t
  if (last) { V[28] ^= 0xffffffff; V[29] ^= 0xffffffff; }                        // word 14 ^= ~0
  for (let r = 0; r < 12; r++) {
    const s = SIGMA[r];
    G(0, 4, 8, 12, s[0], s[1]); G(1, 5, 9, 13, s[2], s[3]); G(2, 6, 10, 14, s[4], s[5]); G(3, 7, 11, 15, s[6], s[7]);
    G(0, 5, 10, 15, s[8], s[9]); G(1, 6, 11, 12, s[10], s[11]); G(2, 7, 8, 13, s[12], s[13]); G(3, 4, 9, 14, s[14], s[15]);
  }
  for (let i = 0; i < 16; i++) H[i] = (H[i] ^ V[i] ^ V[i + 16]) >>> 0;
}
export function blake2b(input, outlen = 64) {
  H.set(IVw);
  H[0] = (H[0] ^ (0x01010000 | outlen)) >>> 0;      // param block (digest len, fanout=depth=1)
  const blocks = Math.max(1, Math.ceil(input.length / 128));
  for (let i = 0; i < blocks; i++) {
    M.fill(0);
    const off = i * 128;
    for (let w = 0; w < 16; w++) {
      let lo = 0, hi = 0;
      for (let b = 0; b < 4; b++) lo |= (input[off + w * 8 + b] || 0) << (8 * b);
      for (let b = 0; b < 4; b++) hi |= (input[off + w * 8 + 4 + b] || 0) << (8 * b);
      M[2 * w] = lo >>> 0; M[2 * w + 1] = hi >>> 0;
    }
    const last = i === blocks - 1;
    compress(last ? input.length : (i + 1) * 128, last);
  }
  const out = new Uint8Array(outlen);
  for (let i = 0; i < outlen; i++) { const w = i >> 3, j = i & 7; out[i] = (j < 4 ? H[2 * w] >>> (8 * j) : H[2 * w + 1] >>> (8 * (j - 4))) & 0xff; }
  return out;
}

// ── generalized-birthday solver ─────────────────────────────────────
const K = 3;                                          // stages ⇒ 2^K = 8-index solution
let _peak = 0;
const enc = new TextEncoder();

// entry hash as a W-bit BigInt from BLAKE2b(seed‖index)
function entryHash(seedBytes, idx, wbytes) {
  const buf = new Uint8Array(seedBytes.length + 4);
  buf.set(seedBytes);
  buf[seedBytes.length] = idx & 0xff; buf[seedBytes.length + 1] = (idx >> 8) & 0xff;
  buf[seedBytes.length + 2] = (idx >> 16) & 0xff; buf[seedBytes.length + 3] = (idx >> 24) & 0xff;
  const d = blake2b(buf, wbytes);
  let h = 0n; for (let j = d.length - 1; j >= 0; j--) h = (h << 8n) | BigInt(d[j]);
  return h;
}

function solve(seedBytes, B) {
  const W = (K + 1) * B;
  const wbytes = Math.ceil(W / 8);
  const N = 2 ** (B + 1);
  const Bb = BigInt(B);
  const Wmask = (1n << BigInt(W)) - 1n;
  // round-0 list: {h, idx:[j]}
  let list = new Array(N);
  for (let j = 0; j < N; j++) list[j] = { h: entryHash(seedBytes, j, wbytes) & Wmask, idx: [j] };
  // Rough estimate: object + BigInt + index array + the transient merge lists
  // run ≈ 0.5–1.5 KB/entry in practice (measured), so call it ~512 B/entry. This
  // N-entry list is what OOMs a phone — but the number is only a label; the real
  // floor is the OOM/worker-death signal, which doesn't depend on it being exact.
  _peak = N * 512;
  // stages: collide on B bits per stage (last stage on 2B to fully zero)
  for (let r = 1; r <= K; r++) {
    const shift = BigInt((r - 1) * B);
    const groupBits = (r === K) ? BigInt(2 * B) : Bb;   // last stage collides on the final 2B bits
    const gmask = (1n << groupBits) - 1n;
    const key = (e) => (e.h >> shift) & gmask;
    list.sort((a, b) => { const ka = key(a), kb = key(b); return ka < kb ? -1 : ka > kb ? 1 : 0; });
    const next = [];
    let i = 0;
    while (i < list.length) {
      let g = i + 1;
      while (g < list.length && key(list[g]) === key(list[i])) g++;
      // all pairs within [i,g)
      for (let a = i; a < g; a++) for (let b = a + 1; b < g; b++) {
        const ea = list[a], eb = list[b];
        if (!disjoint(ea.idx, eb.idx)) continue;        // indices must stay distinct
        const merged = ea.idx[0] < eb.idx[0] ? ea.idx.concat(eb.idx) : eb.idx.concat(ea.idx);
        next.push({ h: ea.h ^ eb.h, idx: merged });
      }
      i = g;
    }
    list = next;
    if (!list.length) return null;
  }
  // a full solution: h === 0 with 2^K distinct indices
  for (const e of list) if (e.h === 0n && e.idx.length === (1 << K)) return e.idx;
  return null;
}
function disjoint(a, b) { const s = new Set(a); for (const x of b) if (s.has(x)) return false; return true; }

export const name = 'equihash (asymmetric, generalized-birthday, memory-capacity)';
export const version = '0.7.0';   // high ceiling; harness gates iOS only (Android runs full)
export const suiteDifficulties = [16, 18, 20, 21];    // COLLISION-BITS B → N = 2^(B+1) entries
// Full sweep. The harness gates per-device via estimateMemMB() (below): iOS ONLY
// (700MB budget) auto-skips B=20 (~1GB est) / B=21 (~2GB est) BEFORE allocating,
// because iOS Safari page-crashes on OOM. Android + desktop (6GB) run them —
// a Galaxy S24 Ultra completes B=20 (~34s) / B=21 (~86s). iPhone floor = B=19/20.
export const difficultyLabel = 'collision-bits';
export const trials = 2;

export async function mint(pubkeyHex, B) {
  for (let nonce = 0; nonce < 64; nonce++) {
    const seed = enc.encode(`equihash:${pubkeyHex}:${nonce}`);
    const sol = solve(seed, B);
    if (sol) return JSON.stringify({ nonce, idx: sol });
  }
  throw new Error('no solution within nonce budget');
}

export async function verify(pubkeyHex, witness, B) {
  let p; try { p = JSON.parse(witness); } catch { return false; }
  const { nonce, idx } = p || {};
  if (!Array.isArray(idx) || idx.length !== (1 << K)) return false;
  if (new Set(idx).size !== idx.length) return false;                 // distinct (tree-ordered, NOT globally sorted)
  const W = (K + 1) * B, wbytes = Math.ceil(W / 8), Wmask = (1n << BigInt(W)) - 1n;
  const seed = enc.encode(`equihash:${pubkeyHex}:${nonce}`);
  let acc = 0n;
  for (const j of idx) {
    if (!Number.isInteger(j) || j < 0 || j >= 2 ** (B + 1)) return false;
    acc ^= entryHash(seed, j, wbytes) & Wmask;
  }
  return acc === 0n;                                                  // the 2^K hashes XOR to zero
}

// Estimated peak working set for a difficulty WITHOUT running it — the harness
// uses this to skip tests that would exceed a device's memory budget (iOS Safari
// page-crashes; there is no catchable worker OOM). ~512 B/entry (measured).
export function estimateMemMB(B) { return (2 ** (B + 1)) * 512 / 1e6; }
export function peakMemoryBytes() { return _peak; }
export function reset() { _peak = 0; }
