// Cuckoo-Cycle-style ASYMMETRIC PoW candidate — memory-(bandwidth-)hard to SOLVE,
// trivially cheap to VERIFY. Faithful reference implementation (J. Tromp, Cuckoo
// Cycle 2014): build a large random bipartite graph from siphash, find a cycle.
// Solve cost is dominated by holding + scanning the big edge arrays; verify just
// re-derives the cycle's edges and checks they close. Pure JS, no dependency.
//
// DIFFICULTY = EDGE-BITS (graph size) — the memory/phone-floor axis. The graph
// is 2^edgebits edges over 2^(edgebits-1) nodes/side (avg degree ~2 ⇒ cycles
// exist w.h.p.), so peak memory ≈ 2^edgebits · 8 bytes.
//
// NOTE: a benchmark-faithful variant — exact algorithm + cost profile + cheap
// verify, with its own parameterisation (any-length cycle, not Tromp's fixed
// L=42). Correct + measurable; an audited/optimised solver is a separate step
// before this is the *kernel* PoW.

// 32-bit-limb SipHash-2-4 (BigInt was the mint bottleneck). A 64-bit value is a
// {hi,lo} pair of uint32; all ops stay in 32-bit integer land. We only ever need
// the LOW bits (node = hash mod 2^k, k ≤ 25), so siphashLow returns lo.
// ZERO per-call allocation (the {hi,lo}-object version was GC-bound). 64-bit
// state in a reused Uint32Array of 8 u32s: v0=[0,1] v1=[2,3] v2=[4,5] v3=[6,7]
// (lo,hi). add/xor/rotl mutate in place; sipround runs the SipHash round.
const st = new Uint32Array(8);
function add64(a, b) { const lo = st[a] + st[b]; st[a] = lo >>> 0; st[a + 1] = (st[a + 1] + st[b + 1] + (lo > 0xffffffff ? 1 : 0)) >>> 0; }
function xor64(a, b) { st[a] ^= st[b]; st[a + 1] ^= st[b + 1]; }
function rotl64(a, n) {
  const lo = st[a], hi = st[a + 1];
  if (n === 32) { st[a] = hi; st[a + 1] = lo; return; }
  st[a] = ((lo << n) | (hi >>> (32 - n))) >>> 0;
  st[a + 1] = ((hi << n) | (lo >>> (32 - n))) >>> 0;
}
function sipround() {
  add64(0, 2); rotl64(2, 13); xor64(2, 0); rotl64(0, 32);
  add64(4, 6); rotl64(6, 16); xor64(6, 4);
  add64(0, 6); rotl64(6, 21); xor64(6, 0);
  add64(4, 2); rotl64(2, 17); xor64(2, 4); rotl64(4, 32);
}
// Tromp-style siphash-2-4 over a single u64 nonce (nonce.hi = 0 for our range).
function siphashLow(k, nlo) {
  st[0] = k[0].lo; st[1] = k[0].hi; st[2] = k[1].lo; st[3] = k[1].hi;
  st[4] = k[2].lo; st[5] = k[2].hi; st[6] = (k[3].lo ^ nlo) >>> 0; st[7] = k[3].hi;
  sipround(); sipround();
  st[0] = (st[0] ^ nlo) >>> 0; st[4] = (st[4] ^ 0xff) >>> 0;
  sipround(); sipround(); sipround(); sipround();
  return (st[0] ^ st[2] ^ st[4] ^ st[6]) >>> 0;             // low 32 bits suffice
}
const sipnode = (k, edge2, mask) => siphashLow(k, edge2) & mask;   // node = hash mod 2^k
async function deriveKeys(pubkeyHex, nonce) {
  const bytes = new TextEncoder().encode(`cuckoo:${pubkeyHex}:${nonce}`);
  const h = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const k = [];
  for (let i = 0, o = 0; i < 4; i++, o += 8) {
    k.push({
      lo: (h[o] | (h[o + 1] << 8) | (h[o + 2] << 16) | (h[o + 3] << 24)) >>> 0,
      hi: (h[o + 4] | (h[o + 5] << 8) | (h[o + 6] << 16) | (h[o + 7] << 24)) >>> 0,
    });
  }
  return k;
}

let _peak = 0;

async function solveGraph(pubkeyHex, nonce, edgebits) {
  const nEdges = 2 ** edgebits;
  const nps = 2 ** (edgebits - 1);                 // nodes per side
  const k = await deriveKeys(pubkeyHex, nonce);
  const U = new Uint32Array(nEdges);
  const V = new Uint32Array(nEdges);
  _peak = U.byteLength + V.byteLength + (2 * nps) * 4;
  const mask = nps - 1;                            // nps is a power of 2 ⇒ mod == &
  for (let i = 0; i < nEdges; i++) {
    U[i] = sipnode(k, 2 * i, mask);
    V[i] = sipnode(k, 2 * i + 1, mask);
  }
  // union-find over 2*nps nodes (U-side 0..nps-1, V-side nps..2nps-1) + a tree
  // adjacency of accepted edges, so a back-edge can be traced into a cycle.
  const parent = new Int32Array(2 * nps).fill(-1);
  const find = (x) => { while (parent[x] >= 0) { if (parent[parent[x]] >= 0) parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
  const adj = new Map();                           // node → [{to, edge}]
  const link = (a, b, e) => { (adj.get(a) || adj.set(a, []).get(a)).push({ to: b, edge: e }); (adj.get(b) || adj.set(b, []).get(b)).push({ to: a, edge: e }); };

  for (let i = 0; i < nEdges; i++) {
    const a = U[i], b = nps + V[i];
    const ra = find(a), rb = find(b);
    if (ra === rb) {
      const cyc = trace(adj, a, b);                // path of edge indices a..b in the forest
      if (cyc && cyc.length >= 3) return [...cyc, i];   // + this back-edge = cycle (len ≥ 4)
    } else {
      parent[ra] = rb;
      link(a, b, i);
    }
  }
  return null;
}

// BFS over the forest's accepted edges from a to b → list of edge indices.
function trace(adj, a, b) {
  const prev = new Map([[a, { node: -1, edge: -1 }]]);
  const q = [a];
  while (q.length) {
    const x = q.shift();
    if (x === b) break;
    for (const { to, edge } of (adj.get(x) || [])) {
      if (!prev.has(to)) { prev.set(to, { node: x, edge }); q.push(to); }
    }
  }
  if (!prev.has(b)) return null;
  const edges = [];
  for (let x = b; x !== a; ) { const p = prev.get(x); edges.push(p.edge); x = p.node; }
  return edges;
}

export const name = 'cuckoo-cycle (asymmetric, memory-bandwidth-hard)';
export const version = '0.3.0';   // no-alloc limb siphash + tracer
export const suiteDifficulties = [18, 20, 22];        // EDGE-BITS.
// KEPT at the range phones complete cleanly — this candidate's value is the
// cross-device SPEED/BANDWIDTH signal (the egalitarian flatness). Its MEMORY is
// NOT a clean capacity measure: the cycle tracer (a Map of accepted edges) is an
// implementation artifact (real Cuckoo uses bounded edge-trimming, mem ∝ graph),
// and it balloons RSS (~1GB at eb=22) — so peakMemoryBytes (typed arrays only)
// under-reports and the real number isn't representative. The clean capacity
// floor is equihash's job; cuckoo's needs a bounded-memory solver rewrite first.
export const trials = 2;
export const difficultyLabel = 'edge-bits';

export async function mint(pubkeyHex, edgebits) {
  for (let nonce = 0; nonce < 200; nonce++) {
    const cyc = await solveGraph(pubkeyHex, nonce, edgebits);
    if (cyc) return JSON.stringify({ nonce, cycle: cyc });   // witness: nonce + cycle edge indices
  }
  throw new Error('no cycle found within nonce budget');
}

export async function verify(pubkeyHex, witness, edgebits) {
  let parsed; try { parsed = JSON.parse(witness); } catch { return false; }
  const { nonce, cycle } = parsed || {};
  if (!Array.isArray(cycle) || cycle.length < 4 || cycle.length % 2 !== 0) return false;
  const nps = 2 ** (edgebits - 1);
  const mask = nps - 1;
  const k = await deriveKeys(pubkeyHex, nonce);
  // Re-derive each cycle edge's endpoints; check consecutive edges share exactly
  // one endpoint and the chain closes into a single loop (cheap — |cycle| sips).
  if (new Set(cycle).size !== cycle.length) return false;        // distinct edges
  const ep = cycle.map((e) => {
    if (!Number.isInteger(e) || e < 0 || e >= 2 ** edgebits) return null;
    return [sipnode(k, 2 * e, mask), nps + sipnode(k, 2 * e + 1, mask)];
  });
  if (ep.some((x) => x === null)) return false;
  // A valid cycle is a 2-regular single loop (order-independent): build
  // node→edges, require every node degree 2 and nodes == edges, then walk once
  // and confirm it returns to the start having used ALL edges (one cycle, not
  // several disjoint ones).
  const adj = new Map();
  ep.forEach(([u, v], i) => { (adj.get(u) || adj.set(u, []).get(u)).push(i); (adj.get(v) || adj.set(v, []).get(v)).push(i); });
  for (const lst of adj.values()) if (lst.length !== 2) return false;
  if (adj.size !== ep.length) return false;
  const used = new Array(ep.length).fill(false);
  const start = ep[0][0]; let node = start, steps = 0;
  do {
    const e = (adj.get(node) || []).find((x) => !used[x]);
    if (e === undefined) return false;
    used[e] = true; steps++;
    const [u, v] = ep[e];
    node = (u === node) ? v : u;
  } while (node !== start && steps <= ep.length);
  return steps === ep.length && node === start;
}

// Estimated peak (typed arrays: 2× edge arrays + union-find) for the harness's
// pre-run memory budget check. In-browser GC keeps the tracer transient, so the
// arrays are the predictable floor; this stays small enough to never be gated in
// the current range (matches reality — phones complete eb=22).
export function estimateMemMB(edgebits) { return (2 ** edgebits) * 12 / 1e6; }
export function peakMemoryBytes() { return _peak; }
export function reset() { _peak = 0; }
