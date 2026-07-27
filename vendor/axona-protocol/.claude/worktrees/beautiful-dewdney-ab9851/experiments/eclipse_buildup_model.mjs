// =====================================================================
// eclipse_buildup_model.mjs — warm-up cost of B-3 (verified-only synapse
// admission) vs. today (gossip-immediate admission), under REALISTIC
// connection latency.
//
// Why a model, not the live harness: real-WebRTC at the scale needed to
// see gossip-driven table growth isn't tractable in one process, and the
// dht-sim treats connections as instant (the exact "sim hides connection
// cost" gap). So this is a discrete-event model whose ONLY non-obvious
// inputs are calibrated to measured reality:
//   • WebRTC connection bring-up (ICE/STUN/DTLS): ~1.5–3 s   (Antigravity)
//   • axona/4 handshake crypto: ~0.13 ms → negligible, folded into T_conn
//   • P2P connect-failure rate (NAT traversal w/o TURN): a parameter
// Routing realism is kept minimal-but-fair: once the NEW node hands a
// lookup to the converged network, both policies finish identically, so
// the ONLY differentiator we measure is the new node's first hop — its
// table coverage and whether using an entry costs a connection.
//
// Two policies, identical seeded discovery stream (same network, same
// gossip order) for an apples-to-apples head-to-head:
//   A "gossip-immediate" (today): a discovered peer is admitted to the
//      routing table instantly with fabricated metadata; the connection
//      cost is paid LAZILY the first time a lookup routes through it (and
//      a fraction are unreachable "ghosts" that fail mid-lookup).
//   B "verified-only" (B-3): discovery fills a candidate POOL; a budgeted
//      background prober connects+verifies candidates; only succeeded
//      probes enter the table. Lookups use connected entries only.
//
// Run: node experiments/eclipse_buildup_model.mjs
// =====================================================================

// ── seeded RNG (mulberry32) — reproducible runs ─────────────────────
function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── parameters (calibrated; stated so they can be challenged) ───────
const P = {
  M:                 500,     // converged nodes already in the network
  bootstrap:         8,       // peers the bridge hands a joiner (connected at join)
  horizonMs:         60_000,
  tickMs:            100,
  sampleMs:          500,
  lookupsPerSample:  40,      // random-target lookups issued each sample
  // connection cost
  connMeanMs:        2000,    // WebRTC bring-up mean
  connMinMs:         1500,
  connMaxMs:         3000,
  connFailProb:      Number(process.env.CONN_FAIL ?? 0.15),  // P2P connect failure w/o TURN (ghost on the A path)
  // routing latency once we've made a first hop
  warmHopMs:         50,
  networkCompleteMs: 350,     // remaining converged-network hops (same both policies)
  // gossip: each CONNECTED peer introduces a new candidate at this cadence
  gossipPerPeerMs:   2000,
  // policy B prober (env-overridable for sensitivity sweeps)
  probeBudget:       Number(process.env.PROBE_BUDGET ?? 4),  // max concurrent verification probes
  // "good lookup" threshold for time-to-usefulness
  goodLatencyMs:     600,
  seed:              12345,
};

// Setup RNG — builds the fixed network/deck. Each POLICY run then gets a
// fresh RNG re-seeded identically, so both policies face the same network,
// the same discovery order, and the same connection-time/ghost draws — a
// true apples-to-apples head-to-head.
const setup = rng(P.seed);
const idFrom = (r) => (BigInt(Math.floor(r() * 2 ** 31)) << 32n) | BigInt(Math.floor(r() * 2 ** 31));
const dist = (a, b) => a ^ b;

const NET  = Array.from({ length: P.M }, () => idFrom(setup));
const SELF = idFrom(setup);
const RUN_SEED = (P.seed ^ 0x9e3779b9) >>> 0;   // shared by both runs

// Shared discovery stream: a list of {atMs, peerIndex} — when each new
// candidate becomes *known* to the joiner. Bootstrap arrives at t=0; the
// rest are produced by gossip, which only flows from CONNECTED peers — so
// the stream is generated lazily per-policy from each policy's own
// connected set (realistic: you only learn from peers you actually talk
// to). To keep the candidate IDENTITIES identical across policies we draw
// them from one shared shuffled deck.
const deck = NET.map((_, i) => i);
for (let i = deck.length - 1; i > 0; i--) { const j = Math.floor(setup() * (i + 1)); [deck[i], deck[j]] = [deck[j], deck[i]]; }
let deckPtr = 0;
const nextCandidate = () => (deckPtr < deck.length ? deck[deckPtr++] : null);

function bestFirstHop(knownIndices, target) {
  // closest known peer to target (the new node's first hop)
  let best = null, bestD = null;
  for (const idx of knownIndices) {
    const d = dist(NET[idx], target);
    if (bestD === null || d < bestD) { bestD = d; best = idx; }
  }
  return best;
}

// ── Policy A: gossip-immediate admission, lazy connect-on-use ───────
function runPolicyA() {
  deckPtr = 0;
  const r = rng(RUN_SEED);
  const connTime = () => P.connMinMs + r() * (P.connMaxMs - P.connMinMs);
  const randId   = () => idFrom(r);
  const table = new Map();          // peerIndex -> { connected }
  const connecting = new Map();     // peerIndex -> completeAtMs (lazy, from lookups)
  let connections = 0;
  const samples = [];
  // bootstrap: connected at join (one parallel batch ~ connTime)
  const bootDone = connTime();
  const bootset = [];
  for (let i = 0; i < P.bootstrap; i++) { const c = nextCandidate(); if (c != null) { bootset.push(c); table.set(c, { connected: false }); } }
  connections += bootset.length;
  let lastGossip = 0;

  for (let t = 0; t <= P.horizonMs; t += P.tickMs) {
    // bootstrap connections land
    if (t >= bootDone) for (const c of bootset) { const e = table.get(c); if (e) e.connected = true; }
    // complete any lazy connections
    for (const [idx, done] of [...connecting]) if (t >= done) { table.get(idx).connected = true; connecting.delete(idx); }
    // gossip from connected peers → instantly admitted to the table
    const connectedCount = [...table.values()].filter(e => e.connected).length;
    if (connectedCount > 0 && t - lastGossip >= P.gossipPerPeerMs) {
      lastGossip = t;
      for (let g = 0; g < connectedCount; g++) {
        const c = nextCandidate();
        if (c != null && !table.has(c)) table.set(c, { connected: false });
      }
    }
    // sample
    if (t % P.sampleMs === 0) {
      let latSum = 0, fails = 0;
      for (let l = 0; l < P.lookupsPerSample; l++) {
        const target = randId();
        const known = [...table.keys()];
        let lat = 0, hitGhost = false;
        // walk best→next, paying connection cost for cold entries, dropping ghosts
        const tried = new Set();
        let routed = false;
        while (tried.size < known.length) {
          const cand = known.filter(k => !tried.has(k));
          const hop = bestFirstHop(cand, target);
          if (hop == null) break;
          tried.add(hop);
          const e = table.get(hop);
          if (e.connected) { lat += P.warmHopMs; routed = true; break; }
          // cold: pay a connection now
          lat += P.connMeanMs;            // expected wait to use it
          connections++;
          if (r() < P.connFailProb) {   // ghost — unreachable
            hitGhost = true; table.delete(hop); continue;
          }
          e.connected = true;              // warmed for future lookups
          lat += P.warmHopMs; routed = true; break;
        }
        if (!routed) { fails++; continue; }
        if (hitGhost) fails++;             // count lookups that stalled on a ghost
        latSum += lat + P.networkCompleteMs;
      }
      const ok = P.lookupsPerSample - fails;
      samples.push({
        t, known: table.size,
        connected: [...table.values()].filter(e => e.connected).length,
        meanLat: ok > 0 ? latSum / ok : Infinity,
        failPct: 100 * fails / P.lookupsPerSample,
        connections,
      });
    }
  }
  return samples;
}

// ── Policy B: candidate pool + budgeted verification probing ────────
function runPolicyB() {
  deckPtr = 0;
  const r = rng(RUN_SEED);
  const connTime = () => P.connMinMs + r() * (P.connMaxMs - P.connMinMs);
  const randId   = () => idFrom(r);
  const connected = new Set();      // peerIndex (verified)
  const pool = [];                  // discovered, not yet probed
  const inflight = new Map();       // peerIndex -> completeAtMs (+ willFail)
  let connections = 0;
  const samples = [];
  const bootDone = connTime();
  const bootset = [];
  for (let i = 0; i < P.bootstrap; i++) { const c = nextCandidate(); if (c != null) bootset.push(c); }
  connections += bootset.length;
  let lastGossip = 0;

  for (let t = 0; t <= P.horizonMs; t += P.tickMs) {
    if (t >= bootDone) for (const c of bootset) connected.add(c);
    // probes complete
    for (const [idx, info] of [...inflight]) if (t >= info.done) { if (!info.willFail) connected.add(idx); inflight.delete(idx); }
    // gossip from connected peers → into the candidate POOL
    if (connected.size > 0 && t - lastGossip >= P.gossipPerPeerMs) {
      lastGossip = t;
      for (let g = 0; g < connected.size; g++) {
        const c = nextCandidate();
        if (c != null && !connected.has(c) && !inflight.has(c) && !pool.includes(c)) pool.push(c);
      }
    }
    // budgeted prober: keep up to probeBudget verifications in flight
    while (inflight.size < P.probeBudget && pool.length > 0) {
      const c = pool.shift();
      connections++;
      inflight.set(c, { done: t + connTime(), willFail: r() < P.connFailProb });
    }
    // sample
    if (t % P.sampleMs === 0) {
      let latSum = 0, fails = 0;
      const known = [...connected];
      for (let l = 0; l < P.lookupsPerSample; l++) {
        const target = randId();
        const hop = bestFirstHop(known, target);
        if (hop == null) { fails++; continue; }    // nothing usable yet
        latSum += P.warmHopMs + P.networkCompleteMs; // connected → instant first hop, no ghost
      }
      const ok = P.lookupsPerSample - fails;
      samples.push({
        t, known: connected.size + pool.length + inflight.size,
        connected: connected.size,
        meanLat: ok > 0 ? latSum / ok : Infinity,
        failPct: 100 * fails / P.lookupsPerSample,
        connections,
      });
    }
  }
  return samples;
}

function timeToGood(samples) {
  for (const s of samples) if (s.meanLat <= P.goodLatencyMs && s.failPct === 0) return s.t;
  return null;
}

function row(s) {
  const lat = s.meanLat === Infinity ? '   ∞' : String(Math.round(s.meanLat)).padStart(4);
  return `${String(s.t).padStart(6)}  ${String(s.connected).padStart(9)}  ${String(s.known).padStart(7)}  ${lat}ms  ${s.failPct.toFixed(0).padStart(4)}%  ${String(s.connections).padStart(6)}`;
}

function report(name, samples) {
  console.log(`\n=== Policy ${name} ===`);
  console.log('  t(ms)  connected    known  meanLat  fail%   conns');
  for (const tt of [0, 1000, 2000, 5000, 10000, 15000, 30000, 60000]) {
    const s = samples.find(x => x.t === tt);
    if (s) console.log('  ' + row(s));
  }
  const ttg = timeToGood(samples);
  console.log(`  → time to reliable fast lookups (≤${P.goodLatencyMs}ms, 0 fail): ${ttg == null ? 'not reached' : ttg + 'ms'}`);
  const last = samples[samples.length - 1];
  console.log(`  → total connections opened over 60s: ${last.connections}`);
}

const A = runPolicyA();
const B = runPolicyB();

console.log('Eclipse-prevention buildup model — verified-only (B-3) vs gossip-immediate (today)');
console.log(`(seed=${P.seed}, M=${P.M}, T_conn≈${P.connMeanMs}ms, connFail=${P.connFailProb}, probeBudget=${P.probeBudget})`);
console.log('\nColumns: connected = usable verified entries · known = total table/known · meanLat = mean successful-lookup latency · fail% = lookups that stalled (ghost or no route) · conns = cumulative connections opened');
report('A — gossip-immediate (today)', A);
report('B — verified-only (B-3)', B);

// headline deltas
const ag = timeToGood(A), bg = timeToGood(B);
console.log('\n=== Headline ===');
console.log(`Time-to-reliable-fast-lookups:  A=${ag ?? 'n/a'}ms   B=${bg ?? 'n/a'}ms`);
console.log(`Connections opened in 60s:       A=${A.at(-1).connections}   B=${B.at(-1).connections}`);
const aEarly = A.find(s => s.t === 2000), bEarly = B.find(s => s.t === 2000);
console.log(`At 2s — A: ${aEarly.known} known / ${aEarly.connected} usable, ${aEarly.failPct.toFixed(0)}% fail, ${aEarly.meanLat === Infinity ? '∞' : Math.round(aEarly.meanLat)+'ms'}`);
console.log(`        B: ${bEarly.known} known / ${bEarly.connected} usable, ${bEarly.failPct.toFixed(0)}% fail, ${Math.round(bEarly.meanLat)}ms`);
