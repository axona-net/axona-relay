// =====================================================================
// analyze-deliver-hop.mjs — per-hop DELIVER drop from paired hop telemetry,
// to the FROZEN acceptance rules (Aster e618a19a + power correction ec9b4016,
// David-relayed 2026-09-01).
//
// Reads relay-disc-*.jsonl (kernel 4.69.0 deliver:hop_tx / deliver:hop_rx).
//  (1) pair on hopAttemptId, corroborate from/to/hopIdx (not msgId).
//  (2) denominator = ok-write tx only; pre-send failures reported separately.
//  (3) right-censor tx within (matched-pair p99 latency + pre-declared skew) of
//      arm end. Skew is PRE-DECLARED; the window is not tuned after seeing unmatched.
//  (4) dedup tx per hopAttemptId; retries are distinct ids; report attempt- AND
//      message-weighted loss.
//  (5) stratify by hopIdx; per-stratum n, loss, point, Wilson lo/hi, verdict.
//  (6) timestamps only for latency/censoring, never for identity.
//  POWER (ec9b4016): "adequately powered" = n >= N_ADEQUATE, the smallest n where a
//  ZERO-loss result already yields Wilson upper < 3% (computed from this Wilson impl,
//  ~125). n>=30 is a reporting floor only, NOT the falsifier power threshold.
//
// PIVOT (prospective, per stratum, route<=3 = hopIdx<=3):
//  - lower CI > 3%                      -> TRANSPORT LOSS STANDS (real elevated loss).
//  - n>=N_ADEQUATE AND upper CI < 3%    -> supports RETIRE.
//  - else (underpowered, or upper>=3% without lower>3%) -> blocks retire.
//  Verdict: any STANDS -> stands; else every observed route<=3 stratum supports
//  retire (>=1 exists) -> RETIRE; else INCONCLUSIVE.
//
//   node harness/analyze-deliver-hop.mjs [dir] [skewMs]
// =====================================================================
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const DIR = process.argv[2] || 'harness/results';
const SKEW_MS = Number(process.argv[3] || 250);   // PRE-DECLARED collection skew
const MIN_REPORT = 30;                              // reporting floor only
const Z = 1.96;

const files = readdirSync(DIR).filter((f) => /^relay-disc-/.test(f) && f.endsWith('.jsonl'));
if (!files.length) { console.error(`no relay-disc-*.jsonl in ${DIR}`); process.exit(2); }

const tx = new Map(), rx = new Map();
const relaySelf = new Set();   // 12-hex prefixes of nodes that emitted stamps = INSTRUMENTED relays
let txRows = 0, rxRows = 0, badId = 0, dupTx = 0, maxT = 0;
for (const f of files) {
  let text; try { text = readFileSync(join(DIR, f), 'utf8'); } catch { continue; }
  for (const line of text.split('\n')) {
    if (!line) continue;
    let r; try { r = JSON.parse(line); } catch { continue; }
    if (typeof r.t === 'number' && r.t > maxT) maxT = r.t;
    if (typeof r.self === 'string') relaySelf.add(r.self.slice(0, 12));
    if (r.stage === 'deliver:hop_tx') {
      txRows++; if (r.hopAttemptId == null) { badId++; continue; }
      if (tx.has(r.hopAttemptId)) { dupTx++; continue; }
      tx.set(r.hopAttemptId, { hopIdx: r.hopIdx, writeOutcome: r.writeOutcome, reason: r.reason ?? null, msgIds: r.msgIds || [], from: r.from, to: r.to, t: r.t });
    } else if (r.stage === 'deliver:hop_rx') {
      rxRows++; if (r.hopAttemptId != null && !rx.has(r.hopAttemptId)) rx.set(r.hopAttemptId, { hopIdx: r.hopIdx, from: r.from, to: r.to, t: r.t });
    }
  }
}

const mismatchIds = new Set();
// Corroborate on hopAttemptId (globally unique) + receiver id (`to`, full 66-hex on
// both sides) + hopIdx. The sender-side `from` is NOT corroborated: the rx handler's
// fromId is the transport's short handle, not a nodeId, so it is not comparable to the
// tx's full-hex `from` (Aster rule 1 — identity-form limitation, stated, not a shortcut).
const isMatched = (id, t) => {
  const r = rx.get(id); if (!r) return false;
  if (r.hopIdx !== t.hopIdx || r.to !== t.to) { mismatchIds.add(id); return false; }
  return true;
};
const wilson = (k, n) => {
  if (!n) return { est: 0, lo: 0, hi: 0 };
  const ph = k / n, z2 = Z * Z, den = 1 + z2 / n;
  const c = (ph + z2 / (2 * n)) / den, h = (Z * Math.sqrt(ph * (1 - ph) / n + z2 / (4 * n * n))) / den;
  return { est: +(100 * ph).toFixed(2), lo: +(100 * Math.max(0, c - h)).toFixed(2), hi: +(100 * Math.min(1, c + h)).toFixed(2) };
};
let N_ADEQUATE = 1; while (wilson(0, N_ADEQUATE).hi >= 3.0) N_ADEQUATE++;

// PASS 1 — matched-pair latency (pre-censor; informs the window only)
const lat = [];
for (const [id, t] of tx) {
  if (t.writeOutcome !== 'ok') continue;
  if (isMatched(id, t)) { const r = rx.get(id); if (typeof r.t === 'number' && typeof t.t === 'number') lat.push(r.t - t.t); }
}
lat.sort((a, b) => a - b);
const p = (q) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(q / 100 * lat.length))] : 0;
const censorWindow = (lat.length ? Math.max(0, p(99)) : 0) + SKEW_MS;
const censorCutoff = maxT - censorWindow;

// PASS 2 — classify with censoring, stratify.
// SCOPE: only hops whose RECEIVER is an instrumented relay (to-prefix ∈ relaySelf)
// are measurable — the rx stamp lives in the routed route_msg handler, which the
// final relay→subscriber hop bypasses (subscribers receive via the direct DELIVER
// handler and are not instrumented). A tx to an un-instrumented receiver is neither
// delivered-evidence nor loss-evidence; it is excluded, and counted separately.
// This scopes the read to relay→relay ROUTED hops — the multi-hop transport path the
// forward-push-loss hypothesis is actually about.
const byHop = new Map(); const msgHops = new Map(); const failReasons = new Map();
let okTx = 0, silent = 0, localFail = 0, censored = 0, uninstrumented = 0;
for (const [id, t] of tx) {
  if (t.writeOutcome !== 'ok') { localFail++; failReasons.set(t.reason || t.writeOutcome, (failReasons.get(t.reason || t.writeOutcome) || 0) + 1); continue; }
  if (typeof t.t === 'number' && t.t > censorCutoff) { censored++; continue; }
  if (typeof t.to !== 'string' || !relaySelf.has(t.to.slice(0, 12))) { uninstrumented++; continue; }
  okTx++;
  const matched = isMatched(id, t);
  const v = byHop.get(t.hopIdx) || { ok: 0, lost: 0 }; if (matched) v.ok++; else { v.lost++; silent++; } byHop.set(t.hopIdx, v);
  for (const m of t.msgIds) { const mm = msgHops.get(m) || { maxHop: 0, lost: false }; mm.maxHop = Math.max(mm.maxHop, t.hopIdx); if (!matched) mm.lost = true; msgHops.set(m, mm); }
}
let msgs = 0, msgsLost = 0; const routeLenDist = {};
for (const [, mm] of msgHops) { msgs++; if (mm.lost) msgsLost++; routeLenDist[mm.maxHop] = (routeLenDist[mm.maxHop] || 0) + 1; }

// ── VALIDITY GUARD (2026-09-01) ───────────────────────────────────────────
// The metric assumes a tx stamped 'ok' (transport.send RESOLVED) implies the
// receiver's route_msg handler RAN and stamped rx. On the live fleet that does
// NOT hold: send-resolution and handler-execution are different event
// populations. Symptom: 4698 tx vs 553 rx; 21 relays stamp tx, only 6 ever
// stamp rx. A per-hop "loss" derived from asymmetric populations is not loss.
// Two independent tripwires; either firing → the read is instrumentation-VOID
// and NO transport verdict (STANDS/RETIRE) may be emitted from it:
//   (A) receive-coverage: fraction of tx-emitting relays that also emit >=1 rx.
//   (B) population symmetry: rx / ok-tx-to-instrumented-relay.
const txSelf = new Set(), rxSelfIds = new Set();
for (const [, t] of tx) if (typeof t.from === 'string') txSelf.add(t.from.slice(0, 12));
for (const [, r] of rx) if (typeof r.to === 'string') rxSelfIds.add(r.to.slice(0, 12));
let rxCapableSenders = 0; for (const s of txSelf) if (rxSelfIds.has(s)) rxCapableSenders++;
const recvCoverage = txSelf.size ? rxCapableSenders / txSelf.size : 0;
const popSymmetry = okTx ? rxRows / okTx : 0;
const VOID_COVERAGE = 0.6, VOID_SYMMETRY = 0.5;   // pre-declared validity floors
const instrumentationVoid = recvCoverage < VOID_COVERAGE || popSymmetry < VOID_SYMMETRY;

const pooled = wilson(silent, okTx);
console.log('\n============ PER-HOP DELIVER DROP (live 4.69.0; rules e618a19a + power ec9b4016) ============');
console.log(`relay-disc ${files.length} files; tx ${txRows} (dup ${dupTx}, no-id ${badId}); rx ${rxRows}; pair mismatches ${mismatchIds.size}`);
console.log(`matched-pair latency: n=${lat.length}  p50/p95/p99 = ${p(50)}/${p(95)}/${p(99)}ms; skew(pre-declared)=${SKEW_MS}ms; censor window=${censorWindow}ms; censored tx=${censored}`);
console.log(`N_ADEQUATE (zero-loss Wilson-upper<3%) = ${N_ADEQUATE} attempts  [reporting floor n>=${MIN_REPORT} is separate]`);
console.log(`instrumented relays (self-ids seen): ${relaySelf.size}; tx excluded (receiver un-instrumented, i.e. subscriber final hop): ${uninstrumented}`);
console.log(`eligible ok-write tx to an instrumented relay (denominator, post-censor): ${okTx}`);
console.log(`local pre-send failures (separate): ${localFail}  ${JSON.stringify(Object.fromEntries(failReasons))}`);
console.log(`ATTEMPT-weighted per-hop silent loss (pooled): ${pooled.est}%  [95% CI ${pooled.lo}-${pooled.hi}]`);
console.log(`MESSAGE-weighted: ${msgs ? (100 * msgsLost / msgs).toFixed(2) : '0'}%  (${msgsLost}/${msgs} msgs lost at >=1 hop)`);
console.log('per-hop strata:');
let stands = false, retire = true, anyRoute3 = false;
for (const h of [...byHop.keys()].sort((a, b) => a - b)) {
  const v = byHop.get(h), n = v.ok + v.lost, ci = wilson(v.lost, n);
  const powered = n >= N_ADEQUATE, inR3 = h <= 3;
  let contrib = inR3 ? '(blocks retire)' : '(route>3, not in pivot)';
  if (inR3) {
    anyRoute3 = true;
    if (ci.lo > 3) { stands = true; contrib = 'STANDS (lower>3%)'; }
    else if (powered && ci.hi < 3) contrib = 'supports retire';
    else { retire = false; contrib = powered ? 'blocks retire (upper>=3%)' : 'blocks retire (underpowered)'; }
  }
  console.log(`  hop ${h}: n=${n} lost=${v.lost} est=${ci.est}% [${ci.lo}-${ci.hi}] ${powered ? 'POWERED' : 'underpowered'} ${contrib}`);
}
console.log(`VALIDITY: receive-coverage=${(100 * recvCoverage).toFixed(1)}% (senders that also stamp rx: ${rxCapableSenders}/${txSelf.size}; floor ${100 * VOID_COVERAGE}%); population-symmetry rx/okTx=${(100 * popSymmetry).toFixed(1)}% (floor ${100 * VOID_SYMMETRY}%)`);
let verdict;
if (instrumentationVoid) verdict = 'VOID (INSTRUMENTATION) — tx (send-resolved) and rx (handler-ran) are asymmetric populations; a send stamped ok does not imply the hop arrived at an instrumented receiver. No transport loss/retire verdict can be drawn. Diagnosis remains OPEN. Fix: anchor tx and rx to the SAME transport frame event (or a transport delivered/failed callback), redeploy 4.69.0 + LAT_TRACE uniformly across the whole fleet, re-arm.';
else if (stands) verdict = 'TRANSPORT LOSS STANDS — a route<=3 stratum has Wilson lower bound > 3%';
else if (retire && anyRoute3) verdict = 'RETIRE forward-push-loss — every observed route<=3 stratum is adequately powered with Wilson upper < 3%';
else verdict = 'INCONCLUSIVE — route<=3 evidence underpowered or upper CI straddles 3%; neither retire nor stands';
console.log(`route-length distribution (delivered msgs, max hopIdx): ${JSON.stringify(routeLenDist)}`);
console.log(`VERDICT: ${verdict}`);
console.log('=============================================================================================\n');
console.log('RESULT_JSON ' + JSON.stringify({
  files: files.length, txRows, rxRows, dupTx, badId, mismatches: mismatchIds.size,
  matchedPairs: lat.length, latP50: p(50), latP95: p(95), latP99: p(99), skewMs: SKEW_MS, censorWindowMs: censorWindow, censored,
  N_ADEQUATE, okTx, silent, localFail, uninstrumented, pooledEst: pooled.est, pooledCI: [pooled.lo, pooled.hi],
  recvCoverage: +(recvCoverage).toFixed(3), popSymmetry: +(popSymmetry).toFixed(3), instrumentationVoid,
  msgWeightedPct: msgs ? +(100 * msgsLost / msgs).toFixed(2) : 0,
  perHop: Object.fromEntries([...byHop.entries()].map(([h, v]) => { const ci = wilson(v.lost, v.ok + v.lost); return [h, { n: v.ok + v.lost, lost: v.lost, est: ci.est, lo: ci.lo, hi: ci.hi, powered: (v.ok + v.lost) >= N_ADEQUATE }]; })),
  routeLenDist, verdict,
}));
