// =====================================================================
// analyze-deliver-hop.mjs — per-hop DELIVER drop from paired hop telemetry,
// to the FROZEN acceptance rules (Aster e618a19a, David-relayed 2026-09-01).
//
// Reads relay-disc-*.jsonl (kernel 4.69.0 deliver:hop_tx / deliver:hop_rx).
// Rules honoured:
//  (1) pair on hopAttemptId AND corroborate from/to/hopIdx (not msgId).
//  (2) denominator = successful transport writes only (writeOutcome 'ok');
//      pre-send failures (channel-closed/write-error/no-route/declined) reported
//      separately, never in the loss denominator.
//  (3) RIGHT-CENSOR: drop tx whose observation window is incomplete at arm end.
//      window = p99 one-hop latency (matched rx.t - tx.t) + collection skew; the
//      value is computed and published. Censored tx leave BOTH numerator+denominator.
//  (4) dedup repeated tx logs per hopAttemptId; retries are distinct attempts
//      (distinct ids). Report attempt-weighted AND message-weighted loss.
//  (5) stratify by hopIdx (and route length), counts + Wilson 95% CI per stratum.
//  (6) timestamps used ONLY for latency/censoring, never for event identity.
//
// PIVOT (prospective): retire forward-push-loss as primary ONLY IF the Wilson
// UPPER bound for per-hop silent loss is < 3% for EACH adequately-powered
// (n>=NMIN) route<=3 (hopIdx<=3) stratum. If any adequately-powered such stratum
// materially exceeds 3% (lower bound > 3%) -> transport loss STANDS. If strata are
// underpowered and the point estimate is <3% -> INCONCLUSIVE, not falsified.
//
//   node harness/analyze-deliver-hop.mjs [dir] [skewMs]
// =====================================================================
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const DIR = process.argv[2] || 'harness/results';
const SKEW_MS = Number(process.argv[3] || 250);   // clock/log collection skew added to the censor window
const NMIN = 30;                                    // adequately-powered stratum threshold
const Z = 1.96;

const files = readdirSync(DIR).filter((f) => /^relay-disc-/.test(f) && f.endsWith('.jsonl'));
if (!files.length) { console.error(`no relay-disc-*.jsonl in ${DIR}`); process.exit(2); }

const tx = new Map();   // hopAttemptId -> {hopIdx, writeOutcome, reason, msgIds, from, to, t}
const rx = new Map();   // hopAttemptId -> {hopIdx, from, to, t}
let txRows = 0, rxRows = 0, badId = 0, dupTx = 0, maxT = 0;
for (const f of files) {
  let text; try { text = readFileSync(join(DIR, f), 'utf8'); } catch { continue; }
  for (const line of text.split('\n')) {
    if (!line) continue;
    let r; try { r = JSON.parse(line); } catch { continue; }
    if (typeof r.t === 'number' && r.t > maxT) maxT = r.t;
    if (r.stage === 'deliver:hop_tx') {
      txRows++;
      if (r.hopAttemptId == null) { badId++; continue; }
      if (tx.has(r.hopAttemptId)) { dupTx++; continue; }   // (4) dedup repeated tx logs for the same attempt
      tx.set(r.hopAttemptId, { hopIdx: r.hopIdx, writeOutcome: r.writeOutcome, reason: r.reason ?? null, msgIds: r.msgIds || [], from: r.from, to: r.to, t: r.t });
    } else if (r.stage === 'deliver:hop_rx') {
      rxRows++;
      if (r.hopAttemptId != null && !rx.has(r.hopAttemptId)) rx.set(r.hopAttemptId, { hopIdx: r.hopIdx, from: r.from, to: r.to, t: r.t });
    }
  }
}

// (1) matched = rx exists with corroborating from/to/hopIdx; (3) one-hop latency from matched pairs
let mismatch = 0; const lat = [];
const matchedOk = (id, t) => {
  const r = rx.get(id); if (!r) return false;
  if (r.hopIdx !== t.hopIdx || r.from !== t.from || r.to !== t.to) { mismatch++; return false; }
  if (typeof r.t === 'number' && typeof t.t === 'number') lat.push(r.t - t.t);
  return true;
};
// first pass: classify matches to build the latency distribution
for (const [id, t] of tx) { if (t.writeOutcome === 'ok') matchedOk(id, t); }
lat.sort((a, b) => a - b);
const p = (q) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(q / 100 * lat.length))] : 0;
const censorWindow = (lat.length ? Math.max(0, p(99)) : 0) + SKEW_MS;   // (3)
const censorCutoff = maxT - censorWindow;

// Wilson 95% CI for k/n
const wilson = (k, n) => {
  if (!n) return { lo: 0, hi: 0, est: 0 };
  const ph = k / n, z2 = Z * Z;
  const den = 1 + z2 / n, c = (ph + z2 / (2 * n)) / den;
  const h = (Z * Math.sqrt(ph * (1 - ph) / n + z2 / (4 * n * n))) / den;
  return { est: +(100 * ph).toFixed(2), lo: +(100 * Math.max(0, c - h)).toFixed(2), hi: +(100 * Math.min(1, c + h)).toFixed(2) };
};

// classify with censoring; stratify by hopIdx; attempt- and message-weighted
const byHop = new Map();                     // hopIdx -> {ok, lost}
const msgHops = new Map();                   // msgId -> {maxHop, lostAtAnyHop}
let okTx = 0, silent = 0, localFail = 0, censored = 0;
const failReasons = new Map();
for (const [id, t] of tx) {
  if (t.writeOutcome !== 'ok') { localFail++; failReasons.set(t.reason || t.writeOutcome, (failReasons.get(t.reason || t.writeOutcome) || 0) + 1); continue; }
  if (typeof t.t === 'number' && t.t > censorCutoff) { censored++; continue; }   // (3) right-censor incomplete-window tx
  okTx++;
  const matched = matchedOk(id, t);
  const h = byHop.get(t.hopIdx) || { ok: 0, lost: 0 };
  if (matched) h.ok++; else { h.lost++; silent++; }
  byHop.set(t.hopIdx, h);
  for (const m of t.msgIds) {
    const mm = msgHops.get(m) || { maxHop: 0, lost: false };
    mm.maxHop = Math.max(mm.maxHop, t.hopIdx); if (!matched) mm.lost = true;
    msgHops.set(m, mm);
  }
}
// message-weighted loss
let msgs = 0, msgsLost = 0; const routeLenDist = {};
for (const [, mm] of msgHops) { msgs++; if (mm.lost) msgsLost++; routeLenDist[mm.maxHop] = (routeLenDist[mm.maxHop] || 0) + 1; }

const pooled = wilson(silent, okTx);
console.log('\n============ PER-HOP DELIVER DROP (live 4.69.0, frozen rules e618a19a) ============');
console.log(`relay-disc files ${files.length}  tx rows ${txRows} (dup ${dupTx}, no-id ${badId})  rx rows ${rxRows}  pair mismatches ${mismatch}`);
console.log(`one-hop latency p50/p95/p99 = ${p(50)}/${p(95)}/${p(99)}ms; censor window = p99+skew(${SKEW_MS}) = ${censorWindow}ms; censored tx = ${censored}`);
console.log(`eligible ok-write tx (denominator, post-censor): ${okTx}`);
console.log(`local pre-send failures (separate): ${localFail}  ${JSON.stringify(Object.fromEntries(failReasons))}`);
console.log(`ATTEMPT-weighted per-hop silent loss (pooled): ${pooled.est}%  [95% CI ${pooled.lo}–${pooled.hi}]`);
console.log(`MESSAGE-weighted loss: ${msgs ? (100 * msgsLost / msgs).toFixed(2) : '0'}%  (${msgsLost}/${msgs} messages lost at >=1 hop)`);
console.log('per-hop strata (hopIdx: ok/lost, est [CI], powered?):');
let retire = true, stands = false, anyPowered = false;
for (const h of [...byHop.keys()].sort((a, b) => a - b)) {
  const v = byHop.get(h); const n = v.ok + v.lost; const ci = wilson(v.lost, n);
  const powered = n >= NMIN; const inRoute3 = h <= 3;
  console.log(`  hop ${h}: ${v.ok}/${v.lost}  est ${ci.est}% [${ci.lo}–${ci.hi}]  n=${n} ${powered ? 'POWERED' : 'underpowered'}${inRoute3 ? '' : ' (route>3)'}`);
  if (inRoute3 && powered) { anyPowered = true; if (ci.hi >= 3) retire = false; if (ci.lo > 3) stands = true; }
  if (inRoute3 && !powered && ci.est >= 3) retire = false;   // can't retire on an underpowered elevated stratum
}
console.log(`route-length distribution (delivered msgs, max hopIdx): ${JSON.stringify(routeLenDist)}`);
let verdict;
if (stands) verdict = 'TRANSPORT LOSS STANDS — an adequately-powered route<=3 hop stratum materially exceeds 3% (lower CI > 3%)';
else if (retire && anyPowered) verdict = 'RETIRE forward-push-loss — every adequately-powered route<=3 stratum has upper CI < 3%';
else verdict = 'INCONCLUSIVE — route<=3 strata underpowered or upper CI straddles 3%; do not falsify';
console.log(`VERDICT: ${verdict}`);
console.log('====================================================================================\n');
console.log('RESULT_JSON ' + JSON.stringify({
  files: files.length, txRows, rxRows, dupTx, badId, mismatch,
  latP50: p(50), latP95: p(95), latP99: p(99), censorWindowMs: censorWindow, censored,
  okTx, silent, localFail, pooledEst: pooled.est, pooledCI: [pooled.lo, pooled.hi],
  msgWeightedPct: msgs ? +(100 * msgsLost / msgs).toFixed(2) : 0,
  perHop: Object.fromEntries([...byHop.entries()].map(([h, v]) => [h, { ok: v.ok, lost: v.lost, ...wilson(v.lost, v.ok + v.lost) }])),
  routeLenDist, verdict,
}));
