// =============================================================================
// smoke_detector_selftests.mjs — the §7 precondition for ANY network arm.
//
// "This distinguishes 'the network did not fail' from 'the harness could not
// notice the failure.'" (Aster, HARNESS-CONSOLIDATED-11.) One synthetic
// instance of every failure class goes through the REAL analyzer; each must
// produce its expected classification. A clean ledger must produce ZERO
// failure findings — a detector that cries wolf fails the harness the same
// as one that sleeps. The analyzer then runs a SECOND pass over the same
// immutable ledgers and must reproduce the findings byte-for-byte.
//
// Records are written schema-shaped with CONTROLLED wall clocks (latency
// detectors need injected time; the Ledger class stamps real clocks and is
// pinned by its own smoke). Base time is fixed — nothing here reads the
// wall.
//
// Run: node harness/test/smoke_detector_selftests.mjs
// =============================================================================
import { generatePlan } from '../lib/workload.mjs';
import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';

let passed = 0, failed = 0;
const check = (l, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${l}${c ? '' : '  ' + extra}`); c ? passed++ : failed++; };

const SEED = 101, NODES = 3, OPEN_N = 2, OWNED_N = 1, DURATION = 3_600_000;
const plan = generatePlan({ seed: SEED, nodes: NODES, durationMs: DURATION, openN: OPEN_N, ownedN: OWNED_N });
const open0 = plan.topics.findIndex((t) => t.kind === 'open');
const open1 = plan.topics.findIndex((t, i) => t.kind === 'open' && i !== open0);
const owned0 = plan.topics.findIndex((t) => t.kind === 'owned');
const T = (i) => plan.topics[i];

const BASE = Date.parse('2026-08-28T02:00:00Z');
const iso = (ms) => new Date(BASE + ms).toISOString();
const DIR = `${tmpdir()}/selftest-${process.pid}`;
mkdirSync(DIR, { recursive: true });
let mono = 0;
const emit = (peerIdx, rec, atMs) => {
  appendFileSync(`${DIR}/sidecar-${SEED}-${peerIdx}.jsonl`, JSON.stringify({
    ...rec, host: 'm4', os: 'darwin', peerIdx, wall: iso(atMs), mono: (mono += 0.1),
  }) + '\n');
};
const pub = (ti, seq, publisher, atMs, { confirmed = true, error = null } = {}) => {
  const t = T(ti); const nonce = `n-${ti}-${seq}`; const hash = `h-${ti}-${seq}`;
  emit(publisher, { t: 'intent', topic: t.name, topicSeq: seq, nonce, payloadHash: hash, author: 'A'.repeat(64) }, atMs);
  emit(publisher, { t: 'api', topic: t.name, topicSeq: seq, nonce, confirmed, msgId: confirmed ? `m-${ti}-${seq}` : null, error }, atMs + 100);
  return { t, nonce, hash, msgId: `m-${ti}-${seq}` };
};
const obs = (ti, seq, reader, atMs, { via = 'watch', hash = null, msgId = null } = {}) => {
  const t = T(ti);
  emit(reader, { t: 'observe', topic: t.name, topicSeq: seq, nonce: `n-${ti}-${seq}`,
    msgId: msgId ?? `m-${ti}-${seq}`, via, payloadHash: hash ?? `h-${ti}-${seq}` }, atMs);
};

const readersOf = (ti, publisher) => T(ti).requiredReaders.filter((p) => p !== publisher);

// ── 1. CLEAN op on open0 seq 0: all readers observe fast ─────────────
{
  const publisher = 0;
  pub(open0, 0, publisher, 0);
  for (const r of readersOf(open0, publisher)) obs(open0, 0, r, 1_500);
}
// ── 2. STRANDED WRITE on open0 seq 1: one required reader never sees ─
{
  const publisher = 0; const rs = readersOf(open0, publisher);
  pub(open0, 1, publisher, 10_000);
  obs(open0, 1, rs[0], 11_000);                       // rs[1] (if any) never observes
}
// ── 3. LATE (within reconciliation) on open1 seq 0 ───────────────────
{
  const publisher = 1; const rs = readersOf(open1, publisher);
  pub(open1, 0, publisher, 20_000);
  for (const r of rs) obs(open1, 0, r, 20_000 + 90_000);       // 90s > 60s SLO, < 300s reconcile
}
// ── 4. HOURS-LONG (beyond reconciliation) on open1 seq 1 ─────────────
{
  const publisher = 1; const rs = readersOf(open1, publisher);
  pub(open1, 1, publisher, 30_000);
  for (const r of rs) obs(open1, 1, r, 30_000 + 2 * 3_600_000);  // 2h late — retained, reclassified
}
// ── 5. STALE PULL on open0, reader sees head 5 then 3 ────────────────
{
  const r = readersOf(open0, 0)[0];
  emit(r, { t: 'pullHead', topic: T(open0).name, headSeq: 5, headMsgId: 'm-x-5' }, 40_000);
  emit(r, { t: 'pullHead', topic: T(open0).name, headSeq: 3, headMsgId: 'm-x-3' }, 45_000);
}
// ── 6. REPLAY-FOREIGN on owned0 seq 0: observation carries wrong hash ─
{
  const publisher = T(owned0).publishers; const rs = readersOf(owned0, publisher);
  pub(owned0, 0, publisher, 50_000);
  obs(owned0, 0, rs[0], 51_000, { hash: 'WRONG-HASH' });
  for (const r of rs.slice(1)) obs(owned0, 0, r, 51_000);
}
// ── 7. WEDGED WATCH on open1: early watch, then pulls advance in silence ─
{
  const r = readersOf(open1, 1)[0];
  // (the reader's last watch arrival is its case-3/4 observes; pulls advance
  //  far later than 3× cadence with no further watch)
  const lateBase = 3 * 3_600_000;                     // well past any watch
  emit(r, { t: 'pullHead', topic: T(open1).name, headSeq: 10, headMsgId: 'm-w-10' }, lateBase);
  emit(r, { t: 'pullHead', topic: T(open1).name, headSeq: 12, headMsgId: 'm-w-12' }, lateBase + 60_000);
}
// ── 8. SPLIT ROOT on owned0: two readers, same seq, different msgIds ─
{
  const rs = readersOf(owned0, T(owned0).publishers);
  const a = rs[0]; const b = rs.length > 1 ? rs[1] : readersOf(open0, 0)[0];
  emit(a, { t: 'pullHead', topic: T(owned0).name, headSeq: 7, headMsgId: 'm-root-A' }, 70_000);
  emit(b, { t: 'pullHead', topic: T(owned0).name, headSeq: 7, headMsgId: 'm-root-B' }, 75_000);
}
// ── 9. CONFIRMED:FALSE false-negative on open0 seq 2 (delivered anyway) ─
{
  const publisher = 2; const rs = readersOf(open0, publisher);
  pub(open0, 2, publisher, 80_000, { confirmed: false, error: 'timeout' });
  for (const r of rs) obs(open0, 2, r, 82_000);
}
// ── 10. CONFIRMED:FALSE true-failure on open0 seq 3 (nobody sees it) ─
{
  pub(open0, 3, 2, 90_000, { confirmed: false, error: 'timeout' });
}
// ── 11. CHURN EVENT marker ───────────────────────────────────────────
emit(0, { t: 'event', kind: 'churn:kill', detail: { host: 'm4', slot: 1 } }, 95_000);

// ── run the REAL analyzer, twice ─────────────────────────────────────
const run = (out) => {
  let stdout = '';
  let code = 0;
  try {
    stdout = execFileSync('node', ['harness/analyze.mjs', '--dir', DIR, '--seed', String(SEED),
      '--nodes', String(NODES), '--open-n', String(OPEN_N), '--owned-n', String(OWNED_N),
      '--duration-ms', String(DURATION), '--slo-ms', '60000', '--reconcile-ms', '300000',
      '--offsets', '{"m4":0}', '--out', out]).toString();
  } catch (e) { stdout = e.stdout?.toString() ?? ''; code = e.status ?? 1; }
  return { summary: JSON.parse(stdout), code };
};
console.log('detector self-tests\n');
const p1 = run(`${DIR}/findings-1.jsonl`);
const p2 = run(`${DIR}/findings-2.jsonl`);
const f = p1.summary.findings;

check('stranded-write detected', (f['stranded-write'] ?? 0) >= 1);
check('late propagation detected (within reconciliation)', (f['late-propagation'] ?? 0) >= 1);
const findingRows = readFileSync(`${DIR}/findings-1.jsonl`, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
check('hours-long retained + reclassified beyond reconciliation',
  findingRows.some((r) => r.detector === 'late-propagation' && /beyond reconciliation/.test(r.note ?? '')));
check('stale-pull detected', (f['stale-pull'] ?? 0) >= 1);
check('replay-foreign detected', (f['replay-foreign'] ?? 0) >= 1);
check('wedged-watch detected', (f['wedged-watch'] ?? 0) >= 1);
check('split-root detected', (f['split-root'] ?? 0) >= 1);
check('confirmed:false tripartite — false-negative class present',
  findingRows.some((r) => r.detector === 'confirmed-false' && /false-negative/.test(r.class ?? '')));
check('confirmed:false tripartite — true-failure class present',
  findingRows.some((r) => r.detector === 'confirmed-false' && /true-failure/.test(r.class ?? '')));
check('churn event annotated', (f['churn-event'] ?? 0) >= 1);
check('clean op delivered live (no false positive on it)', p1.summary.deliveredLive >= 1
  && !findingRows.some((r) => r.detector === 'stranded-write' && r.seq === 0 && r.topic === T(open0).name));
check('failures present → nonzero exit', p1.code === 3);
check('reproducibility: second pass byte-identical findings',
  readFileSync(`${DIR}/findings-1.jsonl`, 'utf8') === readFileSync(`${DIR}/findings-2.jsonl`, 'utf8'));
check('reproducibility: analyzer hash stable across passes',
  p1.summary.analyzerSha256 === p2.summary.analyzerSha256);
check('percentiles reported, never averages alone',
  p1.summary.latencyMs.p50 !== undefined && p1.summary.latencyMs.p99 !== undefined && p1.summary.latencyMs.max !== undefined);

// ── clean-only fleet: zero failure findings (no wolf-crying) ─────────
{
  const CDIR = `${tmpdir()}/selftest-clean-${process.pid}`;
  mkdirSync(CDIR, { recursive: true });
  const cemit = (peerIdx, rec, atMs) => appendFileSync(`${CDIR}/sidecar-${SEED}-${peerIdx}.jsonl`,
    JSON.stringify({ ...rec, host: 'm4', os: 'darwin', peerIdx, wall: iso(atMs), mono: (mono += 0.1) }) + '\n');
  const publisher = 0; const t = T(open0);
  cemit(publisher, { t: 'intent', topic: t.name, topicSeq: 0, nonce: 'c0', payloadHash: 'ch0', author: 'A'.repeat(64) }, 0);
  cemit(publisher, { t: 'api', topic: t.name, topicSeq: 0, nonce: 'c0', confirmed: true, msgId: 'cm0' }, 100);
  for (const r of readersOf(open0, publisher)) {
    cemit(r, { t: 'observe', topic: t.name, topicSeq: 0, nonce: 'c0', msgId: 'cm0', via: 'watch', payloadHash: 'ch0' }, 1_200);
  }
  let out = ''; let code = 0;
  try {
    out = execFileSync('node', ['harness/analyze.mjs', '--dir', CDIR, '--seed', String(SEED),
      '--nodes', String(NODES), '--open-n', String(OPEN_N), '--owned-n', String(OWNED_N),
      '--duration-ms', String(DURATION), '--offsets', '{"m4":0}', '--out', `${CDIR}/f.jsonl`]).toString();
  } catch (e) { out = e.stdout?.toString() ?? ''; code = e.status ?? 1; }
  const s = JSON.parse(out);
  check('clean fleet: zero failure findings, exit 0', code === 0 && s.missing === 0 && s.indeterminate === 0);
  check('clean fleet: full-set completion scored', s.fullSetComplete === 1);
  rmSync(CDIR, { recursive: true, force: true });
}
rmSync(DIR, { recursive: true, force: true });

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
