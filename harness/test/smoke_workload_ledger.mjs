// smoke_workload_ledger.mjs — harness build unit 1: seeded plan + ledger.
// Run: node harness/test/smoke_workload_ledger.mjs
import { generatePlan, planCanonical } from '../lib/workload.mjs';
import { Ledger, sha256, truncNode } from '../lib/ledger.mjs';
import { readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';

let passed = 0, failed = 0;
const check = (l, c) => { console.log(`  ${c ? '✓' : '✗'} ${l}`); c ? passed++ : failed++; };

console.log('harness workload + ledger smoke\n');

// ── determinism: the §7 attribution gate stands on this ──────────────
{
  const a = planCanonical(generatePlan({ seed: 42, nodes: 8, durationMs: 3_600_000 }));
  const b = planCanonical(generatePlan({ seed: 42, nodes: 8, durationMs: 3_600_000 }));
  const c = planCanonical(generatePlan({ seed: 43, nodes: 8, durationMs: 3_600_000 }));
  check('same seed → byte-identical plan', a === b);
  check('different seed → different plan', a !== c);
  check('plan hash is stable', sha256(a) === sha256(b));
}

// ── shape against the frozen spec ────────────────────────────────────
{
  const p = generatePlan({ seed: 7, nodes: 10, durationMs: 12 * 3_600_000 });
  const open  = p.topics.filter((t) => t.kind === 'open');
  const owned = p.topics.filter((t) => t.kind === 'owned');
  check('open topics in 10..20', open.length >= 10 && open.length <= 20);
  check('owned topics in 5..10', owned.length >= 5 && owned.length <= 10);
  check('open topics: ALL nodes are required readers',
    open.every((t) => t.requiredReaders.length === 10));
  check('owned topics: publisher excluded from its reader set',
    owned.every((t) => !t.requiredReaders.includes(t.publishers)));
  check('owned topics: ≥3 required readers',
    owned.every((t) => t.requiredReaders.length >= 3));
  check('cadences within 30–120s',
    p.topics.every((t) => t.cadenceMs >= 30_000 && t.cadenceMs <= 120_000));
  const sorted = p.schedule.every((e, i) => i === 0 || p.schedule[i - 1].atMs <= e.atMs);
  check('schedule sorted by time', sorted);
  check('bursts present', p.schedule.some((e) => e.burst));
  const perTopicSeq = new Map();
  let seqOk = true;
  for (const e of p.schedule) {
    const prev = perTopicSeq.get(e.topic);
    if (prev !== undefined && e.seq !== prev + 1) seqOk = false;
    perTopicSeq.set(e.topic, e.seq);
  }
  check('per-topic seq strictly monotonic (+1)', seqOk);
  check('every event inside the window', p.schedule.every((e) => e.atMs < p.durationMs));
}

// ── regression: tiny node counts must terminate (the nodes=3 spin) ───
{
  const p3 = generatePlan({ seed: 9, nodes: 3, durationMs: 600_000, openN: 2, ownedN: 2 });
  check('nodes=3 generates (no group-builder spin)', p3.topics.length === 4);
  check('nodes=3 owned groups fit the universe',
    p3.topics.filter((t) => t.kind === 'owned')
      .every((t) => t.requiredReaders.length <= 2 && t.requiredReaders.length >= 1));
}

// ── ledger: three truths, monotonic time, truncation rule ────────────
{
  const path = `${tmpdir()}/harness-ledger-smoke-${process.pid}.jsonl`;
  const led = new Ledger(path, { host: 'm4', os: 'darwin', peerIdx: 0, author: 'a'.repeat(64) });
  led.intent({ topic: 'harness/open-1-0', topicSeq: 0, nonce: 'n0', payloadHash: sha256('hello') });
  led.api({ topic: 'harness/open-1-0', topicSeq: 0, nonce: 'n0', confirmed: true, msgId: 'm0' });
  led.observe({ topic: 'harness/open-1-0', topicSeq: 0, nonce: 'n0', msgId: 'm0', via: 'watch', payloadHash: sha256('hello') });
  led.pullHead({ topic: 'harness/open-1-0', headSeq: 0, headMsgId: 'm0' });
  led.event({ kind: 'churn:kill', detail: { host: 'm4', slot: 3 } });
  const recs = readFileSync(path, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  rmSync(path);
  check('five records emitted', recs.length === 5);
  check('three truths present', ['intent', 'api', 'observe'].every((t) => recs.some((r) => r.t === t)));
  check('every record carries host/os/peerIdx/wall/mono',
    recs.every((r) => r.host === 'm4' && r.os === 'darwin' && r.peerIdx === 0
      && typeof r.wall === 'string' && typeof r.mono === 'number'));
  check('mono is non-decreasing', recs.every((r, i) => i === 0 || recs[i - 1].mono <= r.mono));
  check('intent carries the full author (authors MAY persist)',
    recs.find((r) => r.t === 'intent').author?.length === 64);
  check('truncNode enforces the 12-char transport-id rule', truncNode('f'.repeat(66)).length === 12);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
