// =====================================================================
// fence_log_ctx_evidence.mjs — relay 0.118.0.
//
// The 120-char ctx cap severed four evidence surfaces in one day
// (2026-09-07), each discovered the same way: by reading a production line
// and noticing it ended mid-token. The allowlist cure fixed one name at a
// time. This fence pins the CLASS rule that replaced it, using the real
// payload shapes of all four events that bit — so the fifth is caught by
// construction rather than by another investigation.
//
// Run: node test/fence_log_ctx_evidence.mjs
// =====================================================================
import { renderCtx, isEvidence, CTX_CAP, EVIDENCE_CAP } from '../src/logctx.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const parses = (s) => { try { JSON.parse(s); return true; } catch { return false; } };

console.log('0.118.0 — a severed log line is not a shorter record, it is a false one\n');

// ── the four real surfaces ──────────────────────────────────────────────
console.log('— the four payloads that actually bit —');

// 1. health-dump: `seated`, one entry per role. Cut inside entry ONE at 120.
const healthDump = {
  peers: 12, synaptome: 12, subscriptions: 0, roles: 11, rooted: 0,
  seated: Array.from({ length: 11 }, (_, i) => ({
    topic: `80ac82e6ba${String(i).padStart(2, '0')}`, isRoot: false, kids: 0, cache: 0 })),
};
// 2. routed-outcomes: `top`, worst-first. top[2] was always severed.
const routedOutcomes = {
  ok: 3, failed: 38, tracked: 8,
  top: [{ id: '89956427e2ea', n: 26 }, { id: '80373ead99e7', n: 2 }, { id: '874af2393dbb', n: 1 }],
};
// 3. replicate-all-failed: `targets` — WHO the push failed to.
const replicateAllFailed = {
  topic: '89cf1d361d52', attempted: 2, failed: 2, unsupported: 0, violation: 0,
  targets: [{ id: '89d5f8f53599', v: 'fail' }, { id: '89da74ab2fa6', v: 'fail' }],
};
// 4. kill-replicate-all-failed: same shape. Never investigated, never severed now.
const killReplicate = {
  topic: '89cf1d361d52', msgId: 'ab'.repeat(16), attempted: 2, failed: 2,
  targets: [{ id: '89d5f8f53599', v: 'fail' }, { id: '89da74ab2fa6', v: 'fail' }],
};

for (const [name, ctx] of [
  ['health-dump', healthDump],
  ['routed-outcomes', routedOutcomes],
  ['replicate-all-failed', replicateAllFailed],
  ['kill-replicate-all-failed', killReplicate],
]) {
  const out = renderCtx(`pubsub:${name}`, ctx);
  const full = JSON.stringify(ctx);
  check(`${name}: emitted WHOLE (was severed at ${CTX_CAP})`, out === full,
    `len=${out.length} want=${full.length}`);
  check(`${name}: …and parses`, parses(out));
}

// The regression itself, stated as a length: each of these EXCEEDS the old cap,
// so a name-blind 120-slice would have cut every one.
console.log('\n— the regression these four share —');
for (const [name, ctx] of [['health-dump', healthDump], ['routed-outcomes', routedOutcomes],
                           ['replicate-all-failed', replicateAllFailed]]) {
  check(`${name} exceeds the old cap (so the old code cut it)`,
    JSON.stringify(ctx).length > CTX_CAP, `len=${JSON.stringify(ctx).length}`);
}

// ── the class rule, not the names ───────────────────────────────────────
console.log('\n— decided by shape, never by event name —');
{
  // An event nobody has thought of yet, carrying a list. This is the case the
  // allowlist could not cover, and the whole reason the rule moved to content.
  const future = { thing: 'x'.repeat(150), whom: [{ id: 'aa', n: 1 }, { id: 'bb', n: 2 }] };
  const out = renderCtx('some-event-invented-next-month', future);
  check('an UNKNOWN event carrying an array is emitted whole', out === JSON.stringify(future));
  check('…and parses', parses(out));
}
{
  const nested = { a: 1, detail: { why: 'y'.repeat(200) } };
  check('a nested OBJECT counts as evidence too, not just arrays',
    renderCtx('e', nested) === JSON.stringify(nested));
}
{
  // Flat scalars keep the cheap cap: losing a tail field cannot invent one.
  const chatty = { url: 'wss://bridge.axona.net/' + 'z'.repeat(200), state: 'open' };
  const out = renderCtx('transport-open', chatty);
  check('a FLAT scalar ctx is still capped — by trimming its VALUE',
    JSON.parse(out).url.length === CTX_CAP + '<cut>'.length, `len=${JSON.parse(out).url.length}`);
  check('…and stays parseable, unlike the byte-offset slice it replaces', parses(out));
  check('…and its other fields survive the trim', JSON.parse(out).state === 'open');
  check('isEvidence() says flat scalars are not evidence', isEvidence(chatty) === false);
  check('isEvidence() says an array-bearing ctx IS evidence', isEvidence(routedOutcomes) === true);
}
{
  const small = { a: 1, b: 2 };
  check('a short ctx is untouched whatever its shape',
    renderCtx('e', small) === JSON.stringify(small));
}

// ── armed-* keeps its standing contract ─────────────────────────────────
console.log('\n— armed-* contract survives the rewrite —');
{
  // Flat, long, and NOT evidence by shape — it must still emit whole, because
  // the canary runbook reads thresholds out of it. Name rule, deliberately.
  const armed = { modules: 'm'.repeat(300), ok: true };
  check('armed-* is emitted whole even when flat and over cap',
    renderCtx('armed-modules', armed) === JSON.stringify(armed));
}

// ── the invariant: never a severed token ────────────────────────────────
console.log('\n— the invariant —');
{
  const huge = { topic: 'abc', targets: Array.from({ length: 4000 }, (_, i) => ({ id: `${i}`, v: 'fail' })) };
  const out = renderCtx('replicate-all-failed', huge);
  check('an oversized evidence ctx is REDUCED, not cut', out.length < EVIDENCE_CAP,
    `len=${out.length}`);
  check('…and the reduction still parses as JSON', parses(out));
  const back = JSON.parse(out);
  check('…scalars survive the reduction', back.topic === 'abc');
  check('…and it SAYS how many entries went, rather than implying none',
    typeof back.targets === 'string' && back.targets.includes('4000 entries'), back.targets);
}
{
  // Every branch, one property: what we hand the logger is always parseable.
  const cases = [healthDump, routedOutcomes, replicateAllFailed, killReplicate,
    { flat: 'f'.repeat(400) }, { a: 1 },
    { big: Array.from({ length: 9000 }, (_, i) => i) }];
  check('EVERY branch emits parseable JSON',
    cases.every((c) => parses(renderCtx('e', c))));
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
