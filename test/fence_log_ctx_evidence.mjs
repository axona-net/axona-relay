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
import { renderCtx, isEvidence, isFullFidelity, CTX_CAP, EVIDENCE_CAP } from '../src/logctx.js';

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

// =====================================================================
// 0.127.0 — largest-first reduction, and the health-dump exemption (GH #63).
//
// The reduction above was all-or-nothing. Reading the lookahead census off the
// prod droplets for #62, eleven relays came back whole and the twelfth read:
//
//   "lookahead":"<object omitted: ctx over 4000c>",
//   "seated":"<27 entries omitted: ctx over 4000c>"
//
// One oversized array cost the record every other piece of evidence in it. And
// both values that can overflow grow with load, so the dump went blind exactly
// on the relays worth reading.
//
// A CLAIM THIS FENCE FALSIFIED WHILE IT WAS BEING WRITTEN. I wrote on #63 that
// largest-first would drop `seated` and keep the census. Built to scale, the
// census is the BIGGER value — 2,187 bytes against 1,628 — so largest-first
// drops the census and keeps `seated`. Both were omitted on the observed line,
// so the output said nothing about which was larger, and I assumed. That is why
// the sizes below are asserted rather than described: an assumption about which
// value is bigger is the whole content of the claim.
// =====================================================================
console.log('\n— largest-first: one oversized value must not cost the others —');

// THE #63 SHAPE, to scale, from the sfo3/useast dump of 2026-09-10 00:24 UTC.
const RANK_LABELS = ['0', '1', '2', '3', '4', '5', '6', '7', '8-15', '16-31', '32+'];
const seated27 = Array.from({ length: 27 }, (_, i) => ({
  topic: `898be2a9d3${String(i).padStart(2, '0')}`, isRoot: i % 9 === 0, kids: 0, cache: i * 7,
}));
const census = {
  sinceMs: 14782087, calls: 116506, bypassedAtDestination: 63900, probingCalls: 52606,
  probesEmitted: 1485120, probesPerCall: 28.2, probesEmittedPerSec: 100.5,
  probesFulfilled: 1248653, probesRejected: 236317, probesTerminal: 1100224,
  probesCloserThanMe: 148429, answeredByProbe: 45691, answeredByIncoming: 0, answeredNull: 6910,
  usefulProbeRate: 0.8686, closerReplyRate: 0.1189,
  byRank: RANK_LABELS.map((rank) => ({ rank, sent: 52606, closer: 218, rejected: 37280,
    terminal: 15103, nonCloser: 0, rate: 0.0041, rateOfReplies: 0.0142 })),
  callsWithAnyCloser: 45691,
  topK: [1, 2, 4, 8, 16, 32].map((k) => ({ k, answered: 218, retained: 0.0048 })),
  targetsNearerThanSelf: 13054, incomingCandidateLinks: 0, incomingCouldAnswerCalls: 0,
  incomingWonFinalCalls: 0, incomingCouldAnswerRate: 0,
};
const dump27 = {
  peers: 54, synaptome: 54, subscriptions: 0, roles: 27, rooted: 1, saturated: false,
  helloPressure: 0.412, servicePressure: 0.02, tickLagMaxMs: 2061, tickLagPeakMs: 6792,
  tickDurMs: 94, tickStalls: 7, worstObligation: 'BACKUP', overdueFrac: 0,
  lookahead: census, seated: seated27,
};
{
  check('the shape really does exceed the cap (else the rest proves nothing)',
    JSON.stringify(dump27).length > EVIDENCE_CAP, `len=${JSON.stringify(dump27).length}`);
  check('…and only just: 27 roles tipped it, 17 on the sibling relay did not',
    JSON.stringify(dump27).length - EVIDENCE_CAP < 200,
    `over by ${JSON.stringify(dump27).length - EVIDENCE_CAP}`);
  // The claim I got wrong, pinned as a size relation so it cannot be re-assumed.
  check('THE CENSUS IS THE LARGER VALUE, not `seated`',
    JSON.stringify(census).length > JSON.stringify(seated27).length,
    `census=${JSON.stringify(census).length} seated=${JSON.stringify(seated27).length}`);

  // Under a NON-exempt name, so this tests the reduction and not the exemption.
  const out = renderCtx('some-other-event', dump27);
  const back = JSON.parse(out);
  check('so largest-first drops the CENSUS — the opposite of what #63 claimed',
    typeof back.lookahead === 'string' && back.lookahead.includes('object omitted'),
    typeof back.lookahead);
  check('…and `seated` survives whole',
    Array.isArray(back.seated) && back.seated.length === 27, `${back.seated?.length}`);
  check('…exactly ONE value was dropped, which is the improvement over all-or-nothing',
    [back.lookahead, back.seated].filter((v) => typeof v === 'string').length === 1);
  check('…scalars are untouched', back.peers === 54 && back.worstObligation === 'BACKUP');
  check('…and the result fits the cap', out.length <= EVIDENCE_CAP, `len=${out.length}`);
}
{
  // Stop EARLY. Two oversized arrays where dropping the larger alone suffices:
  // the smaller must stay. A reducer that keeps going drops both.
  const ctx = { a: 1,
    big: Array.from({ length: 900 }, (_, i) => ({ id: `${i}`, v: 'fail' })),
    small: [{ id: 'keep-me', v: 'ok' }] };
  const back = JSON.parse(renderCtx('replicate-all-failed', ctx));
  check('the big array goes', typeof back.big === 'string' && back.big.includes('900 entries'));
  check('the small array STAYS — the reducer stopped as soon as it fit',
    Array.isArray(back.small) && back.small[0].id === 'keep-me', JSON.stringify(back.small));
}
{
  // Equal-sized values must reduce identically everywhere, or two relays in one
  // fleet report different things about the same condition.
  const mk = () => ({ z: Array.from({ length: 400 }, (_, i) => i),
                      a: Array.from({ length: 400 }, (_, i) => i), tail: 'x' });
  check('ties break deterministically by key',
    renderCtx('e', mk()) === renderCtx('e', mk()));
}
{
  // A value smaller than its own placeholder must not be "dropped" — that GROWS
  // the record. Over the cap by a little beats destroying evidence to get under.
  const tiny = { note: 'n'.repeat(4_050), who: [1] };
  const back = JSON.parse(renderCtx('e', tiny));
  check('a value smaller than its own note is left alone, even while over cap',
    Array.isArray(back.who) && back.who[0] === 1, JSON.stringify(back.who));
  check('…and the line is still whole and parseable, which is the real invariant',
    back.note.length === 4_050);
}

console.log('\n— health-dump is a standing full-fidelity contract —');
{
  // THE ACTUAL FIX for #63. Largest-first alone would have taken the census;
  // only the exemption keeps BOTH values on the record that prompted the issue.
  const back = JSON.parse(renderCtx('health-dump', dump27));
  check('the 27-role dump keeps its census',
    back.lookahead?.probesEmitted === 1485120, typeof back.lookahead);
  check('…and its rank buckets, which are the reason to read it',
    back.lookahead?.byRank?.length === 11, `${back.lookahead?.byRank?.length}`);
  check('…and `seated` as well — nothing is traded away',
    Array.isArray(back.seated) && back.seated.length === 27, `${back.seated?.length}`);

  const huge = { peers: 54, roles: 300,
    seated: Array.from({ length: 300 }, (_, i) => ({ topic: `t${i}`, isRoot: false, kids: 0, cache: 0 })),
    lookahead: { probesEmitted: 6236579, byRank: Array.from({ length: 11 }, (_, i) => ({ rank: `${i}`, sent: 468830 })) } };
  check('health-dump is emitted WHOLE however many roles are seated',
    renderCtx('health-dump', huge) === JSON.stringify(huge),
    `len=${renderCtx('health-dump', huge).length} of ${JSON.stringify(huge).length}`);
  check('isFullFidelity says so by name', isFullFidelity('health-dump') === true);
  check('armed-* keeps its contract too', isFullFidelity('armed-modules') === true);
  check('the exemption is EXACT, not a suffix match — an event that merely ends '
    + 'in the name is still reduced', isFullFidelity('pubsub:health-dump') === false);
  check('and an ordinary event is not exempt', isFullFidelity('routed-outcomes') === false);
  check('a non-string event name does not throw', isFullFidelity(undefined) === false);
}
{
  // The sibling events are NOT exempt: they are emitted by the relay itself, on
  // its own schedule, and the shape rule is what bounds them.
  check('health-dump-failed is not exempt', isFullFidelity('health-dump-failed') === false);
  check('health-dump-unavailable is not exempt', isFullFidelity('health-dump-unavailable') === false);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
