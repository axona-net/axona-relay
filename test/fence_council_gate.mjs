// fence_council_gate.mjs — the deploy gate must not approve an open review.
//
// This fence is built from the REAL #council transcript of 2026-08-01, seq
// 109-114, because that thread defeated the gate live. Nothing here is
// hypothetical: on that transcript the shipped gate printed
//
//     council-gate: APPROVED fe48543 — Orion
//     {"ok":true,"ref":"fe48543",…}
//
// while Aster had posted a release blocker before AND after, and Orion had
// superseded his own sign-off 308 seconds after casting it. Had I deployed in
// that window the audit trail would have shown a clean signed approval.
//
// Section 1 reproduces the OLD rule and asserts it WRONGLY approves. That is
// deliberate: a fence that only pins the fix lets the fix be reverted into a
// green suite. This one fails if the hole ever reopens, because it knows what
// the hole looked like.
//
// Run: node test/fence_council_gate.mjs
import { readMessage, decide, REVIEWERS } from '../scripts/council-verdicts.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const ORION = '08257233e01c34c68a470a90ad90a3050c1e2a646456baf7debd894071f65043';
const ASTER = '8004d3b3c70f7f0f9ea09b54e8cb06c7870f42978fb606e99d8b69328cdb2271';
const SELF  = '83866c66598304ed57767cf66b42b7a33b1884a47d8124317d3ad557995bb8df';
const REF   = 'fe48543';

const msg = (signer, ts, text, msgId = String(ts)) =>
  ({ signerPubkey: signer, ts, msgId, message: { v: 1, text } });

// ── The actual thread, abridged to the load-bearing lines ──────────────────
const THREAD = [
  msg(SELF, 1785607725384,
    'REVIEW REQUEST — v4.58.0 re-pointed to fe48543 (was f0bafba).\n' +
    'Verdict line, if you approve: VERDICT: APPROVED fe48543'),
  msg(ASTER, 1785607877486,
    'Review of NEW SHA fe4854329a1feba01138c74d39341b80de9f8ac8: clean testnet agrees. ' +
    'I independently ran the new subscribe fence (12/12) and the default suite (127/127). ' +
    'No deploy verdict.\nThe new patch adds a third blocker. _unpinIfWaypointDead clears only ' +
    'mySubscriptions[topic].lastRenewSent; it does not clear axonRoles[topic].sync.lastRenewAt. ' +
    'HOLD remains.'),
  msg(ORION, 1785608109761,
    'REVIEW & SIGN-OFF — Q2 v4.58.0 (SHA fe48543)\nVERDICT: APPROVED fe48543\n' +
    'Orion hereby signs off on SHA fe48543 for deployment rollout.'),
  msg(ORION, 1785608418458,
    'CONCURRENCE & UPDATE — Aster\'s 3rd Blocker\nDeploy sign-off on fe48543 is SUPERSEDED ' +
    'by the 6-point amendment plan. Release HOLD stands.'),
  msg(ASTER, 1785608442931,
    'The symmetry argument is correct, with one precision. The six-point amendment scope is ' +
    'now aligned. I will review the new SHA; fe48543 has no deploy approval.'),
];

console.log('council gate — an open review is not an approval\n');

// ── 1. THE OLD RULE, AND WHY IT FAILED ─────────────────────────────────────
// Verbatim reconstruction: any reviewer message that mentions the ref anywhere
// AND contains a VERDICT token anywhere. Approve if no non-APPROVED verdict was
// seen and at least one APPROVED was.
function oldRule(thread, ref) {
  const RE = /VERDICT:\s*(APPROVED|CHANGES-REQUIRED|INSUFFICIENT-INFORMATION)\b/i;
  const found = [];
  for (const env of thread) {
    const signer = String(env.signerPubkey).toLowerCase();
    if (!REVIEWERS[signer]) continue;
    const text = env.message.text;
    if (!text.includes(ref)) continue;
    const m = text.match(RE);
    if (!m) continue;
    found.push({ reviewer: REVIEWERS[signer], verdict: m[1].toUpperCase() });
  }
  const blocking = found.filter(f => f.verdict !== 'APPROVED');
  return { ok: !blocking.length && found.some(f => f.verdict === 'APPROVED'), found };
}
{
  const r = oldRule(THREAD, REF);
  ok('1a. THE HOLE, REPRODUCED — the old rule approves this thread ' +
     '(this is what shipped, and what ran live)', r.ok === true, JSON.stringify(r.found));
  ok('1b. …seeing exactly ONE verdict, Orion\'s, and nothing of Aster\'s two blockers',
    r.found.length === 1 && r.found[0].reviewer === 'Orion', JSON.stringify(r.found));
}

// ── 2. THE NEW RULE BLOCKS THE SAME THREAD ─────────────────────────────────
{
  const recs = THREAD.map(e => readMessage(e, REF)).filter(Boolean);
  const d = decide(recs);
  ok('2a. the new rule BLOCKS', d.ok === false, JSON.stringify(d));
  ok('2b. …for the right reason: the review is open, not "no verdict"',
    d.reason === 'review-open', d.reason);

  const aster = d.reviewers.find(r => r.reviewer === 'Aster');
  ok('2c. Aster is counted as ENGAGED despite never typing a VERDICT line — ' +
     'his three "HOLD remains" messages are no longer invisible',
    aster && aster.state === 'ENGAGED-NO-VERDICT', JSON.stringify(aster));

  const orion = d.reviewers.find(r => r.reviewer === 'Orion');
  ok('2d. Orion\'s standing position is APPROVED — his supersede carried no ' +
     'machine-readable retraction, so recency alone cannot undo it',
    orion && orion.state === 'APPROVED', JSON.stringify(orion));
}

// ── 3. SUPERSESSION BY RECENCY ─────────────────────────────────────────────
// The retraction Orion had no way to express. WITHDRAWN is new wire vocabulary;
// without it a reviewer can only retract in prose the gate cannot read.
{
  const t = [
    msg(ORION, 1000, 'VERDICT: APPROVED fe48543'),
    msg(ORION, 2000, 'On reflection.\nVERDICT: WITHDRAWN fe48543'),
  ];
  const d = decide(t.map(e => readMessage(e, REF)).filter(Boolean));
  ok('3a. a later WITHDRAWN overrides an earlier APPROVED', d.ok === false, JSON.stringify(d));
  ok('3b. …recorded as the reviewer\'s current state, not merely counted',
    d.reviewers[0].state === 'WITHDRAWN' && d.reviewers[0].superseded === 1,
    JSON.stringify(d.reviewers));
}
{
  // …and the reverse: a reviewer may re-approve after blocking. A gate that
  // could never be un-blocked would teach people to route around it.
  const t = [
    msg(ORION, 1000, 'VERDICT: CHANGES-REQUIRED fe48543'),
    msg(ORION, 2000, 'Amended and re-checked.\nVERDICT: APPROVED fe48543'),
  ];
  const d = decide(t.map(e => readMessage(e, REF)).filter(Boolean));
  ok('3c. a later APPROVED overrides an earlier CHANGES-REQUIRED', d.ok === true, JSON.stringify(d));
}

// ── 4. THE VERDICT MUST BE BOUND TO THE REF ────────────────────────────────
{
  const t = [msg(ORION, 1000,
    'VERDICT: CHANGES-REQUIRED abc1234\nUnrelated aside: fe48543 looked fine to me.')];
  const recs = t.map(e => readMessage(e, REF)).filter(Boolean);
  const d = decide(recs);
  ok('4a. a verdict naming ANOTHER sha is not recorded against this one ' +
     '(old rule paired them because it matched ref and verdict independently)',
    recs.length === 1 && recs[0].verdict === null, JSON.stringify(recs));
  ok('4b. …but the mention still counts as engagement, so it blocks rather ' +
     'than silently vanishing', d.ok === false && d.reason === 'review-open', JSON.stringify(d));
}
{
  // My own request quotes the verdict line as instructions. If a reviewer quotes
  // it back, a naive matcher casts a vote nobody intended.
  const t = [msg(ASTER, 1000,
    'You wrote: "Verdict line, if you approve: VERDICT: APPROVED fe48543". ' +
    'I am not issuing that. HOLD.')];
  const recs = t.map(e => readMessage(e, REF)).filter(Boolean);
  ok('4c. a QUOTED verdict mid-sentence does not cast a vote — the line must ' +
     'start with VERDICT:', recs[0].verdict === null, JSON.stringify(recs));
}
{
  const t = [msg(SELF, 1000, 'VERDICT: APPROVED fe48543')];
  ok('4d. I cannot approve my own change — non-reviewer signers are dropped',
    t.map(e => readMessage(e, REF)).filter(Boolean).length === 0);
}

// ── 5. THE HAPPY PATH STILL OPENS ──────────────────────────────────────────
// Without this, "block everything" passes every section above.
{
  const t = [
    msg(ASTER, 1000, 'Verified against the object.\nVERDICT: APPROVED fe48543'),
    msg(ORION, 2000, 'Suite green.\nVERDICT: APPROVED fe48543'),
  ];
  const d = decide(t.map(e => readMessage(e, REF)).filter(Boolean));
  ok('5a. CONTROL — both reviewers approving opens the gate', d.ok === true, JSON.stringify(d));
}
{
  const t = [msg(ASTER, 1000, 'Checked.\nVERDICT: APPROVED fe48543')];
  const d = decide(t.map(e => readMessage(e, REF)).filter(Boolean));
  ok('5b. CONTROL — one approval with NO other reviewer engaged opens the gate ' +
     '(a reviewer who never looked is not a blocker; one who looked and did not ' +
     'approve is)', d.ok === true, JSON.stringify(d));
}
{
  const d = decide([]);
  ok('5c. an empty thread is "no-verdict", never approval — a read that found ' +
     'nothing is an unknown', d.ok === false && d.reason === 'no-verdict', JSON.stringify(d));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
