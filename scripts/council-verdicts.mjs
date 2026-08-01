// council-verdicts.mjs — how the gate reads a review thread.
//
// Extracted from council-gate.mjs so the DECISION is testable without a live
// network. The gate keeps the network I/O; this file keeps the judgement, and
// fence_council_gate.mjs drives it with the real #council transcript.
//
// WHY THIS EXISTS. On 2026-08-01 the gate approved fe48543 while the review was
// openly unresolved. Orion posted "VERDICT: APPROVED fe48543" and superseded it
// 308 seconds later; Aster had already posted a release blocker and posted a
// second one after. The gate reported one clean approval. Had I deployed, the
// audit trail would have shown a signed approval and nothing else.
//
// THREE DISTINCT DEFECTS, all the same shape as the kernel work they were
// gating — a stale or partial positive read as current, complete evidence:
//
//   1. NO SUPERSESSION. Any matching APPROVED counted forever. A reviewer could
//      withdraw in prose and the gate would never know. There was also no
//      machine-readable way to withdraw, so the retraction had nowhere to live.
//
//   2. SILENCE FROM AN ENGAGED REVIEWER READ AS CONSENT. Aster wrote "HOLD
//      remains", "No deploy verdict", and "fe48543 has no deploy approval"
//      across three messages. None contained the token the gate greps for, so
//      all three counted as nothing. A reviewer actively holding a release was
//      indistinguishable from a reviewer who had never heard of it.
//
//   3. THE VERDICT WAS NOT BOUND TO THE REF. The old rule was "message mentions
//      <ref>" AND "message contains a VERDICT line" — evaluated independently.
//      A verdict about one SHA in a message that mentions another would be
//      recorded against the wrong change. Now the ref must appear ON the verdict
//      line, which also stops a reviewer who QUOTES a request ("Verdict line, if
//      you approve: VERDICT: APPROVED <sha>") from accidentally casting one.
//
// THE RULE. A deploy proceeds only when every reviewer who has engaged with this
// ref has a current APPROVED, and at least one has. Anything else — a blocking
// verdict, a withdrawal, or engagement without a verdict — is an open review.
// An open review is not an approval, and neither is silence.
export const REVIEWERS = {
  '08257233e01c34c68a470a90ad90a3050c1e2a646456baf7debd894071f65043': 'Orion',
  '8004d3b3c70f7f0f9ea09b54e8cb06c7870f42978fb606e99d8b69328cdb2271': 'Aster',
};

// Anchored at line start, and the ref must be ON the verdict line. WITHDRAWN is
// new: a reviewer needs a way to retract that the gate can actually read.
const VERDICT_LINE =
  /^[\s>*_-]*VERDICT:\s*(APPROVED|CHANGES-REQUIRED|INSUFFICIENT-INFORMATION|WITHDRAWN)\s+(\S+)/gim;

const textOf = (body) =>
  typeof body === 'string' ? body
  : (body && typeof body.text === 'string' ? body.text : JSON.stringify(body ?? ''));

// One reviewer message → what it says about `ref`.
//   null                     — not from a reviewer, or not about this ref at all
//   {verdict: null}          — engaged, but stated no verdict (an open review)
//   {verdict: 'APPROVED'…}   — a verdict, bound to this ref by the same line
export function readMessage(env, ref, reviewers = REVIEWERS) {
  const signer = String(env?.signerPubkey ?? env?.signer ?? '').toLowerCase();
  const reviewer = reviewers[signer];
  if (!reviewer) return null;

  const text = textOf(env?.message);
  let verdict = null;
  VERDICT_LINE.lastIndex = 0;
  for (let m; (m = VERDICT_LINE.exec(text)); ) {
    // Refs may be abbreviated either way round (short sha vs full sha), so accept
    // a prefix match in either direction — but only on the verdict line itself.
    const stated = m[2].replace(/[`.,;:)\]]+$/, '');
    if (stated.startsWith(ref) || ref.startsWith(stated)) verdict = m[1].toUpperCase();
  }
  if (verdict) return { reviewer, signer, verdict, ts: env.ts ?? 0, msgId: env.msgId };

  // No verdict on this ref. Still engagement if the message is ABOUT it — that is
  // the case the old gate threw away, and it is the one that mattered.
  if (!text.includes(ref)) return null;
  return { reviewer, signer, verdict: null, ts: env.ts ?? 0, msgId: env.msgId };
}

// Records → decision. Pure; no clock, no network.
export function decide(records) {
  const byReviewer = new Map();
  for (const r of records) {
    if (!r) continue;
    const cur = byReviewer.get(r.reviewer) ?? { reviewer: r.reviewer, latestVerdict: null, engaged: false, superseded: 0 };
    cur.engaged = true;
    if (r.verdict) {
      if (cur.latestVerdict && r.ts >= cur.latestVerdict.ts) cur.superseded++;
      else if (cur.latestVerdict) { cur.superseded++; }
      // Latest wins. A reviewer's most recent verdict is their position; earlier
      // ones are history, not standing authority.
      if (!cur.latestVerdict || r.ts >= cur.latestVerdict.ts) cur.latestVerdict = r;
    }
    byReviewer.set(r.reviewer, cur);
  }

  const reviewers = [...byReviewer.values()].map((c) => ({
    reviewer: c.reviewer,
    state: !c.latestVerdict ? 'ENGAGED-NO-VERDICT'
         : c.latestVerdict.verdict === 'APPROVED' ? 'APPROVED'
         : c.latestVerdict.verdict,
    ts: c.latestVerdict?.ts ?? null,
    msgId: c.latestVerdict?.msgId ?? null,
    superseded: c.superseded,
  }));

  const approvals = reviewers.filter(r => r.state === 'APPROVED');
  const open      = reviewers.filter(r => r.state !== 'APPROVED');

  if (!reviewers.length) return { ok: false, reason: 'no-verdict', reviewers };
  if (open.length)       return { ok: false, reason: 'review-open', reviewers };
  if (!approvals.length) return { ok: false, reason: 'no-verdict', reviewers };
  return { ok: true, reason: 'approved', reviewers };
}

export default { readMessage, decide, REVIEWERS };
