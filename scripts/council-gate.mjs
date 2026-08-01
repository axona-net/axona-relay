#!/usr/bin/env node
// scripts/council-gate.mjs — the review gate. Exits 0 only when a REVIEWER has
// posted a verdict on #council for the thing being gated.
//
// WHY THIS EXISTS. On 2026-07-31 I posted a review request for Q1 and then
// immediately started Q2 without waiting for an answer. Aster's review came back
// "Q1 came back wrong" while I was already building on top of it. I had treated
// HAVING ASKED as equivalent to HAVING BEEN REVIEWED. David's objection was not
// that I made a mistake — it was that a rule I can quietly skip is not a process.
//
// So the check is EVIDENCE, not assertion. It looks for a message on #council
// signed by Orion or Aster. I cannot forge another agent's Ed25519 signature, so
// I cannot satisfy this gate by claiming a review happened — which is precisely
// the property that a memory note, a checklist, or my own good intentions do not
// have. It is the same principle as the Q1/Q2 kernel work: a request is not a
// response, and "I believe it went out" is not delivery.
//
// A GATE THAT ANY REPLY SATISFIES IS WORSE THAN NO GATE, because it manufactures
// a record of scrutiny that did not happen — the confident-false-negative class
// applied to process. So a bare acknowledgement does not pass. The reviewer must
// state a verdict:
//
//     VERDICT: APPROVED <ref>
//     VERDICT: CHANGES-REQUIRED <ref>
//     VERDICT: INSUFFICIENT-INFORMATION <ref>
//
// where <ref> is the git sha (plan gate: the word PLAN plus a slug). Only
// APPROVED opens the gate; the other two are real answers and are reported as
// such rather than being retried into submission.
//
// Usage:
//   node scripts/council-gate.mjs --ref=<sha|slug> [--window=25] [--json]
//
// Exit 0 = approved. Exit 1 = blocked (with the reason on stderr). Exit 2 = usage.
//
// OVERRIDE is deliberate, loud, and audited — never silent. A gate with no escape
// hatch teaches its subject to route around it; an escape hatch that must be
// declared does not:
//   COUNCIL_OVERRIDE="reason" node scripts/council-gate.mjs --ref=...
// Every override is appended to ~/.axona/council-overrides.log and must be
// reported to David in the same turn it is used.
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { readMessage, decide, REVIEWERS } from './council-verdicts.mjs';
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

// Reviewers, by SIGNER (imported from council-verdicts.mjs). Handles are display
// text and can be set by anyone; the signer is the identity. Matching on handle
// would make the gate spoofable by the one party it is meant to constrain.
const SELF = '83866c66598304ed57767cf66b42b7a33b1884a47d8124317d3ad557995bb8df';

const flags = Object.fromEntries(process.argv.slice(2)
  .filter(a => a.startsWith('--'))
  .map(a => { const [k, ...v] = a.slice(2).split('='); return [k, v.join('=') || true]; }));

const ref = String(flags.ref || '').trim();
if (!ref) {
  console.error('usage: node scripts/council-gate.mjs --ref=<sha|slug> [--window=25] [--json]');
  process.exit(2);
}
const windowSec = Number(flags.window || 25);
const asJson = !!flags.json;

const say = (o) => { if (asJson) console.log(JSON.stringify(o)); return o; };

// ── OVERRIDE ────────────────────────────────────────────────────────────────
const override = process.env.COUNCIL_OVERRIDE;
if (override && String(override).trim()) {
  const line = JSON.stringify({
    ts: new Date().toISOString(), ref, reason: String(override).trim(), cwd: process.cwd(),
  });
  try {
    const p = join(homedir(), '.axona', 'council-overrides.log');
    mkdirSync(dirname(p), { recursive: true });
    appendFileSync(p, line + '\n');
  } catch { /* logging must never be the reason a deploy dies */ }
  console.error('');
  console.error('  ╔════════════════════════════════════════════════════════════════╗');
  console.error('  ║  COUNCIL GATE OVERRIDDEN — NOT REVIEWED                        ║');
  console.error('  ╚════════════════════════════════════════════════════════════════╝');
  console.error(`  ref:    ${ref}`);
  console.error(`  reason: ${String(override).trim()}`);
  console.error('  Logged to ~/.axona/council-overrides.log.');
  console.error('  REPORT THIS TO DAVID IN THIS TURN. An unreported override is a lie.');
  console.error('');
  say({ ok: true, overridden: true, ref, reason: String(override).trim() });
  process.exit(0);
}

// ── READ #council ───────────────────────────────────────────────────────────
// The judgement lives in council-verdicts.mjs so it can be fenced without a
// network (test/fence_council_gate.mjs, built from the transcript that beat the
// previous version of this file on 2026-08-01). This function does I/O only.
let peer, close;
const found = [];
try {
  const s = await connectPeer({ region: 'eagle' });
  peer = s.peer; close = s.close ?? s.stop ?? null;
  await peer.sub({ region: s.regionName ?? 'eagle', name: 'council' }, (env) => {
    try {
      if (String(env.signerPubkey ?? env.signer ?? '').toLowerCase() === SELF) return;  // belt and braces
      const rec = readMessage(env, ref, REVIEWERS);
      if (rec) found.push(rec);
    } catch { /* a malformed message is not a verdict */ }
  }, { since: 'all' });

  await new Promise(r => setTimeout(r, windowSec * 1000));
} catch (e) {
  // A read failure is NOT an approval and NOT a rejection — it is an unknown, and
  // the whole point of this week's work is to stop collapsing those. Block, and
  // say which kind of nothing happened.
  console.error(`  council-gate: could not read #council — ${String(e?.message || e)}`);
  console.error('  This is an UNKNOWN, not a verdict. Blocked. Retry, or override and report.');
  say({ ok: false, reason: 'read-failed', error: String(e?.message || e), ref });
  try { if (close) await close(); } catch {}
  try { cleanupWebRTC?.(); } catch {}
  process.exit(1);
}
try { if (close) await close(); } catch {}
try { cleanupWebRTC?.(); } catch {}
// ── VERDICT ─────────────────────────────────────────────────────────────────
const d = decide(found);
const line = (r) => `    ${r.reviewer.padEnd(6)} ${r.state}` +
  (r.superseded ? `  (superseding ${r.superseded} earlier)` : '') +
  (r.msgId ? `  ${r.msgId.slice(0, 12)}` : '  — engaged on this ref, no verdict');

if (!d.ok) {
  console.error(`  council-gate: BLOCKED on ${ref} — ${d.reason}`);
  for (const r of d.reviewers) console.error(line(r));
  if (d.reason === 'review-open') {
    console.error('  A reviewer who has ENGAGED with this ref and has not APPROVED it is an');
    console.error('  OPEN REVIEW. Silence from someone who is looking is not consent, and a');
    console.error('  superseded approval is not a standing one.');
    console.error(`  Resolve it, then ask for:  VERDICT: APPROVED ${ref}`);
  } else {
    console.error('  A review REQUEST is not a review. Ask on #council and wait for:');
    console.error(`    VERDICT: APPROVED ${ref}`);
    console.error('  from Orion (08257233…) or Aster (8004d3b3…).');
  }
  say({ ok: false, reason: d.reason, ref, reviewers: d.reviewers });
  process.exit(1);
}

console.error(`  council-gate: APPROVED ${ref}`);
for (const r of d.reviewers) console.error(line(r));
say({ ok: true, ref, reviewers: d.reviewers });
process.exit(0);
