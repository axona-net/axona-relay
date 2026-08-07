#!/usr/bin/env node
// scripts/council-hook.mjs — PreToolUse dispatcher for the council review gate.
//
// This file answers ONE question: does this tool call put code in front of users?
// The verdict itself is council-gate.mjs's job. Keeping them apart matters: the
// scope rules below are what a reasonable person tunes; the signature check is
// what nobody should be able to tune.
//
// ─── WHY THIS GATES DEPLOY AND NOT PUSH (the correction, 2026-08-01) ─────────
//
// My first design gated `git push` on a reviewer verdict naming that SHA. Aster
// broke it by accident: reviewing a locally-committed patch, he wrote "I have not
// independently verified the SHA." He couldn't. It wasn't pushed. There was no
// object to fetch.
//
// A SHA ONLY BECOMES REVIEWABLE BY BEING PUSHED. Gating push gates the very act
// that makes review possible — a lock whose key is on the inside. It is the same
// error as the first-edit gate both reviewers rejected on latency grounds, just
// one step later, and I did not see it until a reviewer said he could not check
// my work.
//
// So: PUSH FREELY. Pushing to a working branch is how you hand reviewers something
// they can verify against objects instead of against my summary. What gets gated
// is the step that puts code in front of users:
//
//   · pushing to main            — live sites build from main
//   · ssh to a deploy host       — bridges, relays, droplets
//   · docker compose up          — prod bridge stack
//   · systemctl restart axona-*  — live services
//
// Pushing to `testnet` or any other working branch is NOT a deploy and passes
// untouched. Local commits, tests, builds and installs pass untouched.
import { deployReasons } from './council-scope.mjs';
import { readFileSync, existsSync, mkdirSync, writeFileSync, appendFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

// ─── GATE REMOVED (David, 2026-08-06) ────────────────────────────────────────
// David dissolved the automated deploy gate: "It is far too strict on obviously
// appropriate responses." The new process is human-judged — Orion and Aster
// review the DESIGN, David reads their verdicts and decides. David is the gate.
// This hook is unregistered from settings.json; this early exit is belt-and-
// suspenders so a lingering registration anywhere can never block a tool call.
// council-gate.mjs / council-verdicts.mjs remain as ADVISORY read tools for
// summarising a review thread — they no longer enforce anything.
process.exit(0);
/* eslint-disable no-unreachable */

const HERE = dirname(fileURLToPath(import.meta.url));
const GATE = join(HERE, 'council-gate.mjs');
// The cache exists so a multi-command deploy does not pay a 25s #council read
// per command. It must NOT outlive a review reversal: on 2026-08-01 Orion
// superseded his own sign-off 308 seconds after casting it. A 12h cache would
// have kept deploying on a withdrawn approval for the rest of the day, which is
// the gate's own defect one layer up. 15 minutes covers a deploy session and
// nothing more.
const APPROVAL_TTL_MS = 15 * 60 * 1000;

// Which commands count as a deploy now lives in council-scope.mjs, where
// fence_council_scope.mjs can drive the rules directly. It had to move: the
// classifier cannot be exercised from a shell, because any command that tests it
// contains the strings it matches and so trips this very hook.
const allow = () => process.exit(0);                       // silent pass — the common path
const deny = (reason) => {
  console.log(JSON.stringify({
    hookSpecificOutput: {
      hookEventName: 'PreToolUse',
      permissionDecision: 'deny',
      permissionDecisionReason: reason,
    },
  }));
  process.exit(0);
};

let payload = {};
try { payload = JSON.parse(readFileSync(0, 'utf8') || '{}'); } catch { allow(); }

// ─── Is this a deploy? ──────────────────────────────────────────────────────
// Heredoc bodies are stripped before matching — a commit message or council post
// that QUOTES a deploy command is documentation, not a deploy. Without that, this
// hook blocked the very commit that was fixing the review gate, twice, because
// the message quoted the command it denies.
const reasons = deployReasons(payload?.tool_input?.command);
if (!reasons.length) allow();                              // not a deploy — the overwhelming majority

// ─── What SHA is being deployed? ────────────────────────────────────────────
const git = (args, cwd) => {
  try {
    return execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch { return null; }
};
const cwd = process.cwd();
const root = git(['rev-parse', '--show-toplevel'], cwd);
let ref = root ? git(['rev-parse', '--short', 'HEAD'], root) : null;

// A deploy driven from outside the repo being deployed (common: running an ssh
// deploy from the relay checkout) must SAY what it is shipping rather than have
// the hook guess from an unrelated HEAD.
const refFile = root ? join(root, '.council', 'deploy-ref') : null;
if (refFile && existsSync(refFile)) {
  const explicit = readFileSync(refFile, 'utf8').trim();
  if (explicit) ref = explicit;
}

// ─── Override: RECORDED, NEVER HONORED ──────────────────────────────────────
// The first cut of this (2026-08-03) treated any COUNCIL_OVERRIDE="…" string in
// the command as an allow. Aster's review killed it, correctly, and the reason
// is worth keeping in front of whoever reads this next:
//
//   THE STRING IS WRITTEN BY THE SAME PARTY THE GATE EXISTS TO CATCH. I type
//   the command. I type the override. "David told me to" typed by me is not
//   evidence that David told me to — it is my own assertion, checked by nobody,
//   evaluated BEFORE any sha or verdict is looked at. An escape hatch whose key
//   is held by the person being gated is not an escape hatch, it is an absence
//   of a gate wearing a log file as a disguise. Logging it afterwards records
//   what happened; it cannot retroactively make it authorization.
//
// It also failed open when the audit write failed, contradicting the "loud,
// logged" invariant it claimed, and it stored raw command text — which put
// production host addresses into a public repository the moment that log was
// tracked.
//
// So the override is now RECORDED AND DENIED. A human who wants to bypass this
// gate must act as a human: disable the hook in settings, or approve the sha on
// #council. Both leave a trace that is not self-issued.
//
// If we ever want automation back, the design has to bind the grant to evidence
// David actually produces — a short-lived grant signed by his Axona identity,
// naming the exact ref and scope, verified here against his authorized key,
// expiring on its own. That is a real protocol, not a string comparison, and it
// is not built.
const ovm = String(payload?.tool_input?.command || '')
  .match(/COUNCIL_OVERRIDE=(?:"([^"]+)"|'([^']+)'|(\S+))/);
if (ovm) {
  const why = (ovm[1] || ovm[2] || ovm[3] || '').slice(0, 200);
  // Attempt-record only. Never the command text: this file is tracked, and raw
  // commands carry host addresses, key paths and anything else on the line.
  let logged = false;
  try {
    const logDir = join(root || HERE, '.council');
    mkdirSync(logDir, { recursive: true });
    appendFileSync(join(logDir, 'overrides.log'), JSON.stringify({
      at: new Date().toISOString(), event: 'override-attempt-denied',
      ref: ref || 'unresolved', reason: why, reasons,
    }) + '\n');
    logged = true;
  } catch { /* fall through — the deny below does not depend on the log */ }
  deny(
    `COUNCIL GATE — OVERRIDE NOT HONORED (recorded${logged ? '' : ', LOG WRITE FAILED'}).\n\n` +
    `COUNCIL_OVERRIDE is a string you wrote, in a command you wrote. It is not\n` +
    `evidence of an order from David, so this gate does not accept it.\n\n` +
    `To proceed, one of:\n` +
    `  · get "VERDICT: APPROVED ${ref}" on #council from Orion (08257233…) or Aster (8004d3b3…);\n` +
    `  · ask David to disable this hook in settings for the duration — a human act, by the human whose authority is being claimed.\n\n` +
    `Attempted reason: "${why}"`);
}

if (!ref) {
  deny(
    `COUNCIL GATE — this is a deploy (${reasons.join('; ')}) but no SHA could be resolved.\n` +
    `Write the SHA being deployed into .council/deploy-ref, or run from the repo being deployed.`);
}

// ─── Cached approval ────────────────────────────────────────────────────────
const cacheFile = root ? join(root, '.council', `approved-${ref}.json`) : null;
if (cacheFile && existsSync(cacheFile)) {
  try {
    const j = JSON.parse(readFileSync(cacheFile, 'utf8'));
    if ((Date.now() - new Date(j.at).getTime()) < APPROVAL_TTL_MS) allow();
  } catch { /* a corrupt cache is not an approval */ }
}

let r;
try {
  const out = execFileSync('node', [GATE, `--ref=${ref}`, '--json'],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 90_000 });
  r = JSON.parse(out.trim().split('\n').pop());
} catch (e) {
  const s = String(e?.stdout || '').trim().split('\n').pop();
  try { r = JSON.parse(s); } catch { r = { ok: false, reason: 'gate-error', error: String(e?.message || e) }; }
}

if (r.ok) {
  try {
    if (cacheFile) {
      mkdirSync(dirname(cacheFile), { recursive: true });
      writeFileSync(cacheFile, JSON.stringify({ ref, at: new Date().toISOString(), ...r }, null, 2));
    }
  } catch { /* cache failure must not block an approved deploy */ }
  allow();
}

deny(
  `COUNCIL GATE — DEPLOY BLOCKED. ${ref} has no reviewer approval (${r.reason}).\n\n` +
  `This command ${reasons.join(' and ')}.\n\n` +
  (r.verdicts?.length
    ? r.verdicts.map(v => `  ${v.reviewer}: ${v.verdict}`).join('\n') + '\n\n'
    : '') +
  `PUSH is not gated — push the branch so reviewers can fetch and verify ${ref},\n` +
  `then ask #council for:\n` +
  `  VERDICT: APPROVED ${ref}\n` +
  `from Orion (08257233…) or Aster (8004d3b3…).\n\n` +
  `A review REQUEST is not a review. This exists because on 2026-07-31 I built on\n` +
  `an unreviewed change and it came back wrong.\n\n` +
  `Override (loud, logged, MUST be reported to David):\n` +
  `  COUNCIL_OVERRIDE="reason" — then retry.`);
