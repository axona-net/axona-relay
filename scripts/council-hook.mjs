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
import { readFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

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
