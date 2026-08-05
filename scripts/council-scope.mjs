// council-scope.mjs — does this shell command put code in front of users?
//
// Extracted from council-hook.mjs for the same reason council-verdicts.mjs was:
// the JUDGEMENT should be testable without side effects. This one especially,
// because it cannot be exercised from a shell at all — any command that tests it
// contains the very strings it matches, so the hook fires on the test. On
// 2026-08-01 that cost two blocked attempts before I stopped trying.
//
// SCOPE, NOT VERDICT. This file decides what is gated; council-verdicts.mjs
// decides whether the gate opens. Keeping them apart matters: the rules here are
// what a reasonable person tunes as infrastructure changes; the signature check
// is what nobody should be able to tune.

// Hosts that serve users. Deploying to any of them is the gated act.
export const DEPLOY_HOSTS = [
  'axona-bridge', '64.227.2.28', '24.199.98.119', '161.35.234.165',
  'bridge.axona.net', 'bridge-west.axona.net', 'testnet.axona.net',
];

// HEREDOC BODIES ARE DATA, NOT COMMANDS.
//
// A commit message, a council post, or a runbook that QUOTES a deploy command is
// not a deploy. The hook sees one long string, so on 2026-08-01 it blocked the
// very commit that was fixing the review gate — the message contained the words
// `git push origin testnet:main` inside a heredoc, and that was enough.
//
// This is defect 3 of council-verdicts.mjs in different clothes: quoted text
// treated as a live instruction. Fixing it there and not here would have been a
// fix for the example rather than for the class.
//
// Deliberately conservative — it strips only a well-formed `<<MARKER … MARKER`
// block. An unterminated heredoc strips nothing and the command is scanned
// whole, which fails CLOSED (gated) rather than open.
export const stripHeredocs = (s) =>
  String(s).replace(/<<-?\s*(['"]?)([A-Za-z_][A-Za-z0-9_]*)\1[\s\S]*?^\t*\2$/gm, '<<HEREDOC');

// → array of human-readable reasons this command is a deploy. Empty = not one.
export function deployReasons(rawCommand) {
  const cmd = stripHeredocs(rawCommand || '');
  const reasons = [];
  if (!cmd) return reasons;

  // A push to main. `git push origin testnet:main`, `git push origin main`,
  // `git push origin HEAD:main` — all publish. A push to testnet does not.
  //
  // SCAN EACH `git push` INVOCATION'S OWN ARGUMENTS, NOT THE WHOLE COMMAND.
  // The first cut ANDed two independent tests across the entire string: "is there
  // a git push anywhere" and "is there the word main anywhere". Any compound
  // command satisfying both halves separately was blocked. On 2026-08-05 it
  // refused a pair of testnet pushes because the command also contained
  //     echo "=== push testnet (NOT main) ==="
  // — a label written specifically to say it was NOT a main push. A commit
  // message, an echo, or a filename mentioning the word was enough.
  //
  // This is the same defect as the heredoc one directly below, one layer in:
  // text that merely CONTAINS a deploy string read as if it performed one.
  //
  // Still fails CLOSED on ambiguity. Line continuations are folded first, so a
  // refspec wrapped onto the next line is still seen; an invocation is scanned up
  // to the next shell separator, so anything inside the push's own arguments
  // counts against it.
  const folded = cmd.replace(/\\\r?\n/g, ' ');
  for (const m of folded.matchAll(/\bgit\s+push\b([^\n;&|]*)/g)) {
    if (/(^|[\s:])main\b/.test(m[1])) {
      reasons.push('pushes to main (live sites build from main)');
      break;
    }
  }
  const host = DEPLOY_HOSTS.find(h => cmd.includes(h));
  if (host && /\bssh\b|\bscp\b|\brsync\b/.test(cmd)) {
    reasons.push(`reaches a deploy host (${host})`);
  }
  if (/docker(-|\s+)compose\b[\s\S]*\bup\b/.test(cmd)) reasons.push('brings up the docker stack');
  if (/systemctl\s+(restart|start)\s+\S*axona/.test(cmd)) reasons.push('restarts a live axona service');
  return reasons;
}

export default { deployReasons, stripHeredocs, DEPLOY_HOSTS };
