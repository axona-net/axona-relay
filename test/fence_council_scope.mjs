// fence_council_scope.mjs — what the deploy hook treats as a deploy.
//
// This fence exists because the classifier CANNOT be tested from a shell: any
// command that exercises it contains the strings it matches, so the PreToolUse
// hook fires on the test itself. On 2026-08-01 that blocked two consecutive
// attempts — first a `git commit` whose MESSAGE quoted a push, then the command
// written to verify the fix for that. Untestable-in-place is exactly the
// condition that makes a rule drift, so the rule moved somewhere it can be
// driven directly.
//
// The strings below are DATA in a test file. They are never executed.
//
// Run: node test/fence_council_scope.mjs
import { deployReasons, stripHeredocs } from '../scripts/council-scope.mjs';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const PUSH_MAIN  = ['git', 'push', 'origin', 'testnet:' + 'ma' + 'in'].join(' ');
const PUSH_TNET  = 'git push origin testnet';

console.log('council scope — quoted text is data; only a real command is a command\n');

// ── 1. REAL DEPLOYS ARE GATED ──────────────────────────────────────────────
{
  ok('1a. a push to main is a deploy', deployReasons(PUSH_MAIN).length === 1, JSON.stringify(deployReasons(PUSH_MAIN)));
  ok('1b. ssh to a production bridge is a deploy',
    deployReasons('ssh axona-bridge "cd /opt/axona-bridge && git pull"').length === 1);
  ok('1c. bringing up the docker stack is a deploy',
    deployReasons('docker compose build && docker compose up -d').length === 1);
  ok('1d. restarting a live axona service is a deploy',
    deployReasons('systemctl restart axona-relay-1').length === 1);
}

// ── 2. THE FALSE POSITIVE THAT BLOCKED THE FIX ─────────────────────────────
// Verbatim shape of the commit that could not be made on 2026-08-01: a message
// documenting the hook's behaviour, quoting the command it denies.
{
  const c = [
    "git commit -F - <<'MSG'",
    'council gate: an open review is not an approval',
    '',
    'Verified live end-to-end: the PreToolUse hook denies ' + PUSH_MAIN + '.',
    'MSG',
  ].join('\n');
  ok('2a. a commit whose MESSAGE quotes a push to main is NOT a deploy ' +
     '(this exact command was blocked, twice)',
    deployReasons(c).length === 0, JSON.stringify(deployReasons(c)));
  ok('2b. …and the heredoc body really is what got stripped',
    !stripHeredocs(c).includes('testnet:' + 'ma' + 'in'), stripHeredocs(c));
}
{
  // The same trap on the other channel: posting a runbook to #council.
  const c = [
    'node scripts/mcp-post.mjs "council" "$(cat <<\'EOF\'',
    'To promote, run ' + PUSH_MAIN + ' then ssh axona-bridge and docker compose up -d.',
    'EOF',
    ')"',
  ].join('\n');
  ok('2c. quoting a full deploy runbook into a council post is not a deploy',
    deployReasons(c).length === 0, JSON.stringify(deployReasons(c)));
}

// ── 3. STRIPPING MUST NOT BECOME AN ESCAPE HATCH ───────────────────────────
// If a heredoc could hide a deploy, the gate would be trivially bypassable by
// anyone who noticed — including me, under deadline. It must only hide the BODY.
{
  const c = [
    "cat <<'NOTE' > /tmp/n.txt",
    'just a note',
    'NOTE',
    PUSH_MAIN,
  ].join('\n');
  ok('3a. a REAL deploy after a heredoc still gates — only the body is data',
    deployReasons(c).length === 1, JSON.stringify(deployReasons(c)));
}
{
  // An unterminated heredoc must fail CLOSED. Stripping to end-of-string would
  // let `cmd <<X` + anything become invisible.
  const c = "cat <<'X'\n" + PUSH_MAIN;
  ok('3b. an UNTERMINATED heredoc strips nothing and still gates (fails closed)',
    deployReasons(c).length === 1, JSON.stringify(deployReasons(c)));
}

// ── 4. NON-DEPLOYS STAY SILENT ─────────────────────────────────────────────
// The overwhelming majority. A gate that interrupts ordinary work gets removed.
{
  ok('4a. pushing a working branch is not a deploy', deployReasons(PUSH_TNET).length === 0);
  ok('4b. running the suite is not a deploy', deployReasons('npm test').length === 0);
  ok('4c. a local commit is not a deploy', deployReasons('git commit -m "wip"').length === 0);
  ok('4d. reading a deploy host over https is not a deploy ' +
     '(no ssh/scp/rsync verb)',
    deployReasons('curl -s https://bridge.axona.net/healthz').length === 0,
    JSON.stringify(deployReasons('curl -s https://bridge.axona.net/healthz')));
  ok('4e. an empty command is not a deploy', deployReasons('').length === 0);
  ok('4f. a missing command does not throw', deployReasons(undefined).length === 0);
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
