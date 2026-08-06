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

// ── 5. SCOPE IS THE PUSH'S OWN ARGUMENTS, NOT THE WHOLE COMMAND ────────────
//
// The first cut ANDed two independent tests across the entire command string:
// "is there a git push anywhere" and "is there the word main anywhere". Any
// compound command satisfying both halves separately was blocked. On 2026-08-05
// it refused two testnet pushes because the command also carried an echo label
// reading `push testnet (NOT main)` — written to say it was NOT a main push.
//
// Same defect class as section 2: text that CONTAINS a deploy string read as if
// it performed one. These cases pin the fix in both directions — the loosening
// must not let a real deploy through.
{
  const NOT_MAIN_LABEL = `${PUSH_TNET} && echo "push testnet (NOT ` + 'ma' + 'in' + ')"';
  ok('5a. a testnet push whose LABEL names the branch is not a deploy',
    deployReasons(NOT_MAIN_LABEL).length === 0, JSON.stringify(deployReasons(NOT_MAIN_LABEL)));

  const MSG_NAMES_IT = `git commit -m "live sites build from ` + 'ma' + 'in' + `" && ${PUSH_TNET}`;
  ok('5b. a commit MESSAGE naming the branch is not a deploy',
    deployReasons(MSG_NAMES_IT).length === 0, JSON.stringify(deployReasons(MSG_NAMES_IT)));

  ok('5c. two testnet pushes in one command are not a deploy',
    deployReasons(`cd /a && ${PUSH_TNET}; cd /b && ${PUSH_TNET}`).length === 0);

  const FILE_NAMED = `git add src/` + 'ma' + 'in' + `.js && ${PUSH_TNET}`;
  ok('5d. a FILE named for the branch is not a deploy',
    deployReasons(FILE_NAMED).length === 0, JSON.stringify(deployReasons(FILE_NAMED)));

  // …and the gate must still bite.
  ok('5e. a real deploy buried after a testnet push STILL gates',
    deployReasons(`npm test && ${PUSH_TNET} && ${PUSH_MAIN}`).length === 1);

  const PLAIN = ['git', 'push', 'origin', 'ma' + 'in'].join(' ');
  ok('5f. a plain push to the live branch gates', deployReasons(PLAIN).length === 1);

  const FORCED = ['git', 'push', '--force', 'origin', 'ma' + 'in'].join(' ');
  ok('5g. a FORCED push to the live branch gates', deployReasons(FORCED).length === 1);

  const CONTINUED = ['git', 'push', 'origin', '\\\n  testnet:' + 'ma' + 'in'].join(' ');
  ok('5h. a line-continued refspec gates (continuations folded first)',
    deployReasons(CONTINUED).length === 1, JSON.stringify(deployReasons(CONTINUED)));
}

// ── 6. REFSPEC PARSING — the bypass Aster and Orion both found ─────────────
//
// Section 5 matched the branch word by its delimiter, which misses it behind a
// slash or a force prefix. All three of these published while the classifier
// reported no deploy reason. Reviewers caught it; no test did.
{
  const P = 'git' + ' push';
  const B = 'ma' + 'in';
  const bypasses = [
    [`${P} origin refs/heads/${B}`,            'fully-qualified ref'],
    [`${P} origin HEAD:refs/heads/${B}`,       'HEAD to a qualified ref'],
    [`${P} origin +${B}:refs/heads/${B}`,      'force-prefixed to a qualified ref'],
    [`${P} origin +${B}`,                      'force-prefixed bare'],
    [`${P} --force origin HEAD:refs/heads/${B}`, 'flag + qualified ref'],
    [`${P} origin testnet:refs/heads/${B}`,    'working branch to a qualified ref'],
  ];
  for (const [cmd, why] of bypasses) {
    ok(`6. ${why} gates`, deployReasons(cmd).length === 1, JSON.stringify(deployReasons(cmd)));
  }

  // A refspec-less push publishes the CURRENT branch via push.default, which the
  // command text cannot reveal. Ambiguous, therefore gated.
  ok('6g. a push with no refspec gates (branch unknowable from the text)',
    deployReasons(`${P}`).length === 1);
  ok('6h. …and a remote with no refspec likewise',
    deployReasons(`${P} origin`).length === 1);

  // The working branch must still pass in every spelling.
  ok('6i. a fully-qualified WORKING branch is not a deploy',
    deployReasons(`${P} origin refs/heads/testnet`).length === 0,
    JSON.stringify(deployReasons(`${P} origin refs/heads/testnet`)));
  ok('6j. an explicit working-branch refspec is not a deploy',
    deployReasons(`${P} origin testnet:refs/heads/testnet`).length === 0);
}

// ── 7. THE QUOTING BYPASS (Aster, 67f661d) ─────────────────────────────────
// The gate read the refspec as raw text, so a quoted one did not match — but the
// SHELL removes the quotes before git ever sees them. Every form below is a real
// publish. This is why quoted arguments cannot be treated as data the way a
// heredoc body is: a heredoc feeds a program that consumes it as text, while a
// quoted argument is handed to the program as a live value.
{
  const P = 'git' + ' push';
  const B = 'ma' + 'in';
  const quoted = [
    [`${P} origin 'HEAD:refs/heads/${B}'`,   'single-quoted qualified ref'],
    [`${P} origin "HEAD:refs/heads/${B}"`,   'double-quoted qualified ref'],
    [`${P} origin '${B}'`,                   'single-quoted bare branch'],
    [`${P} origin "testnet:${B}"`,           'double-quoted explicit refspec'],
    [`${P} 'origin' "+${B}"`,                'quoted remote and force-prefixed ref'],
  ];
  for (const [cmd, why] of quoted) {
    ok(`7. ${why} gates`, deployReasons(cmd).length === 1, JSON.stringify(deployReasons(cmd)));
  }
  // …and quoting must not newly gate the working branch.
  ok("7f. a quoted WORKING branch is still not a deploy",
    deployReasons(`${P} origin 'testnet'`).length === 0,
    JSON.stringify(deployReasons(`${P} origin 'testnet'`)));
  ok('7g. a quoted working-branch refspec is still not a deploy',
    deployReasons(`${P} origin "testnet:refs/heads/testnet"`).length === 0);
}

// ── 8. UNRESOLVABLE ARGUMENTS FAIL CLOSED ──────────────────────────────────
// A gate that cannot resolve an argument must not clear it. None of these can be
// evaluated from the command text; all of them can land on a live branch.
{
  const P = 'git' + ' push';
  const dynamic = [
    `${P} origin "$REFSPEC"`,
    `${P} origin \${REFSPEC}`,
    `${P} origin $(printf %s ma"in")`,
    `${P} origin "$(cat .git/HEAD)"`,
    `${P} "$REMOTE" "$BRANCH"`,
  ];
  for (const cmd of dynamic) {
    ok(`8. an unresolvable refspec gates — ${cmd.slice(0, 34)}…`,
      deployReasons(cmd).length === 1, JSON.stringify(deployReasons(cmd)));
  }
  // Ambiguous static forms resolve at push time, not here.
  ok('8f. a bare HEAD gates (resolves to the current branch at push time)',
    deployReasons(`${P} origin HEAD`).length === 1);
  ok('8g. an empty destination gates',
    deployReasons(`${P} origin ma` + `in:`).length === 1);
}

// ── 9. THE DESTINATION IS THE GATED THING, NOT A MENTIONED HOSTNAME ─────────
// A relay told to speak to a bridge URL is a client, not a deployment. The old
// rule ANDed "a remote-shell verb anywhere" with "a deploy host anywhere", so a
// run against a laptop on the LAN was called a deploy because the hostname
// appeared inside an environment variable.
{
  const H = 'testnet.axona' + '.net';
  ok('9a. a remote shell to a LAN box carrying a bridge URL is NOT a deploy',
    deployReasons(`ssh david@192.168.68.50 'BRIDGE=wss://${H} bash start-fleet.sh'`).length === 0,
    JSON.stringify(deployReasons(`ssh david@192.168.68.50 'BRIDGE=wss://${H} bash start-fleet.sh'`)));
  ok('9b. reading a health endpoint is not a deploy',
    deployReasons(`curl -s https://${H}/healthz`).length === 0);

  // Aster: a static quoted literal is still a real destination.
  // NOTE: the remote payload here must be MUTATING. Since the read-only
  // exemption (isReadOnlyRemote, see council_readonly_fence.mjs) landed, a
  // read-only payload like `uptime` on a deploy host no longer gates — that
  // is the intended new policy (David: allow read-only bridge inspection).
  // These cases exist to verify DESTINATION PARSING, so they use a mutating
  // payload; the read-only-vs-mutating distinction is fenced separately.
  ok('9c. a quoted deploy host IS a deploy',
    deployReasons(`ssh '${H}' 'systemctl restart coturn'`).length === 1,
    JSON.stringify(deployReasons(`ssh '${H}' 'systemctl restart coturn'`)));
  ok('9d. …with a user prefix likewise',
    deployReasons(`ssh -i ~/.ssh/id_ed25519_axona "root@${H}" 'systemctl restart coturn'`).length === 1);
  ok('9e. an option VALUE is not mistaken for the destination',
    deployReasons(`ssh -o ProxyCommand=none -p 22 -i /k/id root@${H} 'systemctl restart coturn'`).length === 1);
  ok('9f. an unresolvable destination fails closed',
    deployReasons('ssh "$HOST" uptime').length === 1);
  ok('9g. a copy to a deploy host is a deploy',
    deployReasons(`scp build.tgz root@${H}:/opt/axona-bridge/`).length === 1);
  ok('9h. a copy between local paths is not',
    deployReasons('scp ./a.txt ./b.txt').length === 0);

  // The payload of a remote shell runs AS A COMMAND on that host. A bastion hop
  // hides a deploy behind an innocent destination.
  ok('9i. a deploy nested inside a remote shell gates',
    deployReasons(`ssh bastion.example "ssh ${H} systemctl restart coturn"`).length === 1,
    JSON.stringify(deployReasons(`ssh bastion.example "ssh ${H} systemctl restart coturn"`)));
}

console.log(`\n${fail ? `✗ ${fail} of ${n} failed` : `✓ all ${n} checks passed`}`);
process.exit(fail ? 1 : 0);
