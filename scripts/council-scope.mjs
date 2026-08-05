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
//
// THE RULE THIS FILE KEEPS RELEARNING: read the token, not the string.
//
// Four bypasses have been found here, and all four are one mistake. A condition
// was evaluated against the whole command text instead of against the argument
// that actually carries the meaning.
//
//   1. `git push` anywhere AND the word main anywhere  → an echo saying "NOT
//      main" blocked a testnet push.
//   2. the branch word matched by delimiter            → `refs/heads/main` and
//      `+main:…` published without a reason. (Aster, Orion, 107f6f3)
//   3. the refspec read as raw text                    → `'HEAD:refs/heads/main'`
//      published, because the SHELL removes the quotes before git sees them.
//      (Aster, 67f661d)
//   4. a deploy host matched anywhere in the string    → a run whose remote-shell
//      destination was a laptop on the LAN was called a deploy, because the
//      hostname appeared inside a relay's bridge URL.
//
// So this file now tokenizes. It is not a shell and does not try to be one; it
// recognizes word boundaries, the three quoting forms, and the operators that end
// a command — enough to know WHICH WORD is the refspec and WHICH WORD is the
// remote-shell destination. Anything it cannot resolve to a literal (a variable,
// a command substitution, an unterminated quote) is treated as unresolvable and
// GATES. Failing closed on ambiguity is the whole design: a gate that guesses is
// a gate that can be argued with.

// Hosts that serve users. Deploying to any of them is the gated act.
export const DEPLOY_HOSTS = [
  'axona-bridge', '64.227.2.28', '24.199.98.119', '161.35.234.165',
  'bridge.axona.net', 'bridge-west.axona.net', 'testnet.axona.net',
];

// Branches whose contents reach users. A push landing on one of these is the
// gated act, however the refspec spells it.
export const LIVE_BRANCHES = new Set(['main', 'master']);

// HEREDOC BODIES ARE DATA, NOT COMMANDS.
//
// A commit message, a council post, or a runbook that QUOTES a deploy command is
// not a deploy. The hook sees one long string, so on 2026-08-01 it blocked the
// very commit that was fixing the review gate — the message contained the words
// `git push origin testnet:main` inside a heredoc, and that was enough.
//
// Deliberately conservative — it strips only a well-formed `<<MARKER … MARKER`
// block. An unterminated heredoc strips nothing and the command is scanned
// whole, which fails CLOSED (gated) rather than open.
//
// NOTE the asymmetry with quoted arguments, which are NOT stripped. Aster is
// right that they cannot be: the shell hands a quoted argument to the program as
// a live value, so `ssh 'testnet.axona.net'` is a real deployment and
// `git push origin 'HEAD:refs/heads/main'` is a real publish. A heredoc feeding
// git-commit is consumed as text by a program that does not act on it. Data is
// data because of where it GOES, not because of how it is quoted.
export const stripHeredocs = (s) =>
  String(s).replace(/<<-?\s*(['"]?)([A-Za-z_][A-Za-z0-9_]*)\1[\s\S]*?^\t*\2$/gm, '<<HEREDOC');

/**
 * Split a command into shell words and separators.
 *
 * Each word is `{ value, dynamic, sep:false }` where `value` is the literal text
 * with quotes removed and `dynamic` marks a word whose final value the shell —
 * not this parser — decides. Separators are `{ value, sep:true }`.
 *
 * `dynamic` is the fail-closed signal. `"$REF"`, `${REF}`, `$(printf main)` and
 * an unterminated quote all set it, because none of them can be resolved from
 * the command text, and a gate that cannot resolve an argument must not clear it.
 */
export function tokenize(src) {
  const out = [];
  let cur = null;
  const start = () => (cur ??= { value: '', dynamic: false, sep: false });
  const flush = () => { if (cur) { out.push(cur); cur = null; } };
  let i = 0;

  while (i < src.length) {
    const c = src[i];

    if (c === '\\' && src[i + 1]) { start().value += src[i + 1]; i += 2; continue; }

    if (c === "'") {                       // single quotes: no expansion inside
      const end = src.indexOf("'", i + 1);
      if (end < 0) { start().dynamic = true; break; }   // unterminated → unresolvable
      start().value += src.slice(i + 1, end);
      i = end + 1;
      continue;
    }

    if (c === '"') {                       // double quotes: expansion possible
      let j = i + 1, body = '', dyn = false, closed = false;
      while (j < src.length) {
        if (src[j] === '\\' && src[j + 1]) { body += src[j + 1]; j += 2; continue; }
        if (src[j] === '"') { closed = true; j++; break; }
        if (src[j] === '$' || src[j] === '`') dyn = true;
        body += src[j]; j++;
      }
      const w = start();
      w.value += body;
      if (dyn || !closed) w.dynamic = true;
      i = j;
      continue;
    }

    if (c === '$' || c === '`') { const w = start(); w.dynamic = true; w.value += c; i++; continue; }

    if (c === ' ' || c === '\t') { flush(); i++; continue; }

    const two = src.slice(i, i + 2);
    if (two === '&&' || two === '||') { flush(); out.push({ value: two, sep: true }); i += 2; continue; }
    if (c === ';' || c === '|' || c === '&' || c === '\n' || c === '(' || c === ')') {
      flush(); out.push({ value: c, sep: true }); i++; continue;
    }

    start().value += c;
    i++;
  }
  flush();
  return out;
}

/** Group tokens into individual command invocations, split on separators. */
const invocations = (toks) => {
  const groups = [[]];
  for (const t of toks) {
    if (t.sep) groups.push([]);
    else groups[groups.length - 1].push(t);
  }
  return groups.filter(g => g.length);
};

/** Drop leading `NAME=value` env assignments; returns the words from the verb on. */
const fromVerb = (words) => {
  let k = 0;
  while (k < words.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[k].value)) k++;
  return words.slice(k);
};

// `git push` options that consume the FOLLOWING word. Anything with `=` carries
// its value in the same token and needs no entry here.
const GIT_PUSH_OPT_VALUE = new Set(['-o', '--push-option', '--repo', '--exec', '--receive-pack']);

// Remote-shell options that consume the following word. Over-listing is safe —
// skipping a word that was not an option value only means the destination is
// found one word later, and a missing destination gates.
const SSH_OPT_VALUE = new Set([
  '-b', '-c', '-D', '-E', '-e', '-F', '-I', '-i', '-J', '-L', '-l', '-m',
  '-O', '-o', '-p', '-P', '-Q', '-R', '-S', '-W', '-w',
]);

/** Non-option words of an invocation, skipping options and their values. */
const operands = (words, valueOpts) => {
  const out = [];
  for (let k = 0; k < words.length; k++) {
    const w = words[k];
    if (w.value.startsWith('-') && w.value !== '-') {
      if (valueOpts.has(w.value)) k++;               // its value is not an operand
      continue;
    }
    out.push(w);
  }
  return out;
};

/**
 * The destination ref of a refspec: the part after the LAST colon, minus a force
 * prefix and any `refs/heads/` qualification. `main`, `+main`, `HEAD:main`,
 * `testnet:refs/heads/main` and `refs/heads/main` all resolve to `main`.
 */
const destOf = (spec) => spec
  .slice(spec.lastIndexOf(':') + 1)
  .replace(/^\+/, '')
  .replace(/^refs\/heads\//, '')
  .replace(/\.git$/, '');

/** Host part of a remote-shell destination: `user@host`, `host`, `host:path`. */
const hostOf = (dest) => {
  const afterUser = dest.slice(dest.lastIndexOf('@') + 1);
  const bracket = afterUser.match(/^\[([^\]]+)\]/);      // [2001:db8::1]:path
  if (bracket) return bracket[1];
  const colon = afterUser.indexOf(':');
  return colon >= 0 ? afterUser.slice(0, colon) : afterUser;
};

const isDeployHost = (h) => DEPLOY_HOSTS.some(d => h === d || h.endsWith('.' + d));

// → array of human-readable reasons this command is a deploy. Empty = not one.
export function deployReasons(rawCommand, depth = 0) {
  const cmd = stripHeredocs(rawCommand || '');
  const reasons = [];
  if (!cmd) return reasons;

  const folded = cmd.replace(/\\\r?\n/g, ' ');
  const add = (r) => { if (!reasons.includes(r)) reasons.push(r); };

  for (const words of invocations(tokenize(folded))) {
    const w = fromVerb(words);
    if (!w.length) continue;
    const verb = w[0].value.replace(/^.*\//, '');       // /usr/bin/ssh → ssh

    // ── A PUSH THAT LANDS ON A LIVE BRANCH ───────────────────────────────────
    if (verb === 'git' && w.slice(1).some(t => t.value === 'push')) {
      const args = operands(w.slice(w.findIndex(t => t.value === 'push') + 1), GIT_PUSH_OPT_VALUE);
      const specs = args.slice(1);                      // args[0] is the remote

      // No refspec at all publishes the current branch through push.default,
      // which the command text cannot tell us. Unknowable → gated.
      if (!specs.length) { add('pushes to main (live sites build from main)'); continue; }

      for (const s of specs) {
        if (s.dynamic) { add('pushes to an unresolvable ref (fails closed)'); continue; }
        const dst = destOf(s.value);
        // An empty destination (`main:`) or a bare `HEAD` resolves at push time,
        // not here. Both gate for the same reason a missing refspec does.
        if (!dst || dst === 'HEAD' || LIVE_BRANCHES.has(dst)) {
          add('pushes to main (live sites build from main)');
        }
      }
      continue;
    }

    // ── A COMMAND THAT REACHES A HOST SERVING USERS ──────────────────────────
    // The DESTINATION is the gated thing, never a hostname merely mentioned in
    // some other argument. A relay told to speak to a bridge URL is a client, and
    // reading /healthz is not a deployment.
    if (verb === 'ssh') {
      const args = operands(w.slice(1), SSH_OPT_VALUE);
      const dest = args[0];
      if (!dest) { add('runs a remote shell with no resolvable destination (fails closed)'); continue; }
      if (dest.dynamic) { add('runs a remote shell at an unresolvable destination (fails closed)'); continue; }
      const host = hostOf(dest.value);
      if (isDeployHost(host)) add(`reaches a deploy host (${host})`);

      // Everything after the destination runs AS A COMMAND on that host. A
      // bastion hop is the obvious case: the outer destination is innocent and
      // the payload is not.
      if (depth < 2) {
        const remote = args.slice(1).map(t => t.value).join(' ');
        if (remote) for (const r of deployReasons(remote, depth + 1)) add(r);
      }
      continue;
    }

    if (verb === 'scp' || verb === 'rsync') {
      // Either endpoint may be the remote one, so every operand is examined.
      for (const t of operands(w.slice(1), SSH_OPT_VALUE)) {
        if (!t.value.includes(':') && !t.value.includes('@')) continue;   // a local path
        if (t.dynamic) { add('copies to an unresolvable destination (fails closed)'); continue; }
        const host = hostOf(t.value);
        if (isDeployHost(host)) add(`reaches a deploy host (${host})`);
      }
      continue;
    }
  }

  // These two stay whole-string on purpose. Both name an action with no innocent
  // reading inside a command, and neither has produced a false positive; a
  // narrower parse would only add ways to miss one.
  if (/docker(-|\s+)compose\b[\s\S]*\bup\b/.test(folded)) add('brings up the docker stack');
  if (/systemctl\s+(restart|start)\s+\S*axona/.test(folded)) add('restarts a live axona service');

  return reasons;
}

export default { deployReasons, stripHeredocs, tokenize, DEPLOY_HOSTS, LIVE_BRANCHES };
