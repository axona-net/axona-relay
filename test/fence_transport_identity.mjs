/**
 * fence_transport_identity — static half of INVARIANT I-ID.
 *
 *   Transport identity is ephemeral.  Author identity is durable.
 *
 * smoke_persistence_wiring covers the kernel's own persistence path behaviourally
 * ("restart against the same store yields a different nodeId"). It cannot see a
 * service that hand-rolls its own file read/write — and that is precisely how the
 * MCP session and both bridges acquired durable transport keys without a single
 * kernel test noticing.
 *
 * So this scans SOURCE for the idioms that exist only to persist a transport
 * identity, and for config/comments promising a stable nodeId across restarts.
 * Deployment artifacts are in scope: one instance lived in a compose comment.
 *
 * Copy into each repo (kernel, relay, bridge, apps) and chain into `npm test`.
 */

import { readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';

/**
 * Deliberately NOT banned: `dumpIdentity` / `loadIdentity`. They are legitimate
 * kernel primitives — author identities persist through them, and the identity
 * smokes exercise them directly. Banning them produced ~30 false positives, and
 * a fence that cries wolf gets switched off. Whether an identity actually reaches
 * durable storage is a behavioural question, covered by smoke_persistence_wiring
 * ("restart against the same store ⇒ a different nodeId") and smoke_snapshot.
 *
 * What IS banned below is the stuff with no legitimate use: a configured path for
 * a transport identity, an identity file, a load-or-create helper, and any claim
 * that a nodeId survives a restart.
 */
const BANNED = [
  { re: /\bloadOrCreateNodeIdentity\b/,          why: 'load-or-create implies persistence' },
  { re: /\b[A-Z_]*(NODE|RELAY|BRIDGE)_IDENTITY_PATH\b/, why: 'a filesystem path for a transport identity' },
  // A file *named* identity*.json — note the leading path/quote boundary, so an
  // author store like `claude-mcp-identity.json` (legitimately durable) does not
  // match, while `./identity.relay.json` and `/var/lib/.../identity.west.json` do.
  { re: /[/'"`\s(]identity[.\w-]*\.json/,        why: 'a transport identity on disk' },
  { re: /stable\s+nodeId/i,                      why: 'promises node-identity continuity' },
  { re: /nodeId\s+across\s+restarts/i,           why: 'promises node-identity continuity' },
  { re: /persist(ent|ed)?\s+(node|transport)\s+identity/i, why: 'states the anti-pattern outright' },
];

/**
 * Lines that may legitimately name these idioms: the rule's own statement, and
 * comments recording that the pattern was REMOVED. Keep this list short — every
 * entry is a hole. Negations are allowed because that is how the code documents
 * its own compliance ("never persisted", "no bridge-identity.json").
 */
const ALLOW = [
  /\bNEVER\b|\bnever\b|\bno longer\b|\bREMOVED\b|\bephemeral\b|\bIGNORED\b|\bignored\b/i,
  /INVARIANT|I-ID|used to|before 2026|anti-pattern|forbids/i,
];

const files = execSync(
  "git ls-files '*.js' '*.mjs' '*.ts' '*.json' '*.yml' '*.yaml' '*.md' 'Dockerfile*' " +
  "| grep -v node_modules | grep -v package-lock | grep -v '^vendor/'",
  { encoding: 'utf8', cwd: process.cwd() },
).trim().split('\n').filter(Boolean);

const violations = [];
for (const f of files) {
  if (f.endsWith('fence_transport_identity.mjs')) continue;   // this file
  let lines;
  try { lines = readFileSync(f, 'utf8').split('\n'); } catch { continue; }
  lines.forEach((line, i) => {
    if (ALLOW.some(a => a.test(line))) return;
    for (const { re, why } of BANNED) {
      if (re.test(line)) violations.push({ f, n: i + 1, line: line.trim().slice(0, 110), why });
    }
  });
}

if (violations.length) {
  console.error('\nINVARIANT I-ID VIOLATED — a transport identity must never be persisted:\n');
  for (const v of violations) console.error(`  ${v.f}:${v.n}\n      ${v.line}\n      -> ${v.why}\n`);
  console.error(`${violations.length} violation(s). Author identities may persist; transport identities may not.\n`);
  process.exit(1);
}
console.log(`fence_transport_identity: clean across ${files.length} files (I-ID)`);
