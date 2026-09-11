// redact.test.mjs — offline proof that nothing naming a person or a machine can
// leave collect-manifest.mjs. No network, no ssh, no collection: it feeds
// synthetic identifiers through redact.mjs and then scans the COMMITTED
// manifests, so a regression in either the rule or an artifact fails here.
//
//   node --test harness/baseline/test/
import test from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { makeRedactor, hostTag, scan } from '../redact.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const BASE = resolve(HERE, '..');
const WS = '/Users/someone/Documents/claude';       // synthetic; never the real one
const redact = makeRedactor(WS);

test('workspace root becomes <ws>, nested path kept', () => {
  assert.equal(redact(`${WS}/axona-relay/src/mcp.js`), '<ws>/axona-relay/src/mcp.js');
});

test('every home-directory flavour becomes <home>', () => {
  for (const [input, want] of [
    ['/Users/alice/.ssh/id_ed25519_axona', '<home>/.ssh/id_ed25519_axona'],
    ['/home/mint-box/Documents/claude/axona-relay', '<home>/Documents/claude/axona-relay'],
    ['/c/Users/dave/github/axona-relay', '<home>/github/axona-relay'],
    ['ssh -i /Users/alice/.ssh/k root@10.0.0.1 cd /opt/axona-relay', 'ssh -i <home>/.ssh/k root@10.0.0.1 cd /opt/axona-relay'],
  ]) assert.equal(redact(input), want, input);
});

test('a command line with two homes and the workspace is fully scrubbed', () => {
  const cmd = `diff -rq ${WS}/axona-protocol/src /Users/alice/x/vendor/src; cp /home/bob/a /c/Users/carol/b`;
  const out = redact(cmd);
  assert.equal(out, 'diff -rq <ws>/axona-protocol/src <home>/x/vendor/src; cp <home>/a <home>/b');
  assert.deepEqual(scan(out), []);
});

test('things that are NOT secrets survive: aliases, droplet IPs, /opt, key filenames', () => {
  const s = 'ssh -o ConnectTimeout=20 air bash -l -s; ssh root@143.110.224.247 cd /opt/axona-relay; -i <home>/.ssh/id_ed25519_axona';
  assert.equal(redact(s), s);
});

test('hostname never appears; the tag is 12 hex chars and stable', () => {
  const t = hostTag('SomeBody-MacBook-Pro-9.local');
  assert.match(t, /^[0-9a-f]{12}$/);
  assert.equal(t, hostTag('SomeBody-MacBook-Pro-9.local'));
  assert.notEqual(t, hostTag('other.local'));
  assert.deepEqual(scan(t), []);
});

test('scan() catches what it is meant to catch', () => {
  assert.ok(scan('/Users/alice/x').length > 0);
  assert.ok(scan('/home/bob').length > 0);
  assert.ok(scan('/c/Users/carol').length > 0);
  assert.ok(scan('host MacBook-Pro-9.local').length > 0);
  assert.deepEqual(scan('<home>/x <ws>/y air m1 axona-linux 143.110.224.247'), []);
});

test('every committed manifest and census file is clean', () => {
  const root = join(BASE, 'manifest');
  const files = [];
  for (const day of readdirSync(root)) {
    const d = join(root, day);
    if (!statSync(d).isDirectory()) continue;
    for (const f of readdirSync(d)) if (/\.(json|jsonl)$/.test(f)) files.push(join(d, f));
  }
  assert.ok(files.length >= 2, 'expected at least one manifest + census pair');
  for (const f of files) {
    const hits = scan(readFileSync(f, 'utf8'));
    assert.deepEqual(hits, [], `${f.replace(BASE, '')} contains ${hits.join(', ')}`);
  }
});

test('the collector itself carries no home path or hostname', () => {
  const src = readFileSync(join(BASE, 'collect-manifest.mjs'), 'utf8');
  // the regex literal that DEFINES the rule mentions the prefixes; strip it first
  const withoutRule = src.replace(/\(\\\/Users\|\\\/home\|\\\/c\\\/Users\)/g, '');
  assert.deepEqual(scan(withoutRule), []);
});
