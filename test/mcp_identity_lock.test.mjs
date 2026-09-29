// ONE LIVE PROCESS PER AUTHOR KEY.
//
// Council seq 537 went out signed by axona.bot's author key from a server
// nobody meant to be driving it. The cause was a DEFAULT: a project-scoped
// .mcp.json left MCP_AUTHOR_PATH unset, the fallback resolved to a real key,
// and any editor opening that project signed as its owner.
//
// The guard that existed then printed the identity to stderr and trusted an
// operator to compare two logs. These tests exist because that is
// observability, not enforcement — so each one asserts a REFUSAL, not a
// warning.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { claimAuthorIdentity } from '../src/mcp-session.js';

const tmp = () => mkdtempSync(join(tmpdir(), 'axona-id-'));

test('a free key is claimed and the lock names this process', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  const release = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock, handle: 'axona.bot' });
  const held = JSON.parse(readFileSync(lock, 'utf8'));
  assert.equal(held.pid, process.pid);
  assert.equal(held.handle, 'axona.bot');
  release();
  assert.equal(existsSync(lock), false);
});

// The defect in one assertion: a SECOND live process on one key must not start.
test('a second LIVE holder is REFUSED, not warned', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  // pid 1 is always alive and is never us.
  writeFileSync(lock, JSON.stringify({ pid: 1, handle: 'someone-else', startedAt: Date.now() }));
  assert.throws(
    () => claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock }),
    /REFUSING TO START.*already held by pid 1/s,
  );
});

// A crashed server must not lock its own key out forever — that would turn a
// safety guard into an outage, and an operator would learn to delete the file,
// which defeats it permanently.
test('a STALE lock from a dead process is reclaimed', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  // 2^22 is above the default pid_max on both macOS and Linux, so no live
  // process can hold it — a dead holder without killing anything real.
  writeFileSync(lock, JSON.stringify({ pid: 4194304, handle: 'crashed', startedAt: 1 }));
  const release = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock });
  assert.equal(JSON.parse(readFileSync(lock, 'utf8')).pid, process.pid);
  release();
});

test('re-entry by the SAME process is not a conflict', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  const r1 = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock });
  const r2 = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock });
  assert.equal(JSON.parse(readFileSync(lock, 'utf8')).pid, process.pid);
  r2(); r1();
});

test('a corrupt lock file does not wedge startup', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  writeFileSync(lock, 'not json at all');
  const release = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock });
  assert.equal(JSON.parse(readFileSync(lock, 'utf8')).pid, process.pid);
  release();
});

// STRICT MODE — for an install that wants no fallback under any circumstance.
// The fallback is the original defect, so refusing it outright must be available.
test('strict mode refuses the DEFAULT path outright', () => {
  const d = tmp();
  assert.throws(
    () => claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: join(d, 'id.json.lock'),
                                strict: true, usingDefault: true }),
    /MCP_STRICT_IDENTITY=1 and MCP_AUTHOR_PATH is unset/,
  );
});

test('strict mode allows an EXPLICIT path', () => {
  const d = tmp();
  const release = claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: join(d, 'id.json.lock'),
                                        strict: true, usingDefault: false });
  release();
});

// The refusal has to tell an operator what to do, or they will delete the lock.
test('the refusal names the remedy, not just the problem', () => {
  const d = tmp();
  const lock = join(d, 'id.json.lock');
  writeFileSync(lock, JSON.stringify({ pid: 1, startedAt: Date.now() }));
  try {
    claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock });
    assert.fail('should have refused');
  } catch (e) {
    assert.match(e.message, /MCP_AUTHOR_PATH=/);
    assert.match(e.message, /sign identically/);
  }
});

// COVER THE OTHER BRANCH. pidAlive has two paths: process.kill returning
// normally (alive, ours to signal) and throwing EPERM (alive, someone else's).
// The pid-1 test above only ever exercises EPERM, because pid 1 is root-owned —
// so deleting the success branch left that test green. A live CHILD of this
// process is signalable by us and takes the other path.
test('a live process we CAN signal is also refused (the non-EPERM branch)', async () => {
  const { spawn } = await import('node:child_process');
  const child = spawn(process.execPath, ['-e', 'setTimeout(()=>{},60000)'], { stdio: 'ignore' });
  try {
    await new Promise(r => setTimeout(r, 120));
    const d = tmp();
    const lock = join(d, 'id.json.lock');
    writeFileSync(lock, JSON.stringify({ pid: child.pid, handle: 'peer-install', startedAt: Date.now() }));
    assert.throws(
      () => claimAuthorIdentity({ storePath: join(d, 'id.json'), lockPath: lock }),
      new RegExp(`REFUSING TO START.*already held by pid ${child.pid}`, 's'),
    );
  } finally { child.kill('SIGKILL'); }
});
