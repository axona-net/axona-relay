// =====================================================================
// smoke_persistence_file.js — run the adapter-conformance suite against
//                              FilePersistence + plus impl-specific tests
//                              (atomic writes, lock files, corruption).
// Run: node test/smoke_persistence_file.js
// =====================================================================

import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { join }                              from 'node:path';
import { tmpdir }                            from 'node:os';

import { FilePersistence } from '../src/persistence/file.js';
import { runConformance }  from './persistence-conformance.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// Track temp dirs so we can clean them up at the end.
const tempDirs = [];

async function mkTempDir() {
  const d = await mkdtemp(join(tmpdir(), 'axona-persist-'));
  tempDirs.push(d);
  return d;
}

async function cleanup() {
  for (const d of tempDirs) {
    try { await rm(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
}

// makeAdapter() factory for the conformance suite — each call uses a
// fresh temp dir so prior state doesn't leak.  Disable the lock so
// the conformance suite (which opens/closes many adapters in one
// process) doesn't trip on its own PID.
async function makeAdapter() {
  const dir = await mkTempDir();
  return new FilePersistence({ dir, lock: false });
}

// ── impl-specific tests ──────────────────────────────────────────────

async function testPersistenceAcrossInstances() {
  console.log('\n── persistence across separate adapter instances ──');
  const dir = await mkTempDir();

  const a = new FilePersistence({ dir, lock: false });
  await a.save('greeting', 'hello');
  await a.save('count', 42);
  await a.close();

  const b = new FilePersistence({ dir, lock: false });
  check('value survives close+reopen',
    (await b.load('greeting')) === 'hello');
  check('second key also survives',
    (await b.load('count')) === 42);
  await b.close();
}

async function testAtomicWrite() {
  console.log('\n── atomic write (no .tmp file lingers on success) ──');
  const dir = await mkTempDir();
  const a   = new FilePersistence({ dir, lock: false });
  await a.save('thing', { x: 1 });
  await a.close();

  const tmpPath = join(dir, 'thing.json.tmp');
  let tmpExists = true;
  try { await readFile(tmpPath); } catch { tmpExists = false; }
  check('no .tmp file after successful save', !tmpExists);
}

async function testCorruptDataRejection() {
  console.log('\n── corruption: invalid JSON rejected with typed error ──');
  const dir = await mkTempDir();
  // Pre-create a corrupt file.
  await writeFile(join(dir, 'bad.json'), 'not-json{!@#');

  const a = new FilePersistence({ dir, lock: false });
  let err = null;
  try { await a.load('bad'); } catch (e) { err = e; }
  check('load on corrupt file throws AxonaError',
    err !== null && err.code === 'PERSIST_CORRUPT');
  await a.close();
}

async function testInvalidKey() {
  console.log('\n── invalid keys rejected ──');
  const dir = await mkTempDir();
  const a   = new FilePersistence({ dir, lock: false });

  let threw = false;
  try { await a.save('foo/bar', 'x'); } catch { threw = true; }
  check('key with slash rejected', threw);

  threw = false;
  try { await a.save('.hidden', 'x'); } catch { threw = true; }
  check('key starting with dot rejected', threw);

  await a.close();
}

async function testLockFile() {
  console.log('\n── PID lock prevents concurrent access in same process ──');
  const dir = await mkTempDir();
  const a = new FilePersistence({ dir, lock: true });
  await a.save('seed', 1);   // force init + lock

  let threw = false;
  try {
    const b = new FilePersistence({ dir, lock: true });
    await b.save('other', 2);   // should fail — lock held by `a`
  } catch (err) {
    threw = err.code === 'PERSIST_LOCKED';
  }
  check('second instance with lock=true is rejected', threw);
  await a.close();
}

async function testCustomDir() {
  console.log('\n── custom dir is created if missing ──');
  const parent = await mkTempDir();
  const child  = join(parent, 'nested', 'deeper');
  const a      = new FilePersistence({ dir: child, lock: false });
  await a.save('x', 1);
  check('nested directory was created',
    (await a.load('x')) === 1);
  await a.close();
}

async function main() {
  console.log('FilePersistence smoke');
  await runConformance(makeAdapter, { check, name: 'File' });
  await testPersistenceAcrossInstances();
  await testAtomicWrite();
  await testCorruptDataRejection();
  await testInvalidKey();
  await testLockFile();
  await testCustomDir();
  await cleanup();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(async err => {
  console.error('smoke threw:', err);
  await cleanup();
  process.exit(2);
});
