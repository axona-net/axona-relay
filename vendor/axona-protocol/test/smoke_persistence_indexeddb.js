// =====================================================================
// smoke_persistence_indexeddb.js — run the adapter-conformance suite
//                                   against IndexedDBPersistence using
//                                   fake-indexeddb to provide an IDB
//                                   implementation in Node.
// Run: node test/smoke_persistence_indexeddb.js
// =====================================================================

import 'fake-indexeddb/auto';  // installs indexedDB on globalThis

import { IndexedDBPersistence } from '../src/persistence/indexeddb.js';
import { runConformance }        from './persistence-conformance.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

let dbCounter = 0;

// Each makeAdapter() call uses a fresh DB name so prior state doesn't
// leak across conformance scenarios.
async function makeAdapter() {
  return new IndexedDBPersistence({ dbName: `axona-test-${++dbCounter}` });
}

// ── impl-specific tests ──────────────────────────────────────────────

async function testPersistenceAcrossInstances() {
  console.log('\n── persistence across separate adapter instances ──');
  const dbName = `axona-test-survive-${++dbCounter}`;

  const a = new IndexedDBPersistence({ dbName });
  await a.save('greeting', 'hello');
  await a.save('count',    42);
  await a.close();

  const b = new IndexedDBPersistence({ dbName });
  check('value survives close+reopen',
    (await b.load('greeting')) === 'hello');
  check('second key also survives',
    (await b.load('count')) === 42);
  await b.close();
}

async function testLogSink() {
  console.log('\n── log sink (warn level surfaces) ──');
  const events = [];
  const log = (level, msg, ctx) => events.push({ level, msg, ctx });
  const a = new IndexedDBPersistence({
    dbName: `axona-test-log-${++dbCounter}`,
    log,
  });
  // Normal operations don't log; just verify the sink isn't called
  // for happy path. (Quota errors are the trigger but hard to simulate
  // in fake-indexeddb.)
  await a.save('x', 1);
  await a.load('x');
  check('happy-path operations produce no log events', events.length === 0);
  await a.close();
}

async function main() {
  console.log('IndexedDBPersistence smoke (fake-indexeddb)');
  await runConformance(makeAdapter, { check, name: 'IndexedDB' });
  await testPersistenceAcrossInstances();
  await testLogSink();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
