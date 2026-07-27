// =====================================================================
// smoke_persistence_inmemory.js — run the adapter-conformance suite
//                                  against InMemoryPersistence.
// Run: node test/smoke_persistence_inmemory.js
// =====================================================================

import { InMemoryPersistence } from '../src/persistence/interface.js';
import { runConformance }      from './persistence-conformance.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

async function main() {
  console.log('InMemoryPersistence smoke');
  await runConformance(() => new InMemoryPersistence(), {
    check,
    name: 'InMemory',
  });
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
