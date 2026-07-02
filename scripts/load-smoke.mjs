#!/usr/bin/env node
// load-smoke.mjs — import-load smoke for `npm test`.
//
// Imports the relay's library modules and the full vendored kernel graph
// (barrel + the non-barrel connect.js). No network, no side effects — this
// catches a broken or PARTIAL vendor copy (a missing kernel file makes some
// import throw ERR_MODULE_NOT_FOUND) and any syntax/link error the per-file
// `node --check` pass can't see across module boundaries.
import '../src/polyfill.js';

const MODULES = [
  '../vendor/axona-protocol/src/index.js',     // kernel barrel — resolves the whole graph
  '../vendor/axona-protocol/src/connect.js',   // deliberately NOT in the barrel; vendored separately
  '../src/ops.js',                             // relay core (connectPeer et al.)
  '../src/relay.js',
  '../src/network.js',
];

for (const m of MODULES) {
  await import(m);
  console.log(`  ✓ ${m.replace('../', '')}`);
}

// The vendored kernel must self-report a version (release ritual reads it).
const { KERNEL_VERSION } = await import('../vendor/axona-protocol/src/transport/handshake.js');
if (!/^\d+\.\d+\.\d+$/.test(KERNEL_VERSION ?? '')) {
  console.error(`✗ vendored kernel has no parsable KERNEL_VERSION (got ${JSON.stringify(KERNEL_VERSION)})`);
  process.exit(1);
}
console.log(`load-smoke ok (vendored kernel ${KERNEL_VERSION})`);
process.exit(0);
