// =====================================================================
// smoke_errors.js — verify the typed error hierarchy and wire round-trip.
// Run: node test/smoke_errors.js
// =====================================================================

import {
  AxonaError,
  IdentityError,
  TransportError,
  PublishError,
  SubscribeError,
  PullError,
  MetricsError,
  UpgradeRequiredError,
  ErrorCodes,
  isWireError,
  fromWire,
} from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function testHierarchy() {
  console.log('\n── hierarchy ──');
  const id = new IdentityError('IDENTITY_LOAD_FAILED', 'disk read failed');
  check('IdentityError extends AxonaError',  id instanceof AxonaError);
  check('IdentityError extends Error',       id instanceof Error);
  check('TransportError extends AxonaError', new TransportError('X', 'y') instanceof AxonaError);
  check('PublishError extends AxonaError',   new PublishError('X', 'y') instanceof AxonaError);
  check('SubscribeError extends AxonaError', new SubscribeError('X', 'y') instanceof AxonaError);
  check('PullError extends AxonaError',      new PullError('X', 'y') instanceof AxonaError);
  check('MetricsError extends AxonaError',   new MetricsError('X', 'y') instanceof AxonaError);
  check('UpgradeRequiredError extends AxonaError',
    new UpgradeRequiredError('upgrade') instanceof AxonaError);
}

function testFields() {
  console.log('\n── fields ──');
  const cause = new Error('underlying');
  const err = new TransportError('TRANSPORT_TIMEOUT', 'peer X did not respond', {
    cause,
    context: { peerId: 'abc', topic: 'cats' },
  });
  check('code preserved',      err.code === 'TRANSPORT_TIMEOUT');
  check('message preserved',   err.message === 'peer X did not respond');
  check('name is class name',  err.name === 'TransportError');
  check('cause preserved',     err.cause === cause);
  check('context preserved',   err.context.peerId === 'abc' && err.context.topic === 'cats');
  check('default context = {}', new AxonaError('X', 'y').context !== undefined);
}

function testUpgradeRequired() {
  console.log('\n── UpgradeRequiredError ──');
  const err = new UpgradeRequiredError('peer is too old', {
    context: { reason: 'wire_version_mismatch', serverVersion: '1.0', clientVersion: '0.9' },
  });
  check('code is always UPGRADE_REQUIRED', err.code === 'UPGRADE_REQUIRED');
  check('context carries server + client version',
    err.context.serverVersion === '1.0' && err.context.clientVersion === '0.9');
}

function testWireRoundTrip() {
  console.log('\n── wire round-trip ──');
  const orig = new PublishError('PUBLISH_REPLICATION_FAILED', 'only 2 of 5 K-closest acked', {
    context: { acks: 2, expected: 5, topic: 'cats' },
  });
  const wire = orig.toWire();
  check('wire has __axonaError marker', wire.__axonaError === true);
  check('isWireError(wire) === true',   isWireError(wire));
  check('wire carries class name',      wire.class === 'PublishError');
  check('wire carries code',            wire.code === 'PUBLISH_REPLICATION_FAILED');

  const restored = fromWire(wire);
  check('fromWire returns AxonaError',  restored instanceof AxonaError);
  check('fromWire returns PublishError', restored instanceof PublishError);
  check('restored code matches',         restored.code === orig.code);
  check('restored message matches',      restored.message === orig.message);
  check('restored context matches',      restored.context.acks === 2);
}

function testUnknownClassFallback() {
  console.log('\n── forward-compat: unknown class falls back to AxonaError ──');
  const wire = {
    __axonaError: true,
    class: 'FutureError',
    code:  'FUTURE_CODE',
    message: 'something new',
    context: { detail: 42 },
  };
  const restored = fromWire(wire);
  check('returns AxonaError for unknown class', restored instanceof AxonaError);
  check('code preserved', restored.code === 'FUTURE_CODE');
  check('context preserved', restored.context.detail === 42);
}

function testWireRejection() {
  console.log('\n── isWireError / fromWire rejection ──');
  check('null is not wire error', !isWireError(null));
  check('plain object is not wire error', !isWireError({ class: 'X', code: 'Y' }));
  check('missing class is not wire error', !isWireError({ __axonaError: true, code: 'Y' }));
  let threw = false;
  try { fromWire({ not: 'wire' }); } catch { threw = true; }
  check('fromWire rejects non-wire input', threw);
}

function testCodes() {
  console.log('\n── ErrorCodes constants ──');
  check('ErrorCodes is frozen', Object.isFrozen(ErrorCodes));
  check('IDENTITY_LOAD_FAILED matches', ErrorCodes.IDENTITY_LOAD_FAILED === 'IDENTITY_LOAD_FAILED');
  check('UPGRADE_REQUIRED matches',     ErrorCodes.UPGRADE_REQUIRED === 'UPGRADE_REQUIRED');
  check('all codes are UPPER_SNAKE',
    Object.values(ErrorCodes).every(v => /^[A-Z][A-Z0-9_]*$/.test(v)));
}

function main() {
  console.log('Axona typed-error smoke');
  testHierarchy();
  testFields();
  testUpgradeRequired();
  testWireRoundTrip();
  testUnknownClassFallback();
  testWireRejection();
  testCodes();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
