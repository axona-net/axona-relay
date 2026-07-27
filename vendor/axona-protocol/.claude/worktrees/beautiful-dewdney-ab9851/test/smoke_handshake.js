// =====================================================================
// smoke_handshake.js — version handshake module: parse, compare,
//                       wireCompatible, performClientHandshake,
//                       performServerHandshake.
// Run: node test/smoke_handshake.js
// =====================================================================

import {
  WIRE_VERSION,
  KERNEL_VERSION,
  UPGRADE_CLOSE_CODE,
  buildClientHello,
  buildServerHello,
  parseHello,
  parseVersion,
  compareVersions,
  wireCompatible,
  performClientHandshake,
  performServerHandshake,
} from '../src/transport/handshake.js';
import { UpgradeRequiredError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// ── Constants ────────────────────────────────────────────────────────

function testConstants() {
  console.log('\n── constants ──');
  check('WIRE_VERSION = "4.0"',         WIRE_VERSION === '4.0');   // 2026-06-24 routing-only pub/sub wire flag-day
  check('KERNEL_VERSION is semver-shaped', /^\d+\.\d+\.\d+/.test(KERNEL_VERSION));
  check('UPGRADE_CLOSE_CODE = 4426',     UPGRADE_CLOSE_CODE === 4426);
}

// ── Version parsing + comparison ─────────────────────────────────────

function testParseVersion() {
  console.log('\n── parseVersion ──');
  const v1 = parseVersion('1.2.3');
  check('parses major/minor/patch',
    v1.major === 1 && v1.minor === 2 && v1.patch === 3 && v1.pre === '');

  const v2 = parseVersion('1.0.0-rc.0');
  check('parses prerelease tag', v2.pre === 'rc.0');

  let threw = false;
  try { parseVersion('not-semver'); } catch { threw = true; }
  check('rejects non-semver', threw);

  threw = false;
  try { parseVersion(123); } catch { threw = true; }
  check('rejects non-string', threw);
}

function testCompareVersions() {
  console.log('\n── compareVersions ──');
  check('a < b on major',   compareVersions('1.0.0', '2.0.0') === -1);
  check('a > b on major',   compareVersions('2.0.0', '1.0.0') ===  1);
  check('a < b on minor',   compareVersions('1.1.0', '1.2.0') === -1);
  check('a < b on patch',   compareVersions('1.0.0', '1.0.1') === -1);
  check('equal',            compareVersions('1.2.3', '1.2.3') ===  0);
  check('prerelease < stable',
    compareVersions('1.0.0-rc.0', '1.0.0') === -1);
  check('stable > prerelease',
    compareVersions('1.0.0', '1.0.0-rc.0') ===  1);
  check('two prereleases',
    compareVersions('1.0.0-rc.0', '1.0.0-rc.1') === -1);
}

function testWireCompatible() {
  console.log('\n── wireCompatible ──');
  check('same major = compatible',     wireCompatible('1.0', '1.7'));
  check('same major.minor compatible', wireCompatible('1.0', '1.0'));
  check('major mismatch incompatible', !wireCompatible('1.0', '2.0'));
}

// ── Frame builders ───────────────────────────────────────────────────

function testBuilders() {
  console.log('\n── buildClientHello / buildServerHello / parseHello ──');
  const ch = buildClientHello({ version: '1.0.0', capabilities: ['pubsub'] });
  check('client-hello type', ch.type === 'client-hello');
  check('client-hello version', ch.version === '1.0.0');
  check('client-hello default wireVersion', ch.wireVersion === '4.0');
  check('client-hello capabilities copied',
    Array.isArray(ch.capabilities) && ch.capabilities[0] === 'pubsub');

  const sh = buildServerHello({
    version: '1.0.0-rc.0',
    minPeerVersion: '1.0.0-rc.0',
    downloadUrl: 'https://axona.net',
  });
  check('server-hello type', sh.type === 'server-hello');
  check('server-hello minPeerVersion', sh.minPeerVersion === '1.0.0-rc.0');
  check('server-hello downloadUrl', sh.downloadUrl === 'https://axona.net');

  check('parseHello recognises client-hello',
    parseHello(ch)?.type === 'client-hello');
  check('parseHello recognises server-hello',
    parseHello(sh)?.type === 'server-hello');
  check('parseHello rejects random object',
    parseHello({ type: 'welcome' }) === null);
  check('parseHello rejects missing version',
    parseHello({ type: 'client-hello' }) === null);
  check('parseHello rejects null', parseHello(null) === null);
}

// ── Handshake runner: client side ────────────────────────────────────

function fakeChannel({ inboundFrame, sendDelay = 0 }) {
  const sent = [];
  let resolveAwait;
  const awaitPromise = new Promise(r => { resolveAwait = r; });
  return {
    sent,
    sendFrame: (frame) => sent.push(frame),
    awaitInbound: () => awaitPromise,
    deliver: (frame) => setTimeout(() => resolveAwait(frame), sendDelay),
  };
}

async function testClientHandshakeHappy() {
  console.log('\n── performClientHandshake: happy path ──');
  const ch = fakeChannel({});
  const sh = buildServerHello({
    version: '1.0.0',
    minPeerVersion: '1.0.0-rc.0',
    downloadUrl: 'https://axona.net',
  });
  ch.deliver(sh);

  const result = await performClientHandshake({
    version: '1.0.0',
    sendFrame: ch.sendFrame,
    awaitServerHello: ch.awaitInbound,
    timeoutMs: 1000,
  });
  check('sent client-hello',
    ch.sent.length === 1 && ch.sent[0].type === 'client-hello');
  check('server-hello received',
    result.serverHello.type === 'server-hello');
}

async function testClientHandshakePeerTooOld() {
  console.log('\n── performClientHandshake: peer too old ──');
  const ch = fakeChannel({});
  const sh = buildServerHello({
    version: '2.0.0',
    minPeerVersion: '2.0.0',
    downloadUrl: 'https://axona.net/upgrade',
  });
  ch.deliver(sh);

  let err = null;
  try {
    await performClientHandshake({
      version: '1.0.0',
      sendFrame: ch.sendFrame,
      awaitServerHello: ch.awaitInbound,
      timeoutMs: 1000,
    });
  } catch (e) { err = e; }
  check('throws UpgradeRequiredError',
    err instanceof UpgradeRequiredError);
  check('code = UPGRADE_REQUIRED',
    err?.code === ErrorCodes.UPGRADE_REQUIRED);
  check('reason = peer_too_old',
    err?.context?.reason === 'peer_too_old');
  check('carries server downloadUrl',
    err?.context?.downloadUrl === 'https://axona.net/upgrade');
  check('carries minPeerVersion',
    err?.context?.minPeerVersion === '2.0.0');
}

async function testClientHandshakeWireMismatch() {
  console.log('\n── performClientHandshake: wire-version mismatch ──');
  const ch = fakeChannel({});
  const sh = buildServerHello({
    version: '1.0.0',
    wireVersion: '2.0',
    minPeerVersion: '1.0.0',
  });
  ch.deliver(sh);

  let err = null;
  try {
    await performClientHandshake({
      version: '1.0.0',
      wireVersion: '1.0',
      sendFrame: ch.sendFrame,
      awaitServerHello: ch.awaitInbound,
      timeoutMs: 1000,
    });
  } catch (e) { err = e; }
  check('throws on wire-version mismatch',
    err instanceof UpgradeRequiredError);
  check('reason = wire_version_mismatch',
    err?.context?.reason === 'wire_version_mismatch');
}

async function testClientHandshakeTimeout() {
  console.log('\n── performClientHandshake: timeout ──');
  const ch = fakeChannel({});
  // No deliver() — handshake should time out.
  let err = null;
  try {
    await performClientHandshake({
      version: '1.0.0',
      sendFrame: ch.sendFrame,
      awaitServerHello: ch.awaitInbound,
      timeoutMs: 50,
    });
  } catch (e) { err = e; }
  check('timeout throws UpgradeRequiredError',
    err instanceof UpgradeRequiredError);
  check('reason = handshake_timeout',
    err?.context?.reason === 'handshake_timeout');
}

async function testClientHandshakeMalformed() {
  console.log('\n── performClientHandshake: malformed server response ──');
  const ch = fakeChannel({});
  ch.deliver({ type: 'welcome', connId: 'abc' });   // not a hello

  let err = null;
  try {
    await performClientHandshake({
      version: '1.0.0',
      sendFrame: ch.sendFrame,
      awaitServerHello: ch.awaitInbound,
      timeoutMs: 1000,
    });
  } catch (e) { err = e; }
  check('malformed → UpgradeRequiredError',
    err instanceof UpgradeRequiredError);
  check('reason = malformed_server_hello',
    err?.context?.reason === 'malformed_server_hello');
}

// ── Handshake runner: server side ────────────────────────────────────

async function testServerHandshakeHappy() {
  console.log('\n── performServerHandshake: happy path ──');
  const ch = fakeChannel({});
  const ch_hello = buildClientHello({ version: '1.0.0' });
  ch.deliver(ch_hello);

  const result = await performServerHandshake({
    version: '1.0.0',
    minPeerVersion: '1.0.0-rc.0',
    sendFrame: ch.sendFrame,
    awaitClientHello: ch.awaitInbound,
    timeoutMs: 1000,
  });
  check('client-hello received',
    result.clientHello.type === 'client-hello');
  check('server sent server-hello back',
    ch.sent.length === 1 && ch.sent[0].type === 'server-hello');
  check('server-hello carries minPeerVersion',
    ch.sent[0].minPeerVersion === '1.0.0-rc.0');
}

async function testServerHandshakeClientTooOld() {
  console.log('\n── performServerHandshake: client too old ──');
  const ch = fakeChannel({});
  ch.deliver(buildClientHello({ version: '0.9.0' }));

  let err = null;
  try {
    await performServerHandshake({
      version: '1.0.0',
      minPeerVersion: '1.0.0',
      sendFrame: ch.sendFrame,
      awaitClientHello: ch.awaitInbound,
      downloadUrl: 'https://axona.net/upgrade',
      timeoutMs: 1000,
    });
  } catch (e) { err = e; }
  check('throws UpgradeRequiredError',
    err instanceof UpgradeRequiredError);
  check('reason = peer_too_old',
    err?.context?.reason === 'peer_too_old');
  check('still sent server-hello so client learns the version floor',
    ch.sent.length === 1 && ch.sent[0].type === 'server-hello' &&
    ch.sent[0].minPeerVersion === '1.0.0');
  check('downloadUrl included in error context',
    err?.context?.downloadUrl === 'https://axona.net/upgrade');
}

async function testServerHandshakeWireMismatch() {
  console.log('\n── performServerHandshake: wire-version mismatch ──');
  const ch = fakeChannel({});
  ch.deliver(buildClientHello({ version: '1.0.0', wireVersion: '2.0' }));

  let err = null;
  try {
    await performServerHandshake({
      version: '1.0.0',
      minPeerVersion: '1.0.0',
      wireVersion: '1.0',
      sendFrame: ch.sendFrame,
      awaitClientHello: ch.awaitInbound,
      timeoutMs: 1000,
    });
  } catch (e) { err = e; }
  check('throws wire mismatch',
    err instanceof UpgradeRequiredError);
  check('reason = wire_version_mismatch',
    err?.context?.reason === 'wire_version_mismatch');
}

// ── Loopback: client + server back-to-back over a real channel ───────

async function testLoopback() {
  console.log('\n── loopback: client + server handshake over a shared channel ──');
  // Two queues to simulate two halves of a channel.
  const c2s = [];
  const s2c = [];
  let resolveS = null, resolveC = null;
  const awaitC = () => new Promise(r => { resolveC = r; });
  const awaitS = () => new Promise(r => { resolveS = r; });

  function clientSend(frame) {
    c2s.push(frame);
    // Let server's awaitClientHello fire on next tick.
    queueMicrotask(() => { resolveS?.(c2s.shift()); resolveS = null; });
  }
  function serverSend(frame) {
    s2c.push(frame);
    queueMicrotask(() => { resolveC?.(s2c.shift()); resolveC = null; });
  }

  const clientPromise = performClientHandshake({
    version: '1.0.0',
    sendFrame: clientSend,
    awaitServerHello: awaitC,
    timeoutMs: 1000,
  });
  const serverPromise = performServerHandshake({
    version: '1.0.0',
    minPeerVersion: '1.0.0-rc.0',
    sendFrame: serverSend,
    awaitClientHello: awaitS,
    timeoutMs: 1000,
  });

  const [clientResult, serverResult] = await Promise.all([clientPromise, serverPromise]);
  check('client got server-hello', clientResult.serverHello.type === 'server-hello');
  check('server got client-hello', serverResult.clientHello.type === 'client-hello');
}

// ── main ─────────────────────────────────────────────────────────────

async function main() {
  console.log('Axona version-handshake smoke');
  testConstants();
  testParseVersion();
  testCompareVersions();
  testWireCompatible();
  testBuilders();
  await testClientHandshakeHappy();
  await testClientHandshakePeerTooOld();
  await testClientHandshakeWireMismatch();
  await testClientHandshakeTimeout();
  await testClientHandshakeMalformed();
  await testServerHandshakeHappy();
  await testServerHandshakeClientTooOld();
  await testServerHandshakeWireMismatch();
  await testLoopback();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
