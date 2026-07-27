// =====================================================================
// smoke_health.js — peer.health() / onLog / onError / onUpgradeRequired.
// Run: node test/smoke_health.js
// =====================================================================

import { AxonaPeer }            from '../src/dht/AxonaPeer.js';
import { UpgradeRequiredError, TransportError, ErrorCodes }
                                from '../src/errors.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const SELF  = 'aa' + 'a1'.repeat(32);
const PEER1 = 'bb' + 'b2'.repeat(32);

class MockAxonaManager {
  constructor() {
    this.nodeId = SELF;
    this._publishCounter = 0;
    this._lastSeenTsByTopic = new Map();
    this._deliveryCallback = null;
  }
  pubsubPublish() { return 'p'; }
  pubsubSubscribe()   {}
  pubsubUnsubscribe() {}
  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  inspectRoles() {
    return [{ topicId: '0'.repeat(66), isRoot: true, children: [], replayCacheSize: 7 }];
  }
}

class MockTransport {
  constructor() {
    this.wireVersion = '1.0';
    this._log = () => {};
  }
}

// Web-shaped transport: boundPeers() + .mesh.getPeers() + .webrtc.boundPeers()
// so we can exercise health().transport / meshDegraded.
class MockWebTransport {
  constructor({ openChannels = 0, meshBound = 0, bridgeBound = 1 } = {}) {
    this.wireVersion = '4.0';
    this.bridgeState = 'connected';
    this._log = () => {};
    const open  = Array.from({ length: openChannels }, (_, i) => ({ peerId: `c${i}`, state: 'open' }));
    this.mesh   = { getPeers: () => open };
    // mesh-only bound set
    this.webrtc = { boundPeers: () => Array.from({ length: meshBound }, (_, i) => BigInt(i + 1)) };
    // aggregate bound = bridge + mesh
    this._boundTotal = bridgeBound + meshBound;
  }
  boundPeers() { return Array.from({ length: this._boundTotal }, (_, i) => BigInt(i + 1)); }
}

function makePeer({ withAm = true, withTransport = false, synaptome = [] } = {}) {
  const syn = new Map();
  for (const id of synaptome) syn.set(id, {});
  const node = { id: SELF, alive: true, synaptome: syn };
  return new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node,
    axonaManager: withAm ? new MockAxonaManager() : null,
    transport:   withTransport ? new MockTransport() : null,
  });
}

function testHealthShape() {
  console.log('\n── health() shape ──');
  const peer = makePeer({ synaptome: [PEER1], withTransport: true });
  const h = peer.health();
  check('nodeId is self',          h.nodeId === SELF);
  check('synaptomeSize = 1',       h.synaptomeSize === 1);
  check('peers includes PEER1',    h.peers.includes(PEER1));
  check('subscriptions = 0',       h.subscriptions === 0);
  check('axonRoles is array',      Array.isArray(h.axonRoles));
  check('axonRoles[0].cacheSize',  h.axonRoles[0]?.cacheSize === 7);
  check('axonRoles[0].isRoot',     h.axonRoles[0]?.isRoot === true);
  check('wireVersion = 1.0 (from transport)', h.wireVersion === '1.0');
  check('started flag exposed',    typeof h.started === 'boolean');
}

function testHealthWithoutTransport() {
  console.log('\n── health() without transport ──');
  const peer = makePeer({ withTransport: false });
  const h = peer.health();
  check('wireVersion = null when no transport', h.wireVersion === null);
}

function makeWebPeer(opts) {
  return new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: SELF, alive: true, synaptome: new Map() },
    transport: new MockWebTransport(opts),
  });
}

function testHealthTransportSurface() {
  console.log('\n── health().transport surface ──');
  const h = makeWebPeer({ openChannels: 8, meshBound: 8, bridgeBound: 1 }).health();
  check('transport object present',     h.transport !== null);
  check('boundCount = bridge+mesh (9)', h.transport.boundCount === 9);
  check('meshChannels = 8',             h.transport.meshChannels === 8);
  check('meshOpen = 8',                 h.transport.meshOpen === 8);
  check('meshBound = 8',                h.transport.meshBound === 8);
  check('bridgeState surfaced',         h.transport.bridgeState === 'connected');
  check('sim transport has null transport block', makePeer({ withTransport: true }).health().transport === null);
}

function testMeshDegradedInvariant() {
  console.log('\n── health().meshDegraded (routing-truth) ──');
  // The v2.4.0 bug: 9 DCs open, none bound past the bridge.
  const buggy = makeWebPeer({ openChannels: 9, meshBound: 0, bridgeBound: 1 }).health();
  check('9 open / 0 mesh-bound ⇒ degraded', buggy.meshDegraded === true);

  // Healthy steady state: every open channel is bound.
  const healthy = makeWebPeer({ openChannels: 8, meshBound: 8, bridgeBound: 1 }).health();
  check('8 open / 8 bound ⇒ not degraded', healthy.meshDegraded === false);

  // Mid-handshake transient: one channel still binding — not flagged.
  const transient = makeWebPeer({ openChannels: 3, meshBound: 2, bridgeBound: 1 }).health();
  check('3 open / 2 bound (gap 1) ⇒ not degraded', transient.meshDegraded === false);

  // Sustained gap ≥2 ⇒ flagged.
  const gap = makeWebPeer({ openChannels: 5, meshBound: 2, bridgeBound: 1 }).health();
  check('5 open / 2 bound (gap 3) ⇒ degraded', gap.meshDegraded === true);

  // Empty mesh ⇒ never degraded.
  const empty = makeWebPeer({ openChannels: 0, meshBound: 0, bridgeBound: 1 }).health();
  check('0 open ⇒ not degraded', empty.meshDegraded === false);
}

function testOnLogValidation() {
  console.log('\n── onLog validation ──');
  const peer = makePeer();
  let threw = false;
  try { peer.onLog('invalid', () => {}); } catch { threw = true; }
  check('rejects invalid level', threw);

  threw = false;
  try { peer.onLog('info', 'not-fn'); } catch { threw = true; }
  check('rejects non-function handler', threw);
}

function testOnLogReceives() {
  console.log('\n── onLog receives transport log events ──');
  const transport = new MockTransport();
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: SELF, alive: true, synaptome: new Map() },
    transport,
  });

  const warns = [];
  const debugs = [];
  peer.onLog('warn',  (msg, ctx) => warns.push({ msg, ctx }));
  peer.onLog('debug', (msg, ctx) => debugs.push({ msg, ctx }));

  // Simulate transport log events.
  transport._log('connection-failed', { peer: PEER1 });
  transport._log('heartbeat-tick',    { peer: PEER1 });

  check('warn-level event routed to warn handler',
    warns.length === 1 && warns[0].msg === 'connection-failed');
  check('non-warn event routed to debug handler',
    debugs.length === 1 && debugs[0].msg === 'heartbeat-tick');
  check('warn ctx preserved',
    warns[0].ctx?.peer === PEER1);
}

function testOnLogUnsubscribe() {
  console.log('\n── onLog unsubscribe ──');
  const transport = new MockTransport();
  const peer = new AxonaPeer({
    engine: { onEvent: () => () => {} },
    node:   { id: SELF, alive: true, synaptome: new Map() },
    transport,
  });
  const seen = [];
  const unsub = peer.onLog('debug', (msg) => seen.push(msg));
  transport._log('tick-1', {});
  unsub();
  transport._log('tick-2', {});
  check('only events before unsubscribe arrive',
    seen.length === 1 && seen[0] === 'tick-1');
}

function testOnError() {
  console.log('\n── onError ──');
  const peer = makePeer();
  const errs = [];
  peer.onError(e => errs.push(e));

  const fakeError = new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
    'peer x went away');
  peer._emitError(fakeError);
  check('onError receives the error',
    errs.length === 1 && errs[0] === fakeError);
}

function testOnUpgradeRequiredFanout() {
  console.log('\n── onUpgradeRequired ──');
  const peer = makePeer();
  const upgrades = [];
  const errors   = [];
  peer.onUpgradeRequired(e => upgrades.push(e));
  peer.onError(e => errors.push(e));

  const upErr = new UpgradeRequiredError('peer too old',
    { context: { reason: 'peer_too_old', minPeerVersion: '1.0.0' } });
  peer._emitError(upErr);

  check('UpgradeRequiredError fires onUpgradeRequired',
    upgrades.length === 1 && upgrades[0] === upErr);
  check('UpgradeRequiredError ALSO fires onError (fan-out)',
    errors.length === 1);
}

function main() {
  console.log('Axona health + onLog/onError (A6) smoke');
  testHealthShape();
  testHealthWithoutTransport();
  testHealthTransportSurface();
  testMeshDegradedInvariant();
  testOnLogValidation();
  testOnLogReceives();
  testOnLogUnsubscribe();
  testOnError();
  testOnUpgradeRequiredFanout();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
