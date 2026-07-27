// =====================================================================
// smoke_departure_hint.mjs — #364-B: bridge departure hints, TEMPORARY
// testnet-era crutch with the reachability guard.
//
// The bridge includes the departed connection's authenticated nodeId in its
// peer-left broadcast; WebRTCTransport.reportPeerDeparted fires the standard
// peer-died path so pub/sub ghosts purge immediately. THE GUARD is the
// contract that keeps the hint safe forever: a hint is ignored whenever we
// hold an active channel to the subject — our own connectivity outranks the
// bridge's opinion (a grown network's nodes legitimately drop their bridge
// socket while remaining valid mesh members).
//
// Run: node test/smoke_departure_hint.mjs
// =====================================================================
import { WebRTCTransport } from '../src/transport/web/webrtc.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

const t = new WebRTCTransport({ mesh: null, log: () => {} });
const died = [];
t.onPeerDied((id) => died.push(id));

const GHOST = 0x80abcdefn;     // never connected — the ghost-slice case
const BOUND = 0x80123456n;     // active channel — the future graduated case

// bind BOUND to a live meshId the way the hello handshake would
t._meshIdByNodeId.set(BOUND, 'c9x');
t._nodeIdByMeshId.set('c9x', BOUND);

check('hint for a node we cannot reach → acted on (peer-died fired)',
  t.reportPeerDeparted(GHOST) === true && died.length === 1 && died[0] === GHOST);

check('hint for a node with an ACTIVE channel → IGNORED (our channel outranks the bridge)',
  t.reportPeerDeparted(BOUND) === false && died.length === 1);

check('malformed hint (non-bigint) → rejected, nothing fired',
  t.reportPeerDeparted('80abcdef') === false && died.length === 1);

check('handler that throws does not break the fan-out', (() => {
  t.onPeerDied(() => { throw new Error('boom'); });
  const tail = [];
  t.onPeerDied((id) => tail.push(id));
  return t.reportPeerDeparted(0x80fedcban) === true && tail.length === 1;
})());

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
