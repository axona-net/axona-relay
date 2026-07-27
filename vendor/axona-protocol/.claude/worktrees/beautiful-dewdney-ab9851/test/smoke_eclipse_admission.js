// =====================================================================
// smoke_eclipse_admission.js — B-3 eclipse prevention: verified-only
// synapse admission, reinforce gating, local_probe disclosure cap.
//
// Routing-table entries are the substrate of an eclipse attack. B-3:
//   • gossip (triadic_introduce / hop_cache / lateral_spread) may only
//     SUGGEST a peer; on identity-binding transports a synapse is admitted
//     only after first-party verification (the peer binds via axona/4),
//     never directly from the message.
//   • reinforce may only refresh a synapse whose peer is currently bound.
//   • local_probe returns a bounded sample, not the whole synaptome.
// Non-binding transports (the in-process sim) keep prior behavior.
//
// Run: node test/smoke_eclipse_admission.js
// =====================================================================

import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { Synapse }   from '../src/dht/Synapse.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const SELF = 1n;
const CAND = 0x1234n;

const DOMAIN = { simEpoch: 0, MAX_SYNAPTOME: 50, RECENCY_HALF_LIFE: 10, INERTIA_DURATION: 8, MAX_HOPS: 20, TRIADIC_THRESHOLD: 3 };

function makeNode() {
  const synaptome = new Map();
  const node = {
    id: SELF, synaptome, connections: new Map(),
    addSynapse: (syn) => synaptome.set(syn.peerId, syn),
    transport: null,
  };
  return node;
}

function bindingTransport() {
  const calls = { open: [] };
  let bound = [];
  const ntf = {}, req = {};
  return {
    _calls: calls, _setBound: (a) => { bound = a; }, _ntf: ntf, _req: req,
    boundPeers:      () => bound,
    onPeerBound:     () => () => {},
    openConnection:  async (pid) => { calls.open.push(pid); return true; },
    getLatency:      () => 50,
    closeConnection: async () => {},
    notify:          async () => {},
    onNotification:  (t, h) => { ntf[t] = h; },
    onRequest:       (t, h) => { req[t] = h; },
  };
}

function simTransport() {
  return {
    openConnection: async () => true,
    getLatency: () => 50,
    closeConnection: async () => {},
    notify: async () => {},
    onNotification: () => {}, onRequest: () => {},
  };
}

function makePeer(transport) {
  const node = makeNode();
  node.transport = transport;
  return new AxonaPeer({ node, transport, domain: DOMAIN });
}

async function testGossipDoesNotDirectlyAdmit() {
  console.log('\n── layer 1: gossip on a binding transport does NOT insert a synapse ──');
  const t = bindingTransport();
  const peer = makePeer(t);
  await peer._considerCandidate(CAND, 'triadic');
  check('candidate NOT inserted directly from gossip', !peer._node.synaptome.has(CAND));
  check('a verification connection was triggered', t._calls.open.includes(CAND));
}

async function testAlreadyBoundAdmitsViaVerifiedPath() {
  console.log('\n── layer 1: an already-bound peer is admitted (verified path) ──');
  const t = bindingTransport();
  t._setBound([CAND]);
  const peer = makePeer(t);
  await peer._considerCandidate(CAND, 'triadic');
  check('bound candidate admitted to synaptome', peer._node.synaptome.has(CAND));
  check('no redundant connection opened', !t._calls.open.includes(CAND));
}

async function testProbeBudgetCaps() {
  console.log('\n── layer 1: verification probes are budget-capped ──');
  const t = bindingTransport();
  const peer = makePeer(t);
  peer._verifyProbes = 8;   // MAX_VERIFY_PROBES
  await peer._considerCandidate(CAND, 'triadic');
  check('no probe opened once budget is exhausted', t._calls.open.length === 0);
  check('candidate not admitted', !peer._node.synaptome.has(CAND));
}

async function testSimTransportPreservesDirectAdmit() {
  console.log('\n── layer 1: non-binding (sim) transport keeps direct admission ──');
  const peer = makePeer(simTransport());
  await peer._considerCandidate(CAND, 'triadic');
  check('candidate admitted directly on sim transport', peer._node.synaptome.has(CAND));
}

async function testReinforceBoundGate() {
  console.log('\n── layer 2: reinforce only honored for a bound synapse ──');
  const t = bindingTransport();
  const peer = makePeer(t);
  peer._installRoutingHandlers();
  // seed two synapses
  const BOUND = 0xAAAn, UNBOUND = 0xBBBn;
  for (const id of [BOUND, UNBOUND]) {
    const s = new Synapse({ peerId: id, latencyMs: 50, stratum: 1 });
    s.weight = 0.5; s.inertia = -100;       // evictable (inertia in the past)
    peer._node.synaptome.set(id, s);
  }
  t._setBound([BOUND]);                       // only BOUND is identity-verified
  const reinforce = t._ntf['reinforce'];
  reinforce('whoever', { synapsePeerId: BOUND });
  reinforce('whoever', { synapsePeerId: UNBOUND });
  check('bound synapse reinforced (inertia refreshed)', peer._node.synaptome.get(BOUND).inertia >= DOMAIN.simEpoch);
  check('unbound synapse NOT reinforced (stays evictable)', peer._node.synaptome.get(UNBOUND).inertia < DOMAIN.simEpoch);
}

async function testLocalProbeCapped() {
  console.log('\n── layer 7 (D-4): local_probe discloses a bounded sample ──');
  const t = bindingTransport();
  const peer = makePeer(t);
  peer._installRoutingHandlers();
  for (let i = 0; i < 30; i++) {
    const id = BigInt(0x100 + i);
    peer._node.synaptome.set(id, new Synapse({ peerId: id, latencyMs: 50, stratum: 1 }));
  }
  const out = await t._req['local_probe'](999n, {});
  check('local_probe returns at most LOCAL_PROBE_MAX (8)', Array.isArray(out) && out.length <= 8);
  check('full synaptome (30) is NOT disclosed', out.length < 30);
}

async function main() {
  console.log('B-3 eclipse prevention: verified admission + reinforce gate + probe cap');
  await testGossipDoesNotDirectlyAdmit();
  await testAlreadyBoundAdmitsViaVerifiedPath();
  await testProbeBudgetCaps();
  await testSimTransportPreservesDirectAdmit();
  await testReinforceBoundGate();
  await testLocalProbeCapped();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
