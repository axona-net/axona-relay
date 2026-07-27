// smoke_pubsub_recruit_wiring.mjs — PARITY GUARDRAIL.
//
// The axon tree's scalability depends on sub-axon recruitment using BATCH
// ADOPTION (pickRelayPeer: pick a relay XOR-closest to the new subscriber from
// the whole synaptome). The fallback (promote an existing child one-at-a-time)
// degenerates into a deep near-linear chain as a topic grows — measured in
// dht-sim at depth ~21 / 600 subscribers vs ~4 with batch adoption.
//
// This drifted silently once already: AxonaPeer DEFINED _pickRelayPeer but never
// passed it to the AxonaManager it builds, so production ran the deep-chain
// fallback while the simulator's membership test ran with pickRelayPeer ON — so
// the sim validated a tree shape production never used. This test pins the
// invariant: the manager a production-shaped AxonaPeer builds MUST have
// pickRelayPeer wired. If it fails, recruitment has regressed to the deep-chain
// fallback — re-wire it in AxonaPeer._buildDefaultManager.

import {
  AxonaPeer, AxonaDomain, NeuronNode, SimNetwork, simTransport, createNodeIdentity,
} from '../src/index.js';

let n = 0, fail = 0;
const ok = (name, cond) => { n++; if (!cond) { fail++; console.error('FAIL', name); } else console.log('ok', name); };

const net      = new SimNetwork();
const identity = await createNodeIdentity({ lat: 38.0, lng: -77.0 });
const transport = simTransport({ network: net, identity, heartbeatMs: 0 });
await transport.start(identity.id);
const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: 38.0, lng: -77.0 });
node.transport = transport;

// Production-shaped construction: exactly how axona-peer / axona-relay build it
// (no axonaManager, no pickRelayPeer passed in — the kernel must wire it itself).
const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: identity, transport });
await peer.start();

const am = peer._requireAxonaManager('guardrail');
ok('default AxonaManager builds', !!am);
ok('pickRelayPeer wired on the DEFAULT manager (batch adoption, not deep-chain fallback)',
   typeof am.pickRelayPeer === 'function');
ok('shouldRecruitSubAxon present (recruitment fires past maxDirectSubs)',
   typeof am.shouldRecruitSubAxon === 'function');
ok('maxDirectSubs is a positive bound (per-axon fan-out cap)',
   Number.isFinite(am.maxDirectSubs) && am.maxDirectSubs > 0);

console.log(fail ? `\n✗ ${fail}/${n} FAILED` : `\n✓ all ${n} passed`);
process.exit(fail ? 1 : 0);
