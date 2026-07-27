// =====================================================================
// smoke_pull_envelope.mjs — peer.pull() returns the FULL envelope over the
// REAL wire path (task #355).
//
// smoke_pull_metrics.js exercises pull against a mock AxonaManager that
// honored the documented contract ("@returns envelope or null") — but the
// real PULLRESP handler unwrapped `parsed.message ?? parsed`, discarding
// msgId/ts/signer at the last step. Every publish-confirm loop comparing
// env.msgId, and every pull-then-act flow (kill / reply / verify by msgId),
// silently failed on live networks while the mock-backed smoke stayed green.
//
// This smoke drives pub → pull across two real peers (SimTransport mini-mesh,
// default adapter, real _onPull/_onPullResp wire handlers) and asserts the
// puller receives the same envelope shape a sub() callback delivers:
//   msgId (the id pub() returned), ts, message, signature — for both
//   pull-by-msgId and pull-latest (null msgId).
//
// Run: node test/smoke_pull_envelope.mjs
// =====================================================================

import {
  AxonaPeer, AxonaDomain, NeuronNode, Synapse, SimNetwork, simTransport,
  createNodeIdentity, createAuthorIdentity, clz264,
} from '../src/index.js';

const wait = (ms) => new Promise(r => setTimeout(r, ms));
let passed = 0, failed = 0;
const check = (l, c, extra = '') => {
  console.log(`  ${c ? '✓' : '✗'} ${l}${c ? '' : '  ' + extra}`);
  c ? passed++ : failed++;
};

const LAT = 38, LNG = -77;
async function makePeer(network, domain) {
  const identity = await createNodeIdentity({ lat: LAT, lng: LNG });
  const transport = simTransport({ network, identity, heartbeatMs: 0 });
  await transport.start(identity.id);
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: LAT, lng: LNG });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });
  await peer.start();
  peer._requireAxonaManager?.('pull-envelope-smoke');
  return { peer, node, hex: identity.id, big: node.id };
}

const network = new SimNetwork();
const domain = new AxonaDomain();
const peers = [];
for (let i = 0; i < 4; i++) peers.push(await makePeer(network, domain));
for (let i = 0; i < peers.length; i++) {
  for (let j = i + 1; j < peers.length; j++) {
    const a = peers[i], b = peers[j];
    const s1 = new Synapse({ peerId: b.big, latencyMs: 1, stratum: clz264(a.big ^ b.big) });
    s1.weight = 0.5; s1.inertia = 0; a.node.synaptome.set(b.big, s1);
    const s2 = new Synapse({ peerId: a.big, latencyMs: 1, stratum: clz264(b.big ^ a.big) });
    s2.weight = 0.5; s2.inertia = 0; b.node.synaptome.set(a.big, s2);
    await a.peer._transport.openConnection(b.hex);
  }
}
await wait(150);

const topic = { region: 'useast', name: `pull-envelope-${Math.floor(Math.random() * 1e9)}` };
const author = await createAuthorIdentity();
const publisher = peers[0];
const msgId = await publisher.peer.pub(topic, { hello: 'envelope' }, { signWith: author });
await wait(400);

console.log('\n── pull-by-msgId from a DIFFERENT peer (real wire path) ──');
const puller = peers[3];
const env = await puller.peer.pull(msgId, { topic, timeoutMs: 3000 });
check('pull resolved non-null', env !== null);
check('envelope.msgId matches what pub() returned', env?.msgId === msgId, `got ${env?.msgId}`);
check('envelope.ts is a number', typeof env?.ts === 'number');
check('envelope.message is the published body', env?.message?.hello === 'envelope',
  `got ${JSON.stringify(env?.message ?? env)}`);
check('envelope carries the signature', typeof env?.signature === 'string' && env.signature.length > 0);

console.log('\n── pull-latest (null msgId) has the same shape ──');
const latest = await puller.peer.pull(null, { topic, timeoutMs: 3000 });
check('pull-latest resolved non-null', latest !== null);
check('pull-latest envelope.msgId matches', latest?.msgId === msgId);
check('pull-latest message matches', latest?.message?.hello === 'envelope');

console.log('\n── the confirm-loop pattern (#355 regression) now works ──');
const confirmed = !!latest && (latest.msgId === msgId || latest?.message?.hello === 'envelope');
check('publish-confirm comparison succeeds', confirmed === true);

for (const p of peers) { try { await p.peer.stop?.(); } catch { /* */ } }
console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'}  (${passed} passed, ${failed} failed)`);
process.exit(failed === 0 ? 0 : 1);
