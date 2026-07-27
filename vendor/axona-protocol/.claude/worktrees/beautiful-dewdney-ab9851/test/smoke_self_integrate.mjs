// =====================================================================
// smoke_self_integrate.mjs — join() self-integration + SimTransport bind events.
//
// A node joining via a single sponsor must WEAVE ITSELF IN: discover its own
// neighbourhood (findKClosest(ownId)) and open authenticated channels so the
// neighbours ADOPT it (reachability lives in their tables, not the joiner's).
// Two layers:
//   1. SimTransport fires onPeerBound on BOTH ends of an open (contract parity
//      with web/node) → the kernel's auto-admit flow runs in the sim.
//   2. peer.join(sponsor) → _selfIntegrate: after join the peer holds MORE than
//      the sponsor, AND a non-sponsor neighbour now holds the joiner.
//
// Run: node test/smoke_self_integrate.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex, toHex }           from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok) => { console.log(`  ${ok ? '✓' : '✗'} ${label}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(network, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, transport, node };
}

// ── 1. SimTransport bind events fire on both ends ────────────────────────
async function testBindEvents() {
  console.log('\n── SimTransport fires onPeerBound on both ends ──');
  const net = new SimNetwork();
  const domain = new AxonaDomain();
  const a = await makePeer(net, domain, 51, -0.1);
  const b = await makePeer(net, domain, 35, 139);
  const aSaw = [], bSaw = [];
  a.transport.onPeerBound((big) => aSaw.push(big));
  b.transport.onPeerBound((big) => bSaw.push(big));
  await a.transport.openConnection(b.id.id);
  check('opener saw the target bind', aSaw.some(x => x === fromHex(b.id.id)));
  check('target saw the opener bind', bSaw.some(x => x === fromHex(a.id.id)));
  check('boundPeers() reports the open channel', a.transport.boundPeers().some(x => x === fromHex(b.id.id)));
}

// ── 2. join(sponsor) self-integrates into a real mini-mesh ───────────────
async function testSelfIntegrate() {
  console.log('\n── join(sponsor) weaves the joiner in (findKClosest + adopt) ──');
  const net = new SimNetwork();
  const domain = new AxonaDomain();
  // 8 base peers, fully meshed (openConnection auto-seeds via onPeerBound).
  const base = [];
  for (let i = 0; i < 8; i++) base.push(await makePeer(net, domain, (i * 17) % 80 - 40, (i * 53) % 360 - 180));
  for (let i = 0; i < base.length; i++)
    for (let j = i + 1; j < base.length; j++)
      await base[i].transport.openConnection(base[j].id.id);
  await wait(20);
  check('base mesh wired (peer 0 knows several)', base[0].node.synaptome.size >= 3);

  // Newcomer joins via ONE sponsor (base[0]) — the production join shape.
  const joiner = await makePeer(net, domain, 10, 20);
  const sponsorHex = base[0].id.id;
  await joiner.peer.join(sponsorHex);
  await wait(20);

  const joinerBig = fromHex(joiner.id.id);
  const sponsorBig = fromHex(sponsorHex);

  check('joiner holds MORE than just the sponsor (self-integrated)',
    joiner.node.synaptome.size > 1);

  // Reachability: at least one NON-SPONSOR base peer now holds the joiner
  // (adopted it via the bind flow when the joiner opened a channel).
  const adopters = base.filter(b => b.node.id !== sponsorBig && b.node.synaptome.has(joinerBig));
  check('a non-sponsor neighbour adopted the joiner (reachable)', adopters.length >= 1);

  // And the joiner is routable: some base peer can route a packet to it.
  let reached = false;
  joiner.peer.onRoutedMessage('selfint_probe', (_p, meta) => (meta.targetId === joinerBig ? (reached = true, 'consumed') : null));
  const r = await base[1].peer.routeMessage(joinerBig, 'selfint_probe', {});
  check('a base peer routes a packet to the joiner', !!(r && r.consumed && r.atNode === joinerBig));
}

async function main() {
  console.log('Axona self-integration smoke (v4.7.0)');
  await testBindEvents();
  await testSelfIntegrate();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
