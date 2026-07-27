// =====================================================================
// smoke_default_adapter_lookup.mjs — the default dht adapter's lookup()
// conforms to the LookupResult consumer contract (task #354).
//
// Field capture (prod, 2026-07-18, warm-topic live-delivery gap): the
// standalone adapter's lookup returned the bare closest id, but every
// consumer reads `r.path` — rootElection._rootHint_'s self-closest escape,
// _verifyRoots, repairPlane._emptyRootProbe, pubsubLeaveHandoff — so root
// self-verification and the iterative strand-escape were silent no-ops on
// EVERY standalone peer (browser connect(), relay scripts, MCP). A
// spurious/interloper root claim was never demoted; split cohorts never
// self-healed.
//
// This smoke proves, over a real mini-mesh (SimTransport):
//   1. am.dht.lookup(target) resolves { found:true, path:[…], hops } with
//      path a non-empty array and the TERMINUS (path tail) = the XOR-closest
//      live node — exactly the read pattern the consumers use
//   2. non-bigint input → null (unchanged guard)
//   3. END-TO-END: a peer holding a WRONG root claim (a strictly closer
//      live node exists) self-verifies via the periodic tick and DEMOTES —
//      the healing #354 restores
//
// Run: node test/smoke_default_adapter_lookup.mjs
// =====================================================================
import { AxonaPeer }                from '../src/dht/AxonaPeer.js';
import { AxonaDomain }              from '../src/dht/AxonaDomain.js';
import { NeuronNode }               from '../src/dht/NeuronNode.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }       from '../src/identity/index.js';
import { fromHex }                  from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => {
  console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`);
  ok ? passed++ : failed++;
};
const wait = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(network, domain, lat, lng) {
  const id = await createNodeIdentity({ lat, lng });
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: fromHex(id.id), lat, lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain, node, nodeIdentity: id, transport });
  await peer.start();
  return { peer, id, node, transport };
}

async function main() {
  const net = new SimNetwork();
  const domain = new AxonaDomain();
  const peers = [];
  for (let i = 0; i < 6; i++) {
    peers.push(await makePeer(net, domain, (i * 23) % 80 - 40, (i * 61) % 360 - 180));
  }
  // Full mesh so findKClosest probes can traverse.
  for (let i = 0; i < peers.length; i++) {
    for (let j = i + 1; j < peers.length; j++) {
      await peers[i].transport.openConnection(peers[j].id.id);
    }
  }
  await wait(200);

  console.log('\n── 1. LookupResult shape + terminus = XOR-closest ──');
  const a = peers[0];
  const am = a.peer._requireAxonaManager('smoke');
  // Target: an id adjacent to peer[3]'s, so the globally-closest is known.
  const targetOwner = peers[3];
  const target = fromHex(targetOwner.id.id) ^ 1n;
  const r = await am.dht.lookup(target);
  check('lookup resolves an object', !!r && typeof r === 'object', `got ${typeof r}`);
  check('r.path is a non-empty array (the consumer gate)', !!r && Array.isArray(r.path) && r.path.length > 0);
  check('r.found is true', !!r && r.found === true);
  check('r.hops = path.length - 1', !!r && r.hops === r.path.length - 1);
  const terminus = r && Array.isArray(r.path) && r.path.length ? r.path[r.path.length - 1] : null;
  const ids = peers.map(p => fromHex(p.id.id));
  const trueClosest = ids.slice().sort((x, y) => (x ^ target) < (y ^ target) ? -1 : 1)[0];
  check('terminus (path tail) is the XOR-closest live node', terminus === trueClosest,
    `terminus ${terminus?.toString(16).slice(0, 10)} vs closest ${trueClosest.toString(16).slice(0, 10)}`);
  check('every path entry is a bigint id', !!r && r.path.every(p => typeof p === 'bigint'));

  console.log('\n── 2. invalid input stays null ──');
  check('lookup(non-bigint) → null', (await am.dht.lookup('nope')) === null);

  console.log('\n── 3. wrong root claim self-verifies and demotes ──');
  // Peer FAR from the target claims root while the true closest (peers[3]) is
  // alive and reachable. Before #354 the verify lookup resolved null forever
  // and the claim stuck; now the tick's _verifyRoots must demote it.
  const wrong = peers.reduce((w, p) => {
    const d = fromHex(p.id.id) ^ target;
    return (w === null || d > (fromHex(w.id.id) ^ target)) ? p : w;
  }, null);
  const wam = wrong.peer._requireAxonaManager('smoke');
  wam._becomeRoot(target, 'smoke-wrong-claim');
  const claim = wam.axonRoles.get(target);
  check('claim formed', claim?.isRoot === true);
  // The claim must HOLD CACHE to model the interloper (an empty claim is
  // swept by the idle eviction before the verify timer fires — that sweep is
  // itself correct behavior, but it's not the case #354 is about).
  wam._cachePush(claim, { msgId: 'ab'.repeat(32), publishTs: Date.now(), json: '{"smoke":1}', seq: 1 });
  // ROOT_VERIFY_FIRST_MS = 6s, tick = 5s → the check fires by ~11s.
  let demoted = false;
  for (let i = 0; i < 30 && !demoted; i++) {
    await wait(500);
    const role = wam.axonRoles.get(target);
    demoted = !role || role.isRoot === false;   // demoted (or demoted then idle-swept)
  }
  check('spurious claim demoted by root self-verification', demoted);
  const beacon = wam._rootBeacons.get(target);
  check('verified root pointer seeded toward the true closest', !!beacon && beacon.verified === true);

  for (const p of peers) { try { await p.peer.stop?.(); } catch { /* */ } }
  console.log(`\n${failed === 0 ? 'PASS' : 'FAIL'}  (${passed} passed, ${failed} failed)`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
