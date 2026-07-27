// smoke_connect.mjs — the one-call bootstrap (v4.16.0).
//
//   const { peer, author } = await connect({ bridge, location });
//
// connect() collapses the identity/transport/node/domain/peer assembly and
// the start + ready lifecycle into one call. These tests exercise it over an
// injected sim transport (no bridge needed): return shape, author-handling
// variants, argument validation, ready() integration, a two-peer pub/sub
// roundtrip between connect()-built peers, and disconnect().
//
// Run: node test/smoke_connect.mjs
import { connect } from '../src/connect.js';
import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { Synapse } from '../src/dht/Synapse.js';
import { clz264 } from '../src/utils/hexid.js';

let n = 0, fail = 0;
const ok = (m, c) => { console.log(`  ${c ? '✓' : '✗'} ${m}`); c ? n++ : fail++; };
const HERE = { lat: 38.0, lng: -77.0 };

const network = new SimNetwork();
async function simPeer(opts = {}) {
  const nodeIdentity = await createNodeIdentity(HERE);
  const transport = simTransport({ network, identity: nodeIdentity, heartbeatMs: 0 });
  return connect({ transport, nodeIdentity, location: HERE, ready: false, ...opts });
}
// Admit b into a's synaptome (the sim shortcut real transports do via hello).
function admit(a, b) {
  const remote = b.peer._node.id;
  const syn = new Synapse({ peerId: remote, latencyMs: 1, stratum: clz264(a.peer._node.id ^ remote) });
  syn._addedBy = 'smoke';
  a.peer._node.synaptome.set(remote, syn);
}

// ── 1. Return shape ──────────────────────────────────────────────────
{
  console.log('\n── return shape ──');
  const c = await simPeer();
  ok('peer is started (has nodeId)', typeof c.peer?.getNodeId() === 'string' || typeof c.peer?.getNodeId() === 'bigint');
  ok('nodeIdentity present', typeof c.nodeIdentity?.id === 'string');
  ok('author minted by default (ephemeral)', typeof c.author?.authorId === 'string');
  ok('author has a sign()', typeof c.author?.sign === 'function');
  ok('transport surfaced', !!c.transport);
  ok('status is null when ready:false', c.status === null);
  ok('disconnect is a function', typeof c.disconnect === 'function');
  await c.disconnect();
  ok('disconnect() resolves', true);
}

// ── 2. Author variants ───────────────────────────────────────────────
{
  console.log('\n── author variants ──');
  const none = await simPeer({ author: false });
  ok('author:false → null', none.author === null);
  await none.disconnect();

  const mine = await createAuthorIdentity();
  const pass = await simPeer({ author: mine });
  ok('author:<identity> passes through unchanged', pass.author === mine);
  await pass.disconnect();
}

// ── 3. Validation ────────────────────────────────────────────────────
{
  console.log('\n── argument validation ──');
  let threw = false;
  try { await connect({}); } catch (e) { threw = e instanceof TypeError; }
  ok('no location + no nodeIdentity → TypeError', threw);

  threw = false;
  try { await connect({ location: HERE }); } catch (e) { threw = e instanceof TypeError; }
  ok('no bridge + no transport → TypeError', threw);
}

// ── 4. ready() integration + two-peer roundtrip ──────────────────────
{
  console.log('\n── ready() + pub/sub roundtrip between connect()-built peers ──');
  const a = await simPeer();
  const b = await simPeer();
  await a.transport.openConnection(b.nodeIdentity.id);
  admit(a, b); admit(b, a);

  const status = await a.peer.ready({ minPeers: 1, timeoutMs: 2000, pollMs: 20 });
  ok('ready() resolves ready:true once a peer is admitted', status.ready === true && status.peers >= 1);

  const TOPIC = { region: 'useast', name: 'connect-smoke' };
  const got = [];
  const sub = await b.peer.sub(TOPIC, (env) => got.push(env), { since: 'all' });
  await new Promise(r => setTimeout(r, 100));
  await a.peer.pub(TOPIC, 'hello from connect()', { signWith: a.author });
  await new Promise(r => setTimeout(r, 300));
  ok('message delivered a → b', got.length >= 1 && got[0].message === 'hello from connect()');
  ok('envelope signed by the connect()-minted author', got[0]?.signerPubkey === a.author.authorId);

  await sub.stop();
  await a.disconnect(); await b.disconnect();
}

console.log(`\n${fail ? '✗' : '✓'} smoke_connect: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
