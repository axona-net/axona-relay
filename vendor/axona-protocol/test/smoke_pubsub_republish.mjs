// =====================================================================
// smoke_pubsub_republish.mjs — re-publishing identical content (same author +
// same message => same msgId) UPSERTS: it replaces the older cache entry
// (one entry per msgId, with a fresh hold) and delivers exactly once, rather
// than adding a duplicate + double-delivering.
//
// Guards the v3.3.0 change to _onPublish / _onPublishDirect. Before it, the
// live publish path deduped only on the random per-publish publishId, so a
// re-publish double-stored (current_count → 2) and double-delivered.
//
// Run: node test/smoke_pubsub_republish.mjs
// =====================================================================

import { AxonaPeer, AxonaDomain, NeuronNode, AxonaManager, Synapse,
         SimNetwork, simTransport, createNodeIdentity, createAuthorIdentity, clz264 }
  from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, ok) => { console.log(`  ${ok ? '✓' : '✗'} ${label}`); ok ? passed++ : failed++; };
const wait  = (ms) => new Promise(r => setTimeout(r, ms));
const REGION = { lat: 38, lng: -77 };

async function makePeer(network) {
  const id = await createNodeIdentity(REGION);
  const transport = simTransport({ network, identity: id, heartbeatMs: 0 });
  await transport.start(id.id);
  const node = new NeuronNode({ id: BigInt('0x' + id.id), lat: REGION.lat, lng: REGION.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: id, transport });
  await peer.start();
  const dht = {
    getSelfId:       () => peer.getNodeId(),
    findKClosest:    (...a) => peer.findKClosest(...a),
    routeMessage:    (...a) => peer.routeMessage(...a),
    sendDirect:      async (pid, t, p) => {
      if (pid === peer.getNodeId()) {
        const h = peer._directHandlers?.get(t); if (!h) return false;
        await h(p, { fromId: peer.getNodeId(), type: t }); return true;
      }
      return peer.sendDirect(pid, t, p);
    },
    onRoutedMessage: (t, h) => peer.onRoutedMessage(t, h),
    onDirectMessage: (t, h) => peer.onDirectMessage(t, h),
  };
  peer._axonaManager = new AxonaManager({ dht });
  return { peer, id };
}

function admit(localPeer, remoteHex) {
  const rb = BigInt('0x' + remoteHex);
  const syn = new Synapse({ peerId: rb, latencyMs: 1, stratum: clz264(localPeer._node.id ^ rb) });
  syn.weight = 0.5; syn.inertia = 0; syn._addedBy = 'test';
  localPeer._node.synaptome.set(rb, syn);
}

async function main() {
  console.log('Axona re-publish upsert smoke');
  const net = new SimNetwork();
  const A = await makePeer(net), B = await makePeer(net);
  const author = await createAuthorIdentity();
  await A.peer._transport.openConnection(B.id.id);
  admit(A.peer, B.id.id); admit(B.peer, A.id.id);
  await wait(50);

  const topic = { region: 'useast', name: 'republish-smoke' };
  const got = [];
  await B.peer.sub(topic, (e) => { if (e && 'message' in e) got.push(e.msgId); }, { since: 'all' });
  await wait(80);

  const id1 = await A.peer.pub(topic, 'same message', { signWith: author });
  await wait(120);
  const id2 = await A.peer.pub(topic, 'same message', { signWith: author });   // identical content
  await wait(200);

  check('re-publish yields the same msgId', id1 === id2);
  check('delivered exactly once (no double-delivery)', got.length === 1);
  check('delivered msgId matches', got[0] === id1);

  // Inspect A's local replay cache directly. (peer.metrics() is now owner-only;
  // this is an OPEN topic, so we read the rooting peer's own state via the
  // local, network-free rootedTopics() inspector instead.)
  const liveCount = () =>
    (A.peer.rootedTopics().find(t => t.descriptor?.name === 'republish-smoke')?.current_count ?? 0);
  check('replay cache holds ONE entry (older replaced, not duplicated)', liveCount() === 1);

  // Sanity: a genuinely different message is still a distinct, delivered entry.
  const id3 = await A.peer.pub(topic, 'a different message', { signWith: author });
  await wait(200);
  check('distinct content → distinct msgId', id3 !== id1);
  check('distinct content delivered', got.includes(id3));
  check('cache now holds two distinct messages', liveCount() === 2);

  // Direct upsert regression: _addToReplayCache must hold ONE entry per
  // postHash no matter which ingress path adds it. The single-root sim above
  // can't reach the multi-path case (a node that is BOTH a root and a child
  // re-caches the re-publish via _onDeliver), so assert the centralized upsert
  // directly — this is the invariant that keeps current_count at 1 on the live
  // multi-root mesh.
  const mgr  = A.peer._axonaManager;
  const role = { replayCache: [] };
  mgr._addToReplayCache(role, { json: '{"seq":1,"ts":1}', publishId: 'p1', publishTs: 1, postHash: 'HASH_X' });
  mgr._addToReplayCache(role, { json: '{"seq":2,"ts":2}', publishId: 'p2', publishTs: 2, postHash: 'HASH_X' }); // re-cache via another path
  check('_addToReplayCache upserts by postHash (one entry, any path)', role.replayCache.length === 1);
  mgr._addToReplayCache(role, { json: '{"seq":3,"ts":3}', publishId: 'p3', publishTs: 3, postHash: 'HASH_Y' });
  check('distinct postHash still adds a second entry', role.replayCache.length === 2);

  // postHash-absent path (e.g. a deliver frame that omits it): the content id
  // must be backfilled from the envelope's msgId so the upsert still collapses
  // a re-cache — the bug that left keyspace-hosting roots holding two copies.
  const role2 = { replayCache: [] };
  const envJson = JSON.stringify({ msgId: 'MID_Z', seq: 5, ts: 5, message: 'hi' });
  mgr._addToReplayCache(role2, { json: envJson, publishId: 'q1', publishTs: 5 });               // no postHash
  mgr._addToReplayCache(role2, { json: envJson, publishId: 'q2', publishTs: 5 });               // re-cache, still no postHash
  check('postHash backfilled from envelope msgId → upsert (one entry)', role2.replayCache.length === 1);
  check('backfilled entry carries the msgId as postHash', role2.replayCache[0].postHash === 'MID_Z');

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch(e => { console.error('smoke threw:', e); process.exit(2); });
