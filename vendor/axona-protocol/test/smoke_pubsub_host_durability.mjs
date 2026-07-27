// =====================================================================
// smoke_pubsub_host_durability.mjs — regression for the HOST cache-migration
// bug (kernel v4.8.0).
//
// A node that host()s a topic (durable participant, NOT an app subscriber)
// holds the feed. When the elected root dies and a DIFFERENT node becomes the
// fresh (empty) root, the host must migrate its cache UP to that new root so a
// brand-new late subscriber still recovers the full pre-death history.
//
// The bug: the hosted-topic re-announce in refreshTick used a raw SUB that
// omitted the `hw` high-water field (unlike _sendSubscribe), so the fresh root
// never learned the host held history → never issued PULLUP → the host's cache
// stayed STRANDED below an empty root and the history was effectively lost.
// Without the fix the post-death late subscriber gets 0.
//
// The existing smoke_pubsub_durability.mjs exercises the SUBSCRIBER-relay path
// (child relays renew via _sendSubscribe, which always carried hw) so it did
// not catch this. This smoke isolates the host() path.
//
// Run: node test/smoke_pubsub_host_durability.mjs
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  addNode(idBig) {
    const handlers = new Map(); const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, got: [] };
    am.onPubsubDelivery((_t, _j, msgId, ts) => rec.got.push({ msgId, ts }));
    this.nodes.set(idBig, rec);
    return rec;
  }
  kill(idBig) { const n = this.nodes.get(idBig); if (n) n.alive = false; }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) { if (!n.alive) continue; const d = id ^ target; if (bestD === null || d < bestD) { bestD = d; best = id; } }
    return best;
  }
  async settle(cap = 2_000_000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > cap) throw new Error('settle: did not converge');
      const j = this.queue.shift();
      const n = this.nodes.get(j.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(j.type);
      if (!h) continue;
      await h(j.payload, j.meta);
    }
  }
  async tickAll() { for (const n of this.nodes.values()) if (n.alive) await n.am.refreshTick(); await this.settle(); }
}
const cacheSize = (rec, topicBig) => (rec.am.axonRoles.get(topicBig)?.cache.length ?? 0);

// Build a real stamped cache entry from an envelope (mirrors _cachePush shape).
async function cacheEntry(desc, author, message, seq, ts) {
  const e = await buildEnvelope({ topic: desc, message, seq, identity: author, ts });
  const json = JSON.stringify(e);
  return { msgId: e.msgId, publishTs: ts, json, bytes: json.length + 80 };
}

async function main() {
  console.log('Axona pub/sub — HOST cache migrates to a freshly-promoted root (v4.8.0)');
  const author = await createAuthorIdentity();
  const M = 4;

  const fab = new Fabric();
  const desc = { region: 'useast', owner: null, name: 'host-durable', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);

  // newRoot is XOR-closest to the topic (so routed re-announces terminate at it);
  // host sits far away in keyspace so it is a relay, never the root.
  const newRoot = fab.addNode(topicId ^ 0x01n);
  const host    = fab.addNode(topicId ^ ((1n << 240n) | 0xABCDn));

  // Construct the exact stranded state the bug produced (seen live as
  // `relay(subs=0,cache=M)` after the original root departed): the host holds
  // the full feed in a relay role with NO downstream subscribers, is hosting
  // the topic, and is pinned upstream to the fresh (empty) root. This state is
  // renewed ONLY via the _hostedTopics loop in refreshTick — the path the fix
  // touches. (subs>0 would also hit the subscription-renewal loop, which always
  // carried hw; subs=0 is precisely what the bug stranded.)
  const hostRole = host.am._becomeRoot(topicId);   // make a local role…
  hostRole.isRoot = false;                          // …as a relay, not root
  for (let k = 0; k < M; k++) {
    host.am._cachePush(hostRole, await cacheEntry(desc, author, { k }, k + 1, fab.clock + k));
  }
  host.am._hostedTopics.add(topicId);
  host.am._upstream.set(topicId, [newRoot.id.toString(16).padStart(66, '0')]);

  // newRoot becomes the (empty) root for the topic.
  newRoot.am.pubsubSubscribe(topicId);
  await fab.settle();
  check('host is a relay holding the full feed, subs=0 (stranded-state precondition)',
    cacheSize(host, topicId) === M && host.am.axonRoles.get(topicId)?.subscribers.size === 0 && !host.am.axonRoles.get(topicId)?.isRoot,
    `(cache=${cacheSize(host, topicId)})`);
  check('newRoot is the elected root and starts EMPTY',
    fab._closestAlive(topicId) === newRoot.id && cacheSize(newRoot, topicId) === 0,
    `(cache=${cacheSize(newRoot, topicId)})`);

  // ── refreshTick: the host re-announces its hosted topic. WITH the fix this
  //    goes through _sendSubscribe and advertises hw=M → newRoot issues PULLUP
  //    → host REPLAYUPs its cache → newRoot adopts the full history. ──
  fab.clock += 61_000; await fab.tickAll();
  check('the fresh root recovered the full history via host PULLUP→REPLAYUP',
    cacheSize(newRoot, topicId) === M, `(new-root cache ${cacheSize(newRoot, topicId)}/${M}) — 0 means the host cache stayed stranded`);

  // the real consequence: a BRAND-NEW late subscriber recovers all M.
  const lateB = fab.addNode(topicId ^ 0x07n);
  lateB.am._lastSeenTsByTopic.set(topicId, 0);
  lateB.am.pubsubSubscribe(topicId);
  await fab.settle();
  check('post-recovery late subscriber recovers ALL pre-death history from the migrated cache',
    lateB.got.length === M, `(${lateB.got.length}/${M})`);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
