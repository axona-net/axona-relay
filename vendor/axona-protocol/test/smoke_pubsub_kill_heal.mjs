// =====================================================================
// smoke_pubsub_kill_heal.mjs — persistent, confirmation-gated publish/kill
// retry under packet loss (v4.8.6).
//
// A routed PUB/KILL is one-shot fire-and-forget. Under loss the initial send +
// a single heal both dropping = the message never reaches the root and is lost
// for EVERYONE (the ~1/3 publish-strand under 10-30% loss; the ~30% "kill not
// received" flake). Fix: retain the publish/kill keyed by msgId and RE-SEND it
// toward the current root hint each refreshTick until the publisher observes its
// own msgId (implicit ACK → _confirmPending) or maxTries/TTL — idempotent (the
// root dedups by msgId). Two quick publishes to the same topic each retry
// independently (msgId-keyed, not topic-keyed).
//
// Run: node test/smoke_pubsub_kill_heal.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');

function mkManager({ closest }) {
  const selfId = 0x89n << 248n | 0x11n;
  const sends = [];
  const dht = {
    getSelfId: () => selfId,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => { sends.push({ target, type, payload }); },
    neighbors: () => [],
    async findKClosest() { return closest != null ? [closest] : []; },
  };
  const am = new AxonaManager({ dht, now: () => Date.now(), renewMs: 60_000, renewFastMs: 5_000, dropMs: 180_000 });
  am.nodeId = selfId;
  return { am, sends, selfId };
}
const byType = (sends, t) => sends.filter(s => String(s.type).includes(t));
const root = 0x89n << 248n | 0xabcdn;

// ── 1. warm hint → publish + kill carry via=[root] ──────────────────
{
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0xbeefn;
  await am.warmRootHint(topic, 1000);
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'p1', message: 1 }));
  am.pubsubKill(topic, { msgId: 'k1' });
  const p = byType(sends, 'pub'), k = byType(sends, 'kill');
  ok('warm publish carries via=[root]', p[0]?.payload.via?.[0] === idHex(root));
  ok('warm kill carries via=[root]',    k[0]?.payload.via?.[0] === idHex(root));
}

// ── 2. two quick publishes to the SAME topic are BOTH retained (msgId-keyed) ──
{
  const { am } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0x5678n;
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'a', message: 1 }));
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'b', message: 2 }));
  ok('both publishes retained independently (msgId-keyed, no overwrite)',
     am._pendingPub.has('a') && am._pendingPub.has('b'));
}

// ── 3. refreshTick RE-SENDS pending publish/kill (persistent, not one-shot) ──
{
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0x9999n;
  await am.warmRootHint(topic, 1000);
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'p', message: 1 }));
  am.pubsubKill(topic, { msgId: 'k' });
  const before = sends.length;
  await am.refreshTick();
  await am.refreshTick();
  ok('refreshTick re-sends the pending publish (>1 PUB total)', byType(sends, 'pub').length >= 2);
  ok('refreshTick re-sends the pending kill (>1 KILL total)',   byType(sends, 'kill').length >= 2);
  ok('re-sends went out after the initial send', sends.length > before);
}

// ── 4. _confirmPending (implicit ACK) stops the retry ───────────────
{
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0x1111n;
  await am.warmRootHint(topic, 1000);
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'p', message: 1 }));
  am._confirmPending(topic, 'p');                    // publisher observed its own msg
  ok('confirmed publish removed from _pendingPub', !am._pendingPub.has('p'));
  const after = byType(sends, 'pub').length;
  await am.refreshTick(); await am.refreshTick();
  ok('no further re-sends after confirmation', byType(sends, 'pub').length === after);
}

// ── 5. maxTries bounds an un-confirmed publish (no unbounded re-send) ──
{
  const { am, sends } = mkManager({ closest: root });
  const topic = 0x89n << 248n | 0x2222n;
  await am.warmRootHint(topic, 1000);
  am.pubsubPublish(topic, JSON.stringify({ msgId: 'p', message: 1 }));
  for (let i = 0; i < 20; i++) await am.refreshTick();   // never confirmed
  ok('un-confirmed publish eventually dropped (maxTries bound)', !am._pendingPub.has('p'));
  ok('total PUB sends bounded (initial + ≤maxTries)', byType(sends, 'pub').length <= 1 + 6);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_pubsub_kill_heal: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
