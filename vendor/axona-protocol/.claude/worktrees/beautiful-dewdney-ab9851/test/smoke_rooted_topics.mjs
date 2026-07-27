// =====================================================================
// smoke_rooted_topics.mjs — AxonaManager.rootedTopics() (routing-only 4.x).
//
// The PRODUCER side of the derived metric-topic convention: a root enumerates the
// topics it serves, each with a locally-computed snapshot (no network), so an infra
// node republishes them to metricTopic(T). rootedTopics() was dropped in the v3.12
// clean break (silently killing all 4.x metrics) and re-added in v4.10.1 with the
// message counter (seq) + cache count.
//
//   1. a rooted topic is enumerated with descriptor recovered from the cached envelope
//   2. current_count = messages currently in cache
//   3. seq = the root's dense message counter (high-water)   ← v4.10.1
//   4. subscribers = this member's subscriber count; bytes = cached bytes
//   5. a non-root role is NOT enumerated
//   6. a role with no cached envelope → descriptor:null (caller skips it)
//
//   node test/smoke_rooted_topics.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const REG = 0x87n << 248n, idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x11n, OPEN = REG | 0xa1n, OWNED = REG | 0xb2n, EMPTY = REG | 0xc3n, NOTROOT = REG | 0xd4n;

function mk() {
  const dht = { getSelfId: () => SELF, onRoutedMessage: () => {}, routeMessage: () => {}, neighbors: () => [], bridgeId: () => null };
  const am = new AxonaManager({ dht, now: () => 1_700_000_000_000 }); am.nodeId = SELF;
  return am;
}
// push a cache entry whose json carries a topic descriptor (rootedTopics JSON.parses .topic)
const envJson = (desc, i) => JSON.stringify({ msgId: 'm' + i, topic: desc, message: 'x' + i });
function seedRoot(am, topicBig, desc, { count = 1, seq = 0, subs = 0 } = {}) {
  const role = am._becomeRoot(topicBig);
  for (let i = 0; i < count; i++) { const j = envJson(desc, i); role.cache.push({ msgId: 'm' + i, publishTs: 100 + i, json: j, seq: i + 1, bytes: j.length }); role.cacheIds.add('m' + i); role.cacheBytes += j.length; }
  role.seq = seq;
  for (let i = 0; i < subs; i++) role.subscribers.set('s' + i, { since: 0, lastRenewed: 0 });
  return role;
}

const am = mk();
seedRoot(am, OPEN,  { region: 'useast', name: 'lobby', write: 'open' }, { count: 3, seq: 7, subs: 2 });
seedRoot(am, OWNED, { region: 'useast', owner: 'aa', name: 'feed', write: 'owner' }, { count: 1, seq: 1, subs: 0 });
// empty root: is root, subscribers, but no cache
{ const r = am._becomeRoot(EMPTY); r.subscribers.set('s0', { since: 0, lastRenewed: 0 }); }
// a NON-root role (backup) must not be enumerated
{ const r = am._becomeRoot(NOTROOT); r.isRoot = false; r.cache.push({ msgId: 'z', publishTs: 1, json: envJson({ region: 'useast', name: 'z' }, 0), seq: 1, bytes: 10 }); }

const rooted = am.rootedTopics();
const byId = (b) => rooted.find(r => r.topicId === idHex(b));

const L = byId(OPEN);
ok('rooted open topic enumerated', !!L);
ok('current_count = messages in cache (3)', L?.current_count === 3);
ok('seq = the message counter (7)', L?.seq === 7);
ok('subscribers = 2', L?.subscribers === 2);
ok('bytes > 0 (cached envelope bytes)', typeof L?.bytes === 'number' && L.bytes > 0);
ok('descriptor recovered from cached envelope (name)', L?.descriptor?.name === 'lobby');
ok('descriptor recovered (write:open)', L?.descriptor?.write === 'open');

const O = byId(OWNED);
ok('owned topic enumerated + descriptor write:owner + owner set', O?.descriptor?.write === 'owner' && O?.descriptor?.owner === 'aa');

const E = byId(EMPTY);
ok('empty root enumerated', !!E);
ok('empty root current_count = 0', E?.current_count === 0);
ok('empty root descriptor = null (no envelope to recover → caller skips)', E?.descriptor === null);

ok('non-root role is NOT enumerated', !byId(NOTROOT));

console.log(`\n${fail ? '✗' : '✓'} smoke_rooted_topics: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
