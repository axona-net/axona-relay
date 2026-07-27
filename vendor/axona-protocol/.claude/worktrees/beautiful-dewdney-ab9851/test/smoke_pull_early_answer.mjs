// smoke_pull_early_answer.mjs — cache-hit early-answer for pull (v4.11.1).
//
// A pull for a SPECIFIC message (postHash) is a read of immutable replicated state:
// any node en route that holds it in cache can answer, without reaching the root.
//   1. by-msgId at a NON-terminal node that HOLDS it → answers (PULLRESP), stops the walk
//   2. by-msgId at a node that does NOT hold it → forwards (no answer)
//   3. pull-LATEST (no postHash) at a non-terminal node → does NOT early-answer (forwards)
//   4. pull-LATEST at the terminus → answers with the newest cache entry
//   5. by-msgId at the terminus that holds it → answers (terminus path)
//
// Run: node test/smoke_pull_early_answer.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const T_PULLRESP = 'pubsub:pullresp';
const REG = 0x87n << 248n;
const idHex = (b) => b.toString(16).padStart(66, '0');

const SELF = REG | 0x11n, TOPIC = REG | 0xabcn, REQ = REG | 0x99n;
const MSGID = 'a'.repeat(64), OTHER = 'b'.repeat(64);

function mk({ seedCache = true } = {}) {
  const sent = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => { sent.push({ target, type, payload }); },
    neighbors: () => [], bridgeId: () => null, findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht, now: () => Date.now() });
  am.nodeId = SELF;
  // Seed a role holding two cached messages (NOT marked root — an en-route replica).
  const role = am._becomeRoot(TOPIC);
  role.isRoot = false;
  if (seedCache) {
    role.cache.push({ msgId: OTHER, publishTs: 100, json: JSON.stringify({ msgId: OTHER, message: 'older' }), seq: 1 });
    role.cache.push({ msgId: MSGID, publishTs: 200, json: JSON.stringify({ msgId: MSGID, message: 'target' }), seq: 2 });
  }
  return { am, sent };
}

const pullResps = (sent) => sent.filter(s => s.type === T_PULLRESP);

// 1. by-msgId at a NON-terminal replica that holds it → early-answer
{
  const { am, sent } = mk();
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: MSGID, corrId: 'c1', requesterId: idHex(REQ), via: [] }, { isTerminal: false });
  const r = pullResps(sent);
  ok('non-terminal holder answers a by-msgId pull', ret === 'consumed' && r.length === 1, `(ret=${ret}, resps=${r.length})`);
  ok('answer carries the requested message json', r.length === 1 && JSON.parse(r[0].payload.json)?.message === 'target');
  ok('answer is routed to the requester', r.length === 1 && r[0].target === REQ);
}

// 2. by-msgId at a node that does NOT hold it → forward (no answer)
{
  const { am, sent } = mk({ seedCache: false });
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: MSGID, corrId: 'c2', requesterId: idHex(REQ), via: [] }, { isTerminal: false });
  ok('non-holder forwards a by-msgId pull (no early-answer)', ret === undefined && pullResps(sent).length === 0, `(ret=${ret}, resps=${pullResps(sent).length})`);
}

// 3. pull-LATEST at a NON-terminal replica that HOLDS cache → early-answers with its newest (v4.11.2)
{
  const { am, sent } = mk();
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: null, corrId: 'c3', requesterId: idHex(REQ), via: [] }, { isTerminal: false });
  const r = pullResps(sent);
  ok('pull-latest early-answers at a non-terminal replica with its newest',
    ret === 'consumed' && r.length === 1 && JSON.parse(r[0].payload.json)?.message === 'target', `(resps=${r.length})`);
}

// 3b. pull-LATEST at a role-holder with EMPTY cache → forwards (nothing on hand)
{
  const { am, sent } = mk({ seedCache: false });
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: null, corrId: 'c3b', requesterId: idHex(REQ), via: [] }, { isTerminal: false });
  ok('pull-latest does NOT early-answer from an empty replica (forwards)', ret === undefined && pullResps(sent).length === 0, `(ret=${ret}, resps=${pullResps(sent).length})`);
}

// 4. pull-LATEST at the terminus → answers with the newest entry
{
  const { am, sent } = mk();
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: null, corrId: 'c4', requesterId: idHex(REQ), via: [] }, { isTerminal: true });
  const r = pullResps(sent);
  ok('pull-latest at the terminus answers with the newest cache entry',
    ret === 'consumed' && r.length === 1 && JSON.parse(r[0].payload.json)?.message === 'target', `(resps=${r.length})`);
}

// 5. by-msgId at the terminus that holds it → answers (terminus path, older message)
{
  const { am, sent } = mk();
  const ret = am._onPull({ topicId: idHex(TOPIC), postHash: OTHER, corrId: 'c5', requesterId: idHex(REQ), via: [] }, { isTerminal: true });
  const r = pullResps(sent);
  ok('by-msgId at terminus returns the exact requested (older) message',
    ret === 'consumed' && r.length === 1 && JSON.parse(r[0].payload.json)?.message === 'older', `(resps=${r.length})`);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_pull_early_answer: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
