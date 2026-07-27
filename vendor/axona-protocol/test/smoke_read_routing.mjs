// smoke_read_routing.mjs — v4.10.1 read/host path routing (cohort-aware).
//
// pull and host used a bare greedy via:[] and so stranded on a local minimum,
// reaching a non-cohort node (pull → false "no message"; host → initial announce
// lost until the next tick). They now route via the warm lookup-assist hint, exactly
// like publish/kill — so they reach a node that actually serves the topic's cohort.
//
//   1. requestPull with a warm root hint routes the PULL to that root (not greedy)
//   2. requestPull with no hint falls back to greedy toward the topic (cold path OK)
//   3. pubsubHost routes its announce via _sendSubscribe (a SUB toward the hinted root),
//      not a bare host-send, and registers the hosted topic
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };
const REG = 0x87n << 248n, idHex = (b) => b.toString(16).padStart(66, '0'), lc = (s) => s.toLowerCase();
const SELF = REG | 0x11n, ROOT = REG | 0xab0n, T1 = REG | 0xabcn, T2 = REG | 0xdefn;

function mk() {
  const sends = [];
  const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => [],
    bridgeId: () => null,
    async findKClosest() { return []; },
  };
  const am = new AxonaManager({ dht, now: () => clock.t }); am.nodeId = SELF;
  return { am, sends, clock };
}
// pre-warm the root hint the cheap, deterministic way: a live root beacon.
const warmHint = (am, topicBig, rootBig) =>
  am._rootBeacons.set(topicBig, { root: lc(idHex(rootBig)), exp: am._now() + 3_600_000, seq: 1 });

// ── 1. pull WITH a warm hint → PULL routed to the root ──
{
  const { am, sends } = mk();
  warmHint(am, T1, ROOT);
  am.requestPull(T1, null, { timeoutMs: 50 });
  const pull = sends.find(s => s.type === 'pubsub:pull');
  ok('requestPull emits a PULL', !!pull);
  ok('PULL is routed to the hinted root (not greedy)', pull?.target === ROOT && pull?.payload?.via?.[0] === lc(idHex(ROOT)));
}

// ── 2. pull with NO hint → greedy toward the topic (cold fallback) ──
{
  const { am, sends } = mk();
  am.requestPull(T1, null, { timeoutMs: 50 });
  const pull = sends.find(s => s.type === 'pubsub:pull');
  ok('cold pull falls back to greedy toward the topic', !!pull && pull.target === T1 && (pull.payload.via || []).length === 0);
}

// ── 3. host routes via _sendSubscribe (lookup-assisted SUB), registers the topic ──
{
  const { am, sends } = mk();
  warmHint(am, T2, ROOT);
  am.pubsubHost(T2);
  const sub = sends.find(s => s.type === 'pubsub:sub');
  ok('pubsubHost emits a SUB (via _sendSubscribe, not a bare host-send)', !!sub);
  ok('host SUB is routed to the hinted root', sub?.target === ROOT);
  ok('hosted topic is registered', am._hostedTopics.has(T2));
}

console.log(`\n${fail ? '✗' : '✓'} smoke_read_routing: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
