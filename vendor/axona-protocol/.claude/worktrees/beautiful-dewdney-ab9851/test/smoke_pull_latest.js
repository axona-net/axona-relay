// =====================================================================
// smoke_pull_latest.js — pull latest + sliding hold on pull (Phase A #6).
//
//   1. _latestInReplayCache returns the highest-ordered (by signed seq)
//      LIVE message; expired entries are skipped.
//   2. _onPullReq with a null postHash serves the latest message.
//   3. A pull slides the message's expiry to now + hold, BOUNDED by its
//      absolute ceiling (a read can't extend life past ceilingAt).
//
// Run: node test/smoke_pull_latest.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const H = 60 * 60 * 1000;
const T = 1_700_000_000_000;
const TOPIC_HEX = '05' + '00'.repeat(32);
const TOPIC_BIG = BigInt('0x' + TOPIC_HEX);
const REQ_HEX   = '06' + '11'.repeat(32);

function setup(clockRef) {
  const sent = [];
  const dht = {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, routeMessage: async () => {},
    sendDirect: async (to, type, payload) => { sent.push({ to, type, payload }); return true; },
  };
  const am = new AxonaManager({ dht, now: () => clockRef.t });
  const role = { isRoot: true, children: new Map(), replayCache: [] };
  am.axonRoles.set(TOPIC_BIG, role);
  return { am, role, sent };
}
function add(am, role, seq, postHash) {
  am._addToReplayCache(role, { json: JSON.stringify({ seq }), publishId: `p${seq}`, postHash, seq, ts: T });
}

function testLatest() {
  console.log('\n── _latestInReplayCache returns the highest-seq live message ──');
  const clk = { t: T };
  const { am, role } = setup(clk);
  add(am, role, 5, 'h5');
  add(am, role, 1, 'h1');
  add(am, role, 3, 'h3');
  const latest = am._latestInReplayCache(role);
  check('latest is highest seq (5)', latest && latest.postHash === 'h5');
}

async function testPullReqLatest() {
  console.log('\n── _onPullReq with null postHash serves the latest ──');
  const clk = { t: T };
  const { am, role, sent } = setup(clk);
  add(am, role, 5, 'h5');
  add(am, role, 7, 'h7');   // newest
  add(am, role, 3, 'h3');
  await am._onPullReq({ topicId: TOPIC_HEX, postHash: null, requesterId: REQ_HEX, requestId: 'r1' }, {});
  const resp = sent.find(s => s.type === 'pubsub:pullResp');
  check('a pullResp was sent', !!resp);
  check('status FOUND',        resp?.payload.status === 'FOUND');
  check('served the latest (h7)', resp?.payload.postHash === 'h7');
}

function testSlidingHold() {
  console.log('\n── a pull slides expiry to now + hold, bounded by the ceiling ──');
  const clk = { t: T };
  const { am, role } = setup(clk);
  add(am, role, 1, 'h1');
  const e = role.replayCache[0];
  check('initial expiry = T + 24h', e.expiresAt === T + 24 * H);
  check('ceiling = T + 48h',         e.ceilingAt === T + 48 * H);

  // Pull 10h later → expiry slides to (T+10h)+24h = T+34h.
  clk.t = T + 10 * H;
  am._onPullReq({ topicId: TOPIC_HEX, postHash: 'h1', requesterId: REQ_HEX, requestId: 'r2' }, {});
  check('expiry slid forward to now + hold', e.expiresAt === T + 34 * H);

  // Pull 30h later → (T+30h)+24h = T+54h would exceed the ceiling → capped at T+48h.
  clk.t = T + 30 * H;
  am._onPullReq({ topicId: TOPIC_HEX, postHash: 'h1', requesterId: REQ_HEX, requestId: 'r3' }, {});
  check('slide capped at the absolute ceiling', e.expiresAt === T + 48 * H);
}

function testLatestSkipsExpired() {
  console.log('\n── latest skips expired entries ──');
  const clk = { t: T };
  const { am, role } = setup(clk);
  add(am, role, 9, 'h9');           // expires T+24h
  add(am, role, 2, 'h2');           // expires T+24h
  clk.t = T + 25 * H;               // both expired
  check('no live latest once expired', am._latestInReplayCache(role) === null);
}

async function main() {
  console.log('Axona pull-latest + sliding hold (Phase A #6) smoke');
  testLatest();
  await testPullReqLatest();
  testSlidingHold();
  testLatestSkipsExpired();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
