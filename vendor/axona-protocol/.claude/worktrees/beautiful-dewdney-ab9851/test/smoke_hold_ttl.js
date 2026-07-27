// =====================================================================
// smoke_hold_ttl.js — message hold time / absolute-ceiling expiry
//                     (Phase A #5).
//
//   1. _addToReplayCache stamps expiresAt = ts + hold and ceilingAt =
//      ts + 48h ceiling; the owner hold is capped at 48h.
//   2. _isExpired / _sweepExpired drop messages past expiry.
//   3. _findInReplayCache reports a miss (and drops) an expired message.
//
// Run: node test/smoke_hold_ttl.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const H  = 60 * 60 * 1000;
const T  = 1_700_000_000_000;
function stubDht() {
  return {
    getSelfId: () => 1n, onRoutedMessage: () => {}, onDirectMessage: () => {},
    onEvent: () => () => {}, sendDirect: async () => true, routeMessage: async () => {},
  };
}
function entry(seq, ts) {
  return { json: '{}', publishId: `p${seq}`, postHash: `h${seq}`, seq, ts };
}

function testDefaultHold() {
  console.log('\n── default 24h hold; ceiling at 48h ──');
  const am = new AxonaManager({ dht: stubDht(), now: () => T });
  const role = { replayCache: [] };
  am._addToReplayCache(role, entry(1, T));
  const e = role.replayCache[0];
  check('expiresAt = ts + 24h', e.expiresAt === T + 24 * H);
  check('ceilingAt = ts + 48h', e.ceilingAt === T + 48 * H);
  check('not expired at publish time', am._isExpired(e, T) === false);
}

function testOwnerHoldCappedAt48h() {
  console.log('\n── owner hold is capped at the 48h ceiling ──');
  const am = new AxonaManager({ dht: stubDht(), now: () => T });
  const role = { replayCache: [], maxHoldMs: 100 * H };   // owner asks 100h
  am._addToReplayCache(role, entry(1, T));
  check('expiry capped at ts + 48h', role.replayCache[0].expiresAt === T + 48 * H);
}

function testSweep() {
  console.log('\n── expired messages are swept ──');
  let clock = T;
  const am = new AxonaManager({ dht: stubDht(), now: () => clock });
  const role = { replayCache: [] };
  am._addToReplayCache(role, entry(1, T));            // expires T+24h
  am._addToReplayCache(role, entry(2, T + 30 * H));   // expires T+54h

  clock = T + 25 * H;                                  // past #1, before #2
  am._sweepRole(role);
  check('expired entry removed', !role.replayCache.some(e => e.seq === 1));
  check('live entry kept',        role.replayCache.some(e => e.seq === 2));

  // _sweepExpired walks registered roles: register and confirm it sweeps too.
  const role2 = { replayCache: [] };
  am._addToReplayCache(role2, entry(3, T));            // expires T+24h, already past
  am.axonRoles.set(123n, role2);
  am._sweepExpired();
  check('_sweepExpired clears a registered role', role2.replayCache.length === 0);
}

function testFindReportsExpiredMiss() {
  console.log('\n── _findInReplayCache misses (and drops) an expired message ──');
  let clock = T;
  const am = new AxonaManager({ dht: stubDht(), now: () => clock });
  const role = { replayCache: [] };
  am._addToReplayCache(role, entry(7, T));
  check('found while live', am._findInReplayCache(role, 'h7') !== null);
  clock = T + 25 * H;
  check('miss once expired', am._findInReplayCache(role, 'h7') === null);
  check('expired entry dropped from cache', role.replayCache.length === 0);
}

async function main() {
  console.log('Axona hold time / expiry (Phase A #5) smoke');
  testDefaultHold();
  testOwnerHoldCappedAt48h();
  testSweep();
  testFindReportsExpiredMiss();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
