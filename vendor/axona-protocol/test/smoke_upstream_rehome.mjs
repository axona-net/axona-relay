// smoke_upstream_rehome.mjs — a dead upstream pin re-homes at the NEXT tick.
//
// External-review finding (validated 2026-07-13): a subscriber pinned to an
// upstream that dies was never a blackhole — the next renewal routed toward
// the corpse is popped at the live terminal ('reroute') and re-seats — but
// while pinned, `attached` stayed true, so an app subscriber's adaptive
// renewal could sit at the backed-off ceiling (RENEW_MS = 60s of staleness)
// before that healing renewal fired, and the reachable-root fallback stayed
// gated off. Fix: pubsubPeerDied sweeps upstream pins on the dead peer and
// resets the renewal clock, so re-homing happens on the very next tick.
//
// Run: node test/smoke_upstream_rehome.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(66, '0');

function makeManager({ selfBig }) {
  const routed = [];
  const dht = {
    getSelfId: () => selfBig,
    onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => routed.push({ target, type, payload }),
    neighbors: () => [],
    bridgeId: () => null,
  };
  const am = new AxonaManager({ dht });
  return { am, routed };
}

async function main() {
  console.log('dead-upstream pin sweep + next-tick re-home\n');
  const SELF = (0x80n << 248n) | 0x1000n;
  const T1   = SELF ^ 0x10n;               // topic with the dying upstream
  const T2   = SELF ^ 0x20n;               // control topic, different upstream
  const DEAD = idHex(T1 ^ 0x1n);           // the upstream that dies (non-neighbour)
  const LIVE = idHex(T2 ^ 0x1n);

  const { am, routed } = makeManager({ selfBig: SELF });

  // Two app subscriptions, both pinned + fully backed off (worst case: the
  // stale window before the fix was up to renewMs = 60s).
  am.pubsubSubscribe(T1);
  am.pubsubSubscribe(T2);
  am._upstream.set(T1, [DEAD]);
  am._upstream.set(T2, [LIVE]);
  for (const t of [T1, T2]) {
    const s = am.mySubscriptions.get(t);
    s.interval = am.renewMs;             // backed off to the 60s ceiling
    s.lastRenewSent = am._now();         // renewal just fired — worst case
  }
  // The dead upstream also had a cached root beacon (it was the root).
  am._rootBeacons.set(T1, { root: DEAD, at: am._now(), exp: am._now() + 50_000 });

  // ── the sweep ──────────────────────────────────────────────────────
  am.pubsubPeerDied(DEAD);
  check('dead upstream pin dropped', !am._upstream.has(T1));
  check('other topic\'s live pin untouched', am._upstream.get(T2)?.[0] === LIVE);
  check('dead peer\'s root beacon purged (pre-existing sweep)', !am._rootBeacons.has(T1));
  const s1 = am.mySubscriptions.get(T1);
  check('renewal interval snapped to fast', s1.interval === am.renewFastMs, `${s1.interval}`);
  check('renewal clock reset (fires on the NEXT tick, not in ≤60s)', s1.lastRenewSent === 0);
  check('control topic keeps its backed-off clock', am.mySubscriptions.get(T2).lastRenewSent > 0);

  // ── next tick re-homes unpinned ────────────────────────────────────
  routed.length = 0;
  await am.refreshTick();
  const subs = routed.filter(r => r.type === 'pubsub:sub'
    && r.payload.topicId === idHex(T1));
  check('next tick sent the healing renewal for the swept topic', subs.length >= 1, `${subs.length}`);
  check('…routed toward the topic, not via the corpse',
    subs.every(r => !(r.payload.via || []).includes(DEAD)));
  check('unattached clock armed (reachable-root fallback re-enabled)', am._unattachedSince.has(T1));
  const t2subs = routed.filter(r => r.type === 'pubsub:sub' && r.payload.topicId === idHex(T2));
  check('control topic did NOT renew early (still backed off)', t2subs.length === 0, `${t2subs.length}`);

  // ── idempotent / malformed input ───────────────────────────────────
  am.pubsubPeerDied(DEAD);                  // second sweep: no-op, no throw
  am.pubsubPeerDied(null);
  am.pubsubPeerDied(42);
  check('re-sweep + malformed ids are safe no-ops', true);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
