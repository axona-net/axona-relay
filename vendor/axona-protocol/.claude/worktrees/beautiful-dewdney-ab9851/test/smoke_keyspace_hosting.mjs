// =====================================================================
// smoke_keyspace_hosting.mjs — no-arg host() (keyspace hosting) actually
// keeps the node rooting topics in its neighborhood.
//
// In the routing-only kernel a node becomes ROOT for a topic reactively (a
// SUB/PUB routes to it as the terminus). Without keyspace hosting, a root role
// with zero subscribers and an empty cache is torn down on the next refreshTick
// — so a relay that called peer.host() (no topic) volunteered nothing durable
// (the `_hostKeyspace` flag was set but read nowhere). This pins the fix: a
// keyspace host RETAINS any role it has become root for, so it stays an
// always-on convergence anchor; a non-hosting node still tears the empty role
// down.
//
//   node test/smoke_keyspace_hosting.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

// Minimal dht adapter: routing is a no-op (we drive role state directly and
// only care about teardown behaviour in refreshTick).
function makeMgr(selfId) {
  const dht = {
    getSelfId: () => selfId,
    routeMessage: async () => {},
    onRoutedMessage: () => {},
    onDirectMessage: () => {},
  };
  return new AxonaManager({ dht, now: () => 1_700_000_000_000 });
}

async function main() {
  console.log('Axona keyspace hosting (no-arg host)');
  const SELF  = 0x89n << 248n;                 // a node in region 0x89
  const TOPIC = (0x89n << 248n) | 0xbeefn;     // a topic that landed near it

  // ── 1. NOT keyspace-hosting: an empty root role is torn down ──
  {
    const mgr = makeMgr(SELF);
    mgr._becomeRoot(TOPIC);                      // reactive root (terminus), empty cache, no subs
    check('1a. root role created', mgr.axonRoles.has(TOPIC) && mgr.axonRoles.get(TOPIC).isRoot);
    await mgr.refreshTick();
    check('1b. without keyspace hosting, empty root is torn down', !mgr.axonRoles.has(TOPIC));
  }

  // ── 2. keyspace-hosting: the same empty root PERSISTS ──
  {
    const mgr = makeMgr(SELF);
    mgr.pubsubHostKeyspace(true);                // peer.host() with no topic
    mgr._becomeRoot(TOPIC);
    await mgr.refreshTick();
    check('2a. keyspace host retains the rooted topic across refreshTick', mgr.axonRoles.has(TOPIC));
    check('2b. retained role is still root', mgr.axonRoles.get(TOPIC)?.isRoot === true);
    // survives repeated ticks (durable anchor, not a one-tick reprieve)
    await mgr.refreshTick(); await mgr.refreshTick();
    check('2c. survives repeated ticks', mgr.axonRoles.has(TOPIC));
    // turning keyspace hosting off lets it tear down again
    mgr.pubsubHostKeyspace(false);
    await mgr.refreshTick();
    check('2d. keyspace off → empty root tears down again', !mgr.axonRoles.has(TOPIC));
  }

  // ── 3. keyspace hosting does NOT pin a non-root role ──
  // (root-ness is decided by routing; a delegated child with no subscribers is
  // redundant and may still tear down even under keyspace hosting.)
  {
    const mgr = makeMgr(SELF);
    mgr.pubsubHostKeyspace(true);
    const child = mgr._becomeRoot(TOPIC); child.isRoot = false;   // demote to non-root child
    await mgr.refreshTick();
    check('3. keyspace hosting does not retain a non-root empty role', !mgr.axonRoles.has(TOPIC));
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
