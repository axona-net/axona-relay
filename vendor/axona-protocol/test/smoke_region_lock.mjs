// smoke_region_lock.mjs — region-occupancy rule (v4.13.0).
//
// A topic is anchored to its region byte. Only nodes IN THAT REGION may form the
// topic's axon-tree infrastructure (root + child relays); a region with no node
// cannot be rooted by a neighbour (which would hotspot the neighbour). Leaf
// subscribers / publishers may be from any region.
//   1. _topicDecision: in-region terminus → 'handle'; out-of-region terminus → 'reject'
//   2. _onSub at an out-of-region terminus seats/roots NOTHING (drop)
//   3. _onSub at an in-region terminus forms the root
//   4. _promoteChild promotes ONLY in-region leaves (foreign leaf stays a direct leaf)
//   5. _onAdopt refuses to become a child relay for a foreign-region topic
//   6. pubsubHost refuses a foreign-region topic
//
// Run: node test/smoke_region_lock.mjs
//
// NOTE (v4.15.0): the region lock is OFF BY DEFAULT (pre-critical-mass). This smoke
// turns it ON to exercise enforcement, then flips it OFF to prove the permissive
// fallback (out-of-region rooting allowed).
import { AxonaManager, configureRegionLock } from '../src/pubsub/AxonaManager.js';

configureRegionLock({ enforce: true });   // enforcement path for the assertions below

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${m} ${extra}`); c ? n++ : fail++; };
const idHex = (b) => b.toString(16).padStart(66, '0');
const mk = (region, tag) => (BigInt(region) << 256n) | BigInt(tag);   // region byte = top byte

const SELF   = mk(0x89, 0x11);              // this node is in region 0x89
const HOME   = mk(0x89, 0xabc);             // a topic in OUR region
const FOREIGN = mk(0x12, 0xabc);            // a topic in a region with no node here

function newAM() {
  const sent = [];
  const dht = {
    getSelfId: () => SELF,
    onRoutedMessage: () => {},
    routeMessage: (_t, type, payload) => sent.push({ type, payload }),
    neighbors: () => [], bridgeId: () => null, findKClosest: async () => [],
  };
  const am = new AxonaManager({ dht, now: () => Date.now() });
  am.nodeId = SELF;
  return { am, sent };
}
const termIn  = { isTerminal: true };
const bare = (t) => ({ topicId: idHex(t), via: [] });

// 1. _topicDecision region gate
{
  const { am } = newAM();
  ok('in-region bare terminus → handle',   am._topicDecision(bare(HOME),    termIn) === 'handle');
  ok('out-of-region bare terminus → reject', am._topicDecision(bare(FOREIGN), termIn) === 'reject');
}

// 2+3. _onSub honors the gate
{
  const { am } = newAM();
  am._onSub({ ...bare(FOREIGN), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);
  ok('out-of-region subscribe roots/seats nothing', !am.axonRoles.has(FOREIGN));

  am._onSub({ ...bare(HOME), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);
  const role = am.axonRoles.get(HOME);
  ok('in-region subscribe forms the root', !!role && role.isRoot);
}

// 4. _promoteChild promotes only in-region leaves
{
  const { am } = newAM();
  am._onSub({ ...bare(HOME), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);  // seat one in-region so root exists
  const role = am.axonRoles.get(HOME);
  const inRegionLeaf  = idHex(mk(0x89, 0xAA));
  const foreignLeaf   = idHex(mk(0x12, 0xBB));
  role.subscribers.set(inRegionLeaf, { since: 0, lastRenewed: am._now() });
  role.subscribers.set(foreignLeaf,  { since: 0, lastRenewed: am._now() });
  // enough leaves to trigger a promotion; only in-region ones are eligible
  role.subscribers.set(idHex(mk(0x89, 0xCC)), { since: 0, lastRenewed: am._now() });
  am._promoteChild(role);
  ok('an in-region leaf was promoted to a child relay', role.children.size >= 1 && [...role.children].every(c => c.startsWith('12') === false));
  ok('the foreign-region leaf was NOT promoted', !role.children.has(foreignLeaf));
}

// 5. _onAdopt refuses a foreign-region topic
{
  const { am } = newAM();
  am._onAdopt({ topicId: idHex(FOREIGN), parent: idHex(mk(0x12, 0x1)), subs: [] }, { targetId: SELF });
  ok('_onAdopt refuses to relay a foreign-region topic', !am.axonRoles.has(FOREIGN));
  am._onAdopt({ topicId: idHex(HOME), parent: idHex(mk(0x89, 0x1)), subs: [] }, { targetId: SELF });
  ok('_onAdopt accepts an in-region topic', am.axonRoles.has(HOME));
}

// 6. pubsubHost refuses a foreign region
{
  const { am } = newAM();
  am.pubsubHost(FOREIGN);
  ok('pubsubHost refuses a foreign-region topic', !am._hostedTopics.has(FOREIGN));
  am.pubsubHost(HOME);
  ok('pubsubHost accepts an in-region topic', am._hostedTopics.has(HOME));
}

// 7. Region lock OFF (default pre-critical-mass) → out-of-region rooting allowed
{
  configureRegionLock({ enforce: false });
  const { am } = newAM();
  ok('lock off: out-of-region bare terminus → handle (not reject)',
     am._topicDecision(bare(FOREIGN), termIn) === 'handle');
  am._onSub({ ...bare(FOREIGN), subscriberId: idHex(mk(0x89, 0x77)), since: 0 }, termIn);
  const role = am.axonRoles.get(FOREIGN);
  ok('lock off: out-of-region subscribe forms a root anyway', !!role && role.isRoot);
  am.pubsubHost(FOREIGN);
  ok('lock off: pubsubHost accepts a foreign-region topic', am._hostedTopics.has(FOREIGN));
  configureRegionLock({ enforce: true });   // restore for any later runs in-process
}

console.log(`\n${fail ? '✗' : '✓'} smoke_region_lock: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
