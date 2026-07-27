// =====================================================================
// smoke_pubsub_authz.js — pub/sub subscribe authorization (finding C4).
//
// A direct subscribe (`pubsub:subscribe-k`) registers the subscriber as a
// child the axon relays the topic's full feed to — directly, by nodeId.
// If the axon trusted the payload's `subscriberId`, an attacker could name
// a victim and turn the axon into a reflection/amplification (DRDoS) source.
//
// axona/4 makes `meta.fromId` the *proven* channel peer, so the axon now
// requires `subscriberId === meta.fromId` on subscribe-k / unsubscribe-k.
// These tests drive the real AxonaManager handlers directly.
//
// Run: node test/smoke_pubsub_authz.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { resolveTopic } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const hex   = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);  // 66-char
const big   = (h) => BigInt('0x' + h);

const ROOT      = hex(0x01);
const SUBSCRIBER= hex(0x02);
const ATTACKER  = hex(0x03);
const VICTIM    = hex(0x04);
const TOPIC     = hex(0x05);

function makeManager() {
  const sent = [];
  const dht = {
    getSelfId:        () => big(ROOT),
    onRoutedMessage:  () => {},
    onDirectMessage:  () => {},
    routeMessage:     () => {},
    sendDirect:       async (to, type, body) => { sent.push({ to, type, body }); return true; },
    findKClosest:     undefined,
  };
  const am = new AxonaManager({ dht });
  return { am, sent };
}

function childIds(am, topicHex) {
  const role = am.axonRoles.get(big(topicHex));
  if (!role) return null;
  return new Set([...role.children.keys()].map(k => k.toString()));
}

async function testLegitSubscribeAdmitted() {
  console.log('\n── legit subscribe (subscriberId === fromId) is admitted ──');
  const { am } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER },
    { fromId: SUBSCRIBER });
  const kids = childIds(am, TOPIC);
  check('role created for topic', kids !== null);
  check('authenticated subscriber registered as child',
    kids && kids.has(big(SUBSCRIBER).toString()));
}

async function testSpoofedSubscribeDropped() {
  console.log('\n── spoofed subscribe (attacker names a victim) is dropped ──');
  const { am, sent } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: VICTIM },   // claims the victim…
    { fromId: ATTACKER });                       // …but the proven sender is the attacker
  check('no role/children allocated for spoofed subscribe', childIds(am, TOPIC) === null);
  check('no replay/deliver fired at the victim',
    !sent.some(s => s.to === big(VICTIM)));
}

async function testNoFromIdBackCompat() {
  console.log('\n── missing fromId (local/sim context) keeps working ──');
  const { am } = makeManager();
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER },
    {});                                          // no fromId → cannot verify → admit
  const kids = childIds(am, TOPIC);
  check('subscriber admitted when fromId absent', kids && kids.has(big(SUBSCRIBER).toString()));
}

async function testUnsubscribeAuthz() {
  console.log('\n── unsubscribe requires the authenticated subscriber ──');
  const { am } = makeManager();
  // Seed a legit subscription first.
  await am._onSubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });

  // Attacker tries to unsubscribe the victim's... actually the subscriber.
  am._onUnsubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: ATTACKER });
  check('spoofed unsubscribe does NOT remove the subscription',
    childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));

  // The real subscriber can unsubscribe itself.
  am._onUnsubscribeDirect(
    { topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  check('authenticated unsubscribe removes the subscription',
    !childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));
}

// ── B-1: the ROUTED subscribe path (the residual C4 left open) ──────
// `pubsub:subscribe` travels multi-hop; meta.fromId is the proven PREVIOUS
// hop. An attacker can route a subscribe naming a victim as subscriberId.
// The axon must enroll only the authenticated channel peer — never a
// third party — or it becomes a reflection/amplification source (the
// victim gets the topic's fan-out + a ≤100-msg replay blast).

async function testRoutedSpoofedSubscribeNotEnrolled() {
  console.log('\n── routed subscribe: attacker names a victim → not enrolled, no replay blast ──');
  const { am, sent } = makeManager();
  // Seed a real role with a populated replay cache, so a regression that
  // wrongly enrolled the victim would fire a replay blast we can detect.
  await am._onSubscribeDirect({ topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  const role = am.axonRoles.get(big(TOPIC));
  role.replayCache = [{ json: '{"m":1}', publishId: 'p:1', publishTs: 1000 }];

  // Attacker routes a subscribe naming the victim; proven previous hop is the attacker.
  const r = await am._onSubscribe(
    { topicId: TOPIC, subscriberId: VICTIM },
    { fromId: ATTACKER, isTerminal: false });
  check('victim NOT added as a child', !childIds(am, TOPIC).has(big(VICTIM).toString()));
  check('no replay-batch / deliver fired at the victim', !sent.some(s => s.to === big(VICTIM)));
  check('handler forwards (keeps routing) rather than consuming', r === 'forward');
}

async function testRoutedVouchedSubscribeEnrolled() {
  console.log('\n── routed subscribe: adjacent subscriber (subscriberId === fromId) → enrolled ──');
  const { am } = makeManager();
  await am._onSubscribeDirect({ topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  await am._onSubscribe(
    { topicId: TOPIC, subscriberId: ATTACKER },     // here "ATTACKER" id is just another peer…
    { fromId: ATTACKER, isTerminal: false });        // …and it vouches for itself, so it's allowed
  check('self-vouched routed subscriber is enrolled',
    childIds(am, TOPIC).has(big(ATTACKER).toString()));
}

async function testRoutedTerminalSpoofNoRoot() {
  console.log('\n── routed subscribe at terminal: spoof does NOT seed a victim-keyed root ──');
  const { am } = makeManager();
  const r = await am._onSubscribe(
    { topicId: TOPIC, subscriberId: VICTIM },
    { fromId: ATTACKER, isTerminal: true });
  check('no root created for an unvouched terminal subscribe', am.axonRoles.get(big(TOPIC)) == null);
  check('handler forwards rather than consuming', r === 'forward');
}

async function testRoutedUnsubscribeAuthz() {
  console.log('\n── routed unsubscribe: attacker cannot silence a victim ──');
  const { am } = makeManager();
  await am._onSubscribeDirect({ topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  await am._onUnsubscribe({ topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: ATTACKER });
  check('spoofed routed unsubscribe does NOT remove the subscription',
    childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));
  await am._onUnsubscribe({ topicId: TOPIC, subscriberId: SUBSCRIBER }, { fromId: SUBSCRIBER });
  check('authenticated routed unsubscribe removes it',
    !childIds(am, TOPIC).has(big(SUBSCRIBER).toString()));
}

// ── B-2: lazy-axon promotion proximity gate (memory-DoS) ────────────
// A node must only allocate a role + replay cache for a topic it is
// plausibly among the K-closest to; otherwise an attacker floods
// publishes for random topicIds and every node self-promotes → heap
// exhaustion. The gate uses the node's local K-closest view.

function makeManagerK(kClosestFor) {
  const sent = [];
  const dht = {
    getSelfId:       () => big(ROOT),
    onRoutedMessage: () => {},
    onDirectMessage: () => {},
    routeMessage:    () => {},
    sendDirect:      async (to, type, body) => { sent.push({ to, type, body }); return true; },
    findKClosest:    async (topicId, K) => kClosestFor(topicId).slice(0, K),
  };
  return { am: new AxonaManager({ dht, rootSetSize: 5 }), sent };
}

async function testLazyAxonProximityGate() {
  console.log('\n── B-2: lazy-axon promotion gated on proximity ──');
  const NEAR  = hex(0x06);
  const FAR   = hex(0x07);
  const peers = [hex(0x21), hex(0x22), hex(0x23), hex(0x24), hex(0x25)].map(big);
  // self (ROOT) is in NEAR's K-closest set, but NOT in FAR's.
  const { am } = makeManagerK((topicId) =>
    topicId === big(NEAR) ? [big(ROOT), ...peers] : [...peers]);

  await am._onPublishDirect({ topicId: FAR, publisher: ATTACKER, json: '{}', publishId: 'x:1', publishTs: 1 });
  check('far/random topic does NOT promote a role (DoS flood blocked)', am.axonRoles.get(big(FAR)) == null);

  await am._onPublishDirect({ topicId: NEAR, publisher: ATTACKER, json: '{}', publishId: 'x:2', publishTs: 2 });
  check('topic we ARE K-closest to promotes normally', am.axonRoles.get(big(NEAR)) != null);
}

async function testLazyAxonGateFailsOpenWithoutView() {
  console.log('\n── B-2: gate fails open with no K-closest view (sim/legacy) ──');
  const { am } = makeManager();   // findKClosest undefined → cannot compute proximity
  await am._onPublishDirect({ topicId: TOPIC, publisher: ATTACKER, json: '{}', publishId: 'y:1', publishTs: 1 });
  check('promotion still works when proximity is unknowable', am.axonRoles.get(big(TOPIC)) != null);
}

// ── B-4: publisher-signature verification at root ingress ───────────
// A root axon caches a publish and fans it out to its subscriber subtree.
// If it forwarded before checking the publisher signature, spoofed-sig
// spam would be amplified through the tree before leaf nodes rejected it.
// The axon now verifies the envelope at ingress: a claimed-but-invalid
// signature is dropped; unsigned/anonymous and non-envelope payloads pass.

// v0.3: a root binds the SIGNED topic descriptor to the routed topic id
// (_topicPolicyOk), so an envelope must be routed to the topic id its descriptor
// resolves to — otherwise it's dropped for descriptor/topic mismatch, not for
// the signature reason these tests target. Route to the real resolved id.
const NEWS = { region: 'useast', name: 'news' };

async function testIngressDropsForgedSignature() {
  console.log('\n── B-4: forged-signature publish dropped at ingress ──');
  const id  = await createAuthorIdentity();
  const env = await buildEnvelope({ topic: NEWS, message: 'hello', identity: id, sign: true });
  const forged = { ...env, message: 'evil-rewritten-after-signing' };  // sig no longer matches
  const topicBig = BigInt('0x' + (await resolveTopic(NEWS)).topicId);

  const { am } = makeManager();
  await am._onPublishDirect({ topicId: topicBig, publisher: null, json: JSON.stringify(forged), publishId: 'f:1', publishTs: 1 });
  check('forged-signature publish NOT cached/promoted', am.axonRoles.get(topicBig) == null);

  const { am: am2 } = makeManager();
  await am2._onPublishDirect({ topicId: topicBig, publisher: null, json: JSON.stringify(env), publishId: 'v:1', publishTs: 1 });
  check('validly-signed publish accepted (role created)', am2.axonRoles.get(topicBig) != null);
}

async function testIngressAllowsUnsignedAndRaw() {
  console.log('\n── B-4: unsigned + non-envelope publishes pass (no sig to forge) ──');
  const unsigned = await buildEnvelope({ topic: NEWS, message: 'hi', sign: false });
  const topicBig = BigInt('0x' + (await resolveTopic(NEWS)).topicId);
  const { am } = makeManager();
  await am._onPublishDirect({ topicId: topicBig, publisher: null, json: JSON.stringify(unsigned), publishId: 'u:1', publishTs: 1 });
  check('unsigned (anonymous) publish accepted', am.axonRoles.get(topicBig) != null);

  // A raw, non-envelope payload has no descriptor to bind — it still passes
  // ingress (the policy gate is a no-op for non-envelopes); route to TOPIC.
  const { am: am2 } = makeManager();
  await am2._onPublishDirect({ topicId: TOPIC, publisher: null, json: 'raw-non-envelope-string', publishId: 'r:1', publishTs: 1 });
  check('non-envelope raw payload accepted', am2.axonRoles.get(big(TOPIC)) != null);
}

// ── D-1: inbound caps ───────────────────────────────────────────────

async function testAdoptRespectsMaxDirectSubs() {
  console.log('\n── D-1: adopt-subscribers respects maxDirectSubs ──');
  const sent = [];
  const dht = {
    getSelfId: () => big(ROOT), onRoutedMessage: () => {}, onDirectMessage: () => {},
    routeMessage: async () => {}, sendDirect: async (to, t, b) => { sent.push({ to }); return true; },
    findKClosest: undefined,
  };
  const am = new AxonaManager({ dht, maxDirectSubs: 4 });
  // An adopt message naming 50 distinct subscribers must not overfill us.
  const many = Array.from({ length: 50 }, (_, i) => hex(0x40 + i));
  await am._onAdoptSubscribers({ topicId: TOPIC, subscriberIds: many }, { fromId: ATTACKER });
  const role = am.axonRoles.get(big(TOPIC));
  check('child map capped at maxDirectSubs', role && role.children.size <= 4);
}

async function testOversizePublishDropped() {
  console.log('\n── D-1: oversize publish payload dropped at ingress ──');
  const { am } = makeManager();
  const huge = 'x'.repeat(256 * 1024 + 1);   // > MAX_PUBLISH_BYTES (256 KiB)
  await am._onPublishDirect({ topicId: TOPIC, publisher: null, json: huge, publishId: 'big:1', publishTs: 1 });
  check('oversize publish (>256 KiB) not cached/promoted', am.axonRoles.get(big(TOPIC)) == null);
  // A payload under the cap is accepted (verifies the threshold moved, not just "huge fails").
  const { am: am2 } = makeManager();
  const under = 'y'.repeat(200 * 1024);      // < 256 KiB
  await am2._onPublishDirect({ topicId: TOPIC, publisher: null, json: under, publishId: 'big:2', publishTs: 2 });
  check('200 KiB publish accepted (under 256 KiB cap)', am2.axonRoles.get(big(TOPIC)) != null);
}

async function main() {
  console.log('Axona pub/sub trust boundary (C4 + B-1 + B-2 + B-4 + D-1) smoke');
  await testLegitSubscribeAdmitted();
  await testSpoofedSubscribeDropped();
  await testNoFromIdBackCompat();
  await testUnsubscribeAuthz();
  await testRoutedSpoofedSubscribeNotEnrolled();
  await testRoutedVouchedSubscribeEnrolled();
  await testRoutedTerminalSpoofNoRoot();
  await testRoutedUnsubscribeAuthz();
  await testLazyAxonProximityGate();
  await testLazyAxonGateFailsOpenWithoutView();
  await testIngressDropsForgedSignature();
  await testIngressAllowsUnsignedAndRaw();
  await testAdoptRespectsMaxDirectSubs();
  await testOversizePublishDropped();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
