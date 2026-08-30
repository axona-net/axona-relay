// harness/disc-probe.mjs — root-registration DISCRIMINATOR (council, 2026-08-30).
//
// Answers: do SUB and PUB resolve to the same root? If not, which mechanism —
//   (1) self-root split      both sides _becomeRoot on the same topicId
//   (2) asymmetric resolve    SUB greedy vs PUB lookup-assisted diverge on ONE node
//   (3) migration no-handoff  they agree on a snapshot but the pinned root drifts
// and: when a PUB reaches the true root, is the subscriber in its live fanout?
//
// LAT_TRACE=1 makes the kernel emit `pubsub:disc` events (became-root, pub-root,
// sub-root, root-members). We also introspect each peer's AxonaManager directly
// (same process) to run Aster's decisive check: on ONE node, resolve the root the
// SUB way (where it actually pinned, _upstream) vs the PUB way (warmRootHint →
// _rootHint_). Divergence there is same-snapshot → algorithm/canonicalization,
// not table asymmetry.
//
//   BRIDGE=wss://testnet.axona.net SETTLE_MS=30000 N=5 node harness/disc-probe.mjs
import '../src/polyfill.js';
process.env.LAT_TRACE = '1';
import { connectPeer } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const REGION = process.env.REGION || 'eagle';
const N = Number(process.env.N || 5);
const SETTLE_MS = Number(process.env.SETTLE_MS || 30000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = (x) => (x == null ? null : String(x).slice(0, 12));

const disc = [];                                     // { peer, t, ev, ... }
const rec = (tag) => (msg, ctx) => { if (msg === 'pubsub:disc' && ctx) disc.push({ peer: tag, ...ctx }); };

console.error(`connecting sub+pub to ${BRIDGE} (LAT_TRACE on)`);
const sub = await connectPeer({ region: REGION, bridge: BRIDGE });
const pub = await connectPeer({ region: REGION, bridge: BRIDGE });
sub.peer.onLog('info', rec('sub'));
pub.peer.onLog('info', rec('pub'));
const subAm = sub.peer._axonaManager, pubAm = pub.peer._axonaManager;
console.error('connected  sub', S(sub.nodeId), ' pub', S(pub.nodeId));

const topic = `harness/disc-${S(sub.author.authorId)}`;
const desc = { region: REGION, name: topic };
const topicBig = await deriveTopicIdBig({ region: REGION, name: topic });
console.error('topicId', S(topicBig));

await sub.peer.sub(desc, () => {}, { since: 'all' });
console.error(`settling ${SETTLE_MS}ms`);
await sleep(SETTLE_MS);
for (let i = 0; i < N; i++) { await pub.peer.pub(desc, { v: 1, i }, { signWith: pub.author }); await sleep(1500); }

// ── Aster's decisive check — snapshot the LIVE state now, BEFORE any drain/close.
const safe = async (fn) => { try { return await fn(); } catch (e) { return `ERR:${String(e?.message).slice(0, 40)}`; } };
const subPin = S(subAm?._upstream?.get(topicBig)?.[0]);                 // where the SUB actually pinned (greedy)
const subHasRole = !!subAm?.axonRoles?.has(topicBig);                   // did the SUB self-root?
const pubHasRole = !!pubAm?.axonRoles?.has(topicBig);                   // did the PUB self-root?
// PUB-path resolution on EACH node (lookup-assisted): warm then read the hint.
const subPubResolve = await safe(async () => { await subAm.warmRootHint?.(topicBig); return S(subAm._rootHint_?.(topicBig)); });
const pubPubResolve = await safe(async () => { await pubAm.warmRootHint?.(topicBig); return S(pubAm._rootHint_?.(topicBig)); });
console.error('published; draining 20s');
await sleep(20000);

// ── report ────────────────────────────────────────────────────────────
console.log('\n===== DISCRIMINATOR =====');
console.log('topicId          ', S(topicBig));
console.log('sub nodeId       ', S(sub.nodeId));
console.log('pub nodeId       ', S(pub.nodeId));
console.log('\n-- disc events --');
for (const d of disc) console.log(`  [${d.peer}] ${d.ev.padEnd(12)} self=${d.self}` +
  (d.why ? ` why=${d.why}` : '') + (d.hint !== undefined ? ` hint=${d.hint}` : '') +
  (d.root !== undefined ? ` root=${d.root}` : '') + (d.n !== undefined ? ` fanoutN=${d.n} members=${JSON.stringify(d.members)}` : ''));
if (!disc.length) console.log('  (none — root is an un-instrumented fleet relay; see resolution check below)');

console.log('\n-- resolution --');
console.log('SUB actually pinned to (greedy) :', subPin, subHasRole ? '(SUB SELF-ROOTED)' : '');
console.log('PUB-path resolves on SUB node   :', subPubResolve, '  ← same node, SUB-pin vs PUB-resolve');
console.log('PUB-path resolves on PUB node   :', pubPubResolve, pubHasRole ? '(PUB SELF-ROOTED)' : '');

console.log('\n-- verdict --');
const subMembers = disc.filter((d) => d.ev === 'root-members');
if (subHasRole && pubHasRole) console.log('MECHANISM 1 (self-root split): both SUB and PUB became root on the same topicId.');
if (subPin && subPubResolve && subPin !== subPubResolve && subPubResolve.indexOf('ERR') < 0)
  console.log('MECHANISM 2 (asymmetric resolution): on the SUB node, its greedy SUB pin != its lookup-assisted PUB resolution — same snapshot, different algorithm.');
if (subPubResolve && pubPubResolve && subPubResolve === pubPubResolve && subPin && subPin !== subPubResolve)
  console.log('NOTE: both nodes AGREE on the true root via lookup, but the SUB pinned elsewhere → the SUB path is not using lookup-assist (registration lands off the true root).');
if (subMembers.length) {
  const anyHasSub = subMembers.some((m) => (m.members || []).some((k) => k === S(sub.nodeId)));
  console.log(`root-members captured: subscriber ${anyHasSub ? 'IS' : 'is NOT'} in the true root's live fanout set` + (anyHasSub ? '' : ' → missed live fanout (install/ACK ordering)'));
}

for (const p of [sub, pub]) { try { await p.close(); } catch { /* */ } }
process.exit(0);
