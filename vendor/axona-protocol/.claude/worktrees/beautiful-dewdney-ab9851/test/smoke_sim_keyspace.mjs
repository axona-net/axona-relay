// =====================================================================
// smoke_sim_keyspace.mjs — the SHRUNK simulator keyspace profile.
//
// The simulator drives the real kernel but at a small ID width so churn
// tests scale to many nodes. `configureKeyspace({ hashBits: 64 })` (set
// ONCE, before any identity is minted) shrinks the hash component
// 256 → 64 bits:
//
//     nodeId / topicId = 8-bit region ‖ 64-bit hash  = 72 bits / 18 hex
//     authorId         =                 64-bit hash  = 64 bits / 16 hex
//
// Production (the default profile, NOT configured here) stays 264-bit;
// that path is covered by every other smoke. This file proves the kernel
// mints identities, derives topics, and ROUTES + DELIVERS pub/sub at the
// shrunk width — including the decision-(B) relaxed verification: a
// 64-bit authorId can't be a verifiable 256-bit Ed25519 pubkey, so the
// envelope verifier SKIPS the crypto check under the sim profile while
// keeping structure + msgId + owner-write-policy enforcement.
//
// Run: node test/smoke_sim_keyspace.mjs
// =====================================================================

import { configureKeyspace, getKeyspace, HEX_CHARS, AUTHOR_HEX_CHARS, ID_BITS, AUTHOR_ID_BITS, AUTH_VERIFY_RELAXED }
  from '../src/utils/hexid.js';

// MUST configure BEFORE importing/minting anything that reads the profile.
configureKeyspace({ hashBits: 64 });

const { AxonaManager }      = await import('../src/pubsub/AxonaManager.js');
const { buildEnvelope, verifyEnvelope } = await import('../src/pubsub/envelope.js');
const { deriveTopicIdBig }  = await import('../src/pubsub/post.js');
const { createNodeIdentity, createAuthorIdentity } = await import('../src/identity/index.js');
const { regionCenter }      = await import('../src/utils/region-names.js');
const __LOC = regionCenter('useast');  // region-lock: co-region mesh nodes with the 'useast' topics

let passed = 0, failed = 0;
const check = (label, cond) => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
};
const idHex = (big) => big.toString(16).padStart(HEX_CHARS, '0');

// ── routing fabric (closest-alive terminus, same as the fundamental gate) ──
class Fabric {
  constructor() { this.nodes = new Map(); this.queue = []; this.clock = Date.now(); }
  now() { return this.clock; }
  addNode(idBig) {
    const handlers = new Map();
    const self = this;
    const dht = {
      getSelfId: () => idBig,
      onRoutedMessage: (type, h) => handlers.set(type, h),
      routeMessage: (target, type, payload) => {
        const dest = self._closestAlive(target);
        if (dest === null) return;
        self.queue.push({ dest, type, payload, meta: { targetId: target, isTerminal: true, hopCount: 1, fromId: idHex(idBig) } });
      },
    };
    const am = new AxonaManager({ dht, now: () => self.clock, renewMs: 60_000, dropMs: 180_000 });
    const rec = { id: idBig, am, handlers, alive: true, received: [] };
    am.onPubsubDelivery((topicId, json, msgId, ts) => rec.received.push({ topicId, json, msgId, ts }));
    this.nodes.set(idBig, rec);
    return rec;
  }
  _closestAlive(target) {
    let best = null, bestD = null;
    for (const [id, n] of this.nodes) {
      if (!n.alive) continue;
      const d = id ^ target;
      if (bestD === null || d < bestD) { bestD = d; best = id; }
    }
    return best;
  }
  async settle(maxJobs = 100000) {
    let i = 0;
    while (this.queue.length) {
      if (++i > maxJobs) throw new Error('fabric.settle: did not converge');
      const job = this.queue.shift();
      const n = this.nodes.get(job.dest);
      if (!n || !n.alive) continue;
      const h = n.handlers.get(job.type);
      if (!h) continue;
      await h(job.payload, job.meta);
    }
  }
}

let SEQ = 1;
async function signedJson(descriptor, message, author) {
  const env = await buildEnvelope({ topic: descriptor, message, seq: SEQ++, identity: author, sign: !!author });
  return { json: JSON.stringify(env), msgId: env.msgId, env };
}
async function makeNodes(fab, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const id = await createNodeIdentity({ ...__LOC });
    out.push(fab.addNode(BigInt('0x' + id.id)));
  }
  return out;
}

// ── tests ──────────────────────────────────────────────────────────────

async function testProfile() {
  console.log('\n── the shrunk profile is active (72-bit node/topic, 64-bit author) ──');
  const ks = getKeyspace();
  check('idBits = 72',              ID_BITS === 72 && ks.idBits === 72);
  check('authorIdBits = 64',       AUTHOR_ID_BITS === 64 && ks.authorIdBits === 64);
  check('hexChars = 18',           HEX_CHARS === 18 && ks.hexChars === 18);
  check('authorHexChars = 16',     AUTHOR_HEX_CHARS === 16 && ks.authorHexChars === 16);
  check('NOT the production default', ks.isProductionDefault === false);
  check('AUTH_VERIFY_RELAXED is on', AUTH_VERIFY_RELAXED === true);
}

async function testIdentityWidths() {
  console.log('\n── identities + topics mint at the shrunk width ──');
  const node = await createNodeIdentity({ lat: 37.7, lng: -122.4 });
  check('nodeId is 18 hex chars', typeof node.id === 'string' && node.id.length === 18);
  const author = await createAuthorIdentity();
  check('authorId is 16 hex chars', typeof author.authorId === 'string' && author.authorId.length === 16);
  check('pubkeyHex is still the full 64-hex Ed25519 key', author.pubkeyHex.length === 64);

  const openId  = await deriveTopicIdBig({ region: 'useast', owner: null, name: 'lobby', write: 'open' });
  check('open topicId is 18 hex chars', idHex(openId).length === 18);
  // owner topics key off the AUTHOR ID (the public id), not the raw pubkey
  const ownId   = await deriveTopicIdBig({ region: 'useast', owner: author.authorId, name: 'wall', write: 'owner' });
  check('owner topicId is 18 hex chars', idHex(ownId).length === 18);
  check('open and owner ids differ', openId !== ownId);
}

async function testRelaxedVerify() {
  console.log('\n── relaxed verification: a signed 64-bit-author envelope verifies ──');
  const author = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'verify', write: 'open' };
  const { env } = await signedJson(desc, { hi: 1 }, author);
  check('signerPubkey is the 16-hex authorId (not the 64-hex pubkey)',
        env.signerPubkey === author.authorId && env.signerPubkey.length === 16);
  const v = await verifyEnvelope(env);
  check('verifyEnvelope ok (crypto check skipped, structure+msgId kept)', v.ok === true && v.signed === true);
  // tamper the message → msgId recompute still catches it even with crypto relaxed
  const tampered = { ...env, message: { hi: 999 } };
  const vt = await verifyEnvelope(tampered);
  check('a tampered message is still rejected (bad msgid)', vt.ok === false);
}

async function testDelivery() {
  console.log('\n── pub/sub routes + delivers at 72-bit (1 root, 5 subs, ×15) ──');
  const author = await createAuthorIdentity();
  let allFive = 0;
  const RUNS = 15;
  for (let run = 0; run < RUNS; run++) {
    const fab = new Fabric();
    const nodes = await makeNodes(fab, 8);
    const desc = { region: 'useast', owner: null, name: `room-${run}`, write: 'open' };
    const topicId = await deriveTopicIdBig(desc);
    const subs = nodes.slice(0, 5);
    for (const s of subs) s.am.pubsubSubscribe(topicId);
    await fab.settle();
    const { json } = await signedJson(desc, { hello: run }, author);
    nodes[7].am.pubsubPublish(topicId, json);
    await fab.settle();
    const got = subs.filter(s => s.received.length === 1).length;
    if (got === 5) allFive++; else console.log(`    run ${run}: only ${got}/5`);
  }
  check(`all 5 received in ${RUNS}/${RUNS} configs`, allFive === RUNS);
}

async function testOwnerPolicy() {
  console.log('\n── owner-only write policy holds at the shrunk width ──');
  const owner    = await createAuthorIdentity();
  const stranger = await createAuthorIdentity();
  const fab = new Fabric();
  const nodes = await makeNodes(fab, 6);
  const desc = { region: 'useast', owner: owner.authorId, name: 'wall', write: 'owner' };
  const topicId = await deriveTopicIdBig(desc);
  const sub = nodes[1];
  sub.am.pubsubSubscribe(topicId);
  await fab.settle();

  // stranger → dropped at root (signerPubkey=stranger.authorId ≠ owner.authorId)
  const bad = await signedJson(desc, { evil: 1 }, stranger);
  nodes[0].am.pubsubPublish(topicId, bad.json);
  await fab.settle();
  check('non-owner publish to owner-only topic dropped', sub.received.length === 0);

  // owner → delivered
  const good = await signedJson(desc, { ok: 1 }, owner);
  nodes[0].am.pubsubPublish(topicId, good.json);
  await fab.settle();
  check('owner publish to owner-only topic delivered', sub.received.length === 1);
}

async function testFastIdentity() {
  console.log('\n── fast (no-keygen) sim node identity ──');
  const a = await createNodeIdentity({ lat: 37.7, lng: -122.4, fast: true });
  check('fast nodeId is 18 hex chars', typeof a.id === 'string' && a.id.length === 18);
  check('fast identity has no private key', a.privateKey === null);
  check('fast marker set', a.fast === true);
  // region byte still tracks geography (top byte present)
  check('fast nodeId carries a region byte', /^[0-9a-f]{2}/.test(a.id));
  // uniqueness across many mints
  const ids = new Set();
  for (let i = 0; i < 500; i++) ids.add((await createNodeIdentity({ lat: 30, lng: -100, fast: true })).id);
  check('500 fast ids are unique', ids.size === 500);
  // routes/delivers: build a small mesh of fast-identity peers via the Fabric
  const fab = new Fabric();
  for (let i = 0; i < 8; i++) {
    const id = await createNodeIdentity({ ...__LOC, fast: true });
    fab.addNode(BigInt('0x' + id.id));
  }
  const author = await createAuthorIdentity();
  const desc = { region: 'useast', owner: null, name: 'fast-room', write: 'open' };
  const topicId = await deriveTopicIdBig(desc);
  const subs = [...fab.nodes.values()].slice(0, 5);
  for (const s of subs) s.am.pubsubSubscribe(topicId);
  await fab.settle();
  const { json } = await signedJson(desc, { hi: 1 }, author);
  [...fab.nodes.values()][7].am.pubsubPublish(topicId, json);
  await fab.settle();
  check('fast-identity mesh delivers to all 5 subs', subs.filter(s => s.received.length === 1).length === 5);
}

async function main() {
  console.log('Axona — sim-configurable keyspace (72-bit/64-bit shrunk profile)');
  await testProfile();
  await testIdentityWidths();
  await testFastIdentity();
  await testRelaxedVerify();
  await testDelivery();
  await testOwnerPolicy();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
