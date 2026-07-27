// =====================================================================
// smoke_dual_key.mjs — authorship is signWith-only; the node/transport
// key NEVER signs a publish (the LEAK-transport-signed guard), v0.3.
//
// v0.3 removed the dual-key default-signer model entirely: there is NO
// publishIdentity constructor arg and NO default signer. A peer holds a
// NODE identity (its connection/transport keypair, whose pubkey forms the
// nodeId) that signs the handshake/routing — but it must never sign a
// publish. Authorship is supplied per-publish via { signWith: <author> }
// (an AUTHOR identity). This test pins the key-separation invariant:
//
//   1. an author-signed publish ⇒ env.signerPubkey === author.authorId
//   2. the node/transport key is NEVER the signer, and no node-key
//      material (pubkey or nodeId) ever appears in the envelope (unlinkability)
//   3. omitting signWith ⇒ PublishError(PUBLISH_NO_PUBLISH_IDENTITY)
//      (no silent fallback to the node key, no silent anonymity)
//   4. many authors through one peer (per-call { signWith })
//   5. the signed envelope verifies (signature ok + msgId recomputes)
//   6. durable authorship across a node-id rotation: a fresh node identity,
//      the SAME author ⇒ same signerPubkey (recognizable author)
//   7. an explicit anonymous publish ({ signWith: ANONYMOUS }) is unsigned
//   8. a bad signWith (no private key) ⇒ PublishError, never an unsigned publish
//
//   node test/smoke_dual_key.mjs
// =====================================================================
import { AxonaPeer, ANONYMOUS } from '../src/dht/AxonaPeer.js';
import { createNodeIdentity, createAuthorIdentity } from '../src/identity/index.js';
import { verifyEnvelope, computeMsgId } from '../src/pubsub/envelope.js';
import { PublishError, ErrorCodes } from '../src/errors.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };

// Minimal AxonaManager mock: captures the serialized envelope per publish.
function mockManager(nodeId) {
  let n = 0;
  return {
    nodeId, published: [],
    pubsubPublish(topicId, json, meta) { const id = `${nodeId}:${++n}`; this.published.push({ topicId, json, meta }); return id; },
    pubsubSubscribe() {}, pubsubUnsubscribe() {}, onPubsubDelivery() {},
    _lastSeenTsByTopic: new Map(),
  };
}
function makePeer({ node }) {
  const engine = { onEvent: () => () => {}, simEpoch: 0 };
  const am     = mockManager(node.id);
  const peer   = new AxonaPeer({ engine, node: { id: BigInt('0x' + node.id), alive: true }, axonaManager: am, nodeIdentity: node });
  return { peer, am };
}
const lastEnv = (am) => JSON.parse(am.published[am.published.length - 1].json);

const SF    = { lat: 37.77, lng: -122.42 };
const TOPIC = { region: 'useast', name: 'feed' };

async function main() {
  console.log('authorship via signWith — node/transport key never signs (v0.3)');

  const node    = await createNodeIdentity(SF);    // the connection/transport identity
  const authorA = await createAuthorIdentity();    // a persistent author persona
  const authorB = await createAuthorIdentity();    // a second author, same peer

  // ── 1. author-signed publish ⇒ signerPubkey = the Author ID ──
  {
    const { peer, am } = makePeer({ node });
    await peer.pub(TOPIC, { hello: 1 }, { signWith: authorA });
    const env = lastEnv(am);
    check('1. signerPubkey = author A’s Author ID', env.signerPubkey === authorA.authorId);
    check('1. signerPubkey ≠ node/transport pubkey', env.signerPubkey !== node.pubkeyHex);
  }

  // ── 2. node-key material never appears in the envelope (unlinkability) ──
  {
    const { peer, am } = makePeer({ node });
    await peer.pub(TOPIC, { e: 5 }, { signWith: authorA });
    const json = am.published[0].json;
    check('2. envelope carries the author pubkey', json.includes(authorA.authorId));
    check('2. envelope contains NO node-key material (pubkey or nodeId)',
      !json.includes(node.pubkeyHex) && !json.includes(node.id));
  }

  // ── 3. omitting signWith ⇒ refused (the transport key must not sign) ──
  {
    const { peer } = makePeer({ node });
    let err = null;
    try { await peer.pub(TOPIC, { c: 3 }); } catch (e) { err = e; }
    check('3. no signer ⇒ PublishError(PUBLISH_NO_PUBLISH_IDENTITY)',
      err instanceof PublishError && err.code === ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY);
  }

  // ── 4. many authors through one peer (per-call signWith) ──
  {
    const { peer, am } = makePeer({ node });
    await peer.pub(TOPIC, { a: 1 }, { signWith: authorA });
    await peer.pub(TOPIC, { b: 2 }, { signWith: authorB });
    const e1 = JSON.parse(am.published[0].json), e2 = JSON.parse(am.published[1].json);
    check('4. first publish signed by author A', e1.signerPubkey === authorA.authorId);
    check('4. second publish signed by author B', e2.signerPubkey === authorB.authorId);
    check('4. two distinct authors from one peer', e1.signerPubkey !== e2.signerPubkey);
  }

  // ── 5. the signed envelope verifies (signature + msgId) ──
  {
    const { peer, am } = makePeer({ node });
    await peer.pub(TOPIC, { d: 4 }, { signWith: authorA });
    const env = lastEnv(am);
    const res = await verifyEnvelope(env);
    check('5. verifyEnvelope ok for the author-signed envelope', res?.ok === true);
    const recomputed = await computeMsgId({ publisher: env.signerPubkey, message: env.message });
    check('5. msgId recomputes from (signerPubkey, message)', recomputed === env.msgId);
  }

  // ── 6. durable authorship across a node-id rotation ──
  {
    const newNode = await createNodeIdentity(SF);    // simulate a restart: fresh node id
    check('6. node id rotated', newNode.id !== node.id);
    const { peer, am } = makePeer({ node: newNode });
    await peer.pub(TOPIC, { f: 6 }, { signWith: authorA });
    const env = lastEnv(am);
    check('6. same author ⇒ same signerPubkey after node rotation', env.signerPubkey === authorA.authorId);
  }

  // ── 7. explicit anonymous publish is unsigned ──
  {
    const { peer, am } = makePeer({ node });
    await peer.pub(TOPIC, { c: 3 }, { signWith: ANONYMOUS });
    check('7. signWith: ANONYMOUS ⇒ unsigned (no signerPubkey)', lastEnv(am).signerPubkey == null);
  }

  // ── 8. a bad signWith never silently downgrades to unsigned ──
  {
    const { peer } = makePeer({ node });
    let err = null;
    try { await peer.pub(TOPIC, { g: 7 }, { signWith: { pubkeyHex: 'deadbeef' } }); }  // no privateKey
    catch (e) { err = e; }
    check('8. invalid signWith ⇒ PublishError(PUBLISH_SIGN_FAILED)',
      err instanceof PublishError && err.code === ErrorCodes.PUBLISH_SIGN_FAILED);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
