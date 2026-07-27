// =====================================================================
// smoke_pubsub_posthash.js — the replay-cache / pull / kill key (postHash)
//   must equal the verified content hash of the envelope.
//
// postHash is what pull(msgId), kill(msgId)/tombstones, and the replay cache
// all index on — but it rides as an UNSIGNED sibling wire field, outside the
// signed bytes. Without reconciliation, a publisher or relay could cache
// content under a postHash that is NOT its true content hash: pull(realMsgId)
// would miss, kill(realMsgId) would never match (an un-killable message), and
// a different message's id could be poisoned. AxonaManager._postHashConsistent
// recomputes computeMsgId({publisher, message}) from the (already
// signature-verified) envelope and drops a PRESENT-but-mismatched postHash.
//
// Honest publishes set postHash = envelope.msgId (AxonaPeer.pub), so honest
// traffic is unaffected. An ABSENT postHash is lenient (the message is simply
// un-addressable — no worse than before, and not a poisoning lever).
//
// Run: node test/smoke_pubsub_posthash.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { buildEnvelope, computeMsgId } from '../src/pubsub/envelope.js';
import { deriveTopicId } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

// v0.3: an envelope's topic is the structured DESCRIPTOR object, not a string.
const TOPIC_DESC = (name) => ({ region: 0x89, owner: null, name, write: 'open' });

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);
const big = (h) => BigInt('0x' + h);
const TOPIC = hex(0x05);

function makeManager() {
  const dht = {
    getSelfId: () => big(hex(0x01)),
    onRoutedMessage: () => {}, onDirectMessage: () => {},
    routeMessage: () => {}, sendDirect: async () => true, findKClosest: undefined,
  };
  return new AxonaManager({ dht });
}

async function main() {
  console.log('pub/sub postHash ↔ verified content-hash reconciliation\n');

  const id = await createAuthorIdentity();
  const signed   = await buildEnvelope({ topic: TOPIC_DESC('news'), message: 'hello', identity: id, sign: true });
  const anon      = await buildEnvelope({ topic: TOPIC_DESC('news'), message: 'hi', sign: false });
  const realSigned = signed.msgId;                       // = computeMsgId({publisher:pub, message})
  const realAnon   = anon.msgId;                         // = computeMsgId({publisher:null, message})
  const FAKE       = 'dead'.repeat(16);                  // 64-hex, not the real hash

  // ── unit matrix: _postHashConsistent ──────────────────────────────
  console.log('── _postHashConsistent matrix ──');
  const am = makeManager();
  check('signed: honest postHash (=msgId) is consistent',
    (await am._postHashConsistent(JSON.stringify(signed), realSigned)) === true);
  check('signed: tampered postHash is REJECTED',
    (await am._postHashConsistent(JSON.stringify(signed), FAKE)) === false);
  check('signed: poisoning another id (postHash = anon.msgId) is REJECTED',
    (await am._postHashConsistent(JSON.stringify(signed), realAnon)) === false);
  check('anonymous: honest postHash is consistent',
    (await am._postHashConsistent(JSON.stringify(anon), realAnon)) === true);
  check('anonymous: tampered postHash is REJECTED',
    (await am._postHashConsistent(JSON.stringify(anon), FAKE)) === false);
  check('absent postHash (null) is lenient — nothing to reconcile',
    (await am._postHashConsistent(JSON.stringify(signed), null)) === true);
  check('absent postHash (undefined) is lenient',
    (await am._postHashConsistent(JSON.stringify(signed), undefined)) === true);
  check('non-envelope json with a present postHash passes (no content hash to bind)',
    (await am._postHashConsistent('raw-non-envelope', FAKE)) === true);
  // sanity: recomputed expectation matches the envelope's own msgId
  check('computeMsgId(signed content) === envelope.msgId',
    (await computeMsgId({ publisher: signed.signerPubkey, message: signed.message })) === realSigned);

  // ── integration: ingress drops a tampered postHash, accepts honest ─
  // v0.3: the routed topicId must equal resolveTopic(env.topic).topicId — the
  // root recomputes it from the SIGNED descriptor and rejects a mismatch.
  console.log('\n── ingress (_onPublishDirect) ──');
  const NEWS_TID = await deriveTopicId(TOPIC_DESC('news'));   // matches `signed`'s descriptor
  {
    const m = makeManager();
    await m._onPublishDirect(
      { topicId: NEWS_TID, publisher: big('a' + id.pubkeyHex), json: JSON.stringify(signed),
        publishId: 'bad:1', publishTs: 1, postHash: FAKE });
    check('tampered-postHash publish NOT cached/promoted', m.axonRoles.get(big(NEWS_TID)) == null);
  }
  {
    const m = makeManager();
    await m._onPublishDirect(
      { topicId: NEWS_TID, publisher: big('a' + id.pubkeyHex), json: JSON.stringify(signed),
        publishId: 'ok:1', publishTs: 1, postHash: realSigned });
    check('honest-postHash publish accepted (role created)', m.axonRoles.get(big(NEWS_TID)) != null);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('fatal:', e); process.exit(2); });
