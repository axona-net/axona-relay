// smoke_author_class.mjs — author-class attestation: build/verify, tamper, binding,
// topic derivation. Pure crypto + addressing; no network.
import {
  createAuthorIdentity, deriveTopicId,
  authorClassTopic, buildAuthorClass, verifyAuthorClass,
  AUTHOR_CLASS_KIND, AUTHOR_CLASS_NAME, AUTHOR_CLASS_REGION,
} from '../src/index.js';

let n = 0, fail = 0;
const ok = (name, cond) => { n++; if (!cond) { fail++; console.error('FAIL', name); } else console.log('ok', name); };

const alice = await createAuthorIdentity({ extractable: true });
const bob   = await createAuthorIdentity({ extractable: true });

// 1. build + verify round-trip
const att = await buildAuthorClass({ class: 'agent', operator: 'ed25519:op', label: 'mcp peer', signWith: alice });
ok('kind tag', att.kind === AUTHOR_CLASS_KIND);
ok('class carried', att.class === 'agent');
ok('author == signer', att.author === alice.pubkeyHex.toLowerCase());
ok('signature present', typeof att.signature === 'string' && att.signature.startsWith('ed25519:'));
const v = await verifyAuthorClass(att);
ok('verify ok', v.ok && v.class === 'agent' && v.operator === 'ed25519:op' && v.label === 'mcp peer');

// 2. tamper: flip class after signing → verify fails
const tampered = { ...att, class: 'human' };
ok('tampered class rejected', !(await verifyAuthorClass(tampered)).ok);

// 3. tamper: swap author to bob (keep alice's sig) → fails
ok('swapped author rejected', !(await verifyAuthorClass({ ...att, author: bob.pubkeyHex.toLowerCase() })).ok);

// 4. expectedAuthor binding (inline-echo rule): wrong expected → fail, right → ok
ok('expectedAuthor mismatch rejected', !(await verifyAuthorClass(att, { expectedAuthor: bob.pubkeyHex })).ok);
ok('expectedAuthor match ok', (await verifyAuthorClass(att, { expectedAuthor: alice.pubkeyHex })).ok);

// 5. bad class at build time throws
let threw = false; try { await buildAuthorClass({ class: 'robot', signWith: alice }); } catch { threw = true; }
ok('bad class throws', threw);

// 6. profile topic: pinned region + owner-only + derivable from Author ID; resolves to a topic id
const t = authorClassTopic(alice.pubkeyHex);
ok('topic region pinned', t.region === AUTHOR_CLASS_REGION && t.name === AUTHOR_CLASS_NAME);
ok('topic owner == author', t.owner === alice.pubkeyHex.toLowerCase());
const idA = await deriveTopicId(t);
const idA2 = await deriveTopicId(authorClassTopic(alice.pubkeyHex));   // deterministic from the key alone
const idB = await deriveTopicId(authorClassTopic(bob.pubkeyHex));
ok('topic id deterministic from Author ID', idA === idA2);
ok('distinct authors → distinct topics', idA !== idB);
// owner present ⇒ write defaults to 'owner' ⇒ only the author may publish (kernel ingress enforces)
const open = await deriveTopicId({ region: AUTHOR_CLASS_REGION, name: AUTHOR_CLASS_NAME });
ok('owner-only topic ≠ same-name open topic', idA !== open);

// 7. unstated: a non-attestation verifies as not-ok (caller treats as unstated)
ok('garbage → not ok', !(await verifyAuthorClass({ kind: 'nope' })).ok);

// 8. operator countersignature (v1.1): bob is alice's operator and vouches back
const co = await buildAuthorClass({ class: 'agent', operatorSignWith: bob, label: 'run by bob', signWith: alice });
ok('operator == operator key', co.operator === bob.pubkeyHex.toLowerCase());
ok('operatorProof present', typeof co.operatorProof === 'string' && co.operatorProof.startsWith('ed25519:'));
const cv = await verifyAuthorClass(co);
ok('countersigned verifies + operatorVerified', cv.ok && cv.operatorVerified === true && cv.operator === bob.pubkeyHex.toLowerCase());
// self-asserted operator (string, no proof) → ok but operatorVerified:false
const sa = await buildAuthorClass({ class: 'agent', operator: 'ed25519:some-handle', signWith: alice });
const sav = await verifyAuthorClass(sa);
ok('self-asserted operator → operatorVerified:false', sav.ok && sav.operatorVerified === false && sav.operator === 'ed25519:some-handle');
// tampered operatorProof → whole attestation rejected
ok('bad operatorProof rejected', !(await verifyAuthorClass({ ...co, operatorProof: 'ed25519:' + '00'.repeat(64) })).ok);
// swapped operator (carol's key) but bob's proof → rejected (author sig + proof both bind operator)
const carol = await createAuthorIdentity({ extractable: true });
ok('swapped operator rejected', !(await verifyAuthorClass({ ...co, operator: carol.pubkeyHex.toLowerCase() })).ok);

// 9. infra classes: bridge + relay are valid, build/verify round-trip
for (const cls of ['service', 'bridge', 'relay']) {
  const a = await buildAuthorClass({ class: cls, label: `${cls} node`, signWith: alice });
  const av = await verifyAuthorClass(a);
  ok(`${cls} class builds + verifies`, av.ok && av.class === cls);
}
// 10. an unknown class still throws at build (closed allow-list) but verifies as
//     not-ok (UNSTATED) on the read side — graceful, never a wrong default
let threw2 = false; try { await buildAuthorClass({ class: 'sensor', signWith: alice }); } catch { threw2 = true; }
ok('unknown class throws at build', threw2);
ok('unknown class on read → UNSTATED (not-ok)', !(await verifyAuthorClass({ ...att, class: 'sensor' })).ok);

console.log(fail ? `\n✗ ${fail}/${n} FAILED` : `\n✓ all ${n} passed`);
process.exit(fail ? 1 : 0);
