// =====================================================================
// smoke_identity.js — verify identity creation, sign/verify, and
//                     dump/load roundtrip via the persistence envelope.
// Run: node test/smoke_identity.js
// =====================================================================

import {
  createNodeIdentity,
  dumpIdentity,
  loadIdentity,
  computeNodeId,
} from '../src/identity/index.js';
import {
  IdentityError,
  ErrorCodes,
} from '../src/errors.js';
import {
  isHexId,
  extractS2Prefix,
  fromHex,
} from '../src/utils/hexid.js';
import { geoCellId } from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const LONDON = { lat: 51.5074, lng: -0.1278 };
const TOKYO  = { lat: 35.6762, lng: 139.6503 };

async function testDerive() {
  console.log('\n── createNodeIdentity ──');
  const id = await createNodeIdentity(LONDON);
  check('returns 66-char hex id',     isHexId(id.id));
  check('pubkey is 32 bytes',          id.pubkey instanceof Uint8Array && id.pubkey.length === 32);
  check('pubkeyHex is 64 chars',       id.pubkeyHex.length === 64);
  check('privateKey is CryptoKey',     id.privateKey instanceof CryptoKey);
  check('region preserved',            id.region.lat === LONDON.lat && id.region.lng === LONDON.lng);
  check('createdAt is recent',         Math.abs(id.createdAt - Date.now()) < 5000);

  // nodeId carries the right S2 prefix.
  const expectedS2 = geoCellId(LONDON.lat, LONDON.lng, 8);
  check("nodeId's S2 prefix = geoCellId(lat,lng,8)",
    extractS2Prefix(fromHex(id.id)) === expectedS2);

  // Two identities in different regions should have different S2 prefixes.
  const id2 = await createNodeIdentity(TOKYO);
  check("Tokyo identity has different S2 prefix than London identity",
    extractS2Prefix(fromHex(id.id)) !== extractS2Prefix(fromHex(id2.id)));

  // Two fresh identities in the SAME region differ (random pubkey).
  const id3 = await createNodeIdentity(LONDON);
  check("two London identities differ (random pubkey)", id.id !== id3.id);
}

async function testNodeIdDeterminism() {
  console.log('\n── computeNodeId determinism ──');
  const id = await createNodeIdentity(LONDON);
  const recomputed = await computeNodeId(id.pubkey, LONDON.lat, LONDON.lng);
  check('recomputed nodeId === stored id', recomputed === id.id);

  // Different region → different id even for the same pubkey.
  const otherRegionId = await computeNodeId(id.pubkey, TOKYO.lat, TOKYO.lng);
  check('same pubkey, different region → different id',
    otherRegionId !== id.id);
}

async function testSignVerify() {
  console.log('\n── sign / verify round-trip ──');
  const id = await createNodeIdentity(LONDON);
  const message = new TextEncoder().encode('hello mesh');
  const sig = await id.sign(message);
  check('sign returns 64-byte signature',
    sig instanceof Uint8Array && sig.length === 64);
  check('own-verify(intact) → true',
    await id.verify(message, sig));

  const tampered = new TextEncoder().encode('hello forge');
  check('own-verify(tampered) → false',
    !(await id.verify(tampered, sig)));
}

async function testDumpLoadRoundtrip() {
  console.log('\n── dump / load round-trip ──');
  const orig = await createNodeIdentity(LONDON);
  const env  = await dumpIdentity(orig);

  // Envelope shape.
  check('envelope.id is 66-char hex',         isHexId(env.id));
  check('envelope.pubkey is 64 hex chars',    env.pubkey.length === 64 && /^[0-9a-f]+$/.test(env.pubkey));
  check('envelope.privkey is base64 string',  typeof env.privkey === 'string' && env.privkey.length > 0);
  check('envelope.region matches',            env.region.lat === LONDON.lat);
  check('envelope.createdAt matches',         env.createdAt === orig.createdAt);

  // JSON survives the roundtrip.
  const json    = JSON.stringify(env);
  const parsed  = JSON.parse(json);
  const reloaded = await loadIdentity(parsed);

  check('reloaded.id === original.id',        reloaded.id === orig.id);
  check('reloaded.pubkeyHex === original.pubkeyHex',
    reloaded.pubkeyHex === orig.pubkeyHex);
  check('reloaded.region matches',
    reloaded.region.lat === orig.region.lat &&
    reloaded.region.lng === orig.region.lng);

  // Reloaded identity can sign and verify against original.
  const msg = new TextEncoder().encode('after reload');
  const sig = await reloaded.sign(msg);
  check('reloaded identity can sign',           sig.length === 64);
  check('signature verifies against original',  await orig.verify(msg, sig));
  check('signature verifies against reloaded',  await reloaded.verify(msg, sig));
}

async function testRejection() {
  console.log('\n── input validation ──');
  let threw = false;
  try { await createNodeIdentity({ lat: 'not-a-number', lng: 0 }); }
  catch (e) { threw = e instanceof IdentityError && e.code === ErrorCodes.IDENTITY_INVALID_FORMAT; }
  check('createNodeIdentity rejects non-numeric region', threw);

  threw = false;
  try { await loadIdentity(null); }
  catch (e) { threw = e instanceof IdentityError; }
  check('loadIdentity rejects null', threw);

  threw = false;
  try { await loadIdentity({ id: 'short' }); }
  catch (e) { threw = e instanceof IdentityError; }
  check('loadIdentity rejects malformed envelope', threw);

  // Tampered envelope: change id but keep pubkey/region.
  const orig = await createNodeIdentity(LONDON);
  const env  = await dumpIdentity(orig);
  threw = false;
  try {
    await loadIdentity({ ...env, id: '00' + 'f'.repeat(64) });
  } catch (e) {
    threw = e instanceof IdentityError && e.code === ErrorCodes.IDENTITY_INVALID_FORMAT;
  }
  check('loadIdentity rejects mismatched id ↔ pubkey/region', threw);
}

async function testNonExtractableKey() {
  console.log('\n── H4: non-extractable signing key ──');
  const id = await createNodeIdentity({ ...LONDON, extractable: false });
  check('derives a usable identity', isHexId(id.id) && id.pubkeyHex.length === 64);
  // The private key still signs…
  const msg = new TextEncoder().encode('hello');
  const sig = await id.sign(msg);
  check('non-extractable key still signs', sig instanceof Uint8Array && sig.length === 64);
  check('its signature verifies', await id.verify(msg, sig));
  // …but it cannot be exported (exfiltrated).
  let exportThrew = false;
  try { await crypto.subtle.exportKey('pkcs8', id.privateKey); }
  catch { exportThrew = true; }
  check('private key cannot be exported (pkcs8)', exportThrew);
  // dumpIdentity therefore refuses (persistence requires extractable).
  let dumpThrew = false;
  try { await dumpIdentity(id); }
  catch (e) { dumpThrew = e instanceof IdentityError; }
  check('dumpIdentity refuses a non-extractable identity', dumpThrew);
  // The default (extractable) identity still dumps fine.
  const persistable = await createNodeIdentity(LONDON);
  const env = await dumpIdentity(persistable);
  check('default identity remains persistable', typeof env.privkey === 'string');
}

async function testLoadIdentityKeyMismatch() {
  console.log('\n── M5: loadIdentity verifies privkey ↔ pubkey ──');
  const a = await dumpIdentity(await createNodeIdentity(LONDON));
  const b = await dumpIdentity(await createNodeIdentity(LONDON));
  // Graft b's private key onto a's id/pubkey/region — internally the
  // nodeId still matches a's pubkey, but the private key is the wrong one.
  const frankenstein = { ...a, privkey: b.privkey };
  let threw = false;
  try { await loadIdentity(frankenstein); }
  catch (e) { threw = e instanceof IdentityError; }
  check('loadIdentity rejects privkey that does not match pubkey', threw);
  // Sanity: an intact envelope still loads.
  const ok = await loadIdentity(a);
  check('intact envelope still loads', ok.id === a.id);
}

async function main() {
  console.log('Axona identity smoke');
  await testDerive();
  await testNodeIdDeterminism();
  await testSignVerify();
  await testDumpLoadRoundtrip();
  await testNonExtractableKey();
  await testLoadIdentityKeyMismatch();
  await testRejection();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
