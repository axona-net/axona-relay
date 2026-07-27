// =====================================================================
// smoke_addressing.js — verify 264-bit ID math: hex roundtrip, S2-prefix
//                       composition, XOR distance, clz, deriveTopicId.
// Run: node test/smoke_addressing.js
// =====================================================================

import {
  ID_BITS,
  HASH_BITS,
  S2_BITS,
  HEX_CHARS,
  MAX_ID,
  MAX_HASH,
  MAX_S2,
  toHex,
  fromHex,
  isHexId,
  randomU256,
  assembleId,
  extractS2Prefix,
  extractHash,
  s2PrefixOfHex,
  xorDistance,
  stratumOf,
  clz264,
} from '../src/utils/hexid.js';

import { deriveTopicId } from '../src/pubsub/post.js';
import { resolveRegion } from '../src/utils/region-names.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function testConstants() {
  console.log('\n── constants ──');
  check('ID_BITS = 264',   ID_BITS === 264);
  check('HASH_BITS = 256', HASH_BITS === 256);
  check('S2_BITS = 8',     S2_BITS === 8);
  check('HEX_CHARS = 66',  HEX_CHARS === 66);
  check('MAX_ID = 2^264 - 1',     MAX_ID === (1n << 264n) - 1n);
  check('MAX_HASH = 2^256 - 1',   MAX_HASH === (1n << 256n) - 1n);
  check('MAX_S2 = 255',           MAX_S2 === 255);
}

function testHexRoundtrip() {
  console.log('\n── toHex / fromHex roundtrip ──');
  check('toHex(0n) is 66 zero chars', toHex(0n) === '0'.repeat(66));
  check('toHex(1n) is 65 zeros + 1', toHex(1n) === '0'.repeat(65) + '1');
  check('toHex(MAX_ID) is 66 f chars', toHex(MAX_ID) === 'f'.repeat(66));
  check('fromHex(toHex(x)) === x for random sample',
    [0n, 1n, 0xdeadbeefn, MAX_ID, (1n << 263n)].every(x => fromHex(toHex(x)) === x));
  check('fromHex accepts uppercase',
    fromHex('A'.repeat(66)) === BigInt('0x' + 'a'.repeat(66)));
  let threw = false;
  try { toHex(MAX_ID + 1n); } catch { threw = true; }
  check('toHex rejects out-of-range', threw);
  threw = false;
  try { fromHex('abc'); } catch { threw = true; }
  check('fromHex rejects wrong length', threw);
  threw = false;
  try { fromHex('z'.repeat(66)); } catch { threw = true; }
  check('fromHex rejects non-hex chars', threw);
}

function testIsHexId() {
  console.log('\n── isHexId ──');
  check('valid 66-char hex',         isHexId('0'.repeat(66)));
  check('valid mixed-case 66-char',  isHexId('AbCdEf' + '0'.repeat(60)));
  check('wrong length rejected',     !isHexId('abc'));
  check('non-string rejected',       !isHexId(123n));
  check('non-hex chars rejected',    !isHexId('z'.repeat(66)));
}

function testRandom() {
  console.log('\n── randomU256 ──');
  const a = randomU256();
  const b = randomU256();
  check('returns bigint',           typeof a === 'bigint');
  check('within 256-bit range',     a >= 0n && a <= MAX_HASH);
  check('two draws differ',         a !== b);
}

function testAssembleExtract() {
  console.log('\n── assemble / extract ──');
  const id = assembleId(42, 0xdeadbeefcafebaben);
  check('extractS2Prefix(assembled) === 42', extractS2Prefix(id) === 42);
  check('extractHash(assembled) === hash',   extractHash(id) === 0xdeadbeefcafebaben);
  check('round-trip via hex',
    extractS2Prefix(fromHex(toHex(id))) === 42 &&
    extractHash(fromHex(toHex(id))) === 0xdeadbeefcafebaben);

  // Boundary: prefix = 255, max hash
  const boundary = assembleId(255, MAX_HASH);
  check('boundary id = MAX_ID', boundary === MAX_ID);
  check('boundary prefix = 255', extractS2Prefix(boundary) === 255);

  // s2PrefixOfHex shortcut
  check('s2PrefixOfHex shortcut matches',
    s2PrefixOfHex(toHex(id)) === 42);

  // Reject bad inputs
  let threw = false;
  try { assembleId(256, 0n); } catch { threw = true; }
  check('assembleId rejects s2Prefix > 255', threw);
  threw = false;
  try { assembleId(0, MAX_HASH + 1n); } catch { threw = true; }
  check('assembleId rejects hash > 2^256-1', threw);
}

function testXorDistance() {
  console.log('\n── xorDistance ──');
  check('xorDistance(a, a) === 0n',
    xorDistance(123n, 123n) === 0n);
  check('xorDistance(a, b) === xorDistance(b, a)',
    xorDistance(0xabcdn, 0x1234n) === xorDistance(0x1234n, 0xabcdn));
  check('xorDistance(0, x) === x',
    xorDistance(0n, 0xdeadbeefn) === 0xdeadbeefn);
}

function testClz264() {
  console.log('\n── clz264 ──');
  check('clz264(0n) === 264', clz264(0n) === 264);
  check('clz264(1n) === 263', clz264(1n) === 263);
  check('clz264(MAX_ID) === 0', clz264(MAX_ID) === 0);
  check('clz264(1n << 263n) === 0', clz264(1n << 263n) === 0);
  check('clz264(1n << 100n) === 163', clz264(1n << 100n) === (264 - 1 - 100));
  check('clz264(0xffn) === 256', clz264(0xffn) === 256);
  check('clz264(0xff << 256) hits top 8-bit slot', clz264(0xffn << 256n) === 0);
  check('clz264(0x80n << 256n) === 0', clz264(0x80n << 256n) === 0);
  check('clz264(0x01n << 256n) === 7', clz264(0x01n << 256n) === 7);

  // Stratum helper
  const a = (1n << 263n) | 0xdeadn;
  const b = (1n << 263n) | 0xbeefn;
  // a and b agree on the MSB, differ in low bits; stratum should be > 0
  check('stratumOf identical → 263', stratumOf(a, a) === 263);
  check('stratumOf differing low bits is high (≥248)',
    stratumOf(a, b) >= 248);
}

async function testDeriveTopicId() {
  console.log('\n── deriveTopicId (v0.3 structured descriptor) ──');
  const OWNER = 'a'.repeat(64);   // 64-hex Author ID

  // Open topic: region byte + name-scoped hash → 66-char hex.
  const tid1 = await deriveTopicId({ region: 'useast', name: 'cats' });
  check('returns 66-char hex',
    typeof tid1 === 'string' && tid1.length === 66 && /^[0-9a-f]+$/.test(tid1));
  check('top 2 hex chars = region byte (useast)',
    tid1.slice(0, 2) === resolveRegion('useast').toString(16).padStart(2, '0'));

  const tid2 = await deriveTopicId({ region: 'useast', name: 'cats' });
  check('deterministic (same descriptor → same id)', tid1 === tid2);

  const tid3 = await deriveTopicId({ region: 'useast', name: 'dogs' });
  check('different topic name → different id', tid1 !== tid3);

  // Owner-only vs open are distinct topics (write policy folded into the id).
  const owned = await deriveTopicId({ region: 'useast', owner: OWNER, name: 'feed', write: 'owner' });
  const open  = await deriveTopicId({ region: 'useast', owner: OWNER, name: 'feed', write: 'open' });
  check('write policy changes the topic id', owned !== open);

  // Region is never derived from the author: region omitted (even with an owner)
  // throws unless the caller supplies a selfRegion (the publisher's node region).
  let kdThrew = false;
  try { await deriveTopicId({ owner: OWNER, name: 'profile', write: 'owner' }); } catch { kdThrew = true; }
  check('region omitted throws — never author-derived', kdThrew);
  const withSelf = await deriveTopicId({ owner: OWNER, name: 'profile', write: 'owner' }, 0x89);
  check('selfRegion fallback sets the region byte', withSelf.slice(0, 2) === '89');

  // Reject bad inputs
  let threw = false;
  try { await deriveTopicId({ name: 'cats' }); } catch { threw = true; }
  check('rejects open topic without a region', threw);

  // write is keyed on owner presence (kernel ≥3.2.0):
  const OWN = 'a'.repeat(64);
  // no owner → write ignored → open (does not throw)
  const noOwnerOwnerWrite = await deriveTopicId({ region: 'useast', name: 'x', write: 'owner' });
  const plainOpen         = await deriveTopicId({ region: 'useast', name: 'x' });
  check('no owner: write:\'owner\' ignored → open', noOwnerOwnerWrite === plainOpen);
  // owner + omitted write defaults to owner-only
  const ownerDefault  = await deriveTopicId({ region: 'useast', owner: OWN, name: 'feed' });
  const ownerExplicit = await deriveTopicId({ region: 'useast', owner: OWN, name: 'feed', write: 'owner' });
  check('owner + no write === owner + write:\'owner\'', ownerDefault === ownerExplicit);
  // owner + write:'open' is a distinct topic
  const ownerOpen = await deriveTopicId({ region: 'useast', owner: OWN, name: 'feed', write: 'open' });
  check('owner + write:\'open\' is a distinct id', ownerOpen !== ownerDefault);
}

async function main() {
  console.log('Axona 264-bit addressing smoke');
  testConstants();
  testHexRoundtrip();
  testIsHexId();
  testRandom();
  testAssembleExtract();
  testXorDistance();
  testClz264();
  await testDeriveTopicId();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
