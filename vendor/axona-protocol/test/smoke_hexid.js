// smoke_hexid.js — identifier math for the Axona keyspace (src/utils/hexid.js).
//
// hexid.js is the highest fan-in util in the kernel (18 importers): every id
// that crosses the JSON wire goes through toHex/fromHex/asId, every routing
// decision through xorDistance/clz264/stratumOf, and the simulator reshapes
// the whole keyspace through configureKeyspace. This smoke pins:
//   1. round-trip encoding at the production 264-bit profile, incl. left
//      zero-padding and mixed-case acceptance;
//   2. validation: isHexId / fromHex / asId accept & reject correctly, and
//      rejects carry the BAD_ID_CODE marker (transport boundaries classify
//      malformed-frame drops by that code, not by message text);
//   3. S2-prefix composition/extraction (assembleId / extractS2Prefix /
//      extractHash / s2PrefixOfHex agree on a known id);
//   4. distance math (xorDistance / clz264 / stratumOf) edges;
//   5. keyspace-profile switching: configureKeyspace({hashBits:64}) reshapes
//      every exported width, then the test RESTORES the production profile
//      ({hashBits:256, regionBits:8}) so it stays hermetic.
//
// Imported as a namespace (import * as H) because the width constants are
// exported LET bindings — the namespace object gives the live values after
// configureKeyspace, where destructured copies would not... actually ESM
// named imports are live too, but the namespace makes the "read the current
// value" intent explicit at every use site.
//
// Run: node test/smoke_hexid.js
import * as H from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
// Returns the thrown error (truthy) if fn throws, undefined otherwise.
const thrown = (fn) => { try { fn(); return undefined; } catch (e) { return e; } };

console.log('hexid — 264-bit identifier math\n');

// ── 1. production profile shape ───────────────────────────────────────
{
  const ks = H.getKeyspace();
  check('default profile is production 264-bit (8b region + 256b hash)',
    ks.regionBits === 8 && ks.hashBits === 256 && ks.idBits === 264,
    JSON.stringify(ks));
  check('hex widths: id=66 chars, authorId=64 chars',
    ks.hexChars === 66 && ks.authorHexChars === 64);
  check('isProductionDefault true, not yet explicitly configured',
    ks.isProductionDefault === true && ks.configured === false);
  check('exported constants agree (ID_BITS/HEX_CHARS/MAX_ID/S2_SHIFT)',
    H.ID_BITS === 264 && H.HEX_CHARS === 66 &&
    H.MAX_ID === (1n << 264n) - 1n && H.S2_SHIFT === 256n);
  check('AUTH_VERIFY_RELAXED is false in production profile',
    H.AUTH_VERIFY_RELAXED === false);
}

// ── 2. round-trip encoding + padding ──────────────────────────────────
{
  check('toHex(0n) pads to 66 zeros', H.toHex(0n) === '0'.repeat(66));
  check('toHex(1n) left-pads leading zeros', H.toHex(1n) === '0'.repeat(65) + '1');
  check('toHex(MAX_ID) is 66 f\'s', H.toHex(H.MAX_ID) === 'f'.repeat(66));

  const samples = [0n, 1n, 255n, (1n << 256n) - 1n, (0xABn << 256n) | 12345n, H.MAX_ID];
  check('fromHex(toHex(x)) === x for boundary values',
    samples.every(x => H.fromHex(H.toHex(x)) === x));

  const h = H.toHex((0xCDn << 256n) | 0xDEADBEEFn);
  check('toHex output is lowercase, stable width', h === h.toLowerCase() && h.length === 66);
  check('fromHex accepts UPPERCASE of the same id (mixed-case tolerant)',
    H.fromHex(h.toUpperCase()) === H.fromHex(h));
  check('toHex(fromHex(UPPER)) normalises back to lowercase',
    H.toHex(H.fromHex(h.toUpperCase())) === h);
}

// ── 3. validation: accept/reject + BAD_ID_CODE ────────────────────────
{
  const good = H.toHex(42n);
  check('isHexId accepts a canonical 66-char id', H.isHexId(good) === true);
  check('isHexId accepts uppercase', H.isHexId(good.toUpperCase()) === true);
  check('isHexId rejects wrong widths (65 and 67 chars)',
    H.isHexId(good.slice(1)) === false && H.isHexId(good + '0') === false);
  check('isHexId rejects non-hex chars', H.isHexId('g'.repeat(66)) === false);
  check('isHexId rejects non-strings', H.isHexId(42n) === false && H.isHexId(null) === false);

  let e = thrown(() => H.fromHex(good.slice(1)));
  check('fromHex rejects wrong width with code AXONA_BAD_ID',
    e instanceof RangeError && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.fromHex('z'.repeat(66)));
  check('fromHex rejects non-hex chars with code AXONA_BAD_ID',
    e instanceof RangeError && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.fromHex(123));
  check('fromHex rejects non-strings with code AXONA_BAD_ID',
    e instanceof TypeError && e.code === H.BAD_ID_CODE);

  check('toHex rejects non-bigint', thrown(() => H.toHex(42)) instanceof TypeError);
  check('toHex rejects negative', thrown(() => H.toHex(-1n)) instanceof RangeError);
  check('toHex rejects > MAX_ID', thrown(() => H.toHex(H.MAX_ID + 1n)) instanceof RangeError);
}

// ── 4. asId — the canonical coercion gate ─────────────────────────────
{
  check('asId is idempotent on an in-range bigint', H.asId(1234n) === 1234n);
  check('asId parses a canonical hex string', H.asId(H.toHex(1234n)) === 1234n);
  check('asId accepts a 0x-prefixed hex string', H.asId('0xff') === 255n);
  check('asId is width-LENIENT: short hex accepted (unlike fromHex)',
    H.asId('ab') === 0xABn);

  let e = thrown(() => H.asId(H.MAX_ID + 1n));
  check('asId rejects out-of-range bigint with AXONA_BAD_ID', !!e && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.asId('f'.repeat(67)));
  check('asId rejects an over-MAX hex string with AXONA_BAD_ID', !!e && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.asId(''));
  check('asId rejects the empty string', !!e && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.asId('not hex'));
  check('asId rejects garbage strings', !!e && e.code === H.BAD_ID_CODE);
  e = thrown(() => H.asId(42));
  check('asId rejects plain numbers', !!e && e.code === H.BAD_ID_CODE);
}

// ── 5. S2 prefix composition / extraction ─────────────────────────────
{
  const s2 = 0xAB, hash = (1n << 255n) | 0xDEADBEEFn;
  const id = H.assembleId(s2, hash);
  check('assembleId places the S2 prefix above the hash',
    id === ((0xABn << 256n) | hash));
  check('extractS2Prefix returns the leading 8 bits', H.extractS2Prefix(id) === s2);
  check('extractHash returns the 256-bit hash component', H.extractHash(id) === hash);
  check('s2PrefixOfHex agrees with extractS2Prefix on the hex form',
    H.s2PrefixOfHex(H.toHex(id)) === s2);
  check('s2PrefixOfHex reads first 2 hex chars (leading-zero prefix ok)',
    H.s2PrefixOfHex(H.toHex(H.assembleId(0x05, 7n))) === 0x05);
  // NOTE: s2PrefixOfHex hardcodes a 2-hex-char (8-bit) read — correct for
  // every profile where S2_BITS is 8 (the production default and all current
  // sim profiles); it would misread under a hypothetical non-8-bit regionBits
  // override. Documented, not exercised: no caller configures such a profile.

  check('assembleId rejects s2Prefix out of [0,255]',
    !!thrown(() => H.assembleId(256, 0n)) && !!thrown(() => H.assembleId(-1, 0n)));
  check('assembleId rejects an oversized hash',
    !!thrown(() => H.assembleId(0, H.MAX_HASH + 1n)));
  check('s2PrefixOfHex rejects a non-id string', !!thrown(() => H.s2PrefixOfHex('abcd')));
}

// ── 6. distance math ──────────────────────────────────────────────────
{
  const a = (0x11n << 256n) | 5n, b = (0x22n << 256n) | 9n;
  check('xorDistance(a,a) === 0', H.xorDistance(a, a) === 0n);
  check('xorDistance symmetric', H.xorDistance(a, b) === H.xorDistance(b, a));
  check('clz264(0n) === ID_BITS (264)', H.clz264(0n) === 264);
  check('clz264(1n) === 263', H.clz264(1n) === 263);
  check('clz264(MAX_ID) === 0 (top bit set)', H.clz264(H.MAX_ID) === 0);
  check('clz264(1n << 255n) === 8 (hash MSB is below the S2 prefix)',
    H.clz264(1n << 255n) === 8);
  check('stratumOf(a,a) clamps to ID_BITS-1 (valid bucket index)',
    H.stratumOf(a, a) === 263);
  check('stratumOf(0n, MAX_ID) === 0 (top-bit divergence)',
    H.stratumOf(0n, H.MAX_ID) === 0);
}

// ── 7. randomU256 stays inside the hash slot ──────────────────────────
{
  const r1 = H.randomU256(), r2 = H.randomU256();
  check('randomU256 in [0, MAX_HASH]', r1 >= 0n && r1 <= H.MAX_HASH);
  check('two draws differ (2^-256 false-negative odds)', r1 !== r2);
}

// ── 8. keyspace-profile switch (sim) + RESTORE ────────────────────────
// This block reshapes the live keyspace, so everything after it depends on
// the restore at the end. (Hermetic across processes regardless: the smoke
// runs in its own node process.)
{
  H.configureKeyspace({ hashBits: 64 });                    // 8b region + 64b hash
  const ks = H.getKeyspace();
  check('shrunk profile: idBits 72, hexChars 18', ks.idBits === 72 && ks.hexChars === 18,
    JSON.stringify(ks));
  check('shrunk profile: authorId 64b / 16 hex chars',
    ks.authorIdBits === 64 && ks.authorHexChars === 16);
  check('shrunk profile flagged non-production + configured',
    ks.isProductionDefault === false && ks.configured === true);
  check('live LET bindings updated (ID_BITS/HEX_CHARS/MAX_ID/S2_SHIFT)',
    H.ID_BITS === 72 && H.HEX_CHARS === 18 &&
    H.MAX_ID === (1n << 72n) - 1n && H.S2_SHIFT === 64n);
  check('AUTH_VERIFY_RELAXED flips true below 256-bit authors',
    H.AUTH_VERIFY_RELAXED === true);

  check('toHex now emits 18 chars', H.toHex(1n).length === 18);
  check('round-trip holds at 72 bits', H.fromHex(H.toHex(H.MAX_ID)) === H.MAX_ID);
  check('fromHex now REJECTS the old 66-char width',
    thrown(() => H.fromHex('0'.repeat(66)))?.code === H.BAD_ID_CODE);
  check('isHexId tracks the new width',
    H.isHexId('0'.repeat(18)) === true && H.isHexId('0'.repeat(66)) === false);
  check('asId stays width-lenient: a 66-char zero-padded small id still parses',
    H.asId('0'.repeat(64) + 'ab') === 0xABn);
  check('toHex rejects a value beyond the shrunk MAX_ID',
    !!thrown(() => H.toHex(1n << 72n)));

  const id = H.assembleId(0xAB, 0xCAFEn);
  check('assembleId shifts by the shrunk hash width (64)',
    id === ((0xABn << 64n) | 0xCAFEn));
  check('extractS2Prefix / extractHash / s2PrefixOfHex agree at 72 bits',
    H.extractS2Prefix(id) === 0xAB && H.extractHash(id) === 0xCAFEn &&
    H.s2PrefixOfHex(H.toHex(id)) === 0xAB);
  check('clz264 is width-generic: clz(0)=72, clz(1)=71 in the shrunk profile',
    H.clz264(0n) === 72 && H.clz264(1n) === 71);
  check('stratumOf clamps to the shrunk ID_BITS-1', H.stratumOf(5n, 5n) === 71);
  check('randomU256 truncated to the shrunk hash slot',
    H.randomU256() <= H.MAX_HASH && H.MAX_HASH === (1n << 64n) - 1n);

  check('configureKeyspace rejects hashBits outside [8,256]',
    !!thrown(() => H.configureKeyspace({ hashBits: 4 })) &&
    !!thrown(() => H.configureKeyspace({ hashBits: 257 })) &&
    !!thrown(() => H.configureKeyspace({ hashBits: 64.5 })));
  check('configureKeyspace rejects regionBits outside [0,16]',
    !!thrown(() => H.configureKeyspace({ regionBits: -1 })) &&
    !!thrown(() => H.configureKeyspace({ regionBits: 17 })));

  // ── RESTORE the production profile (hermeticity) ──
  H.configureKeyspace({ hashBits: 256, regionBits: 8 });
  const back = H.getKeyspace();
  check('RESTORED: production widths back (264/66, author 64 hex)',
    back.idBits === 264 && back.hexChars === 66 && back.authorHexChars === 64 &&
    back.isProductionDefault === true);
  check('RESTORED: full round-trip + strict verify again',
    H.fromHex(H.toHex(H.MAX_ID)) === (1n << 264n) - 1n &&
    H.AUTH_VERIFY_RELAXED === false);
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
