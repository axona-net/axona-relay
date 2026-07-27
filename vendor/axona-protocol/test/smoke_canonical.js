// =====================================================================
// smoke_canonical.js — canonical() totality + JSON-validity (finding C-1).
//
// canonical() is the byte source for every signature/hash (post, envelope,
// axona/4 transcript). It must (a) be deterministic with sorted keys,
// (b) always emit VALID JSON (never the literal token `undefined`), and
// (c) match JSON.stringify's value semantics so a value canonicalized at
// the signer equals the same value canonicalized at the verifier after a
// JSON round-trip on the wire. It must ALSO be output-preserving for every
// value that contains no undefined/function/symbol — i.e. everything that
// verified before this fix — so it is not a flag-day change.
//
// Run: node test/smoke_canonical.js
// =====================================================================

import { canonical } from '../src/pubsub/post.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const isValidJson = (s) => { try { JSON.parse(s); return true; } catch { return false; } };

function testNoLiteralUndefined() {
  console.log('\n── never emits the literal token `undefined` (always valid JSON) ──');
  const obj = canonical({ a: undefined, b: 1, c: undefined });
  check('object with undefined values → valid JSON', isValidJson(obj));
  check('undefined-valued keys omitted (matches JSON.stringify)', obj === '{"b":1}');

  const arr = canonical([1, undefined, 2, () => {}, Symbol('x')]);
  check('array with holes → valid JSON', isValidJson(arr));
  check('array holes → null (matches JSON.stringify)', arr === '[1,null,2,null,null]');

  check('bare undefined → "null" (total, valid JSON)', canonical(undefined) === 'null');
  check('bare function → "null"', canonical(() => {}) === 'null');
}

function testMatchesJsonStringifySemantics() {
  console.log('\n── value semantics match JSON.stringify (signer ↔ wire ↔ verifier) ──');
  // A value put on the wire is JSON.stringify'd then JSON.parse'd; the
  // verifier canonicalizes the PARSED value. canonical(x) must equal
  // canonical(JSON.parse(JSON.stringify(x))) for any wire-safe x.
  const samples = [
    { ts: 123, topic: 't', message: { z: 1, a: [3, 2], n: null, b: true } },
    { message: 'hi', ts: 0, topic: '' },
    { a: { d: 4, c: 3 }, b: [ { y: 2, x: 1 } ] },
    { keep: 1, drop: undefined, nan: NaN, inf: Infinity, negz: -0 },
  ];
  for (const s of samples) {
    const direct    = canonical(s);
    const roundTrip = canonical(JSON.parse(JSON.stringify(s)));
    check(`signer == verifier after wire round-trip: ${JSON.stringify(s).slice(0, 36)}…`,
      direct === roundTrip && isValidJson(direct));
  }
  check('NaN → null', canonical({ x: NaN }) === '{"x":null}');
  check('Infinity → null', canonical({ x: Infinity }) === '{"x":null}');
  check('-0 → 0', canonical({ x: -0 }) === '{"x":0}');
}

function testStableKeyOrder() {
  console.log('\n── deterministic sorted-key order at every level ──');
  const a = canonical({ b: 1, a: 2, c: { z: 9, y: 8 } });
  const b = canonical({ c: { y: 8, z: 9 }, a: 2, b: 1 });
  check('key order independent of insertion order', a === b);
  check('sorted output', a === '{"a":2,"b":1,"c":{"y":8,"z":9}}');
}

function testOutputPreservedForCleanValues() {
  console.log('\n── output unchanged for clean values (no flag-day) ──');
  // For values with no undefined/function/symbol, canonical must equal
  // "JSON.stringify with sorted keys" — which is what the OLD code already
  // produced for these. Spot-check the exact byte strings signed/hashed.
  check('typical signed envelope core',
    canonical({ ts: 1700000000000, topic: 'room/42', message: { hello: 'world', n: 7 } })
      === '{"message":{"hello":"world","n":7},"topic":"room/42","ts":1700000000000}');
  check('typical post draft',
    canonical({ publisher: 'ab12', topic_id: 'cd34', topic_name: 'news', timestamp: 5, content: 'x', references: [] })
      === '{"content":"x","publisher":"ab12","references":[],"timestamp":5,"topic_id":"cd34","topic_name":"news"}');
  check('axona/4 transcript shape',
    canonical({ proto: 'axona/4', nodeId: 'aa', pubkey: 'bb', cbv: 'n:1:2:mesh' })
      === '{"cbv":"n:1:2:mesh","nodeId":"aa","proto":"axona/4","pubkey":"bb"}');
  check('nested arrays/objects of strings & finite numbers',
    canonical({ a: [1, 'two', { three: 3 }], b: false, c: null })
      === '{"a":[1,"two",{"three":3}],"b":false,"c":null}');
}

function main() {
  console.log('canonical() totality + JSON-validity (C-1) smoke');
  testNoLiteralUndefined();
  testMatchesJsonStringifySemantics();
  testStableKeyOrder();
  testOutputPreservedForCleanValues();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
