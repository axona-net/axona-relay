// =====================================================================
// smoke_public_topics.mjs — v0.3 OPEN topics (the public-topic analogue).
//
// In v0.3 there is no synthetic-publisher "public" mode and no global
// (0x00) bucket: an anyone-can-publish / anyone-can-subscribe topic is an
// OPEN topic placed in a real region —
//
//     { region, name, write: 'open' }
//     topic_id = [region byte (2 hex)] || SHA-256(canonical({ owner:null, name, write:'open' }))
//
// Anyone who knows the region + name recomputes the SAME topic id (no
// publisher scoping). Region is REQUIRED (no global region exists);
// changing region or name changes the topic id. Owner-only topics
// ({ owner, write:'owner' }) are a DIFFERENT topic id for the same name,
// so an open topic can never collide with an owned one.
//
// Per-message provenance is still verifiable when a publisher signs (the
// envelope carries signerPubkey); the TOPIC just isn't tied to a single
// publisher. That signed-envelope path is covered by smoke_pubsub_v3.mjs.
//
// Run:  node test/smoke_public_topics.mjs
// =====================================================================

import {
  deriveTopicId,
  resolveRegion,
  sha256Hex,
} from '../src/index.js';
import { canonical } from '../src/pubsub/post.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const OWNER = 'a'.repeat(64);   // a 64-hex Author ID
const TOPIC = 'news';

async function testOpenTopicRegionPlaced() {
  console.log('\n── open topic: region-placed, anyone recomputes the same id ──');
  const t1 = await deriveTopicId({ region: 'useast', name: TOPIC });
  const t2 = await deriveTopicId({ region: 'useast', name: TOPIC });

  check('open topic is 66 hex chars', t1.length === 66);
  check('two callers compute the SAME open topic id', t1 === t2);
  const regionByte = resolveRegion('useast').toString(16).padStart(2, '0');
  check('top byte == the region byte (region-placed, not 0x00)',
    t1.slice(0, 2) === regionByte);

  // The hash half is sha256(canonical({ owner:null, name, write:'open' })).
  const expected = regionByte + (await sha256Hex(canonical({ owner: null, name: TOPIC, write: 'open' })));
  check('open id == [region] || sha256(canonical({owner:null,name,write:open}))',
    t1 === expected);
}

async function testRegionAndNameScope() {
  console.log('\n── region + name both scope the topic id ──');
  const useast = await deriveTopicId({ region: 'useast', name: TOPIC });
  const iberia = await deriveTopicId({ region: 'iberia', name: TOPIC });
  check('same name, different region → different topic id', useast !== iberia);

  const other = await deriveTopicId({ region: 'useast', name: 'weather' });
  check('same region, different name → different topic id', useast !== other);

  // region code == region name.
  const byCode = await deriveTopicId({ region: resolveRegion('useast'), name: TOPIC });
  check('region code == region name', useast === byCode);
}

async function testOpenVsOwner() {
  console.log('\n── open ≠ owner (same name can never collide across policies) ──');
  const open  = await deriveTopicId({ region: 'useast', name: TOPIC, write: 'open' });
  const owned = await deriveTopicId({ region: 'useast', owner: OWNER, name: TOPIC, write: 'owner' });
  check('open topic id differs from owner-only topic id', open !== owned);
}

async function testEdgeCases() {
  console.log('\n── input validation ──');
  let threw = false;
  try { await deriveTopicId({ region: 'useast', name: '' }); } catch { threw = true; }
  check('empty name throws', threw);

  threw = false;
  try { await deriveTopicId({ region: 'useast', name: undefined }); } catch { threw = true; }
  check('undefined name throws', threw);

  // Region is REQUIRED for an open topic (no global region).
  threw = false;
  try { await deriveTopicId({ name: TOPIC }); } catch { threw = true; }
  check('open topic without a region throws (no global region)', threw);

  threw = false;
  try { await deriveTopicId({ region: 'no-such-region', name: TOPIC }); } catch { threw = true; }
  check('unknown region throws', threw);
}

async function main() {
  console.log('v0.3 open topics (the public-topic analogue): structured { region, name }\n');

  await testOpenTopicRegionPlaced();
  await testRegionAndNameScope();
  await testOpenVsOwner();
  await testEdgeCases();

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
