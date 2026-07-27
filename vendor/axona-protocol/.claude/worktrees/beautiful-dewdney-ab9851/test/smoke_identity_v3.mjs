// smoke_identity_v3.mjs — Identity & Authorship v0.3, Phase 1:
//   • createNodeIdentity  → connection key + nodeId + region
//   • createAuthorIdentity → keypair-only (Author ID), NO id/region; persistAs
//   • region module       → regionCenter, POPULATED_REGIONS, resolveRegion
import assert from 'node:assert';
import {
  createNodeIdentity, createAuthorIdentity,
  regionCenter, POPULATED_REGIONS, regionName, resolveRegion,
} from '../src/index.js';

let n = 0; const ok = (m) => { console.log(`  ok ${++n} - ${m}`); };

// ── node identity: has nodeId + region ──
const node = await createNodeIdentity({ lat: 38.0, lng: -78.5 });   // us-east-ish
assert.equal(typeof node.id, 'string');
assert.equal(node.id.length, 66, 'nodeId is 66-hex');
assert.equal(typeof node.pubkeyHex, 'string');
assert.ok(node.region && typeof node.region.lat === 'number', 'node has region');
ok('createNodeIdentity → 66-hex nodeId + region');

// ── author identity: keypair only, NO id, NO region ──
const author = await createAuthorIdentity();
assert.equal(author.kind, 'author');
assert.equal(author.authorId, author.pubkeyHex, 'authorId === pubkeyHex');
assert.equal(author.authorId.length, 64, 'Author ID is 64-hex (raw pubkey)');
assert.equal(author.id, undefined, 'author has NO nodeId');
assert.equal(author.region, undefined, 'author has NO region');
ok('createAuthorIdentity → keypair only, no id/region');

// author signs + verifies its own signature
const msg = new TextEncoder().encode('hello v0.3');
const sig = await author.sign(msg);
assert.ok(await author.verify(msg, sig), 'author sign↔verify round-trips');
// two ephemeral authors differ
const author2 = await createAuthorIdentity();
assert.notEqual(author.authorId, author2.authorId, 'distinct authors → distinct Author IDs');
ok('author sign/verify + distinct personas');

// ── persistAs: load-or-create against a custom in-memory store ──
const mem = new Map();
const store = { get: (k) => mem.get(k) ?? null, set: (k, v) => void mem.set(k, v) };
const a1 = await createAuthorIdentity({ persistAs: 'me', store });
const a2 = await createAuthorIdentity({ persistAs: 'me', store });   // should RELOAD the same key
assert.equal(a1.authorId, a2.authorId, 'persistAs reloads the same Author ID');
const reSig = await a2.sign(msg);
assert.ok(await a1.verify(msg, reSig), 'reloaded author key still valid');
ok('createAuthorIdentity({persistAs}) durable round-trip');

// ── region module ──
const c = regionCenter('useast');
assert.ok(c && typeof c.lat === 'number' && typeof c.lng === 'number', 'regionCenter(name) → {lat,lng}');
assert.deepEqual(regionCenter(0x89), regionCenter('useast'), 'regionCenter by code == by name');
ok('regionCenter(name|code)');

assert.ok(POPULATED_REGIONS.length > 0 && POPULATED_REGIONS.length < 192, 'populated ⊂ all 192');
// Canonical model (v4.23.0): regions carry neutral ANIMAL names; every legacy
// name ('useast' → eagle/0x89) still resolves as a hidden alias.
assert.ok(POPULATED_REGIONS.some((r) => r.name === 'eagle'), 'eagle (0x89, legacy useast) is populated');
assert.ok(!POPULATED_REGIONS.some((r) => /^(pac|atl|ind|sou|arc)_[0-9a-f]{2}$/.test(r.name)),
  'no open-ocean cells in populated set');
ok(`POPULATED_REGIONS (${POPULATED_REGIONS.length}/192, ocean excluded)`);

// POPULATED_REGIONS is a list of real, inhabited cells (e.g. for a UI picker);
// a topic's region is never derived from the author key.
assert.ok(POPULATED_REGIONS.every((p) => typeof p.code === 'number' && typeof p.name === 'string'),
  'POPULATED_REGIONS entries are { code, name }');
assert.ok(POPULATED_REGIONS.some((p) => p.code === 0x89 && regionName(p.code) === 'eagle'),
  'includes a known land region (0x89, canonical name)');
ok('POPULATED_REGIONS is a usable {code,name} land list (no author-derived regions)');

console.log(`\nsmoke_identity_v3: ${n} checks passed`);
