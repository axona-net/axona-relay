import assert from 'node:assert';
import { deriveTopicId } from '../src/pubsub/post.js';
import { resolveRegion } from '../src/utils/region-names.js';
const OWNER = 'a'.repeat(64);
let n=0; const ok=(m)=>console.log(`  ok ${++n} - ${m}`);

const t1 = await deriveTopicId({ region:'useast', name:'lobby' });
assert.equal(t1.length, 66);
assert.equal(t1.slice(0,2), resolveRegion('useast').toString(16).padStart(2,'0'), 'prefix == useast byte');
ok('open topic (region,name) → region-prefixed 66-hex');

assert.equal(t1, await deriveTopicId({ region:'useast', name:'lobby' }), 'deterministic');
assert.equal(t1, await deriveTopicId({ region:0x89, name:'lobby' }), 'region code == name');
ok('deterministic + region code/name equivalence');

const owned = await deriveTopicId({ region:'useast', owner:OWNER, name:'feed', write:'owner' });
const open  = await deriveTopicId({ region:'useast', owner:OWNER, name:'feed', write:'open' });
assert.notEqual(owned, open, 'write policy changes the topic id');
ok('owner-only vs open are distinct topics');

// region omitted + owner → NO longer key-derived; resolveTopic has no node
// context, so it throws. (A peer.pub/sub supplies its node region as the
// fallback; bare deriveTopicId does not.)
let kdThrew=false; try { await deriveTopicId({ owner:OWNER, name:'profile', write:'owner' }); } catch { kdThrew=true; }
assert.ok(kdThrew, 'region omitted (even with owner) throws — region is never author-derived');
// supplying a selfRegion fallback resolves to that explicit region
const withSelf = await deriveTopicId({ owner:OWNER, name:'profile', write:'owner' }, 0x89);
assert.equal(withSelf.slice(0,2), '89', 'selfRegion fallback sets the region byte');
ok('region never derived from author; selfRegion fallback works');

let threw=false; try { await deriveTopicId({ name:'lobby' }); } catch { threw=true; }
assert.ok(threw, 'open topic without region throws'); ok('no global region: open topic requires a region');

// write is keyed on owner presence:
const OWN = 'a'.repeat(64);   // a fake 64-hex Author ID
// no owner → write is ignored → open (does NOT throw)
const noOwnerOwnerWrite = await deriveTopicId({ region:'useast', name:'x', write:'owner' });
const plainOpen         = await deriveTopicId({ region:'useast', name:'x' });
assert.equal(noOwnerOwnerWrite, plainOpen, "no owner ⇒ write ignored ⇒ open");
ok("no owner: write:'owner' is ignored, resolves open");
// owner + omitted write defaults to owner-only, identical to explicit write:'owner'
const ownerDefault = await deriveTopicId({ region:'useast', owner:OWN, name:'feed' });
const ownerExplicit = await deriveTopicId({ region:'useast', owner:OWN, name:'feed', write:'owner' });
assert.equal(ownerDefault, ownerExplicit, "owner + no write === owner + write:'owner'");
ok("owner present: write defaults to 'owner' (same id as explicit)");
// owner + write:'open' is a DIFFERENT (owner-namespaced open) topic
const ownerOpen = await deriveTopicId({ region:'useast', owner:OWN, name:'feed', write:'open' });
assert.notEqual(ownerOpen, ownerDefault, "owner+open differs from owner+owner");
ok("owner + write:'open' is a distinct (inbox) topic id");

console.log(`\nsmoke_addressing v3: ${n} checks passed`);
