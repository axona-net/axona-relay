// smoke_id_invariant.mjs — the address-type invariant (v4.14.0).
//
// THE RULE: an Axona address (nodeId / peerId / topicId / targetId) is a BigInt
// everywhere INSIDE the system. A hex string is legitimate ONLY on the JSON wire
// or as an identity object's serialized `.id`/`.authorId`. Construction and wire
// ingress both run through the single gate `asId()`, so internal code never has to
// ask "string or bigint?" — it can assume bigint.
//
// Run: node test/smoke_id_invariant.mjs
import { asId, toHex, BAD_ID_CODE, MAX_ID } from '../src/utils/hexid.js';
import { NeuronNode } from '../src/dht/NeuronNode.js';
import { Synapse } from '../src/dht/Synapse.js';
import { createNodeIdentity } from '../src/identity/index.js';

let n = 0, fail = 0;
const ok = (m, c) => { console.log(`  ${c ? '✓' : '✗'} ${m}`); c ? n++ : fail++; };
const threw = (fn, codeOrName) => {
  try { fn(); return false; }
  catch (e) { return codeOrName ? (e.code === codeOrName || e.name === codeOrName) : true; }
};

// ── asId: the coercion gate ─────────────────────────────────────────────
{
  console.log('\n── asId() coercion gate ──');
  const big = 0x89abcn;
  ok('bigint in → same bigint out (idempotent)', asId(big) === big);
  ok('hex string in → bigint out', asId('89abc') === big);
  ok('0x-prefixed hex → same value', asId('0x89abc') === big);
  ok('hex and bigint converge', asId('89abc') === asId(0x89abcn));
  ok('round-trips through toHex', asId(toHex(big)) === big);
  ok('non-hex string throws (BAD_ID)', threw(() => asId('xyz not hex'), BAD_ID_CODE));
  ok('empty string throws', threw(() => asId(''), BAD_ID_CODE));
  ok('number throws (only bigint|string allowed)', threw(() => asId(1234), BAD_ID_CODE));
  ok('null throws', threw(() => asId(null), BAD_ID_CODE));
  ok('negative bigint throws (RangeError)', threw(() => asId(-1n), BAD_ID_CODE));
  ok('> MAX_ID throws (RangeError)', threw(() => asId(MAX_ID + 1n), BAD_ID_CODE));
}

// ── DHTNode / NeuronNode: construction coerces to bigint ────────────────
{
  console.log('\n── NeuronNode.id is BigInt however it was built ──');
  const idnt = await createNodeIdentity({ lat: 38, lng: -77 });
  ok('identity.id is a hex string (the serialized form — exception #2)', typeof idnt.id === 'string');

  const fromHexStr = new NeuronNode({ id: idnt.id, lat: 38, lng: -77 });
  ok('built from a hex string → node.id is a bigint', typeof fromHexStr.id === 'bigint');
  ok('the bigint equals the parsed hex', fromHexStr.id === asId(idnt.id));

  const fromBig = new NeuronNode({ id: asId(idnt.id), lat: 38, lng: -77 });
  ok('built from a bigint → node.id is a bigint', typeof fromBig.id === 'bigint');
  ok('both construction paths yield the identical id', fromHexStr.id === fromBig.id);

  ok('a garbage id throws at construction (fail loud, not deep in routing)',
     threw(() => new NeuronNode({ id: 'not-an-id!!', lat: 0, lng: 0 })));
}

// ── Synapse: peerId coerces to bigint ───────────────────────────────────
{
  console.log('\n── Synapse.peerId is BigInt however it was built ──');
  const hex = toHex(0x123n);
  ok('from hex → bigint peerId', typeof new Synapse({ peerId: hex, latencyMs: 10, stratum: 1 }).peerId === 'bigint');
  ok('from bigint → bigint peerId', typeof new Synapse({ peerId: 0x123n, latencyMs: 10, stratum: 1 }).peerId === 'bigint');
  ok('hex and bigint synapses match', new Synapse({ peerId: hex, latencyMs: 10, stratum: 1 }).peerId
                                    === new Synapse({ peerId: 0x123n, latencyMs: 10, stratum: 1 }).peerId);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_id_invariant: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
