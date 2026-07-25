// scripts/who-roots.mjs — DIAGNOSTIC: for a topic descriptor, derive the topic
// id and ask the network who is XOR-closest to it. Answers "where does this
// topic actually live?" from the keyspace, not from any publisher.
//
// Usage: node scripts/who-roots.mjs axona.bot eagle
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { deriveTopicId } from '../vendor/axona-protocol/src/pubsub/post.js';

const [name, region = 'eagle'] = process.argv.slice(2);
const s = await connectPeer({ region });
const descriptor = { region: s.regionName, name };

const tid = await deriveTopicId(descriptor);
const tHex = typeof tid === 'string' ? tid : tid.toString(16).padStart(66, '0');
console.log(`\ntopic ${name} @ ${s.regionName}`);
console.log(`topicId  ${tHex}`);
console.log(`my probe ${s.peer.nodeId ? s.peer.nodeId.toString(16).slice(0, 20) : '?'}…`);

const tBig = typeof tid === 'string' ? BigInt('0x' + tid) : tid;
const arr = await s.peer.findKClosest(tBig, 6);
console.log(`\nK-closest to the topic id (keyspace truth):`);
for (const id of (Array.isArray(arr) ? arr : [])) {
  let b; try { b = typeof id === 'bigint' ? id : BigInt('0x' + String(id)); } catch { continue; }
  const hex = b.toString(16).padStart(66, '0');
  // PAD before slicing: toString(16) strips leading zeros, so 0x0070… printed as
  // "706c23…" and compared visually ABOVE 0x1485… — distances were not comparable.
  const dist = (b ^ tBig).toString(16).padStart(66, '0').slice(0, 10);
  console.log(`  ${hex.slice(0, 24)}…  xor≈${dist}`);
}

// Is the standing MCP peer among them?
const MCP = '89879454b77730eb2e3569d083ab30917b3aa3b745809f5801f20173813ded1481';
const inSet = (Array.isArray(arr) ? arr : []).some(id => {
  try { const b = typeof id === 'bigint' ? id : BigInt('0x' + String(id)); return b.toString(16).padStart(66, '0') === MCP; }
  catch { return false; }
});
console.log(`\nstanding MCP peer (${MCP.slice(0, 12)}…) in the K-closest set: ${inSet ? 'YES' : 'no'}`);

try { await s.disconnect?.(); } catch { /* */ }
cleanupWebRTC();
process.exit(0);
