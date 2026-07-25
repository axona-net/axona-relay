// scripts/cohort-liveness.mjs — DIAGNOSTIC: for a topic, ask the keyspace who is
// K-closest, then probe each of those nodes for LIFE.
//
// Why this exists (2026-07-25): who-roots.mjs answers "who should hold this topic"
// and it answers confidently — findKClosest reads ids out of routing tables without
// contacting anybody. If those ids belong to departed nodes, the answer is a list of
// GHOSTS: every publish routed there vanishes while lookup keeps reporting a
// healthy-looking cohort. That is exactly the #393 symptom — cohort populated,
// publishes unconfirmed, reads empty.
//
// CORRECTION (2026-07-25): an earlier version of this comment claimed ghosts are
// PERMANENT because axona-relay re-mints its transport id on every start. That is
// wrong. A departing node — relay or browser — is handled by one general path:
// leave() sends `peer-leaving`, _evictAndReplace drops the synapse, onPeerLeave
// fires, and dropMs ages out whatever stops renewing. Relays are not special.
// A stale id here means a node that has departed but not yet been evicted from
// THIS probe's table, or one that is simply unreachable right now — a snapshot,
// not a permanent condition.
//
// The real consequence of the ephemeral id is different and narrower: a restarted
// relay does not reclaim its old KEYSPACE POSITION, so its roles never return to
// it and "who is closest to what" is reshuffled on every deploy.
//
// Liveness test = try to open an authenticated channel (transport.openConnection).
// Live → true. Ghost → false/throw/timeout. Ids already in our own mesh are alive by
// construction and are reported as such without a redundant dial.
//
// Usage: node scripts/cohort-liveness.mjs [name] [region] [owner|'self'] [write]
//        node scripts/cohort-liveness.mjs axona.bot eagle self owner
//        RELAY_NETWORK=testnet node scripts/cohort-liveness.mjs axona.bot eagle self owner
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { deriveTopicId } from '../vendor/axona-protocol/src/pubsub/post.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const [name = 'axona.bot', region = 'eagle', ownerArg, write] = process.argv.slice(2);
const PROBE_MS = Number(process.env.PROBE_MS) || 8000;

// 'self' → the MCP author id (owner of #axona.bot). owner+write fold into the topic
// id, so omitting them probes a DIFFERENT topic than the one under investigation.
let owner = ownerArg;
if (ownerArg === 'self') {
  const path = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
  const read = () => { try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return {}; } };
  owner = (await createAuthorIdentity({ persistAs: 'claude', store: { get: (k) => read()[k] ?? null, set: () => {} } })).authorId;
}

const s = await connectPeer({ region });
const descriptor = { region: s.regionName, name, ...(owner ? { owner } : {}), ...(write ? { write } : {}) };
const tid = await deriveTopicId(descriptor);
const tBig = typeof tid === 'string' ? BigInt('0x' + tid) : tid;
const tHex = typeof tid === 'string' ? tid : tid.toString(16).padStart(66, '0');

console.log(`\ntopic     ${name} @ ${s.regionName}  write=${descriptor.write ?? 'open'} owner=${(owner ?? 'none').slice(0, 12)}`);
console.log(`topicId   ${tHex}`);
console.log(`my probe  ${String(s.nodeId).slice(0, 20)}…`);

const transport = s.peer.node?.transport ?? s.peer._transport ?? null;
const meshNow = (() => { try { return new Set((s.peer.peers?.() ?? []).map(String)); } catch { return new Set(); } })();
console.log(`my mesh   ${meshNow.size} peer(s)\n`);

const arr = await s.peer.findKClosest(tBig, 8);
const ids = (Array.isArray(arr) ? arr : []).map((id) => {
  try { return (typeof id === 'bigint' ? id : BigInt('0x' + String(id))).toString(16).padStart(66, '0'); }
  catch { return null; }
}).filter(Boolean);

if (!ids.length) console.log('findKClosest returned NOTHING — no candidates for this topic in the table at all.');
else console.log(`K-closest to the topic id, with LIVENESS (openConnection, ${PROBE_MS}ms budget):`);

const withTimeout = (p, ms) => Promise.race([
  Promise.resolve(p).then((v) => ({ v })).catch((e) => ({ err: e })),
  new Promise((r) => setTimeout(() => r({ timeout: true }), ms)),
]);

let alive = 0, ghost = 0;
for (const hex of ids) {
  let verdict, detail = '';
  if (meshNow.has(hex)) {
    verdict = 'ALIVE'; detail = '(already in our mesh)'; alive++;
  } else if (!transport || typeof transport.openConnection !== 'function') {
    verdict = 'UNKNOWN'; detail = '(no openConnection on this transport)';
  } else {
    const r = await withTimeout(transport.openConnection(hex), PROBE_MS);
    if (r.timeout)          { verdict = 'GHOST'; detail = `(no channel in ${PROBE_MS}ms)`; ghost++; }
    else if (r.err)         { verdict = 'GHOST'; detail = `(${String(r.err.message).slice(0, 40)})`; ghost++; }
    else if (r.v === false) { verdict = 'GHOST'; detail = '(openConnection → false)'; ghost++; }
    else                    { verdict = 'ALIVE'; detail = '(channel opened)'; alive++; }
  }
  // PAD before slicing — toString(16) strips leading zeros and made distances
  // incomparable as strings (0x0070… printed "706c23…", looking larger than 0x1485…).
  const dist = (BigInt('0x' + hex) ^ tBig).toString(16).padStart(66, '0').slice(0, 10);
  console.log(`  ${verdict.padEnd(7)} ${hex.slice(0, 24)}…  xor≈${dist}  ${detail}`);
}

console.log(`\nverdict: ${alive} alive / ${ghost} ghost of ${ids.length} candidate holder(s)`);
if (ids.length && alive === 0) {
  console.log('ALL-GHOST COHORT — every node the keyspace names for this topic is unreachable.');
  console.log('A publish routed here cannot land and a read cannot be served, yet lookup looks healthy.');
}

try { await s.close(); } catch { /* */ }
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
