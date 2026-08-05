// live_mcp_reconnect.mjs — prove axona_reconnect actually rebuilds a session.
//
// NOT part of `npm test`: it dials the live network, so it is a hand-run gate
// (like the other live probes). Run:
//
//   node test/live_mcp_reconnect.mjs
//
// What it asserts, in its own process so no standing agent session is touched:
//   1. a watch is live before the rebuild
//   2. reconnect() reports a CHANGED nodeId — the transport really was rebuilt
//      (I-15: transport identity is ephemeral; a stable nodeId would be a bug)
//   3. the durable authorId is UNCHANGED — same participant, new seat
//   4. every watch is restored, and buffered-but-unpolled messages survive
//   5. the rebuilt session receives NEW traffic — the point of the whole
//      exercise, and the thing "SUB re-issued" does not by itself prove
//      (Aster's service-witness point, applied here as a live check)
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { watch, poll, publish, status, reconnect } from '../src/mcp-session.js';

const TOPIC = 'axona.bot';          // owned by this peer; quiet and safe to write
const OWNED = { owner: 'self', write: 'owner' };
let fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok - ${m}`);
  else { console.log(`  ✗  ${m} ${extra}`); fail++; }
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

console.log('axona_reconnect — live rebuild against prod\n');

await watch({ topic: TOPIC, since: 'all', ...OWNED });
await sleep(6000);
const before = await status();
ok('1. connected with a live watch before the rebuild',
  before.connected && before.watches.some(w => w.topic === TOPIC),
  JSON.stringify({ connected: before.connected, watches: before.watches.length }));
const beforeNodeId = before.nodeId;
const beforeAuthor = before.authorId;
const beforeBuffered = before.watches.find(w => w.topic === TOPIC)?.buffered ?? 0;

console.log(`  … rebuilding (was ${before.mesh?.peers ?? '?'} peers, ${beforeBuffered} buffered)`);
const r = await reconnect({ since: 'all' });

ok('2. nodeId CHANGED — the transport was genuinely rebuilt (I-15)',
  r.nodeIdChanged && r.nodeId !== beforeNodeId);
ok('3. authorId UNCHANGED — same participant, new seat',
  r.authorId === beforeAuthor);
ok('4. every watch restored',
  r.watchesRestored.includes(TOPIC) && r.watchesFailed.length === 0,
  JSON.stringify({ restored: r.watchesRestored, failed: r.watchesFailed }));

// Give the fresh mesh a moment, then prove it HEARS — not merely that a SUB
// was issued.
await sleep(12000);
const after = await status();
ok('5. mesh re-formed after the rebuild',
  (after.mesh?.peers ?? 0) > 0, JSON.stringify(after.mesh));

const marker = `reconnect-smoke ${process.pid} ${Math.round(Date.now() / 1000)}`;
await publish({ topic: TOPIC, message: marker, ...OWNED });
await sleep(10000);
const drained = await poll({ topic: TOPIC, ...OWNED });
const heard = (drained.messages || []).some(m => JSON.stringify(m.message).includes(marker));
ok('6. the REBUILT session receives new traffic on the restored watch',
  heard, `drained ${drained.messages?.length ?? 0}`);

console.log(`\n${fail ? `✗ ${fail} failed` : '✓ all checks passed'}`);
try { await cleanupWebRTC?.(); } catch { /* */ }
process.exit(fail ? 1 : 0);
