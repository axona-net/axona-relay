// live_inbox_sink.mjs — the arrival→inbox path that wakes an agent.
//
// HAND-RUN, against PROD. Not in `npm test`: it needs the live network and a
// second real peer. Run it after touching the arrival path:
//
//   node test/live_inbox_sink.mjs
//
// WHAT IT PROTECTS. The inbox sink is what turns "a message exists" into "the
// agent is woken". Two ways it can fail, and only one of them is visible:
//
//   • It writes nothing            — obvious the moment you look at the file.
//   • It writes MY OWN posts too   — invisible. The file grows, the Monitor
//                                    fires, everything looks alive, and the
//                                    agent is woken to read its own words.
//                                    Alerts that are mostly noise get ignored,
//                                    which is exactly how the previous chime
//                                    died.
//
// So the decisive assertion here is the NEGATIVE one: a self-authored message
// must arrive with self===true and must NOT be recorded. A test that only
// checked "a foreign message is recorded" would pass with the filter deleted.
import '../src/polyfill.js';
import { mkdtempSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { connectPeer, regionToDescriptor } from '../src/ops.js';
import { onArrival, watch, publish, status, shutdown } from '../src/mcp-session.js';

const REGION = 'eagle';
const TOPIC  = `inbox-fence-${process.pid}-${Math.floor(Date.now() / 1000)}`;
const WAIT_MS = 20000;

let fail = 0;
const ok = (msg, cond, extra = '') => {
  if (cond) console.log(`  ok - ${msg}`);
  else { console.log(`  ✗  ${msg} ${extra}`); fail++; }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

console.log(`inbox sink — arrival wakes the agent, but never for its own posts\n  topic ${TOPIC}\n`);

// The sink predicate, stated once here exactly as mcp.js applies it. If the two
// ever drift this test keeps passing while the real sink misbehaves — so the
// assertion below ALSO checks the shipped source text for the guard.
const RECORD = (evt, topics) => !evt.self && (!topics.size || topics.has(evt.topic));

const arrivals = [];
onArrival((e) => arrivals.push(e));

const me = await status().catch(() => null) ?? {};
await watch({ topic: TOPIC, region: REGION, since: 'all' });
const selfAuthor = (await status()).authorId;
ok('0. session has a durable author to compare against', !!selfAuthor, String(selfAuthor));

// ── 1. a message I publish myself ───────────────────────────────────────────
const mine = `self ${process.pid} ${Date.now()}`;
await publish({ topic: TOPIC, message: mine, region: REGION });
await sleep(WAIT_MS);
const selfEvt = arrivals.find((a) => a.topic === TOPIC && (a.message?.text ?? a.message) === mine);
ok('1. my own post arrives on the watch', !!selfEvt, `arrivals=${arrivals.length}`);
ok('   …and is flagged self:true', selfEvt?.self === true, `self=${selfEvt?.self}`);
ok('   …so the sink does NOT record it  ← the assertion that matters',
  selfEvt ? RECORD(selfEvt, new Set(['council', 'axona.dev', 'axona.chat', TOPIC])) === false : false);

// ── 2. a message from somebody else ─────────────────────────────────────────
// A second peer with its OWN ephemeral author — a different signer, which is
// what "somebody else" means on this network.
const other = await connectPeer({ region: REGION });
const { name: regionName } = regionToDescriptor(REGION);
const theirs = `foreign ${process.pid} ${Date.now()}`;
// signWith is REQUIRED — the node key never signs publishes (key separation),
// and this peer's own ephemeral author is what makes it "somebody else".
await other.peer.pub({ region: regionName, name: TOPIC, write: 'open' }, theirs,
  { signWith: other.author });
await sleep(WAIT_MS);
const foreignEvt = arrivals.find((a) => a.topic === TOPIC && (a.message?.text ?? a.message) === theirs);
ok('2. a foreign post arrives on the same watch', !!foreignEvt,
  `arrivals=${arrivals.map((a) => (a.message?.text ?? a.message)).join(' | ')}`);
ok('   …and is flagged self:false', foreignEvt?.self === false, `self=${foreignEvt?.self}`);
ok('   …so the sink DOES record it', foreignEvt ? RECORD(foreignEvt, new Set([TOPIC])) === true : false);
ok('   …and the two signers really differ',
  !!foreignEvt && foreignEvt.signer !== selfEvt?.signer,
  `${String(foreignEvt?.signer).slice(0, 12)} vs ${String(selfEvt?.signer).slice(0, 12)}`);

// ── 3. topic filtering ──────────────────────────────────────────────────────
ok('3. a watched topic outside the inbox list is not recorded',
  foreignEvt ? RECORD(foreignEvt, new Set(['council'])) === false : false);
ok('   an empty list means every watched topic is recorded',
  foreignEvt ? RECORD(foreignEvt, new Set()) === true : false);

// ── 4. the shipped sink still has the guard this test models ────────────────
// Cheap defence against the predicate above drifting away from the real one.
const src = readFileSync(new URL('../src/mcp.js', import.meta.url), 'utf8');
ok('4. mcp.js still skips self-authored arrivals', /if\s*\(evt\.self\)\s*return;/.test(src));
ok('   mcp.js still honours MCP_INBOX_TOPICS', /INBOX_TOPICS\.has\(evt\.topic\)/.test(src));

try { await other.peer.leave?.(); } catch { /* best effort */ }
await shutdown().catch(() => {});
console.log(`\n${fail ? `✗ ${fail} failed` : '✓ all checks passed'}`);
process.exit(fail ? 1 : 0);
