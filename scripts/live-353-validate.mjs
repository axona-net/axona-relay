// live-353-validate.mjs — live testnet validation of the #353/#352 fixes
// (kernel 4.29.0 on the testnet bridge + local fleet).
//
// Scenario A (warm, the captured bot-post shape): a STANDING subscriber is
// seated on a warm topic; an EPHEMERAL publisher joins, posts, and dies
// immediately. Pre-4.28.1 those posts stranded at the ephemeral's claim and
// the seated subscriber never saw them. Post-fix the incumbent's verify /
// union must deliver them (bounded by ROOT_VERIFY_MS 45s + slack).
//
// Scenario B (cold, #352): a watcher subscribes FIRST on a fresh topic; a
// publisher appears later. The watcher must receive everything.
//
//   BRIDGE=wss://testnet.axona.net node scripts/live-353-validate.mjs
import './../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

// Post-mortem: where does each session think the topic lives?
async function dumpAttachment(label, s, topic) {
  try {
    const t = await deriveTopicIdBig(topic);
    const am = s.peer._axonaManager;
    const role = am?.axonRoles?.get(t);
    const up = am?._upstream?.get(t);
    let terminus = null;
    try {
      const r = await am?.dht?.lookup?.(t);
      terminus = (r && Array.isArray(r.path) && r.path.length)
        ? r.path[r.path.length - 1].toString(16).slice(0, 10) : null;
    } catch { /* */ }
    log(`POSTMORTEM ${label}`, {
      self: s.nodeId?.slice?.(0, 10) ?? String(am?.nodeId?.toString?.(16)).slice(0, 10),
      isRoot: role?.isRoot ?? null, cache: role?.cache?.length ?? null,
      backupOf: role?.backupOf?.slice?.(0, 10) ?? null,
      upstream: up?.[0]?.slice?.(0, 10) ?? null,
      lookupTerminus: terminus,
    });
  } catch (e) { log(`POSTMORTEM ${label} failed`, { e: String(e?.message || e) }); }
}

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const wait = (ms) => new Promise(r => setTimeout(r, ms));
const stamp = () => new Date().toISOString().slice(11, 19);
const log = (m, x = {}) => console.log(`[${stamp()}] ${m} ${JSON.stringify(x)}`);
let pass = 0, fail = 0;
const check = (l, c, extra = '') => { console.log(`  ${c ? '✓' : '✗'} ${l}${c ? '' : '  ' + extra}`); c ? pass++ : fail++; };

const author = await createAuthorIdentity();

// ── A. warm topic + ephemeral publisher ────────────────────────────────
{
  const topic = { region: 'useast', name: `v353-warm-${Math.floor(Math.random() * 1e9)}`, write: 'open' };
  log('A: standing subscriber connecting', { topic: topic.name, bridge: BRIDGE });
  const S = await connectPeer({ region: 'useast', bridge: BRIDGE });
  const seen = new Set();
  await S.peer.sub(topic, (env) => { seen.add(env.message); }, { since: 'all' });
  await wait(3000);

  log('A: warm publisher posting m1..m3');
  const P = await connectPeer({ region: 'useast', bridge: BRIDGE });
  for (const m of ['m1', 'm2', 'm3']) await P.peer.pub(topic, m, { signWith: author });
  let d = Date.now() + 45_000;
  while (Date.now() < d && !['m1', 'm2', 'm3'].every(m => seen.has(m))) await wait(1000);
  check('A1. seated subscriber got the warm baseline m1..m3',
    ['m1', 'm2', 'm3'].every(m => seen.has(m)), `has ${[...seen].join(',')}`);
  await P.close();

  log('A: EPHEMERAL publisher posts m4..m6 and dies');
  const E = await connectPeer({ region: 'useast', bridge: BRIDGE });
  for (const m of ['m4', 'm5', 'm6']) await E.peer.pub(topic, m, { signWith: author });
  await wait(1500);                          // one beat, then gone — the bot-post shape
  await E.close();
  const tDead = Date.now();

  d = Date.now() + 90_000;                   // 45s verify cadence + heal slack
  while (Date.now() < d && !['m4', 'm5', 'm6'].every(m => seen.has(m))) await wait(1000);
  const healed = ['m4', 'm5', 'm6'].every(m => seen.has(m));
  check('A2. seated subscriber received the ephemeral\'s posts (<90s)', healed,
    `has ${[...seen].join(',')}`);
  if (healed) log('A: healed', { secondsAfterDeath: Math.round((Date.now() - tDead) / 1000) });

  log('A: fresh probe replays since:all');
  const F = await connectPeer({ region: 'useast', bridge: BRIDGE });
  const fseen = new Set();
  await F.peer.sub(topic, (env) => fseen.add(env.message), { since: 'all' });
  d = Date.now() + 30_000;
  while (Date.now() < d && fseen.size < 6) await wait(1000);
  check('A3. fresh probe replays 6/6', ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'].every(m => fseen.has(m)),
    `got ${[...fseen].join(',')}`);
  await F.close(); await S.close();
}

// ── B. cold watcher-first ──────────────────────────────────────────────
{
  const topic = { region: 'useast', name: `v352-cold-${Math.floor(Math.random() * 1e9)}`, write: 'open' };
  log('B: watcher subscribes FIRST on a fresh topic', { topic: topic.name });
  const W = await connectPeer({ region: 'useast', bridge: BRIDGE });
  const wseen = new Set();
  await W.peer.sub(topic, (env) => wseen.add(env.message), { since: 'all' });
  await wait(8000);                          // watcher-first settling (the #352 trigger)

  log('B: publisher appears and posts c1..c3');
  const P2 = await connectPeer({ region: 'useast', bridge: BRIDGE });
  for (const m of ['c1', 'c2', 'c3']) await P2.peer.pub(topic, m, { signWith: author });
  let d = Date.now() + 90_000;
  while (Date.now() < d && !['c1', 'c2', 'c3'].every(m => wseen.has(m))) await wait(1000);
  const b1ok = ['c1', 'c2', 'c3'].every(m => wseen.has(m));
  check('B1. watcher received c1..c3', b1ok, `has ${[...wseen].join(',')}`);
  if (!b1ok) { await dumpAttachment('watcher', W, topic); await dumpAttachment('publisher', P2, topic); }
  await P2.close();

  const F2 = await connectPeer({ region: 'useast', bridge: BRIDGE });
  const f2 = new Set();
  await F2.peer.sub(topic, (env) => f2.add(env.message), { since: 'all' });
  d = Date.now() + 30_000;
  while (Date.now() < d && f2.size < 3) await wait(1000);
  check('B2. fresh probe replays 3/3', ['c1', 'c2', 'c3'].every(m => f2.has(m)), `got ${[...f2].join(',')}`);
  await F2.close(); await W.close();
}

console.log(`\n${fail === 0 ? 'PASS' : 'FAIL'}  (${pass} passed, ${fail} failed)`);
try { cleanupWebRTC(); } catch { /* */ }
process.exit(fail === 0 ? 0 : 1);
