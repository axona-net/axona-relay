// persistent-session-smoke.mjs — verify the persistent MCP peer end-to-end on a
// live network: standing watch, cross-peer receive, poll-drain, session publish,
// PLUS the v0.18 additions — durable nodeId (round-trips from disk), host(),
// long-poll (wake-on-arrival), and the onArrival push sink. Defaults to testnet.
//
//   RELAY_NETWORK=testnet node scripts/persistent-session-smoke.mjs
process.env.RELAY_NETWORK   = process.env.RELAY_NETWORK || 'testnet';
process.env.MCP_AUTHOR_PATH = process.env.MCP_AUTHOR_PATH || '/tmp/claude-mcp-identity.smoke.json';

const S = await import('../src/mcp-session.js');
const { connectPeer, regionToDescriptor } = await import('../src/ops.js');
const { cleanupWebRTC } = await import('../src/polyfill.js');
const { loadIdentity } = await import('../vendor/axona-protocol/src/identity/index.js');
const { readFileSync } = await import('node:fs');

const RUN = `${process.pid}-${Math.floor(performance.now())}`;
const TOPIC = `claude/persist-smoke/${RUN}`;
const HOSTT = `claude/host-smoke/${RUN}`;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
let pass = true;
const check = (n, ok, d = '') => { console.log(`${ok ? 'PASS' : 'FAIL'}  ${n}${d ? ' — ' + d : ''}`); if (!ok) pass = false; };

const arrivals = [];
S.onArrival((e) => arrivals.push(e));

let B;
try {
  console.log(`[smoke] network=${process.env.RELAY_NETWORK} topic=${TOPIC}`);
  const w = await S.watch({ topic: TOPIC, since: 'all' });
  check('watch opened', w.ok && w.watching);
  const st0 = await S.status();
  check('connected w/ durable node + author', st0.connected && !!st0.nodeId && !!st0.authorId, `node ${String(st0.nodeId).slice(0,10)}… author ${String(st0.authorId).slice(0,10)}…`);

  // durable nodeId: the on-disk envelope must round-trip to the SAME nodeId
  const env = JSON.parse(readFileSync(process.env.MCP_AUTHOR_PATH, 'utf8'));
  const reloaded = await loadIdentity(env.node);
  check('persistent nodeId round-trips from disk', reloaded.id === st0.nodeId, `disk ${String(reloaded.id).slice(0,10)}…`);

  // cross-peer publish → standing watch buffers it + push sink fires
  B = await connectPeer({ region: 'useast' });
  const { name: regionName } = regionToDescriptor('useast');
  const want1 = `hello-1-${RUN}`;
  await B.peer.pub({ region: regionName, name: TOPIC }, want1, { signWith: B.author });
  await wait(7000);
  check('onArrival push fired', arrivals.some((a) => a.message === want1), `arrivals=${arrivals.length}`);
  const p = await S.poll({ topic: TOPIC });
  check('poll drained the arrival', (p.messages || []).some((m) => m.message === want1) && p.remaining === 0);

  // LONG-POLL: block on an empty buffer, then a publish should wake it early
  const want2 = `hello-2-${RUN}`;
  const t0 = performance.now();
  const longPoll = S.poll({ topic: TOPIC, wait: true, timeoutSec: 12 });
  await wait(1500);
  await B.peer.pub({ region: regionName, name: TOPIC }, want2, { signWith: B.author });
  const lp = await longPoll;
  const elapsedSec = (performance.now() - t0) / 1000;
  check('long-poll woke on arrival (not timeout)', (lp.messages || []).some((m) => m.message === want2) && elapsedSec < 11, `elapsed ${elapsedSec.toFixed(1)}s`);

  // host(): Claude roots its own topic
  const h = await S.host({ topic: HOSTT });
  check('host() ok', h.ok && h.hosting);
  const st1 = await S.status();
  check('status lists hosted topic', st1.hosted.some((x) => x.topic === HOSTT), `hosted=${st1.hosted.length}`);

  // regression: since:'live' must be accepted (was 'new' → kernel rejected → phantom watch)
  const lw = await S.watch({ topic: `claude/live-smoke/${RUN}`, since: 'live' });
  check('watch since:live accepted (no phantom)', lw.ok && lw.watching && lw.alreadyWatching === false);

  // session publishes under the stable author
  const pub = await S.publish({ topic: `${TOPIC}/reply`, message: 'ack' });
  check('session.publish signed by stable author', pub.ok && pub.signer === st0.authorId);

  console.log('[smoke] status:', JSON.stringify({ node: String(st1.nodeId).slice(0,10)+'…', mesh: st1.mesh, watches: st1.watches.length, hosted: st1.hosted.length }));
} catch (e) {
  check('no exception', false, String(e?.stack || e?.message || e).slice(0, 300));
} finally {
  try { if (B) await B.close(); } catch { /* */ }
  await S.shutdown();
  try { cleanupWebRTC(); } catch { /* */ }
}
console.log(pass ? '\n✓ ALL PASS' : '\n✗ FAILURES');
process.exit(pass ? 0 : 1);
