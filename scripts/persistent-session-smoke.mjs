// persistent-session-smoke.mjs — verify the persistent MCP peer end-to-end on a
// live network: the session WATCHES a topic, a SEPARATE peer publishes to it,
// and the session POLLs and must see it (the standing-subscriber path). Also
// exercises session.publish + status. Defaults to testnet.
//
//   RELAY_NETWORK=testnet node scripts/persistent-session-smoke.mjs
process.env.RELAY_NETWORK = process.env.RELAY_NETWORK || 'testnet';
process.env.MCP_AUTHOR_PATH = process.env.MCP_AUTHOR_PATH || '/tmp/claude-mcp-author.smoke.json';

const { ensureSession, watch, poll, publish, status, shutdown } = await import('../src/mcp-session.js');
const { connectPeer, regionToDescriptor } = await import('../src/ops.js');
const { cleanupWebRTC } = await import('../src/polyfill.js');

const RUN = `${process.pid}-${Math.floor(performance.now())}`;
const TOPIC = `claude/persist-smoke/${RUN}`;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
let pass = true;
const check = (name, ok, detail = '') => { console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`); if (!ok) pass = false; };

let B;
try {
  console.log(`[smoke] network=${process.env.RELAY_NETWORK} topic=${TOPIC}`);

  // 1. session connects (persistent peer, durable author) + opens a standing watch
  console.log('[smoke] session.watch …');
  const w = await watch({ topic: TOPIC, since: 'all' });
  check('watch opened', w.ok && w.watching, `alreadyWatching=${w.alreadyWatching}`);
  const st0 = await status();
  check('session connected w/ stable author', st0.connected && !!st0.authorId, `author ${String(st0.authorId).slice(0, 12)}…`);

  // 2. a SEPARATE ephemeral peer publishes to the watched topic
  console.log('[smoke] second peer publishes …');
  B = await connectPeer({ region: 'useast' });
  const { name: regionName } = regionToDescriptor('useast');
  const want = `hello-from-B-${RUN}`;
  const msgId = await B.peer.pub({ region: regionName, name: TOPIC }, want, { signWith: B.author });
  console.log(`[smoke]   B published msgId=${String(msgId).slice(0, 12)}…`);

  // 3. give it a few seconds to propagate, then POLL the session's buffer
  await wait(7000);
  const p = await poll({ topic: TOPIC });
  const got = (p.messages || []).map((m) => m.message);
  check('poll received B\'s message', got.includes(want), `buffer=[${got.join(', ')}]`);
  check('poll drained the buffer', p.remaining === 0, `remaining=${p.remaining}`);

  // 4. a second poll right after is empty (drain worked)
  const p2 = await poll({ topic: TOPIC });
  check('second poll empty', (p2.messages || []).length === 0);

  // 5. session.publish (the persistent peer is also a publisher)
  const pub = await publish({ topic: `${TOPIC}/reply`, message: 'ack' });
  check('session.publish returns msgId+signer', pub.ok && !!pub.msgId && pub.signer === st0.authorId);

  // 6. status reflects the live watch
  const st = await status();
  const watchSeen = st.watches.find((x) => x.topic === TOPIC);
  check('status lists the watch', !!watchSeen, `total=${watchSeen?.total}`);
  console.log('[smoke] status:', JSON.stringify({ connected: st.connected, nodeId: String(st.nodeId).slice(0,10)+'…', mesh: st.mesh, watches: st.watches.length }));
} catch (e) {
  check('no exception', false, String(e?.stack || e?.message || e).slice(0, 300));
} finally {
  try { if (B) await B.close(); } catch { /* */ }
  await shutdown();          // unsubs the watch, closes session peer, cleanupWebRTC()
  try { cleanupWebRTC(); } catch { /* */ }
}

console.log(pass ? '\n✓ ALL PASS' : '\n✗ FAILURES');
process.exit(pass ? 0 : 1);
