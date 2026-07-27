// smoke_leave_teardown.mjs — leave() silences the peer (v4.19.4).
//
// Field incident (alert-bot, 2026-07-10): a bot that published and then called
// leave() burned 100% CPU for ~40s. Cause: nothing ever called
// AxonaManager.stop(), so the refreshTick interval survived leave() and kept
// re-sending every UNCONFIRMED publish (pendingPub implicit-ack retries) and
// re-running iterative lookups against a dead transport until the pending TTLs
// burned off. Also: the drain window was capped at 50ms (`Math.min(timeoutMs,
// 50)`), silently defeating the caller's timeout, and the leave handoff awaited
// unbounded network lookups.
//
// This smoke proves, against a live sim-transport pair:
//   1. after leave(): the manager tick timer is CLEARED and pendingPub/Kill are
//      EMPTY — the peer is silent (no post-leave sends).
//   2. drain is EVIDENCE-based: with nothing pending, leave() returns fast
//      (well under the 5s default timeout).
//   3. with an artificially stuck pending entry, leave() drains up to ~the
//      caller's bound and STILL exits silenced (bounded, then cleared).
//   3b. the alert-bot pin (v4.23.2): a STALLED pending set (non-root,
//      non-subscribed publisher — nothing ever confirms) exits early at ~STALL_MS
//      instead of riding the full 5s window (field: leaveMs pinned at ~5040ms).
//   3c. while confirmations ARE arriving (pending shrinking), the drain keeps
//      waiting past STALL_MS — progress resets the stall clock.
//   4. stop() performs the same teardown (the abrupt path).
//
// Run: node test/smoke_leave_teardown.mjs
import {
  AxonaPeer, AxonaDomain, NeuronNode, SimNetwork, simTransport,
  createNodeIdentity, createAuthorIdentity, regionCenter,
} from '../src/index.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const __LOC = regionCenter('useast');

async function makePeer(net) {
  const identity = await createNodeIdentity({ lat: __LOC.lat, lng: __LOC.lng });
  const transport = simTransport({ network: net, identity, heartbeatMs: 0 });
  await transport.start(identity.id);
  const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: __LOC.lat, lng: __LOC.lng });
  node.transport = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: identity, transport });
  await peer.start();
  return { peer, identity, transport };
}

async function main() {
  console.log('leave()/stop() teardown — the peer goes silent\n');
  const net = new SimNetwork();
  const a = await makePeer(net);
  const b = await makePeer(net);
  const author = await createAuthorIdentity();
  const TOPIC = { region: 'useast', name: 'leave-teardown-smoke' };

  // b subscribes so a's publishes have a live topic; a publishes.
  await b.peer.sub(TOPIC, () => {}, { since: 'all' });
  await a.peer.pub(TOPIC, 'm1', { signWith: author });
  const am = a.peer._axonaManager;
  check('manager exists + tick armed after publish', !!am && am._timer != null);

  // ── 1+2: fast, evidence-based drain and full silence ────────────────
  const t0 = Date.now();
  await a.peer.leave();                       // default timeoutMs 5000
  const leaveMs = Date.now() - t0;
  check('leave() returned quickly when nothing pending (<2500ms)', leaveMs < 2500, `${leaveMs}ms`);
  check('tick timer CLEARED by leave()', am._timer == null);
  check('pendingPub empty after leave()', (am._pendingPub?.size ?? 0) === 0);
  check('pendingKill empty after leave()', (am._pendingKill?.size ?? 0) === 0);

  // no post-leave sends: watch routeMessage for 1.5s
  let postLeaveSends = 0;
  const dht = am.dht;
  const origRoute = dht.routeMessage;
  dht.routeMessage = (...args) => { postLeaveSends++; return origRoute?.apply(dht, args); };
  await sleep(1500);
  dht.routeMessage = origRoute;
  check('peer is SILENT after leave (no routed sends)', postLeaveSends === 0, `${postLeaveSends} sends`);

  // ── 3: bounded drain with a stuck pending ───────────────────────────
  const c = await makePeer(net);
  await c.peer.pub(TOPIC, 'm-c', { signWith: author });   // forces the lazy manager to exist
  const cam = c.peer._axonaManager;
  // simulate an unconfirmable in-flight publish (implicit ack never arrives)
  cam._pendingPub.set('deadbeef', { at: Date.now(), tries: 0, topicBig: 1n, json: '{}' });
  const t1 = Date.now();
  await c.peer.leave({ timeoutMs: 700 });
  const boundedMs = Date.now() - t1;
  check('stuck pending: leave() drained ~the bound then exited', boundedMs >= 600 && boundedMs < 4500, `${boundedMs}ms`);
  check('stuck pending cleared on departure', (cam._pendingPub?.size ?? 0) === 0);
  check('tick cleared on bounded-drain path too', cam._timer == null);

  // ── 3b: the alert-bot pin — a STALLED pending at the DEFAULT 5s timeout must
  // exit early (no confirmation is coming), not ride the full window. Before the
  // stall-exit fix this pinned leaveMs at ~5040ms (field: ~90 topics published
  // then leave, publisher non-root + non-subscribed so nothing confirms). ──────
  const e = await makePeer(net);
  await e.peer.pub(TOPIC, 'm-e', { signWith: author });
  const eam = e.peer._axonaManager;
  for (let i = 0; i < 90; i++) eam._pendingPub.set('stuck' + i, { at: Date.now(), tries: 0, topicBig: 1n, json: '{}' });
  const t2 = Date.now();
  await e.peer.leave();                        // DEFAULT timeoutMs 5000
  const stalledMs = Date.now() - t2;
  check('alert-bot pin: stalled pending exits ~STALL_MS, not the 5s window (<2800ms)', stalledMs < 2800, `${stalledMs}ms`);
  check('stalled pending cleared on departure', (eam._pendingPub?.size ?? 0) === 0);

  // ── 3c: while confirmations ARE arriving (pending SHRINKS) the drain keeps
  // waiting past STALL_MS — progress resets the stall clock, so a genuinely
  // draining publisher is unaffected by the early-exit. ────────────────────────
  const f = await makePeer(net);
  await f.peer.pub(TOPIC, 'm-f', { signWith: author });
  const fam = f.peer._axonaManager;
  for (let i = 0; i < 40; i++) fam._pendingPub.set('drain' + i, { at: Date.now(), tries: 0, topicBig: 1n, json: '{}' });
  // delete one entry every 250ms → steady progress well past STALL_MS (1.5s)
  const drainTimer = setInterval(() => {
    const k = [...fam._pendingPub.keys()].find(x => x.startsWith('drain'));
    if (k) fam._pendingPub.delete(k);
  }, 250);
  if (typeof drainTimer.unref === 'function') drainTimer.unref();
  const t3 = Date.now();
  await f.peer.leave();
  const progressMs = Date.now() - t3;
  clearInterval(drainTimer);
  check('progress resets stall clock: drain honors the window while shrinking (>2000ms)', progressMs > 2000, `${progressMs}ms`);

  // ── 4: stop() (abrupt path) performs the same teardown ──────────────
  const d = await makePeer(net);
  await d.peer.pub(TOPIC, 'm2', { signWith: author });
  const dam = d.peer._axonaManager;
  check('d tick armed pre-stop', dam._timer != null);
  await d.peer.stop();
  check('stop() clears the tick', dam._timer == null);
  check('stop() clears pendings', (dam._pendingPub?.size ?? 0) === 0);

  await b.peer.stop();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('smoke threw:', e); process.exit(2); });
