// level-probe.mjs — L1/L2 probe driver for the level-isolation ladder (spec v2).
// PURE HARNESS: no kernel change. A "probe" is a pubsub:deliver route_msg ADDRESSED to a
// specific node with a throwaway topic and a fresh msgId. The target isn't subscribed, so
// it does nothing but stamp the receipt (rx-ledger, already deployed at 4.72.0). routeMessage
// mints the edgeAttemptId and emits the sender row at its forward sites, so tx/rx join
// automatically. Reconstruct with reconstruct-transitions.mjs after collecting the fleet ledger.
//
//   L1  probe a DIRECT neighbor (peer.peers()) — 1 hop, transport soundness.
//   L2  probe a NON-neighbor node (multi-hop routeMessage) — routing soundness. [--level l2]
//
// Env: LEVEL=l1|l2, PROBES_PER_TARGET, MAX_TARGETS, JOIN_MS, SETTLE_MS, RUN_ID, REGION, BRIDGE.
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { sha256 } from './lib/ledger.mjs';
import { appendFileSync } from 'node:fs';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const LEVEL = env('LEVEL', 'l1');
const N = Number(env('PROBES_PER_TARGET', '5'));
const MAX_TARGETS = Number(env('MAX_TARGETS', '9999'));
const JOIN_MS = Number(env('JOIN_MS', '20000'));
const SETTLE_MS = Number(env('SETTLE_MS', '15000'));
const RUN_ID = env('RUN_ID', 'probe-' + LEVEL);
const log = (m) => console.error(`[probe ${LEVEL}] ${m}`);

const hex = (x) => (typeof x === 'string' ? x.replace(/^0x/, '') : (typeof x === 'bigint' ? x.toString(16) : String(x)));
const probeTopic = sha256('level-isolation-probe-topic');   // throwaway; no node hosts/subscribes it

log(`connecting ${BRIDGE} region=${REGION}`);
// Capture the driver's ledger rows (tx-ledger + node-start) via the CANONICAL wiring —
// connectPeer's onLog param (level,event,ctx), the same hook relays use. A post-hoc
// peer.onLog does NOT work: the manager's emit sink is fixed at construction to the
// onLog passed here, so attaching later captures nothing (the earlier 0-row bug).
const LEDGER_DIR = env('LEDGER_DIR', 'harness/results');
const LSF = `${LEDGER_DIR}/latstage-probe-${process.pid}.jsonl`;
const onLog = (level, event, ctx) => {
  if (event !== 'pubsub:lat-stage' || !ctx) return;
  try { appendFileSync(LSF, JSON.stringify({ ...ctx, host: env('HOST', 'probe') }) + '\n'); } catch { /* */ }
};
const { peer, author } = await connectPeer({ region: REGION, bridge: BRIDGE, onLog });
const self = hex(peer.getNodeId?.() ?? peer.nodeId ?? '');
// DEBUG: is the manager emitting lat-stage at all, and via which hook?
if (env('DEBUG') === '1') {
  const tr = peer._node?.transport;
  const keys = tr ? Object.keys(tr).filter((k) => /conn|chan|mesh|peer|bound|bridge/i.test(k)) : [];
  const sz = (m) => { try { return m?.size ?? (Array.isArray(m) ? m.length : (m && typeof m === 'object' ? Object.keys(m).length : undefined)); } catch { return undefined; } };
  const dump = {};
  for (const k of keys) dump[k] = sz(tr[k]);
  log(`DEBUG transport conn-ish props: ${JSON.stringify(dump)}`);
  try { log(`DEBUG isConnected fn=${typeof tr?.isConnected}; connIdByNodeId size=${sz(tr?._connIdByNodeId)}; mesh boundPeers=${sz(tr?._mesh?._boundPeers ?? tr?._mesh?.boundPeers)}`); } catch {}
}
log(`connected self=${self.slice(0, 12)} author=${(author?.authorId || '').slice(0, 12)}; ledger→${LSF}; joining ${JOIN_MS}ms`);
await new Promise((r) => setTimeout(r, JOIN_MS));

let neighbors = [];
try { neighbors = (peer.peers?.() || []).map(hex).filter(Boolean); } catch (e) { log('peers() threw: ' + e.message); }
log(`direct neighbors: ${neighbors.length}${neighbors.length ? ' e.g. ' + neighbors[0].slice(0, 12) : ''}`);
if (!neighbors.length) { log('NO NEIGHBORS — mesh not formed or peers() unavailable; aborting'); try { await peer.leave?.({ timeoutMs: 5000 }); } catch {} process.exit(3); }

// L1 targets = direct neighbors. L2 targets = neighbors' addresses probed with extra hops is
// not reconstructable without a non-neighbor set; for L2 we still address a neighbor but the
// intent (multi-hop) needs a distant id — deferred until census discovery. First: validate L1.
// TRUE L1: a DIRECT one-hop transport.send to a connected neighbor — no overlay lookup,
// no greedy routing (the earlier routeMessage bug delivered to the wrong node). The frame
// is a route_msg whose targetId IS the neighbor (terminal, hops:0), so the neighbor's
// route_msg handler stamps rx-ledger. We mint the edgeAttemptId ourselves and pass it as
// hopAttemptId, so the neighbor echoes it and the join is EXACT on (msgId, edgeAttemptId).
const transport = peer._node?.transport;
if (!transport || typeof transport.send !== 'function') {
  log('FATAL: no transport.send on peer._node — cannot direct-probe'); try { await peer.leave?.({ timeoutMs: 5000 }); } catch {} process.exit(4);
}
const targets = neighbors.slice(0, MAX_TARGETS);
log(`probing ${targets.length} targets x ${N} via DIRECT transport.send (LEVEL=${LEVEL})`);

let sent = 0, threw = 0, skipped = 0, seq = 0;
for (const t of targets) {
  const tBig = BigInt('0x' + t);
  const connected = (typeof transport.isConnected === 'function') ? transport.isConnected(tBig) : true;
  if (!connected) { skipped++; log(`skip ${t.slice(0, 12)}: not a direct channel (L1 requires one)`); continue; }
  for (let i = 0; i < N; i++) {
    const msgId = sha256(`${self}|${t}|${i}|${Date.now()}|${Math.random()}`);
    const eid = `p${seq++}@${self.slice(-6)}`;
    const frame = { type: 'pubsub:deliver', targetId: t, hops: 0, originId: self, hopAttemptId: eid,
      payload: { topicId: probeTopic, from: self, msgs: [{ msgId, publishTs: Date.now(), json: { probe: true, run: RUN_ID, lvl: LEVEL }, seq: i }] } };
    let disp = 'accepted', outcome = 'sent';
    try { await transport.send(tBig, 'route_msg', frame); sent++; }
    catch (e) { threw++; disp = 'attempted-failed'; outcome = String(e?.message || e); if (threw <= 3) log(`send threw: ${outcome}`); }
    try { appendFileSync(LSF, JSON.stringify({ stage: 'tx-ledger', runId: RUN_ID, msgId, from: self, to: t, edgeAttemptId: eid, hopIdx: 1, disposition: disp, outcome, host: env('HOST', 'probe'), t: Date.now() }) + '\n'); } catch { /* */ }
  }
}
log(`direct-sent to ${targets.length - skipped} connected targets (skipped ${skipped} unconnected)`);
log(`sent=${sent} threw=${threw}; settling ${SETTLE_MS}ms for receipts to flush`);
await new Promise((r) => setTimeout(r, SETTLE_MS));
if (env('DEBUG') === '1') globalThis.__dumpSeen?.();
try { await peer.leave?.({ timeoutMs: 8000 }); } catch {}
log(`done. run=${RUN_ID}. Now collect fleet ledger + node harness/reconstruct-transitions.mjs harness/results`);
process.exit(0);
