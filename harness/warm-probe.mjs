// warm-probe.mjs — CONTINUOUS, warm-up-aware L1 connectivity probe (level-isolation).
// Premise (David): every known peer connection should become live once warmed up, so this
// is NOT one-shot. It watches each peer's ping/pong health and fires a direct probe the
// instant that peer proves live; peers that never prove live within the deadline (default
// 5 min) are reported as failures.
//
// Health signal = getLatency(nodeId) >= 0 — a real RTT from the mesh pong buffer, i.e. the
// 1 Hz ping/pong is currently proving the channel (mesh.js). Probe = a direct transport.send
// of a terminal route_msg to that peer (no overlay routing); receipts land as rx-ledger and
// join by the driver-minted edgeAttemptId. Also records per-target WHY-not-live (channel
// path + isConnected) for the stragglers.
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { sha256 } from './lib/ledger.mjs';
import { appendFileSync } from 'node:fs';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const TIMEOUT_MS = Number(env('TIMEOUT_MS', '300000'));  // 5 min deadline
const POLL_MS = Number(env('POLL_MS', '3000'));
const PROBES = Number(env('PROBES_PER_TARGET', '3'));
const RUN_ID = env('RUN_ID', 'warm');
const LEDGER_DIR = env('LEDGER_DIR', 'harness/results');
const log = (m) => console.error(`[warm] ${m}`);
const hex = (x) => (typeof x === 'string' ? x.replace(/^0x/, '') : (typeof x === 'bigint' ? x.toString(16) : String(x)));
const LSF = `${LEDGER_DIR}/latstage-warm-${process.pid}.jsonl`;
const onLog = (l, e, c) => { if (e === 'pubsub:lat-stage' && c) { try { appendFileSync(LSF, JSON.stringify({ ...c, host: env('HOST', 'warm') }) + '\n'); } catch {} } };

log(`connecting ${BRIDGE}; deadline ${TIMEOUT_MS / 1000}s, poll ${POLL_MS / 1000}s`);
const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE, onLog });
const self = hex(peer.getNodeId?.() ?? '');
const transport = peer._node?.transport;
if (!transport || typeof transport.send !== 'function') { log('FATAL: no transport.send'); process.exit(4); }
const probeTopic = sha256('level-isolation-probe-topic');

const getLat = (tBig) => { try { const l = transport.getLatency?.(tBig); return (typeof l === 'number') ? l : -1; } catch { return -1; } };
const chan = (tBig) => { try { return transport.channelIdFor?.(tBig) ?? '?'; } catch { return '?'; } };
const conn = (tBig) => { try { return transport.isConnected?.(tBig) === true; } catch { return false; } };

async function sendProbe(t, tBig) {
  for (let i = 0; i < PROBES; i++) {
    const msgId = sha256(`${self}|${t}|${i}|${Date.now()}|${Math.random()}`);
    const eid = `w${(sendProbe._s = (sendProbe._s | 0) + 1)}@${self.slice(-6)}`;
    const frame = { type: 'pubsub:deliver', targetId: t, hops: 0, originId: self, hopAttemptId: eid,
      payload: { topicId: probeTopic, from: self, msgs: [{ msgId, publishTs: Date.now(), json: { probe: true, run: RUN_ID }, seq: i }] } };
    let disp = 'accepted', outcome = 'sent';
    try { await transport.send(tBig, 'route_msg', frame); } catch (e) { disp = 'attempted-failed'; outcome = String(e?.message || e); }
    try { appendFileSync(LSF, JSON.stringify({ stage: 'tx-ledger', runId: RUN_ID, msgId, from: self, to: t, edgeAttemptId: eid, hopIdx: 1, disposition: disp, outcome, host: env('HOST', 'warm'), t: Date.now() }) + '\n'); } catch {}
  }
}

const t0 = Date.now(), deadline = t0 + TIMEOUT_MS;
const status = new Map();   // target -> { firstSeen, liveAt|null }
while (Date.now() < deadline) {
  let known = []; try { known = (peer.peers?.() || []).map(hex).filter(Boolean); } catch {}
  for (const t of known) if (!status.has(t)) status.set(t, { firstSeen: Date.now(), liveAt: null });
  let pending = 0;
  for (const [t, s] of status) {
    if (s.liveAt) continue;
    const tBig = BigInt('0x' + t);
    if (getLat(tBig) >= 0) { s.liveAt = Date.now(); await sendProbe(t, tBig); }
    else pending++;
  }
  const liveN = [...status.values()].filter((s) => s.liveAt).length;
  log(`t+${Math.round((Date.now() - t0) / 1000)}s  known=${status.size} live+probed=${liveN} pending=${pending}`);
  if (status.size > 0 && pending === 0) { log('ALL known connections became live'); break; }
  await new Promise((r) => setTimeout(r, POLL_MS));
}

const total = status.size, liveN = [...status.values()].filter((s) => s.liveAt).length;
const never = [...status.entries()].filter(([, s]) => !s.liveAt).map(([t]) => t);
const times = [...status.values()].filter((s) => s.liveAt).map((s) => Math.round((s.liveAt - t0) / 1000)).sort((a, b) => a - b);
log(`RESULT: ${liveN}/${total} connections proved live+probed within ${TIMEOUT_MS / 1000}s`);
if (times.length) log(`time-to-live (s): min=${times[0]} median=${times[Math.floor(times.length / 2)]} max=${times[times.length - 1]}`);
if (never.length) {
  log(`NEVER LIVE (${never.length}/${total}) — reporting as failed:`);
  for (const t of never) { const tBig = BigInt('0x' + t); log(`  ${t.slice(0, 12)}  getLatency=${getLat(tBig)} isConnected=${conn(tBig)} channel=${chan(tBig)}`); }
}
try { await peer.leave?.({ timeoutMs: 8000 }); } catch {}
process.exit(never.length ? 1 : 0);
