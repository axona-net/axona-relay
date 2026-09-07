// dc-probe.mjs — DATACHANNEL FORENSICS, decisive isolation.
//
// Question: ping/pong (mesh.js 1Hz keepalive) rides the SAME RTCDataChannel as
// app req/reply (webrtc.js {k:'req'} -> mesh.send -> dc.send). If ping/pong
// round-trips but app req/reply does NOT, the loss is a low-level datachannel
// fact, not routing/pubsub. This probe removes routing/pubsub/ledger entirely:
// for each open+pong-healthy mesh peer it sends ONE 'local_probe' request
// (registered on every relay, pure local reply, no forwarding) over the mesh
// sub-transport and classifies the exact outcome.
//
//   OK         -> resolved: datachannel req/reply works bidirectionally
//   TIMEOUT    -> "timeout awaiting 'local_probe'": frame did not round-trip
//   REMOTE_ERR -> "remote handler error: ...": reached handler, it threw
//   SEND_FAIL  -> "mesh.send failed": dc not open at send time
//
// Correlated per-row with pong health (pongs, lastPongAgeMs) so a
// "pong-healthy but app-frame-TIMEOUT" split is visible directly.
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const SETTLE = Number(env('SETTLE_MS', '20000'));
const SEND_TIMEOUT = Number(env('SEND_TIMEOUT_MS', '8000'));
const hex = (x) => (typeof x === 'string' ? x.replace(/^0x/, '') : (typeof x === 'bigint' ? x.toString(16) : String(x)));
const log = (m) => console.error(`[dc-probe] ${m}`);

const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE });
log(`connected ${BRIDGE}; settling ${SETTLE / 1000}s for warm-up`);
await new Promise((r) => setTimeout(r, SETTLE));

const t = peer._node?.transport;
const subs = t?._subs || [];
const meshSub = subs.find((s) => s?.mesh || /webrtc|mesh/i.test(s?.constructor?.name || ''));
if (!meshSub) { log('NO mesh sub-transport found; aborting'); try { await peer.leave?.({ timeoutMs: 5000 }); } catch {} process.exit(2); }
const meshMgr = meshSub.mesh || t.mesh;

// pong stats are keyed by meshId (the short id getPeers reports); the
// authoritative nodeId->meshId map lives on the WebRTCTransport.
const mp = (meshMgr?.getPeers?.() || []);
log(`mesh.getPeers() = ${mp.length}`);
const now = Date.now();
const statByMesh = new Map();
for (const p of mp) {
  const mId = String(p.peerId ?? p.id ?? p.nodeId);
  statByMesh.set(mId, {
    state: p.state ?? '?',
    pongs: p.pongs ?? 0,
    lastPongAgeMs: (p.lastPongAt && p.lastPongAt > 0) ? (now - p.lastPongAt) : null,
  });
}
// nodeId (BigInt, map key) is what send() needs; meshId (value) joins pong stats
const n2m = meshSub._meshIdByNodeId;
if (!n2m || typeof n2m.entries !== 'function') { log('NO _meshIdByNodeId map; aborting'); try { await peer.leave?.({ timeoutMs: 5000 }); } catch {} process.exit(2); }
const targets = [];
for (const [nodeId, meshId] of n2m.entries()) {
  const st = statByMesh.get(String(meshId)) || { state: '?', pongs: 0, lastPongAgeMs: null };
  targets.push({ id: nodeId, meshId: String(meshId), ...st });
}
log(`targets (nodeId->meshId bound) = ${targets.length}`);

function classify(err) {
  const m = err?.message || String(err);
  if (/timeout awaiting/i.test(m)) return ['TIMEOUT', m];
  if (/remote handler error/i.test(m)) return ['REMOTE_ERR', m];
  if (/mesh\.send failed/i.test(m)) return ['SEND_FAIL', m];
  return ['OTHER', m];
}

const results = await Promise.all(targets.map(async (p) => {
  const started = Date.now();
  let outcome, detail = '', payloadLen = null;
  try {
    // race the transport's own timeout with our own hard cap
    const res = await Promise.race([
      meshSub.send(p.id, 'local_probe', {}),
      new Promise((_, rej) => setTimeout(() => rej(new Error(`hard-cap timeout awaiting 'local_probe'`)), SEND_TIMEOUT)),
    ]);
    outcome = 'OK';
    payloadLen = Array.isArray(res) ? res.length : (res == null ? 0 : -1);
  } catch (err) {
    [outcome, detail] = classify(err);
  }
  return { ...p, outcome, detail, elapsedMs: Date.now() - started, payloadLen };
}));

// stable print: pong-healthy first, then by outcome
results.sort((a, b) => (a.outcome < b.outcome ? -1 : a.outcome > b.outcome ? 1 : 0));
const counts = {};
for (const r of results) {
  counts[r.outcome] = (counts[r.outcome] || 0) + 1;
  const pong = r.pongs > 0 ? `pongs=${r.pongs} pongAge=${r.lastPongAgeMs ?? '-'}ms` : 'NO-PONG';
  const extra = r.outcome === 'OK' ? `payload=${r.payloadLen}peers` : r.detail.slice(0, 70);
  log(`${hex(r.id).slice(0, 12)}  state=${r.state.padEnd(9)} ${pong.padEnd(26)}  -> ${r.outcome.padEnd(10)} ${r.elapsedMs}ms  ${extra}`);
}

const healthy = results.filter((r) => r.pongs > 0).length;
const healthyOK = results.filter((r) => r.pongs > 0 && r.outcome === 'OK').length;
log('─'.repeat(60));
log(`SUMMARY: ${targets.length} mesh peers | pong-healthy=${healthy} | outcomes=${JSON.stringify(counts)}`);
log(`DECISIVE: pong-healthy peers that ALSO answered local_probe (OK): ${healthyOK}/${healthy}`);
if (healthy > 0 && healthyOK < healthy) {
  log(`>>> SPLIT: ${healthy - healthyOK} peers are pong-healthy on the datachannel but app req/reply did NOT round-trip.`);
} else if (healthy > 0 && healthyOK === healthy) {
  log(`>>> NO SPLIT: every pong-healthy datachannel also carries app req/reply. Loss is ABOVE the transport.`);
}
try { await peer.leave?.({ timeoutMs: 6000 }); } catch {}
process.exit(0);
