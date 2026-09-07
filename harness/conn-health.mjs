// conn-health.mjs — watch a peer's REAL connectivity warm up over time.
// Answers: is the connection warm-up occurring (does meshBound/boundCount climb), and
// how many peers are actually authenticated/live (ping/pong bound) vs merely gossip-known
// in the synaptome (peer.peers()). peer.health().transport is the authoritative signal:
//   synaptomeSize / peers  = KNOWN (gossip)          — overstates reach
//   transport.meshOpen     = open data channels
//   transport.meshBound    = authenticated binds     — the real, deliverable set
//   meshDegraded           = open >> bound (routing not flowing despite looking connected)
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const EVERY = Number(env('EVERY_MS', '10000'));
const FOR = Number(env('FOR_MS', '120000'));
const log = (m) => console.error(`[conn-health] ${m}`);

log(`connecting ${BRIDGE}`);
const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE });
log('connected; watching warm-up...');
const t0 = Date.now();
const tick = () => {
  let h; try { h = peer.health(); } catch (e) { log('health() threw: ' + e.message); return; }
  const tr = h.transport || {};
  const dt = Math.round((Date.now() - t0) / 1000);
  log(`t+${dt}s  synaptome=${h.synaptomeSize} peers(known)=${(h.peers || []).length}  ` +
    `boundCount=${tr.boundCount} meshChannels=${tr.meshChannels} meshOpen=${tr.meshOpen} meshBound=${tr.meshBound} ` +
    `degraded=${h.meshDegraded ?? tr.meshDegraded ?? '?'}  signaling=${tr.signaling ? JSON.stringify(tr.signaling) : 'n/a'}`);
};
tick();
const iv = setInterval(tick, EVERY);
setTimeout(async () => { clearInterval(iv); tick(); log('done'); try { await peer.leave?.({ timeoutMs: 6000 }); } catch {} process.exit(0); }, FOR);
