// peer-detail.mjs — per-peer channel-path + liveness detail, to disambiguate "delivers"
// from "has a real mesh datachannel" vs "bridge-reachable only". No conclusions in code;
// prints one row per known peer: id, getLatency, isConnected, channelIdFor (meshId|'bridge'),
// and mesh state (from transport.mesh.getPeers()).
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const SETTLE = Number(env('SETTLE_MS', '15000'));
const hex = (x) => (typeof x === 'string' ? x.replace(/^0x/, '') : (typeof x === 'bigint' ? x.toString(16) : String(x)));
const log = (m) => console.error(`[detail] ${m}`);

const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE });
log(`connected; settling ${SETTLE / 1000}s`);
await new Promise((r) => setTimeout(r, SETTLE));
const t = peer._node?.transport;
// mesh per-peer state map (nodeId hex prefix -> state)
const meshState = new Map();
try {
  const mp = t?.mesh?.getPeers?.() || [];
  for (const p of mp) { const id = hex(p.peerId ?? p.id ?? p.nodeId); if (id) meshState.set(id.slice(0, 12), p.state ?? '?'); }
  log(`mesh.getPeers() count=${mp.length}`);
} catch (e) { log('mesh.getPeers threw: ' + e.message); }

const known = (peer.peers?.() || []).map(hex).filter(Boolean);
log(`known peers=${known.length}`);
for (const id of known) {
  const b = BigInt('0x' + id);
  let lat = -1, cn = false, ch = '?';
  try { lat = t.getLatency?.(b); } catch {}
  try { cn = t.isConnected?.(b) === true; } catch {}
  try { ch = t.channelIdFor?.(b) ?? '?'; } catch {}
  const chKind = ch === 'bridge' ? 'BRIDGE' : (ch && ch !== '?' ? 'mesh' : ch);
  log(`${id.slice(0, 12)}  lat=${lat}  conn=${cn}  channel=${chKind}  meshState=${meshState.get(id.slice(0, 12)) ?? '-'}`);
}
try { await peer.leave?.({ timeoutMs: 6000 }); } catch {}
process.exit(0);
