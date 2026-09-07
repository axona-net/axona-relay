// peer-route.mjs — for each known peer, show which sub-transport composite._routeFor picks
// for a SEND (mesh vs bridge), and each sub's ownsPeer/isConnected. Replicates _routeFor's
// first-match-in-_subs logic to see if sends divert to the bridge despite a live mesh DC.
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const hex = (x) => (typeof x === 'string' ? x.replace(/^0x/, '') : (typeof x === 'bigint' ? x.toString(16) : String(x)));
const log = (m) => console.error(`[route] ${m}`);
const DELIVERED = new Set(['891510ce048b', '895442108704']);   // the 2 that delivered, for correlation

const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE });
await new Promise((r) => setTimeout(r, Number(env('SETTLE_MS', '15000'))));
const t = peer._node?.transport;
const subs = t?._subs || [];
const tag = (s) => {
  try {
    if (typeof s.getPeers === 'function' || s.mesh || /webrtc|mesh/i.test(s.constructor?.name || '')) return 'mesh';
    if (s.bridgeState !== undefined || /bridge/i.test(s.constructor?.name || '')) return 'bridge';
  } catch {}
  return s?.constructor?.name || '?';
};
log(`_subs order: [${subs.map(tag).join(', ')}]`);
const known = (peer.peers?.() || []).map(hex).filter(Boolean);
log(`known=${known.length}`);
let mesh = 0, bridge = 0, none = 0;
for (const id of known) {
  const b = BigInt('0x' + id);
  let chosen = null, detail = [];
  for (const s of subs) {
    let owns = null, conn = null;
    try { owns = (typeof s.ownsPeer === 'function') ? s.ownsPeer(b) : null; } catch {}
    try { conn = (typeof s.isConnected === 'function') ? s.isConnected(b) : null; } catch {}
    detail.push(`${tag(s)}(owns=${owns},conn=${conn})`);
    if (chosen === null) { const pick = (typeof s.ownsPeer === 'function') ? owns : conn; if (pick) chosen = tag(s); }
  }
  if (chosen === 'mesh') mesh++; else if (chosen === 'bridge') bridge++; else none++;
  const mark = DELIVERED.has(id) ? ' <<< DELIVERED' : '';
  log(`${id.slice(0, 12)}  routeFor=>${chosen}  [${detail.join(' ')}]${mark}`);
}
log(`SUMMARY: send routes to  mesh=${mesh}  bridge=${bridge}  none=${none}  (delivered set was mesh?)`);
try { await peer.leave?.({ timeoutMs: 6000 }); } catch {}
process.exit(0);
