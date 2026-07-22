import './src/polyfill.js';
import { connectPeer } from './src/ops.js';
import { MeshManager } from './vendor/axona-protocol/src/transport/web/mesh.js';

const s = await connectPeer({ region: 'grizzly' });
await new Promise(r => setTimeout(r, 3000));
const t = s.transport ?? s.peer?._node?.transport;
const info = t?.bridgeInfo ?? null;
console.log('bridgeInfo keys:', info ? Object.keys(info).join(',') : 'null');
console.log('welcome advertises turn:', info?.turn === true);
// The real credential lives in the LIVE mesh (kernel keeps it off the debug surface).
const mesh = t?.mesh ?? t?._subs?.map(x => x.mesh).find(Boolean) ?? null;
const turn = mesh?._turn ?? null;
console.log('live mesh turn:', turn ? `username=${String(turn.username).slice(0,14)}… urls=${JSON.stringify(turn.urls)}` : 'NONE');
if (!turn) { try { await s.close(); } catch {}; console.log('FAIL — mesh holds no TURN'); process.exit(1); }
const entry = mesh._iceConfig().iceServers.at(-1);
console.log('encoded by kernel:', entry.username !== turn.username);
console.log('entry:', JSON.stringify(entry));

const pc = new globalThis.RTCPeerConnection({
  iceServers: [{ urls: entry.urls, username: entry.username, credential: entry.credential }],
  iceTransportPolicy: 'relay',
});
const cands = [];
pc.onicecandidate = (e) => { if (e.candidate?.candidate) cands.push(e.candidate.candidate); };
pc.createDataChannel('t');
await pc.setLocalDescription(await pc.createOffer());
await new Promise(r => setTimeout(r, 6000));
const relay = cands.filter(c => c.includes('typ relay'));
console.log(`relay candidates: ${relay.length}`, relay[0]?.slice(0, 88) ?? '');
try { pc.close(); await s.close(); } catch {}
console.log(relay.length > 0 ? 'PASS — NAT-to-NAT relay path open' : 'FAIL');
process.exit(relay.length > 0 ? 0 : 1);
