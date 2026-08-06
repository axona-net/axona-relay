// live_turn_relay_to_relay.mjs — can two relay-forced endpoints pair through
// our TURN server? HAND-RUN, against prod. Not in `npm test`.
//
//   node test/live_turn_relay_to_relay.mjs
//
// WHY. Howard (#34) forced iceTransportPolicy:'relay' on ten hosts and got
// zero in-region connections, and the prod Windows relay fleet keeps logging
// `pubsub:replicate-all-failed` — a root that cannot push to its cohort. Both
// point at the same suspect: two endpoints that each insist on a relayed
// candidate fail to complete a pair. If cohort replication between relays
// depends on relay-to-relay pairing and that pairing is broken, warm-topic
// roots cannot hand off, which is the outage we just rolled to clear.
//
// This isolates the TURN relay-to-relay path from all Axona logic: two bare
// RTCPeerConnections, both forced to relay, both using the SAME bridge-minted
// TURN credential, signalled to each other in-process. No mesh, no kernel.
//
// SCOPE, stated honestly. Both peers run on THIS machine against ONE coturn.
// A failure here is definitive — the TURN server will not relay between two of
// its own allocations, a pure server-config fault with no NAT involved. A
// SUCCESS here does NOT prove cross-NAT relay works; it only clears the
// same-server case and pushes the suspect toward per-host NAT/candidate
// specifics. Say which we got, and do not overclaim.
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { createEphemeralIdentity } from '../src/identity.js';
import { createRelay, startRelay, stopRelay, regionDescriptor } from '../src/relay.js';

const BRIDGE = process.env.BRIDGE || 'wss://bridge.axona.net';
const REGION = process.env.REGION || 'eagle';
const PAIR_TIMEOUT_MS = Number(process.env.PAIR_TIMEOUT_MS || 30000);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const isRelay = (candStr) => / typ relay /.test(candStr || '');
const candType = (c) => (String(c || '').match(/ typ (\w+)/)?.[1]) ?? '?';

let fail = 0;
const ok = (msg, cond, extra = '') => {
  if (cond) console.log(`  ok - ${msg}`);
  else { console.log(`  ✗  ${msg} ${extra}`); fail++; }
};

console.log(`TURN relay-to-relay — do two relay-forced endpoints pair?\n  bridge ${BRIDGE} region ${REGION}\n`);

// ── 1. Harvest a live bridge-minted TURN credential ─────────────────────────
// Reuse mesh._iceConfig() so the credential carries the exact node-safe
// encoding the kernel applies (#343: libdatachannel mangles a raw REST
// username with a colon). Hand-rolling HMAC would test a different code path.
const { center } = regionDescriptor(REGION);
const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
const { peer, transport } = createRelay({ bridgeUrl: BRIDGE, identity, region: center, onLog: () => {} });
await startRelay({ peer, transport });

let iceCfg = null;
for (let i = 0; i < 40; i++) {
  const cfg = transport.mesh?._iceConfig?.();
  if (cfg && Array.isArray(cfg.iceServers) && cfg.iceServers.some((s) => /turns?:/.test(String(s.urls)))) {
    iceCfg = cfg; break;
  }
  await sleep(500);
}
ok('1. harvested a bridge-minted TURN credential', !!iceCfg,
  '(no TURN in iceServers after 20s — bridge minted none?)');
if (!iceCfg) { await cleanup(); finish(); }

const turnServer = iceCfg.iceServers.find((s) => /turns?:/.test(String(s.urls)));
console.log(`     TURN urls: ${JSON.stringify(turnServer.urls)}  user ${String(turnServer.username).slice(0, 24)}…`);

// ── 2. Two endpoints, both forced to relay, same TURN ───────────────────────
const cfg = { iceServers: iceCfg.iceServers, iceTransportPolicy: 'relay' };
const A = new RTCPeerConnection(cfg);
const B = new RTCPeerConnection(cfg);

const seen = { A: [], B: [] };
A.onicecandidate = (e) => { if (e.candidate) { seen.A.push(candType(e.candidate.candidate)); try { B.addIceCandidate(e.candidate); } catch {} } };
B.onicecandidate = (e) => { if (e.candidate) { seen.B.push(candType(e.candidate.candidate)); try { A.addIceCandidate(e.candidate); } catch {} } };

let opened = false;
const dc = A.createDataChannel('probe');
dc.onopen = () => { opened = true; };

// ── 3. Offer/answer in-process ──────────────────────────────────────────────
const offer = await A.createOffer();
await A.setLocalDescription(offer);
await B.setRemoteDescription(offer);
const answer = await B.createAnswer();
await B.setLocalDescription(answer);
await A.setRemoteDescription(answer);

// ── 4. Wait for the pair to complete (or fail) ──────────────────────────────
const deadline = Date.now() + PAIR_TIMEOUT_MS;
while (Date.now() < deadline && !opened) {
  if (A.connectionState === 'failed' || B.connectionState === 'failed') break;
  await sleep(250);
}

const aRelay = seen.A.filter((t) => t === 'relay').length;
const bRelay = seen.B.filter((t) => t === 'relay').length;
console.log('');
ok('2. endpoint A gathered a relay candidate', aRelay > 0, `A candidate types: ${seen.A.join(',') || '(none)'}`);
ok('   endpoint B gathered a relay candidate', bRelay > 0, `B candidate types: ${seen.B.join(',') || '(none)'}`);
ok('3. the two relay-forced endpoints PAIRED (data channel opened)', opened,
  `A=${A.connectionState}/${A.iceConnectionState} B=${B.connectionState}/${B.iceConnectionState}`);

console.log(`\n── verdict ──`);
if (opened) {
  console.log('  Same-server relay-to-relay WORKS. The coturn relays between its own');
  console.log('  allocations. replicate-all-failed is therefore NOT a blanket TURN');
  console.log('  relay-to-relay failure — the suspect moves to per-host NAT / candidate');
  console.log('  specifics (Howard\'s cross-NAT case), which this test does not cover.');
} else if (aRelay > 0 && bRelay > 0) {
  console.log('  Both ends gathered relay candidates and the pair NEVER completed.');
  console.log('  That is the smoking gun: the TURN server is not relaying between two');
  console.log('  of its own allocations (a coturn permission/allocation config fault),');
  console.log('  which would directly cause relay-to-relay cohort replication to fail.');
} else {
  console.log('  At least one end never gathered a relay candidate — TURN allocation');
  console.log('  itself failed (credentials, reachability, or the #343 username mangle).');
  console.log('  Not a pairing question yet; the allocation is broken first.');
}

try { A.close(); B.close(); } catch {}
await cleanup();
finish();

async function cleanup() {
  try { await stopRelay({ peer, transport }); } catch {}
  try { cleanupWebRTC(); } catch {}
}
function finish() {
  console.log(`\n${fail ? `✗ ${fail} check(s) failed` : '✓ all checks passed'}`);
  process.exit(fail ? 1 : 0);
}
