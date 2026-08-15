// =====================================================================
// frame-registry-canary.mjs — REF-1.1 M1 telemetry-only canary (deploy half).
//
// Brings up ONE participating relay slot with the four per-boundary frame-
// contract registries armed in SHADOW (observe-only), joins the live testnet
// mesh, lets real traffic flow through it, and reports the canary INVARIANT over
// a bounded window via the kernel's single rollout predicate
// frameRegistryCanaryVerdict().
//
// This is additive: it is a NEW relay process, NOT a roll of the running fleet
// (roll-fleet.sh is the only fleet-update path). Default-off everywhere else —
// the registry arms only because this slot sets BOTH gates:
//   AXONA_FRAME_REGISTRY=1   → construction-side arming (relay createRelay flag)
//   AXONA_REGISTRY_SHADOW=1   → runtime observe gate (kernel shadowEnabled())
// Missing either ⇒ the slot refuses to run (a blind canary is worse than none).
//
// The verdict is the acceptance signal. Beyond observing===true we ALSO require
// summary.total>0 as a LIVENESS check: a slot that armed and observed but saw no
// frames proves nothing. pass requires the window to be both armed+observing AND
// faults===0 AND no threw/trace-fault (frameRegistryCanaryVerdict).
//
//   Run (testnet):
//     RELAY_NETWORK=testnet RELAY_REGION=eagle \
//     AXONA_FRAME_REGISTRY=1 AXONA_REGISTRY_SHADOW=1 \
//     node scripts/frame-registry-canary.mjs
//
//   Tunables (env): CANARY_WINDOW_MS (default 300000 = 5 min),
//                   CANARY_REPORT_MS (default 20000).
// =====================================================================
import '../src/polyfill.js';   // MUST be first — installs RTCPeerConnection/WebSocket
import { cleanupWebRTC } from '../src/polyfill.js';
import { createEphemeralIdentity } from '../src/identity.js';
import { createRelay, startRelay, stopRelay, KERNEL_VERSION, regionCenter, resolveRegion, regionName } from '../src/relay.js';
import { resolveBridgeUrl } from '../src/network.js';
import { frameRegistryCanaryVerdict, shadowEnabled } from '../vendor/axona-protocol/src/registry/index.js';

const WINDOW_MS = Number(process.env.CANARY_WINDOW_MS) || 300_000;
const REPORT_MS = Number(process.env.CANARY_REPORT_MS) || 20_000;
const REGION_TOK = (process.env.RELAY_REGION || 'eagle').trim();
const bridgeUrl = resolveBridgeUrl();

const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const line = (m) => console.log(`[${stamp()}] ${m}`);

// ── Two-part arming gate: refuse to run blind ──
const constructArmed = process.env.AXONA_FRAME_REGISTRY === '1';
if (!constructArmed) {
  console.error('✗ AXONA_FRAME_REGISTRY!=1 — construction-side arming off. Refusing to run a blind canary.');
  process.exit(2);
}
if (!shadowEnabled()) {
  console.error('✗ AXONA_REGISTRY_SHADOW gate is OFF (kernel shadowEnabled()===false) — refusing to run a blind canary.');
  process.exit(2);
}

const code = resolveRegion(REGION_TOK);
if (code == null) { console.error(`✗ unknown RELAY_REGION "${REGION_TOK}"`); process.exit(2); }
const center = regionCenter(code);

line(`REF-1.1 M1 frame-registry canary — kernel ${KERNEL_VERSION}`);
line(`bridge=${bridgeUrl} region=${regionName(code)} (0x${code.toString(16)}) window=${WINDOW_MS}ms report=${REPORT_MS}ms`);
line(`arming: AXONA_FRAME_REGISTRY=1 (construct) + shadowEnabled()=${shadowEnabled()} (runtime) — both required`);

const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
const relay = createRelay({
  bridgeUrl,
  identity,
  region: { lat: center.lat, lng: center.lng },
  frameRegistry: true,   // construction-side arm (also honored via env; explicit here)
  onLog: () => {},        // quiet — the canary reports the summary, not transport chatter
});

let stopping = false;
async function shutdown(codeOut) {
  if (stopping) return; stopping = true;
  try { await stopRelay(relay); } catch { /* */ }
  try { cleanupWebRTC?.(); } catch { /* */ }
  process.exit(codeOut);
}
process.on('SIGINT',  () => { line('SIGINT — draining'); shutdown(130); });
process.on('SIGTERM', () => { line('SIGTERM — draining'); shutdown(143); });

line('joining testnet mesh…');
const status = await startRelay({ peer: relay.peer, transport: relay.transport, ready: { minPeers: 1, timeoutMs: 20_000 } });
line(`joined: ready=${status.ready ?? '?'} peers=${status.peers ?? '?'} ms=${status.ms ?? '?'}${status.integrateError ? ' integrateErr=' + status.integrateError : ''}`);

// Host this relay's keyspace neighborhood (exactly what a real fleet relay does):
// it force-builds the lazy default AxonaManager — which ARMS the registry (peer was
// constructed with frameRegistry:true) — AND recruits this node as a root/holder
// for topics near its id, so ambient rootbeacon/replicate/deliver frames route here
// and the shadow layer observes real inbound traffic (liveness).
try { await relay.peer.host(); line('host() keyspace — armed manager + recruiting for nearby topics'); }
catch (e) { line(`host() failed: ${e?.message || e}`); }
const sh0 = relay.peer.frameRegistryShadow();
line(`registry built=${sh0.built} rows=${sh0.rows} (expect built=true, rows>0)`);
if (!sh0.built) { console.error('✗ registry did not arm (shadow.built===false) — aborting.'); await shutdown(1); }

function report(final = false) {
  const s = relay.peer.frameRegistrySummary();
  const v = frameRegistryCanaryVerdict(s);
  const live = typeof s.total === 'number' && s.total > 0;   // LIVENESS: traffic actually flowed
  const tag = final ? 'FINAL' : 'tick';
  line(`${tag} verdict pass=${v.pass} ready=${v.ready} | observing=${s.observing} built=${s.built} total=${s.total} faults=${s.faults} dropped=${s.dropped} ring=${s.ringSize}`);
  line(`      verdicts=${JSON.stringify(s.verdicts)} faultKinds=${JSON.stringify(s.faultKinds)} byType=${JSON.stringify(s.byType)}${v.reasons.length ? ' reasons=' + JSON.stringify(v.reasons) : ''}`);
  return { s, v, live };
}

const started = Date.now();
const timer = setInterval(() => {
  report(false);
  if (Date.now() - started >= WINDOW_MS) {
    clearInterval(timer);
    const { s, v, live } = report(true);
    // Acceptance = predicate pass AND liveness (total>0). A pass on an empty
    // window is not evidence the shadow layer is inert under real traffic.
    const ok = v.pass === true && live === true;
    line(`═══ CANARY RESULT: ${ok ? 'PASS' : 'FAIL'} — pass=${v.pass} liveness(total>0)=${live} total=${s.total} faults=${s.faults} ═══`);
    if (!ok && !live) line('      (verdict pass but NO traffic observed — not acceptable as evidence; extend the window or check mesh join)');
    shutdown(ok ? 0 : 1);
  }
}, REPORT_MS);
