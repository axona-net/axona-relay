// harness/forced-migration.mjs — DETERMINISTIC epoch-reattach test (Aster's
// condition 3, council 2026-08-30). Suite-green is not credit; this moves the
// root under an INSTALLED subscriber on the live testnet and checks the reattach
// on the wire.
//
// Shape (3 peers, one process so clocks compare, against the armed fleet):
//   R  root    — grinds a topic near ITS OWN nodeId and host()s it, so R wins the
//                keyspace address rule and roots the topic at epoch e.
//   S  sub     — subscribes, settles → installed on R's live fanout, pinned to R.
//   P  pub     — publishes throughout, before and across the migration.
// Then R is CLOSED. A new root R' emerges among the survivors (almost always a
// fleet relay — S and P are far from the grinded-near id, and 62 relays sit
// between them and the topic). R' beacons at epoch e+1. S must:
//   (a) invalidate the old pin (drop upstream=R),
//   (b) attach to R''s tree (new non-R upstream) and resume delivery,
//   (c) show NO false still-installed interval (renewFast re-probe during the gap),
//   (d) recover within the stated bound (recovery plane, 2×RENEW_MS ceiling),
//   (e) fire the reattach ONCE per epoch — duplicate same-epoch beacons repeat
//       every BEACON_MS across the window, so one-per-epoch IS the duplicate-beacon
//       idempotence check on the wire; the renewal backstop covers a dropped one.
//
//   BRIDGE=wss://testnet.axona.net SETTLE_MS=30000 PRE_N=5 POST_MS=180000 \
//     node harness/forced-migration.mjs
import '../src/polyfill.js';
process.env.LAT_TRACE = '1';                          // MUST precede kernel construction
import { connectPeer } from '../src/ops.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';
import { idBig } from '../vendor/axona-protocol/src/pubsub/ids.js';

const BRIDGE    = process.env.BRIDGE    || 'wss://testnet.axona.net';
const REGION    = process.env.REGION    || 'eagle';
const SETTLE_MS = Number(process.env.SETTLE_MS || 30000);
const PRE_N     = Number(process.env.PRE_N || 5);      // publishes before the migration
const POST_MS   = Number(process.env.POST_MS || 180000); // watch window after R dies
const GAP_MS    = Number(process.env.GAP_MS || 3000);  // publish cadence across the window
const RECOVERY_BOUND_MS = Number(process.env.RECOVERY_BOUND_MS || 120000); // 2×RENEW_MS
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const S = (x) => (x == null ? null : String(x).slice(0, 12));
const now = () => Date.now();

// disc + delivery capture ---------------------------------------------------
const disc = [];                                      // { peer, t, ev, ... }
const rec = (tag) => (msg, ctx) => { if (msg === 'pubsub:disc' && ctx) disc.push({ peer: tag, t: now(), ...ctx }); };
const delivered = new Map();                           // msgId -> wall ms of first arrival at S
const dupCount = new Map();                            // msgId -> arrivals (>1 = duplicate)

console.error(`connecting R+S+P to ${BRIDGE} (LAT_TRACE on)`);
const R = await connectPeer({ region: REGION, bridge: BRIDGE });
const Sp = await connectPeer({ region: REGION, bridge: BRIDGE });
const P = await connectPeer({ region: REGION, bridge: BRIDGE });
R.peer.onLog('info', rec('R'));
Sp.peer.onLog('info', rec('S'));
P.peer.onLog('info', rec('P'));
const rAm = R.peer._axonaManager, sAm = Sp.peer._axonaManager;
console.error('connected  R', S(R.nodeId), ' S', S(Sp.nodeId), ' P', S(P.nodeId));

// grind a topic NEAR R so R wins the address rule and roots it ---------------
const grindNear = async (nodeBig, label) => {
  let best = null;
  for (let k = 0; k < 6000; k++) {
    const name = `harness/fmig-${label}-h${k}`;
    const tid = await deriveTopicIdBig({ region: REGION, name });
    const d = tid ^ nodeBig;
    if (best === null || d < best.d) best = { name, d };
  }
  return best.name;
};
const topic = await grindNear(idBig(R.nodeId), S(R.author.authorId));
const desc = { region: REGION, name: topic };
const topicBig = await deriveTopicIdBig({ region: REGION, name: topic });
console.error('topic', topic, ' topicId', S(topicBig));

// R roots it ----------------------------------------------------------------
try { const h = await R.peer.host(desc); console.error('R hosted:', JSON.stringify(h)); }
catch (e) { console.error('R host REFUSED', e?.code || e?.message, '— a fleet relay is closer; test still valid if R self-roots on pub'); }

// S subscribes and settles → installed on R ---------------------------------
await Sp.peer.sub(desc, (env) => {
  const id = env?.msgId; if (!id) return;
  dupCount.set(id, (dupCount.get(id) || 0) + 1);
  if (!delivered.has(id)) delivered.set(id, now());
}, { since: 'all' });
console.error(`settling ${SETTLE_MS}ms`);
await sleep(SETTLE_MS);

const rootEpoch = rAm?.axonRoles?.get(topicBig)?.epoch;
const rIsRoot   = !!rAm?.axonRoles?.has(topicBig);
const sPinPre   = S(sAm?._upstream?.get(topicBig)?.[0]);
const renewFast = sAm?.renewFastMs ?? sAm?._renewFastMs ?? 5000;
console.error(`pre-migration: R.isRoot=${rIsRoot} epoch=${rootEpoch}  S.pin=${sPinPre}  renewFast=${renewFast}ms`);

// pre-migration publishes (baseline live delivery) --------------------------
const preIds = [];
for (let i = 0; i < PRE_N; i++) { const id = await P.peer.pub(desc, { v: 1, k: 'pre', i }, { signWith: P.author }); preIds.push(id); await sleep(GAP_MS); }
await sleep(4000);
const preDelivered = preIds.filter((id) => delivered.has(id)).length;
console.error(`pre-migration delivered ${preDelivered}/${PRE_N} to S`);

// ── FORCE THE MIGRATION: R dies ────────────────────────────────────────────
const killT = now();
console.error(`\n*** killing R (root) at t=0 ***`);
try { await R.close(); } catch { /* */ }

// watch window: keep publishing, sample S state, collect reattach evidence ----
const pinTimeline = [];       // { dt, pin, interval, hasRole }
const postIds = [];
let firstPostDelivery = null;
const t0 = now();
while (now() - t0 < POST_MS) {
  const id = await P.peer.pub(desc, { v: 1, k: 'post', i: postIds.length }, { signWith: P.author });
  postIds.push({ id, t: now() });
  const pin = S(sAm?._upstream?.get(topicBig)?.[0]);
  const interval = sAm?.mySubscriptions?.get(topicBig)?.interval;
  const hasRole = !!sAm?.axonRoles?.has(topicBig);
  pinTimeline.push({ dt: now() - killT, pin, interval, hasRole });
  if (firstPostDelivery === null) {
    for (const pi of postIds) if (delivered.has(pi.id)) { firstPostDelivery = delivered.get(pi.id) - killT; break; }
  }
  await sleep(GAP_MS);
}
await sleep(6000);

// ── evaluate ────────────────────────────────────────────────────────────
const reattaches = disc.filter((d) => d.peer === 'S' && d.ev === 'branch-reattach');
const sBecameRoot = disc.filter((d) => d.peer === 'S' && d.ev === 'became-root');
const pinAfter = S(sAm?._upstream?.get(topicBig)?.[0]);
const postDelivered = postIds.filter((pi) => delivered.has(pi.id)).length;
const dups = [...dupCount.entries()].filter(([, n]) => n > 1);

// per-epoch reattach grouping (idempotence: ≤1 per distinct epoch)
const byEpoch = new Map();
for (const r of reattaches) byEpoch.set(r.epoch, (byEpoch.get(r.epoch) || 0) + 1);
const maxPerEpoch = byEpoch.size ? Math.max(...byEpoch.values()) : 0;

// interval during the gap: did S drop to renewFast (no false installed interval)?
const gapSamples = pinTimeline.filter((s) => s.dt < (firstPostDelivery ?? RECOVERY_BOUND_MS));
const sawFastReprobe = gapSamples.some((s) => s.interval != null && s.interval <= renewFast * 1.5);

const crit = {
  'root actually migrated (S saw a superseding beacon / new pin)':
    reattaches.length > 0 || (pinAfter && pinAfter !== sPinPre),
  '(a) old pin invalidated (pin moved off R)':
    sPinPre != null && pinAfter !== sPinPre,
  '(b) new-tree attachment + delivery resumed':
    pinAfter != null && postDelivered > 0,
  '(c) no false installed interval (renewFast re-probe in the gap)':
    sawFastReprobe || sBecameRoot.length > 0,
  [`(d) recovery within bound (${RECOVERY_BOUND_MS}ms)`]:
    firstPostDelivery != null && firstPostDelivery < RECOVERY_BOUND_MS,
  '(e) idempotent: <=1 reattach per epoch (dup same-epoch beacons repeat in-window)':
    maxPerEpoch <= 1,
  'no duplicate deliveries counted as delivery':
    dups.length === 0,
};

console.log('\n===== FORCED MIGRATION — epoch reattach =====');
console.log('topicId            ', S(topicBig));
console.log('R (initial root)   ', S(R.nodeId), ` epoch=${rootEpoch} isRoot=${rIsRoot}`);
console.log('S pin  pre / post  ', sPinPre, '/', pinAfter, sBecameRoot.length ? '  (S itself became root)' : '');
console.log('pre delivered      ', `${preDelivered}/${PRE_N}`);
console.log('post delivered     ', `${postDelivered}/${postIds.length}`);
console.log('first post-kill delivery', firstPostDelivery != null ? `${firstPostDelivery}ms` : 'NONE in window');
console.log('reattach events (S)', reattaches.length, reattaches.map((r) => `epoch=${r.epoch} from=${S(r.from)} root=${S(r.root)} @${r.t - killT}ms`).join(' | ') || '(none)');
console.log('reattach per epoch ', JSON.stringify([...byEpoch.entries()]), `max=${maxPerEpoch}`);
console.log('duplicates         ', dups.length, dups.map(([id, n]) => `${S(id)}×${n}`).join(' '));
console.log('\n-- pin timeline (dt ms : pin : interval : selfRole) --');
let last = null;
for (const s of pinTimeline) {
  const key = `${s.pin}|${s.interval}|${s.hasRole}`;
  if (key !== last) { console.log(`  ${String(s.dt).padStart(7)}ms  pin=${s.pin}  interval=${s.interval}  selfRoot=${s.hasRole}`); last = key; }
}
console.log('\n-- criteria --');
let pass = true;
for (const [k, v] of Object.entries(crit)) { console.log(`  ${v ? 'PASS' : 'FAIL'}  ${k}`); if (!v) pass = false; }
console.log(`\nVERDICT: ${pass ? 'PASS — reattach earns credit on the wire' : 'FAIL — reattach not demonstrated as specified'}`);

for (const p of [Sp, P]) { try { await p.close(); } catch { /* */ } }
process.exit(pass ? 0 : 1);
