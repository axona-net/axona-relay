// live-soak.mjs — MULTI-HOUR live delivery soak against the running testnet fleet.
//
// The churn-representative confirmation the single quiet window could not give:
// subscribers stay up for hours while the publisher publishes on a steady cadence,
// so the measurement captures the fleet's NATURAL conn-decay root-flaps — the
// condition that produced the historical ~60-70% at the 120s deadline. Every
// (message, subscriber) pair is scored watch-only; a message is FINALIZED once it
// is MATURE (older than the deadline + margin), and cumulative delivery is printed
// every INTERIM_MS so progress is visible without waiting for the full run.
//
//   SUBS=24 PUB_EVERY_MS=10000 DURATION_MS=10800000 DEADLINE_MS=120000 \
//     INTERIM_MS=600000 BRIDGE=wss://testnet.axona.net node harness/live-soak.mjs
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const SUBS = Number(env('SUBS', '24'));
const PUB_EVERY_MS = Number(env('PUB_EVERY_MS', '10000'));
const DURATION_MS = Number(env('DURATION_MS', String(3 * 3600 * 1000)));  // 3h default
const DEADLINE_MS = Number(env('DEADLINE_MS', '120000'));                  // historical 2x renewal
const MATURE_MS   = DEADLINE_MS + Number(env('MATURE_MARGIN_MS', '30000'));
const INTERIM_MS  = Number(env('INTERIM_MS', '600000'));                   // 10 min
const SETTLE_MS = Number(env('SETTLE_MS', '60000'));
const TOPIC = env('TOPIC', `live-soak-${(Date.now?.() ?? 0)}`);
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();
const stamp = () => new Date().toISOString().slice(11, 19);
const log = (m) => console.log(`[soak ${stamp()}] ${m}`);
const desc = { region: REGION, name: TOPIC };

log(`connecting ${SUBS} subscribers + 1 publisher to ${BRIDGE}  topic=${REGION}/${TOPIC}`);
log(`duration=${(DURATION_MS/3600000).toFixed(1)}h pub-every=${PUB_EVERY_MS/1000}s deadline=${DEADLINE_MS/1000}s interim=${INTERIM_MS/60000}m`);
const subs = [];
for (let i = 0; i < SUBS; i++) {
  try { subs.push(await connectPeer({ region: REGION, bridge: BRIDGE })); }
  catch (e) { log(`sub ${i} connect failed: ${String(e?.message||e).slice(0,50)}`); }
}
const pub = await connectPeer({ region: REGION, bridge: BRIDGE });
log(`connected ${subs.length}/${SUBS} subs; settling ${SETTLE_MS/1000}s`);
await wait(SETTLE_MS);

const recvAt = subs.map(() => new Map());   // per-sub: msgId -> first arrival wall t
for (let i = 0; i < subs.length; i++) {
  const m = recvAt[i];
  try { await subs[i].peer.sub(desc, (envp) => { const id = envp?.msgId; if (id && !m.has(String(id))) m.set(String(id), now()); }); }
  catch (e) { log(`sub ${i} subscribe failed: ${String(e?.message||e).slice(0,50)}`); }
  await wait(20);
}
await wait(3000);

const CUTOFFS = [
  { name: '5s', ms: 5000 }, { name: '15s', ms: 15000 }, { name: '30s', ms: 30000 },
  { name: '60s', ms: 60000 }, { name: `deadline(${DEADLINE_MS/1000}s)`, ms: DEADLINE_MS },
  { name: 'eventual', ms: Infinity },
];
const published = [];   // { id, tPub }

function score(onlyMature) {
  const cutT = now();
  let trials = 0; const got = new Map(CUTOFFS.map((c) => [c.name, 0])); const lat = [];
  let mature = 0;
  for (const msg of published) {
    const isMature = (cutT - msg.tPub) >= MATURE_MS;
    if (onlyMature && !isMature) continue;
    mature++;
    for (let i = 0; i < subs.length; i++) {
      trials++;
      const at = recvAt[i].get(msg.id);
      const arrival = at === undefined ? Infinity : (at - msg.tPub);
      for (const c of CUTOFFS) if (arrival <= c.ms) got.set(c.name, got.get(c.name) + 1);
      if (arrival !== Infinity) lat.push(arrival);
    }
  }
  lat.sort((a, b) => a - b);
  const pctl = (p) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(p / 100 * lat.length))] : 0;
  const pct = (x) => trials ? +(100 * x / trials).toFixed(1) : 0;
  return { mature, trials, got, pct, p50: pctl(50), p90: pctl(90), p99: pctl(99), CUTOFFS };
}
function report(tag) {
  const r = score(true);
  const parts = CUTOFFS.map((c) => `${c.name}=${r.pct(r.got.get(c.name))}%`);
  log(`${tag} matureMsgs=${r.mature} trials=${r.trials} | ${parts.join(' ')} | lat p50/p90/p99=${r.p50}/${r.p90}/${r.p99}ms`);
  log('RESULT_JSON ' + JSON.stringify({
    tag, subs: subs.length, matureMsgs: r.mature, trials: r.trials,
    deliveredDeadlinePct: r.pct(r.got.get(`deadline(${DEADLINE_MS/1000}s)`)),
    eventualPct: r.pct(r.got.get('eventual')),
    pct5s: r.pct(r.got.get('5s')), pct60s: r.pct(r.got.get('60s')),
    p50: r.p50, p90: r.p90, p99: r.p99,
  }));
}

const t0 = now(); let seq = 0, nextInterim = INTERIM_MS;
while (now() - t0 < DURATION_MS) {
  try { const id = String(await pub.peer.pub(desc, { v: 1, seq, t: now() }, { signWith: pub.author })); published.push({ id, tPub: now() }); }
  catch (e) { log(`pub ${seq} err: ${String(e?.message||e).slice(0,50)}`); }
  seq++;
  if (now() - t0 >= nextInterim) { report(`INTERIM +${Math.round((now()-t0)/60000)}m`); nextInterim += INTERIM_MS; }
  await wait(PUB_EVERY_MS);
}
log(`publish window closed (${published.length} msgs); draining ${MATURE_MS/1000}s for final maturity`);
await wait(MATURE_MS + 2000);
report('FINAL');
for (const s of subs) { try { await s.peer.leave?.({ timeoutMs: 5000 }); } catch {} }
try { await pub.peer.leave?.({ timeoutMs: 5000 }); } catch {}
process.exit(0);
