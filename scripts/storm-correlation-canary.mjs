// =====================================================================
// storm-correlation-canary.mjs — live 6522f2f correlation on TESTNET.
//
// THE QUESTION (the one open empirical item from the connection-quality
// arc): on the LIVE testnet mesh, do never-binding candidates actually
// occur, and does synaptome maintenance re-probe them the way the isolated
// kernel test predicts (c16d12b: 3 probes/tick, sustained, forever)? And
// does the 4.67.1 attempt guard bound exactly that, live?
//
// METHOD: a CANARY LEAF PEER — a new node joining the mesh as any peer may.
// It touches NO fleet relay, rolls nothing, changes nothing: it dials, it
// measures, it leaves. Kernel 4.67.1 from the local checkout (the deployed
// fleet stays on its own vendored kernel), web transport + polyfills, the
// same bootstrap order as every relay/CLI (transport → peer → ready →
// integrate; the 2026-07-25 lifecycle).
//
// ARMS (env GUARD):
//   GUARD=0  maintenance ON, guard OFF — the historical configuration that
//            stormed. Expectation if the mechanism correlates: candidates
//            that never bind accumulate attempts far past any budget.
//   GUARD=1  maintenance ON, guard ON — attempts per candidate bounded by
//            maxAttempts with backoff; expiry holds.
//
// PRIVACY: transport/node ids are NEVER persisted (standing rule). The log
// keys candidates by a TRUNCATED SHA-256 of the identity suffix — stable
// within the analysis, useless as a correlator outside it. The canary's own
// identity is minted fresh per run (INVARIANT I-ID) and discarded.
//
// env: GUARD=0|1  RUN_SEC=1200  MAINT_MS=5000  KNEAR=5  MAXPERTICK=3
//      MAXATTEMPTS=4  BASE_MS=20000  BRIDGE=wss://testnet.axona.net
//      OUT=results/storm-canary_<arm>.jsonl  SAMPLE_MS=30000
//
// Run (from axona-relay/):  RELAY_NETWORK=testnet GUARD=0 node scripts/storm-correlation-canary.mjs
// =====================================================================
import '../src/polyfill.js';
import { WebSocketImpl, cleanupWebRTC } from '../src/polyfill.js';
import { createHash } from 'node:crypto';
import { appendFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

// The 4.67.1 kernel — the CHECKOUT, not the relay's vendored copy.
const K = '/Users/croqueteer/Documents/claude/axona-protocol/src';
const { AxonaPeer }          = await import(`${K}/dht/AxonaPeer.js`);
const { AxonaDomain }        = await import(`${K}/dht/AxonaDomain.js`);
const { NeuronNode }         = await import(`${K}/dht/NeuronNode.js`);
const { createNodeIdentity } = await import(`${K}/identity/index.js`);
const { webTransport }       = await import(`${K}/transport/web/index.js`);
const { KERNEL_VERSION }     = await import(`${K}/transport/handshake.js`);

const GUARD       = process.env.GUARD === '1';
const RUN_SEC     = +(process.env.RUN_SEC || 1200);
const MAINT_MS    = +(process.env.MAINT_MS || 5000);
const KNEAR       = +(process.env.KNEAR || 5);
const MAXPERTICK  = +(process.env.MAXPERTICK || 3);
const MAXATTEMPTS = +(process.env.MAXATTEMPTS || 4);
const BASE_MS     = +(process.env.BASE_MS || 20000);
const BRIDGE      = process.env.BRIDGE || 'wss://testnet.axona.net';
const SAMPLE_MS   = +(process.env.SAMPLE_MS || 30000);
const OUT         = process.env.OUT || `results/storm-canary_${GUARD ? 'guarded' : 'unguarded'}_${Date.now()}.jsonl`;

mkdirSync(dirname(OUT), { recursive: true });
writeFileSync(OUT, '');
const rec = (o) => appendFileSync(OUT, JSON.stringify(o) + '\n');
const now = () => Date.now();

// Candidate key: truncated hash of the 256-bit identity suffix. Never the id.
const MASK_256 = (1n << 256n) - 1n;
function candKey(id) {
  let suffix;
  if (typeof id === 'bigint') suffix = (id & MASK_256).toString(16).padStart(64, '0');
  else if (typeof id === 'string' && id.length >= 64) suffix = id.slice(-64);
  else suffix = String(id);
  return createHash('sha256').update(suffix).digest('hex').slice(0, 12);
}

console.log(`storm-correlation canary — kernel ${KERNEL_VERSION} (checkout) → ${BRIDGE}`);
console.log(`arm: maintenance ON (kNear=${KNEAR}, tick=${MAINT_MS}ms, maxPerTick=${MAXPERTICK}); guard ${GUARD ? `ON (maxAttempts=${MAXATTEMPTS}, base=${BASE_MS}ms)` : 'OFF (the historical configuration)'}; run ${RUN_SEC}s`);

const identity = await createNodeIdentity({ lat: 37.77, lng: -122.42 });   // fresh per run, discarded
const transport = webTransport({
  bridgeUrl: BRIDGE,
  identity:  { ...identity, id: identity.id },
  meshRelay: true,
  reconnect: true,
  WebSocketImpl,
  log: () => {},
});
const node = new NeuronNode({ id: BigInt('0x' + identity.id), lat: 37.77, lng: -122.42 });
node.transport = transport;
const domain = new AxonaDomain({ k: 20 });
const peer = new AxonaPeer({
  domain, node, nodeIdentity: identity, transport,
  synaptomeMaintain: { kNear: KNEAR, intervalMs: MAINT_MS, maxPerTick: MAXPERTICK },
  ...(GUARD ? { attemptGuard: { maxAttempts: MAXATTEMPTS, baseMs: BASE_MS, factor: 2, refillWindowMs: 60000 } } : {}),
});

// ── instrumentation: every outbound connection attempt, per candidate ──
const cand = new Map();   // key -> {attempts, ok, fail, err, firstT, lastT, bound}
let attemptsTotal = 0, boundEvents = 0, diedEvents = 0;
const origOpen = transport.openConnection.bind(transport);
transport.openConnection = async (peerId, ...a) => {
  const key = candKey(peerId);
  const c = cand.get(key) ?? { attempts: 0, ok: 0, fail: 0, err: 0, firstT: now(), lastT: 0, bound: false };
  c.attempts++; c.lastT = now(); attemptsTotal++;
  cand.set(key, c);
  try {
    const r = await origOpen(peerId, ...a);
    if (r) { c.ok++; c.bound = true; } else c.fail++;
    return r;
  } catch (e) { c.err++; throw e; }
};
transport.onPeerBound?.((big) => { boundEvents++; const c = cand.get(candKey(big)); if (c) c.bound = true; });
transport.onPeerDied?.(() => { diedEvents++; });

// ── the 2026-07-25 bootstrap order, verbatim ──
await transport.start();
await peer.start();
let ready = null;
try { ready = await peer.ready({}); } catch (e) { console.log('ready() threw:', e?.message ?? e); }
try { await peer.integrate?.(); } catch (e) { console.log('integrate() threw:', e?.message ?? e); }
console.log(`connected: synaptome=${node.synaptome.size} in=${node.incomingSynapses?.size ?? 0} ready=${JSON.stringify(ready ?? {}).slice(0, 120)}`);
rec({ t: now(), ev: 'connected', guard: GUARD, kernel: KERNEL_VERSION, syn: node.synaptome.size });

// ── phase 2: canary-side induced churn (CHURN_MS > 0) ──
// Reopen the near-quota deficit against the LIVE mesh: every CHURN_MS, close
// the canary's CHURN_K nearest bound connections. The far ends are fleet
// relays — for THEM this is one leaf peer disconnecting, ordinary churn; no
// relay's behavior or state is modified. For the CANARY it recreates the
// fleet-wide storm precondition phase 1 showed is required: a deficit that
// keeps reopening, filled from live candidates ~half of which never bind.
const CHURN_MS = +(process.env.CHURN_MS || 0);
const CHURN_K  = +(process.env.CHURN_K || 3);
let churnRounds = 0, churnDrops = 0;
let churner = null;
if (CHURN_MS > 0) {
  const selfBig = node.id;
  churner = setInterval(() => {
    const ids = [...node.synaptome.keys()].filter((k) => typeof k === 'bigint');
    ids.sort((a, b) => { const da = selfBig ^ a, db = selfBig ^ b; return da < db ? -1 : da > db ? 1 : 0; });
    const drop = ids.slice(0, CHURN_K);
    for (const id of drop) {
      node.synaptome.delete(id);
      try { const p = transport.closeConnection(id); p?.catch?.(() => {}); } catch { /* */ }
      churnDrops++;
    }
    churnRounds++;
  }, CHURN_MS);
  console.log(`induced churn: dropping ${CHURN_K} nearest every ${CHURN_MS}ms (canary-side only)`);
}

// ── sampling loop ──
const summary = () => {
  const rows = [...cand.values()];
  const nonBind = rows.filter((c) => !c.bound);
  const maxAtt = rows.reduce((m, c) => Math.max(m, c.attempts), 0);
  const maxNonBindAtt = nonBind.reduce((m, c) => Math.max(m, c.attempts), 0);
  const overBudget = nonBind.filter((c) => c.attempts > MAXATTEMPTS).length;
  return {
    syn: node.synaptome.size, inSyn: node.incomingSynapses?.size ?? 0,
    candidates: rows.length, neverBinders: nonBind.length,
    attemptsTotal, boundEvents, diedEvents,
    maxAttemptsAnyCand: maxAtt, maxAttemptsNeverBinder: maxNonBindAtt,
    neverBindersOverBudget: overBudget,
    ...(CHURN_MS > 0 ? { churnRounds, churnDrops } : {}),
    ...(GUARD ? { guardRefills: peer._attemptGuard?.refills ?? 0, guardCoalesced: peer._attemptGuard?.coalesced ?? 0 } : {}),
  };
};
const sampler = setInterval(() => {
  const s = summary();
  rec({ t: now(), ev: 'sample', ...s });
  console.log(`[${new Date().toISOString().slice(11, 19)}] syn=${s.syn} cand=${s.candidates} neverBind=${s.neverBinders} attempts=${s.attemptsTotal} maxNB=${s.maxAttemptsNeverBinder} overBudget=${s.neverBindersOverBudget}`);
}, SAMPLE_MS);

// ── run, then report and leave cleanly ──
await new Promise((r) => setTimeout(r, RUN_SEC * 1000));
clearInterval(sampler);
if (churner) clearInterval(churner);
const fin = summary();
// per-candidate attempt distribution (never-binders), bucketed
const nb = [...cand.values()].filter((c) => !c.bound).map((c) => c.attempts).sort((a, b) => a - b);
const buckets = { '1': 0, '2-4': 0, '5-9': 0, '10-19': 0, '20+': 0 };
for (const a of nb) { if (a === 1) buckets['1']++; else if (a <= 4) buckets['2-4']++; else if (a <= 9) buckets['5-9']++; else if (a <= 19) buckets['10-19']++; else buckets['20+']++; }
rec({ t: now(), ev: 'final', guard: GUARD, ...fin, neverBinderAttemptBuckets: buckets, runSec: RUN_SEC, maintMs: MAINT_MS });
console.log('\n=== FINAL ===');
console.log(JSON.stringify({ guard: GUARD, ...fin, neverBinderAttemptBuckets: buckets }, null, 1));
console.log(`\nVERDICT INPUTS: never-binders=${fin.neverBinders}; max attempts to a never-binder=${fin.maxAttemptsNeverBinder}; ` +
  (GUARD ? `guard bound=${MAXATTEMPTS} (over-budget should be 0)` : `unguarded (over-budget count = the live storm signal)`));
console.log(`jsonl: ${OUT}`);

try { await peer.leave?.(); } catch { /* */ }
try { await peer.stop?.(); } catch { /* */ }
try { await transport.stop?.(); } catch { /* */ }
cleanupWebRTC();
process.exit(0);
