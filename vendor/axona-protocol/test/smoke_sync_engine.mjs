// smoke_sync_engine.mjs — the Phase 8 sync engine, executable (v0.2 program).
//
// Pins:
//   1. SYNC_POLICIES is COMPLETE: every row carries the full typed shape, and
//      every row that plants a nature on its receiver names that nature's
//      evictor (the principal-liveness rule, I-10, machine-checked).
//   2. SPLIT_UNION's per-pair quench ledger lives INSIDE the engine: a repeat
//      pull at the same lw is suppressed; a DECREASED lw re-arms (4.22.1).
//   3. PUB_DURABLE (#353 — flagged behavior change): a SELF-ROOTED publish
//      confirms its pending entry only AFTER the eager cohort replicate has
//      dispatched, so leave()'s evidence drain holds an ephemeral publisher
//      until its history has left the node. Cohort-less roots confirm at once.
//   4. HANDOFF ingest hook: the heir adopts as root and the engine sends the
//      confirming HANDOFFACK.
//
// Run: node test/smoke_sync_engine.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { SYNC_POLICIES } from '../src/pubsub/syncEngine.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicIdBig } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { T } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const idHex = (b) => b.toString(16).padStart(66, '0');

function mk({ selfBig, neighbors = [], kClosest = null, replicas = 2 } = {}) {
  const sends = [];
  const dht = {
    getSelfId: () => selfBig, onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => neighbors,
    bridgeId: () => null,
  };
  if (kClosest) dht.findKClosest = async () => kClosest;
  const am = new AxonaManager({ dht, now: () => Date.now(), rootReplicas: replicas });
  return { am, sends };
}

console.log('sync engine — one operation, one policy table\n');

// ── 1. Policy-table completeness (typed by natures; I-10) ──────────────────
{
  const REQUIRED = ['mode', 'verb', 'trigger', 'summary', 'createsOnReceiver', 'evictor', 'ledger', 'rateBound'];
  let complete = true, evicted = true;
  for (const [name, row] of Object.entries(SYNC_POLICIES)) {
    for (const f of REQUIRED) if (!(f in row)) { complete = false; console.log(`    missing ${name}.${f}`); }
    if (row.createsOnReceiver && (!row.evictor || /n\/a/.test(row.evictor))) { evicted = false; console.log(`    ${name} plants '${row.createsOnReceiver}' without an evictor`); }
  }
  ok('every policy row carries the full typed shape', complete);
  ok('every nature-planting row names its evictor (principal-liveness, I-10)', evicted);
  ok('all six repair policies + the PUB_DURABLE gate are present',
    ['REPLAY_UP', 'SPLIT_UNION', 'EMPTY_ROOT_PROBE', 'COHORT_REPLICATE', 'UNION_AT_ROOT', 'HANDOFF', 'PUB_DURABLE']
      .every(k => k in SYNC_POLICIES));
}

// ── 2. SPLIT_UNION ledger is engine-enforced (one-shot per (child, lw)) ────
{
  const REG = 0x87n << 248n, SELF = REG | 0x11n, TOPIC = REG | 0xabcn, CHILD = REG | 0x99n;
  const { am, sends } = mk({ selfBig: SELF });
  const role = am._becomeRoot(TOPIC);
  role.cache.push({ msgId: 'm1', publishTs: 500, json: '{}', bytes: 2, seq: 1 });
  sends.length = 0;
  const s1 = am._syncPull(CHILD, TOPIC, 'SPLIT_UNION', { sinceHw: 0, lw: 100, role });
  const s2 = am._syncPull(CHILD, TOPIC, 'SPLIT_UNION', { sinceHw: 0, lw: 100, role });
  const s3 = am._syncPull(CHILD, TOPIC, 'SPLIT_UNION', { sinceHw: 0, lw: 50, role });
  ok('first pull at lw=100 fires', s1 === true);
  ok('repeat pull at the SAME lw is quenched by the ledger', s2 === false);
  ok('a DECREASED lw (deeper split) re-arms the pull', s3 === true);
  ok('exactly two PULLUPs on the wire', sends.filter(s => s.type === T.PULLUP).length === 2);
}

// ── 3. PUB_DURABLE (#353): self-rooted confirm waits for cohort dispatch ───
{
  const author = await createAuthorIdentity();
  const topicDesc = { region: 'useast', name: 'sync-engine-smoke', write: 'open' };
  const TOPIC = await deriveTopicIdBig(topicDesc);
  const SELF = TOPIC ^ 1n;                          // adjacent → plausibly closest
  const NEAR = TOPIC ^ 2n;
  // (a) with a cohort: pending survives until the replicate dispatch
  {
    const { am, sends } = mk({ selfBig: SELF, kClosest: [NEAR], replicas: 2 });
    const role = am._becomeRoot(TOPIC);
    const env = await buildEnvelope({ topic: topicDesc, message: { text: 'hold me' }, seq: 1, identity: author });
    const json = JSON.stringify(env);
    am._pendingPub = new Map([[env.msgId, { topicBig: TOPIC, json, at: Date.now(), tries: 0 }]]);
    await am._ingestPublish(role, json);
    const replicated = sends.some(s => s.type === T.REPLICATE);
    ok('eager cohort REPLICATE dispatched during the publish ingest', replicated);
    ok('pending publish confirmed only after the dispatch (empty now)', am._pendingPub.size === 0);
  }
  // (b) cohort-less (rootReplicas 0): confirm immediately, no replicate
  {
    const { am, sends } = mk({ selfBig: SELF, kClosest: [NEAR], replicas: 0 });
    const role = am._becomeRoot(TOPIC);
    const env = await buildEnvelope({ topic: topicDesc, message: { text: 'solo' }, seq: 2, identity: author });
    const json = JSON.stringify(env);
    am._pendingPub = new Map([[env.msgId, { topicBig: TOPIC, json, at: Date.now(), tries: 0 }]]);
    await am._ingestPublish(role, json);
    ok('cohort-less root sends no REPLICATE', !sends.some(s => s.type === T.REPLICATE));
    ok('…and still confirms its own pending at once', am._pendingPub.size === 0);
  }
}

// ── 4. HANDOFF ingest hook: heir adopts + engine acks ──────────────────────
{
  const REG = 0x87n << 248n, SELF = REG | 0x11n, TOPIC = REG | 0xabcn, LEAVER = REG | 0x77n;
  const { am, sends } = mk({ selfBig: SELF });
  await am._onHandoff({ topicId: idHex(TOPIC), from: idHex(LEAVER).toLowerCase(), msgs: [], dels: [] }, { targetId: SELF });
  const role = am.axonRoles.get(TOPIC);
  ok('heir adopted the topic as ROOT', !!role && role.isRoot === true);
  ok('engine sent the confirming HANDOFFACK to the leaver',
    sends.some(s => s.type === T.HANDOFFACK && s.target === LEAVER));
}

console.log(`\nResult: ${n - fail}/${n} ok${fail ? ` — ${fail} FAILED` : ''}`);
process.exit(fail ? 1 : 0);
