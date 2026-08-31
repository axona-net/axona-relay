// =============================================================================
// harness/soak-account.mjs — the FROZEN measurement contract, executable.
//
// Implements harness/soak-manifest.json exactly, so the contract and the tool
// cannot diverge (Aster's inconsistency, 2026-08-30). Reads the same immutable
// ledgers analyze.mjs reads, plus the disc traces, and emits ONLY the frozen
// schema: denominator, exactly-once live/repair/missing, duplicate as an excess-
// arrival count, per-class latency percentiles, cold/converged apart,
// segmentation by migration/reattach/dead-skip/term-verify, and terminal-verify
// attribution. Buckets are defined here once, from the manifest, and never
// reinterpreted after the run (Vega a88fe61f).
//
//   node harness/soak-account.mjs --dir harness/results --seed 30 --nodes 6 \
//     --region eagle --open-n 4 --owned-n 2 --duration-ms 10800000 \
//     --offsets '{"m4":0,"m1":136,"axona-linux":182,"axona-win":89}' \
//     --manifest harness/soak-manifest.json --out harness/results/account-30.json
// =============================================================================
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { generatePlan } from './lib/workload.mjs';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

const arg = (n, d) => { const i = process.argv.indexOf(`--${n}`); return i >= 0 ? process.argv[i + 1] : d; };
const DIR = arg('dir', 'harness/results');
const SEED = Number(arg('seed'));
const NODES = Number(arg('nodes'));
const REGION = arg('region', 'eagle');
const OFFSETS = JSON.parse(arg('offsets', '{}'));
const DURATION_MS = Number(arg('duration-ms', 10_800_000));
const MANIFEST = JSON.parse(readFileSync(arg('manifest', 'harness/soak-manifest.json'), 'utf8'));
const MISSING_DEADLINE_MS = MANIFEST.bounds.MISSING_DEADLINE_MS;      // 120000
// live/repair split boundary (2026-08-31): a watch delivery within this window of
// api-confirm is the live forward push; beyond it, delivery waited for a renewal
// cycle (repair). Default = RENEW_FAST (5s). Overridable via manifest.bounds.
const LIVE_WINDOW_MS = MANIFEST.bounds.LIVE_WINDOW_MS ?? 5000;
const OUT = arg('out', `${DIR}/account-${SEED}.json`);
if (!Number.isInteger(SEED) || !Number.isInteger(NODES)) { console.error('need --seed --nodes'); process.exit(2); }

const plan = generatePlan({ seed: SEED, nodes: NODES, durationMs: DURATION_MS,
  openN: arg('open-n') ? Number(arg('open-n')) : undefined,
  ownedN: arg('owned-n') ? Number(arg('owned-n')) : undefined });
const topicByName = new Map(plan.topics.map((t, ti) => [t.name, { ...t, ti }]));

// name → topicId hex prefix (the form _disc emits: topicIdBig.toString(16).slice(0,12))
const hexByName = new Map(), nameByHex = new Map();
for (const t of plan.topics) {
  try {
    const big = await deriveTopicIdBig({ region: REGION, name: t.name });
    const hex = big.toString(16).slice(0, 12);
    hexByName.set(t.name, hex); nameByHex.set(hex, t.name);
  } catch { /* */ }
}

// ── load ledgers ─────────────────────────────────────────────────────
const load = (prefix) => {
  const out = [];
  for (const f of readdirSync(DIR).filter((f) => f.startsWith(`${prefix}-${SEED}-`) && f.endsWith('.jsonl')).sort()) {
    for (const line of readFileSync(`${DIR}/${f}`, 'utf8').split('\n')) { if (line.trim()) { try { out.push(JSON.parse(line)); } catch { /* */ } } }
  }
  return out;
};
const cwOf = (r) => (typeof r.wall === 'number' ? r.wall : Date.parse(r.wall)) - (OFFSETS[r.host] ?? 0);

const recs = load('sidecar');
if (!recs.length) { console.error(`no sidecar-${SEED}-*.jsonl in ${DIR}`); process.exit(2); }
for (const r of recs) r.cw = cwOf(r);
recs.sort((a, b) => a.cw - b.cw);

// ── join truths per (topic, seq) ─────────────────────────────────────
const ops = new Map();          // key -> { topic, seq, api, intentCw, obs: Map(peer -> [{via,cw,msgId}]) }
const key = (t, s) => `${t} ${s}`;
const measureStart = new Map(); // peer -> earliest measure-start cw (converged boundary)
const pullHeads = new Map();    // `${topic}:${peer}` -> [{cw, headSeq}] (repair evidence)
for (const r of recs) {
  if (r.t === 'intent' && r.topicSeq >= 0) {
    ops.set(key(r.topic, r.topicSeq), { topic: r.topic, seq: r.topicSeq, publisher: r.peerIdx, intentCw: r.cw, api: null, obs: new Map() });
  } else if (r.t === 'api' && r.topicSeq >= 0) {
    const o = ops.get(key(r.topic, r.topicSeq)); if (o) o.api = { confirmed: r.confirmed, msgId: r.msgId, cw: r.cw };
  } else if (r.t === 'observe') {
    const o = ops.get(key(r.topic, r.topicSeq));
    if (o) { if (!o.obs.has(r.peerIdx)) o.obs.set(r.peerIdx, []); o.obs.get(r.peerIdx).push({ via: r.via, cw: r.cw, msgId: r.msgId }); }
  } else if (r.t === 'pullHead' || r.t === 'head') {
    const k = `${r.topic}:${r.peerIdx}`; if (!pullHeads.has(k)) pullHeads.set(k, []);
    if (r.headSeq != null) pullHeads.get(k).push({ cw: r.cw, headSeq: r.headSeq });
  } else if (r.t === 'event' && r.kind === 'measure-start') {
    if (!measureStart.has(r.peerIdx) || r.cw < measureStart.get(r.peerIdx)) measureStart.set(r.peerIdx, r.cw);
  }
}
for (const arr of pullHeads.values()) arr.sort((a, b) => a.cw - b.cw);
const convergedBoundary = measureStart.size ? Math.max(...measureStart.values()) : -Infinity;
// repair evidence: earliest pull-head for (topic,reader) with cw<=deadline whose
// headSeq reached this message's seq — the reader's store recovered it via pull/
// replay, not the live watch push. Returns that cw, or null.
const repairCw = (topic, peer, seq, deadline) => {
  const hs = pullHeads.get(`${topic}:${peer}`); if (!hs) return null;
  for (const h of hs) { if (h.cw > deadline) break; if (h.headSeq >= seq) return h.cw; }
  return null;
};

// ── disc: per-topic timeline + term-verify attribution ───────────────
// Sidecar disc (disc-<seed>-<peer>.jsonl) AND relay-side disc (relay-disc-*.jsonl,
// written by the routing relays under the LAT_TRACE kernel — Aster condition 1).
// The relay sink interleaves a lat stream; keep only the disc rows.
const loadGlob = (re) => {
  const out = [];
  for (const f of readdirSync(DIR).filter((f) => re.test(f) && f.endsWith('.jsonl')).sort()) {
    for (const line of readFileSync(`${DIR}/${f}`, 'utf8').split('\n')) { if (line.trim()) { try { out.push(JSON.parse(line)); } catch { /* */ } } }
  }
  return out;
};
const disc = [...load('disc'), ...loadGlob(/^relay-disc-/)].filter((d) => d.stream !== 'lat');
for (const d of disc) d.cw = (typeof d.wall === 'number' ? d.wall : Date.parse(d.wall)) - (OFFSETS[d.host] ?? 0);

// root-receipt evidence (David 2026-08-31): the set of msgIds a fleet ROOT received
// (relay-side lat stage root:recv). A message MISSING at a subscriber but present in
// this set reached a root and was NOT pushed down — a fanout/registration failure,
// not a routing failure. Absent from the set = it never reached a root (routing/pub).
const rootRecvMsgIds = new Set();
for (const f of readdirSync(DIR).filter((f) => /^relay-disc-/.test(f) && f.endsWith('.jsonl'))) {
  for (const line of readFileSync(`${DIR}/${f}`, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try { const r = JSON.parse(line); if (r.stream === 'lat' && r.stage === 'root:recv' && r.msgId) rootRecvMsgIds.add(r.msgId); } catch { /* */ }
  }
}
const discByTopic = new Map();  // topicName -> [disc...] (sorted)
for (const d of disc) { const name = nameByHex.get(d.t); if (!name) continue; if (!discByTopic.has(name)) discByTopic.set(name, []); discByTopic.get(name).push(d); }
for (const arr of discByTopic.values()) arr.sort((a, b) => a.cw - b.cw);

// terminal-verify: TWO dimensions (Aster/Orion 8ddefd56). lookupOutcome = what the
// verify lookup returned (self|closer|inconclusive); finalVerifyState = what the
// subscription ended up as (self|attached|deferred|inconclusive), derived from the
// disc sequence on the topic so deferred/inconclusive retries cannot hide in success.
const tv = { attempts: 0, lookup: { self: 0, closer: 0, inconclusive: 0 }, lookupMs: [],
  final: { self: 0, attached: 0, deferred: 0, inconclusive: 0 } };
const FINAL_WINDOW_MS = 30_000;
for (const [name, es] of discByTopic) {
  for (let i = 0; i < es.length; i++) {
    const d = es[i]; if (d.ev !== 'term-verify') continue;
    tv.attempts++;
    const lk = (d.kind === 'self' || d.kind === 'closer') ? d.kind : 'inconclusive';
    tv.lookup[lk]++;
    if (Number.isFinite(d.ms)) tv.lookupMs.push(d.ms);
    // final state: the next became-root / sub-root on this topic within the window
    let attach = null;
    for (let j = i + 1; j < es.length && es[j].cw - d.cw <= FINAL_WINDOW_MS; j++) {
      if (es[j].ev === 'became-root') { attach = 'self'; break; }
      if (es[j].ev === 'sub-root') { attach = 'attached'; break; }
    }
    if (lk === 'self') tv.final.self++;
    else if (attach === 'attached') tv.final.attached++;
    else if (lk === 'inconclusive' && !attach) tv.final.inconclusive++;
    else tv.final.deferred++;
  }
}

// a topic "migrated in the window" if a became-root/sub-root to a NEW root appears
// between publish and deadline; "reattach fired" if a branch-reattach appears.
const topicSegments = (topicName, fromCw, toCw) => {
  const es = (discByTopic.get(topicName) || []).filter((d) => d.cw >= fromCw - 5000 && d.cw <= toCw + 5000);
  const roots = new Set(es.filter((d) => d.ev === 'sub-root' || d.ev === 'became-root').map((d) => d.root).filter(Boolean));
  return { migration: roots.size > 1, reattach: es.some((d) => d.ev === 'branch-reattach') };
};
// local-minimum ESCAPE on a topic in a window: a term-verify that found a strictly
// closer root (kind=closer) — greedy would have stalled, the iterative lookup saved
// it. Per-trial join (Aster condition 3): does a trial whose delivery window saw an
// escape pay a latency tax or a higher miss rate? Needs relay-side disc to fire.
const topicEscape = (topicName, fromCw, toCw) =>
  (discByTopic.get(topicName) || []).some((d) => d.ev === 'term-verify' && d.kind === 'closer' && d.cw >= fromCw - 5000 && d.cw <= toCw + 5000);

// ── frozen classification ────────────────────────────────────────────
const pctl = (a, p) => a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(a.length * p))] : null;
const lat = { live: { cold: [], converged: [] }, repair: { cold: [], converged: [] } };
const migGaps = [];                              // delivery latency of delivered trials under an active migration
const MIGRATION_GAP_SLO_MS = MANIFEST.bounds.MIGRATION_GAP_SLO_MS;   // 10000, distinct from the 120s deadline
let trials = 0;                                   // (trial,reader) pairs on confirmed publishes
let liveN = 0, repairN = 0, missingN = 0;
const missClass = { reachedRootNotDelivered: 0, neverReachedRoot: 0 };  // fanout vs routing loss
const seg = { migration: { live: 0, repair: 0, missing: 0 }, stable: { live: 0, repair: 0, missing: 0 }, reattach: { live: 0, repair: 0, missing: 0 } };
const latBySeg = { stable: [], migration: [] };   // live latency split by segment (is migration slower?)
const causal = { escape: { trials: 0, missing: 0, liveLat: [] }, noEscape: { trials: 0, missing: 0, liveLat: [] } };
// Duplicate accounting (Aster condition 2): a REAL duplicate delivery is the same
// msgId pushed to a reader more than once via WATCH (the live forward path firing
// twice). A watch arrival plus pull-head arrivals is NOT a duplicate — it is the
// harness's own head-sweep re-reading a message it already has. Path-pair split
// makes that distinction visible, so a density change cannot look like a win by
// amplifying duplicate fanout while the sampler noise is filtered out.
const dup = { realTrials: 0, watchExcess: [], totalExcess: [],
  pathPairs: { 'watch-clean': 0, 'watch+pull(sampler)': 0, 'watch-DUP': 0, 'pull-only(repair)': 0, 'pull-repeated(sampler)': 0 },
  realBySegment: { stable: 0, migration: 0 }, realByClass: { live: 0, repair: 0 } };
let confirmedPublishes = 0;

for (const o of ops.values()) {
  const t = topicByName.get(o.topic); if (!t) continue;          // skip coord etc.
  if (!o.api?.confirmed || !o.api.msgId) continue;               // denominator: confirmed only
  confirmedPublishes++;
  const startCw = o.api.cw;                                       // anchor = api-confirm
  const deadline = startCw + MISSING_DEADLINE_MS;
  const readers = t.requiredReaders.filter((p) => p !== o.publisher);
  for (const p of readers) {
    trials++;
    const arrivals = (o.obs.get(p) || []).filter((x) => x.msgId === o.api.msgId).sort((a, b) => a.cw - b.cw);
    const W = arrivals.filter((x) => x.via === 'watch').length;
    const P = arrivals.filter((x) => x.via === 'pull').length;
    // real duplicate = >1 live watch delivery; path-pair classifies the rest
    const realDup = W > 1;
    if (realDup) { dup.realTrials++; dup.watchExcess.push(W - 1); }
    if (arrivals.length > 1) dup.totalExcess.push(arrivals.length - 1);
    const pp = W > 1 ? 'watch-DUP' : (W === 1 && P >= 1) ? 'watch+pull(sampler)' : (W === 1) ? 'watch-clean'
      : (P > 1) ? 'pull-repeated(sampler)' : (P === 1) ? 'pull-only(repair)' : null;
    if (pp) dup.pathPairs[pp] = (dup.pathPairs[pp] ?? 0) + 1;
    // live/repair now split on WATCH-ARRIVAL TIMING (2026-08-31, pull removed). Every
    // delivery a subscriber gets — the root's live fanout push AND a renewal replay —
    // arrives through the watch callback; the difference is latency. A delivery within
    // LIVE_WINDOW_MS of api-confirm is the forward push (live); a later one waited for a
    // renewal cycle (repair). No routed pull, so no observer effect. Supersedes the
    // pull-head repair proxy, which routed to the root and perturbed the run.
    const firstWatch = arrivals.find((x) => x.via === 'watch' && x.cw <= deadline);
    const phase = startCw < convergedBoundary ? 'cold' : 'converged';
    let cls, deliverCw = null;
    if (firstWatch) {
      const dt = firstWatch.cw - startCw; deliverCw = firstWatch.cw;
      if (dt <= LIVE_WINDOW_MS) { cls = 'live'; liveN++; lat.live[phase].push(dt); }
      else { cls = 'repair'; repairN++; lat.repair[phase].push(dt); }
    } else {
      cls = 'missing'; missingN++;
      // classify the loss: did a root receive this message (fanout failure) or not (routing failure)?
      if (rootRecvMsgIds.has(o.api.msgId)) missClass.reachedRootNotDelivered++; else missClass.neverReachedRoot++;
    }
    const sg = topicSegments(o.topic, startCw, deliverCw ?? deadline);
    const bucket = sg.migration ? 'migration' : 'stable';
    seg[bucket][cls]++;
    if (sg.reattach) seg.reattach[cls]++;
    if (realDup) { dup.realBySegment[bucket]++; if (cls === 'live' || cls === 'repair') dup.realByClass[cls]++; }
    // migration-gap SLO (10s): a delivered trial under an active migration must arrive within the SLO
    if (deliverCw != null && sg.migration) migGaps.push(deliverCw - startCw);
    // per-trial causal join (E): latency by segment, and outcome by local-min escape
    if (cls === 'live') latBySeg[bucket].push(deliverCw - startCw);
    const cc = topicEscape(o.topic, startCw, deliverCw ?? deadline) ? causal.escape : causal.noEscape;
    cc.trials++; if (cls === 'missing') cc.missing++; if (cls === 'live') cc.liveLat.push(deliverCw - startCw);
  }
}

const classStat = (a) => ({ n: a.length, p50: pctl(a, 0.5), p95: pctl(a, 0.95), p99: pctl(a, 0.99), max: a.length ? Math.max(...a) : null });
const account = {
  contract: { manifest: 'harness/soak-manifest.json',
    missingDeadlineMs: MISSING_DEADLINE_MS, migrationGapSloMs: MIGRATION_GAP_SLO_MS,
    latencyAnchor: 'api-confirm → first app delivery', delivered: 'live + repair only' },
  denominator: { confirmedPublishes, trials },
  classification: {
    live: liveN, repair: repairN, missing: missingN,
    delivered: liveN + repairN,
    deliveryRatePct: trials ? +(100 * (liveN + repairN) / trials).toFixed(3) : null,
    missingRatePct: trials ? +(100 * missingN / trials).toFixed(3) : null,
  },
  missingClassification: {
    note: 'David 2026-08-31: a missing message that a ROOT received (relay-side root:recv) reached a root but was not pushed to the subscriber — a FANOUT/registration failure; one with no root:recv NEVER reached a root — a ROUTING/publish failure. Needs relay-side disc (LAT_TRACE) to populate.',
    rootRecvMsgIdsSeen: rootRecvMsgIds.size,
    reachedRootNotDelivered: missClass.reachedRootNotDelivered,
    neverReachedRoot: missClass.neverReachedRoot,
    fanoutSharePct: missingN ? +(100 * missClass.reachedRootNotDelivered / missingN).toFixed(1) : null,
  },
  duplicate: {
    note: 'REAL duplicate = same msgId delivered >1x via WATCH (live path fired twice). watch+pull is the head-sweep re-reading, NOT a duplicate. kNear must not win by inflating real duplicates.',
    realDuplicateTrials: dup.realTrials,
    realDuplicateRatePct: trials ? +(100 * dup.realTrials / trials).toFixed(3) : null,
    watchExcessPerTrial: { p50: pctl(dup.watchExcess, 0.5), p95: pctl(dup.watchExcess, 0.95), p99: pctl(dup.watchExcess, 0.99), max: dup.watchExcess.length ? Math.max(...dup.watchExcess) : null },
    realBySegment: dup.realBySegment, realByClass: dup.realByClass,
    pathPairs: dup.pathPairs,
    totalExcessInclSampler: dup.totalExcess.reduce((a, b) => a + b, 0),
  },
  latency: {
    live: { cold: classStat(lat.live.cold), converged: classStat(lat.live.converged) },
    repair: { cold: classStat(lat.repair.cold), converged: classStat(lat.repair.converged) },
  },
  migrationGapSlo: {
    boundMs: MIGRATION_GAP_SLO_MS,
    role: 'pass/fail SLO for delivery under an active root transition (distinct from the 120s completeness deadline)',
    deliveredUnderMigration: migGaps.length,
    withinSlo: migGaps.filter((g) => g <= MIGRATION_GAP_SLO_MS).length,
    overSlo: migGaps.filter((g) => g > MIGRATION_GAP_SLO_MS).length,
    gap: classStat(migGaps),
  },
  segmentation: {
    stable: seg.stable, migration: seg.migration, reattachFired: seg.reattach,
    note: 'migration = topic root changed in the trial window; reattachFired counted where a branch-reattach appeared (may overlap migration)',
  },
  causal: {
    note: 'per-trial join (Aster 3): latency by segment, and outcome split by whether a local-minimum escape (term-verify closer) fired on the topic in the delivery window. Escape correlation needs relay-side disc — it is empty when disc ran sidecar-only.',
    liveLatencyBySegment: { stable: classStat(latBySeg.stable), migration: classStat(latBySeg.migration) },
    byEscape: {
      withEscape: { trials: causal.escape.trials, missRatePct: causal.escape.trials ? +(100 * causal.escape.missing / causal.escape.trials).toFixed(3) : null, liveLatP50: pctl(causal.escape.liveLat, 0.5), liveLatP95: pctl(causal.escape.liveLat, 0.95) },
      noEscape: { trials: causal.noEscape.trials, missRatePct: causal.noEscape.trials ? +(100 * causal.noEscape.missing / causal.noEscape.trials).toFixed(3) : null, liveLatP50: pctl(causal.noEscape.liveLat, 0.5), liveLatP95: pctl(causal.noEscape.liveLat, 0.95) },
    },
  },
  terminalVerify: {
    verifyAttempts: tv.attempts,
    lookupOutcome: tv.lookup,
    finalVerifyState: tv.final,
    lookupMs: { n: tv.lookupMs.length, p50: pctl(tv.lookupMs, 0.5), p95: pctl(tv.lookupMs, 0.95), p99: pctl(tv.lookupMs, 0.99), max: tv.lookupMs.length ? Math.max(...tv.lookupMs) : null },
  },
  discEventsSeen: disc.length,
  toolSha256: createHash('sha256').update(readFileSync(new URL(import.meta.url).pathname)).digest('hex').slice(0, 16),
};
console.log(JSON.stringify(account, null, 1));
writeFileSync(OUT, JSON.stringify(account));
