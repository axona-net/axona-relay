// =====================================================================
// reconcile-delivery.mjs — combined Gate-4 item 4: reconcile the publish-time
// expectation ledger (kernel 73b705d fanout-ledger) against actual app receipts,
// with PATH-AWARE per-edge attribution, to the council-frozen rules
// (Aster 722f8464 + 89ab0777; Orion 6c09ee0d/35a201c4/5e20eb5d; Vega 5e4a6bb7/
// 096003d3/537d83a6/f57c839c).
//
// It answers "did each ELIGIBLE subscriber's app receive each message" and
// localizes every miss WITHOUT conflating strata, and WITHOUT overstating what
// the evidence proves.
//
//   D_intent   who the harness REQUIRED (test intent) — PARTIAL until the harness
//              lifecycle ledger is time-indexed; see UNOBSERVED_BELIEF below.
//   D_belief   who the kernel BELIEVED it owed app delivery (from fanout-ledger)
//   R_app      who delivered to their app (from deliver:app), deduped per (msg,sub)
//
// EXPECTATION SET (Aster 722f8464): tree position and delivery obligation are
// SEPARATE. Expected app recipients per msgId =
//   { recip.sub where child==0 }  UNION  { node where localDelivery==1 }
// deduped by nodeId — NEVER graph leafhood. A node that is both a forwarder and a
// subscriber is counted via its own localDelivery==1.
//
// PATH-AWARE ATTRIBUTION (Aster 89ab0777 — the item-4 correction). A binary
// "forwarded?" flag is WRONG: a multi-hop path can be proven across edge 1 and
// fail at edge 2, which a binary flag would miscall a callback drop. So for each
// MISSED subscriber:
//   - reconstruct the ordered root->...->subscriber path from ledger parent/child
//     edges;
//   - join hopAttempt evidence to EACH edge (an edge A->B is proven iff a
//     deliver:hop_tx from A to B carrying this msgId has a matching rx);
//   - the EARLIEST edge lacking success evidence is the forwarding boundary
//     (FORWARDING_DROP@depth);
//   - classify FINAL_HOP_CALLBACK only when EVERY network edge is proven and the
//     app receipt is absent;
//   - if the path can't be ordered (missing/conflicting parent, unreachable root)
//     -> UNRESOLVED, never a forced bucket.
// A proven hop advances the proven-path boundary; it does not make the whole path
// "forwarded" (send-resolved-on-reply is positive per-edge evidence only).
//
// TREE-COMPLETENESS GATE (Aster 89ab0777). Structural recips==n and
// parent-row-present are necessary but NOT sufficient — a wholly omitted subtree
// disappears with neither symptom. "Exact partition" is therefore scoped to a
// trace-complete, root-connected observed belief tree. A publish is VOID unless:
// exactly one root, acyclic parentage, every ledger node reaches the root, one
// consistent topic, no conflicting edge roles, recips==n on every row, and (when a
// root-members row exists) the root projection reproduces its count. Absence of a
// row is treated as TELEMETRY absence (UNRESOLVED/UNVERIFIED), NOT protocol
// absence, until the kernel emits a trace-write-failure marker (a further kernel
// change, held for David).
//
// D_intent members absent from the ledger -> UNOBSERVED_BELIEF (UNVERIFIED
// divergence), never kernel failure (Aster #1). Requires the time-indexed harness
// ledger, which does not exist yet, so this class is reported as pending.
//
// Receipts deduped per (msg,subscriber), duplicates reported. Prospective
// right-censor within the censor window of arm end.
//
//   node harness/reconcile-delivery.mjs [dir] [--selftest] [--censor=MS]
// =====================================================================
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const DIR = process.argv[2] && !process.argv[2].startsWith('--') ? process.argv[2] : 'harness/results';
const SELFTEST = process.argv.includes('--selftest');
const CENSOR_MS = Number((process.argv.find((a) => a.startsWith('--censor=')) || '').split('=')[1] || 5000);

const pfx = (h) => (typeof h === 'string' ? h.slice(0, 12) : null);
const setDiff = (a, b) => [...a].filter((x) => !b.has(x));

// ---- parse -----------------------------------------------------------
function parse(dir) {
  const files = readdirSync(dir).filter((f) => f.endsWith('.jsonl'));
  const ledgerRows = [], rootMembers = [];
  const hopTx = new Map();          // hopAttemptId -> {msgIds, from, to}
  const hopRx = new Set();
  const appRecv = [], subRecv = [];
  const idMap = new Map();          // (peerIdx|host) -> node prefix (last-wins; legacy)
  const selfTL = [];                // TIME-INDEXED {peerIdx, host, self, wall} — reconnect continuity
  const lifecycle = [];             // sidecar Ledger events: sub-activate / sub-end / topicmap / measure-start / end
  let maxT = 0;
  for (const f of files) {
    let text; try { text = readFileSync(join(dir, f), 'utf8'); } catch { continue; }
    for (const line of text.split('\n')) {
      if (!line) continue;
      let r; try { r = JSON.parse(line); } catch { continue; }
      if (typeof r.t === 'number' && r.t > maxT) maxT = r.t;
      if (typeof r.wall === 'number' && r.wall > maxT) maxT = r.wall;
      if (typeof r.self === 'string' && Number.isInteger(r.peerIdx) && typeof r.host === 'string') {
        idMap.set(`${r.peerIdx}|${r.host}`, pfx(r.self));
        if (typeof r.wall === 'number') selfTL.push({ peerIdx: r.peerIdx, host: r.host, self: pfx(r.self), wall: r.wall });
      }
      // sidecar lifecycle Ledger rows: wall is an ISO string here (not epoch ms)
      if (r.t === 'event' && typeof r.kind === 'string' && Number.isInteger(r.peerIdx)) {
        const wallMs = typeof r.wall === 'string' ? Date.parse(r.wall) : (typeof r.wall === 'number' ? r.wall : null);
        lifecycle.push({ kind: r.kind, detail: r.detail || {}, peerIdx: r.peerIdx, host: r.host, wallMs });
      }
      if (r.ev === 'root-members' && r.msgId) rootMembers.push(r);
      const stage = r.stage;
      if (stage === 'fanout-ledger') ledgerRows.push(r);
      else if (stage === 'deliver:hop_tx') { if (r.hopAttemptId != null && !hopTx.has(r.hopAttemptId)) hopTx.set(r.hopAttemptId, { msgIds: r.msgIds || [], from: pfx(r.from), to: pfx(r.to) }); }
      else if (stage === 'deliver:hop_rx') { if (r.hopAttemptId != null) hopRx.add(r.hopAttemptId); }
      else if (stage === 'deliver:app') appRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t });
      else if (stage === 'sub:recv') subRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t });
    }
  }
  return { ledgerRows, rootMembers, hopTx, hopRx, appRecv, subRecv, idMap, selfTL, lifecycle, maxT };
}

// ---- D_intent (time-indexed, from the harness lifecycle ledger) ------
// Builds, per (peerIdx,host,topicId), the ACTIVE interval [activate, end) and a
// self-timeline so a subscriber's nodeId AT a given publishTs is resolved with
// reconnect continuity. Returns null if no lifecycle ledger is present (D_intent
// then stays UNOBSERVED_BELIEF-pending, never fabricated).
function buildIntent(lifecycle, selfTL) {
  const acts = lifecycle.filter((e) => e.kind === 'sub-activate');
  if (!acts.length) return null;
  const name2topic = new Map();     // `${peerIdx}|${host}|${name}` -> topicId
  for (const e of lifecycle) if (e.kind === 'topicmap' && e.detail?.topicId) name2topic.set(`${e.peerIdx}|${e.host}|${e.detail.name}`, pfx(e.detail.topicId));
  // window end per (peerIdx,host): the sidecar's `end`/measure-end, fallback Infinity
  const endByPeer = new Map();
  for (const e of lifecycle) if ((e.kind === 'end') && e.wallMs != null) endByPeer.set(`${e.peerIdx}|${e.host}`, e.wallMs);
  // intervals keyed by (peerIdx|host|topicId)
  const intervals = new Map();
  for (const e of acts) {
    const tp = pfx(e.detail?.topicId);
    if (!tp || e.wallMs == null) continue;
    intervals.set(`${e.peerIdx}|${e.host}|${tp}`, { peer: `${e.peerIdx}|${e.host}`, topicId: tp, from: e.wallMs, to: Infinity });
  }
  for (const e of lifecycle) {
    if (e.kind !== 'sub-end') continue;
    const tp = name2topic.get(`${e.peerIdx}|${e.host}|${e.detail?.topic}`);
    const k = `${e.peerIdx}|${e.host}|${tp}`;
    if (tp && intervals.has(k) && e.wallMs != null) intervals.get(k).to = e.wallMs;
  }
  // any interval left open closes at its peer's window end
  for (const iv of intervals.values()) if (iv.to === Infinity) { const en = endByPeer.get(iv.peer); if (en != null) iv.to = en; }
  const tl = selfTL.slice().sort((a, b) => a.wall - b.wall);
  const selfAt = (peer, ts) => {         // nodeId active for this peer at ts (reconnect-aware)
    let best = null;
    for (const s of tl) { if (`${s.peerIdx}|${s.host}` !== peer) continue; if (s.wall <= ts) best = s.self; else break; }
    return best;
  };
  return { intervals: [...intervals.values()], selfAt };
}

// ---- per-msg tree model + validity ----------------------------------
// Returns { belief:Set, parentOf:Map(node->parent|null), nodes:Set, void:reason|null }
function buildTree(rows, rootMembersForMsg) {
  const belief = new Set();
  const nodes = new Set();                 // nodes that emitted a ledger row
  const parentClaims = new Map();          // node -> Set(distinct parent prefixes, null excluded)
  const rootFlags = new Set();             // nodes with isRoot==1 or parent==null
  const roleByEdge = new Map();            // `${parent}->${child}` -> Set(childFlagValues) to catch conflicting roles
  const topics = new Set();
  let voidReason = null;

  const claim = (node, parent) => {
    if (!parentClaims.has(node)) parentClaims.set(node, new Set());
    if (parent) parentClaims.get(node).add(parent);
  };

  for (const row of rows) {
    const node = pfx(row.node);
    nodes.add(node);
    if (typeof row.topicId === 'string') topics.add(row.topicId);
    const recips = Array.isArray(row.recips) ? row.recips : [];
    if (Number.isInteger(row.n) && row.n !== recips.length) voidReason = voidReason || 'TELEMETRY_TRUNCATED';
    if (row.isRoot === 1 || row.parent == null) rootFlags.add(node);
    if (row.parent) claim(node, pfx(row.parent));                 // own-row parent claim
    if (row.localDelivery === 1) belief.add(node);
    for (const rc of recips) {
      const sub = pfx(rc.sub);
      claim(sub, node);                                            // the fanning node is a parent claim for the recip
      const ek = `${node}->${sub}`;
      if (!roleByEdge.has(ek)) roleByEdge.set(ek, new Set());
      roleByEdge.get(ek).add(rc.child === 1 ? 1 : 0);
      if (rc.child !== 1) belief.add(sub);                         // child==0 terminal leaf edge -> expected app recipient
    }
  }

  // conflicting edge roles (same edge marked both forwarding and leaf)
  for (const [, roles] of roleByEdge) if (roles.size > 1) voidReason = voidReason || 'CONFLICTING_EDGE_ROLE';
  // conflicting parents (a node claimed by two different parents / reattachment ambiguity)
  const parentOf = new Map();
  for (const [node, parents] of parentClaims) {
    if (parents.size > 1) voidReason = voidReason || 'CONFLICTING_PARENT';
    parentOf.set(node, parents.size === 1 ? [...parents][0] : null);
  }
  // exactly one root
  const rootsByFlag = [...rootFlags];
  const rootsByNoParent = [...nodes].filter((n) => !parentOf.get(n));
  const rootSet = new Set([...rootsByFlag, ...rootsByNoParent]);
  if (rootSet.size !== 1) voidReason = voidReason || `TREE_NO_UNIQUE_ROOT(${rootSet.size})`;
  const root = rootSet.size === 1 ? [...rootSet][0] : null;
  // one topic
  if (topics.size > 1) voidReason = voidReason || 'CONFLICTING_TOPIC';
  // acyclic + every ledger NODE reaches the root
  if (root) {
    for (const n of nodes) {
      let cur = n, steps = 0; const seen = new Set();
      while (cur && cur !== root) {
        if (seen.has(cur)) { voidReason = voidReason || 'TREE_CYCLE'; break; }
        seen.add(cur); cur = parentOf.get(cur); if (++steps > nodes.size + 2) { voidReason = voidReason || 'TREE_CYCLE'; break; }
      }
      if (cur !== root) voidReason = voidReason || 'TREE_UNREACHABLE_ROOT';
    }
  }
  // root-members overlap cross-check (independent root projection)
  if (root && rootMembersForMsg && rootMembersForMsg.length) {
    const rootRow = rows.find((r) => pfx(r.node) === root);
    const projN = rootRow && Array.isArray(rootRow.recips) ? rootRow.recips.length : null;
    for (const rm of rootMembersForMsg) if (Number.isInteger(rm.n) && projN != null && rm.n !== projN) voidReason = voidReason || 'ROOTMEMBERS_MISMATCH';
  }
  return { belief, parentOf, nodes, root, void: voidReason };
}

// ordered edges root->subscriber; null if unreconstructable
function pathEdges(sub, parentOf, root) {
  if (sub === root) return [];                     // origin's own local delivery — no network edges
  const up = [];                                   // sub, parent, grandparent, ...
  let cur = sub, steps = 0; const seen = new Set();
  while (cur && cur !== root) {
    if (seen.has(cur)) return null;                // cycle
    seen.add(cur);
    const p = parentOf.get(cur);
    if (!p) return null;                           // broken/unknown parent -> unreconstructable
    up.push([p, cur]);                             // edge parent->cur
    cur = p; if (++steps > 4096) return null;
  }
  if (cur !== root) return null;
  return up.reverse();                             // root->...->sub order
}

// ---- reconcile -------------------------------------------------------
function reconcile(parsed, opts = {}) {
  const { ledgerRows, rootMembers, hopTx, hopRx, appRecv, idMap, selfTL = [], lifecycle = [], maxT } = parsed;
  const censorMs = opts.censorMs ?? CENSOR_MS;
  const censorCutoff = maxT - censorMs;
  const intent = buildIntent(lifecycle, selfTL);   // null if no lifecycle ledger

  // group ledger rows + root-members by msgId
  const rowsByMsg = new Map(), rmByMsg = new Map(), pubTs = new Map();
  for (const row of ledgerRows) {
    if (!row.msgId) continue;
    if (!rowsByMsg.has(row.msgId)) rowsByMsg.set(row.msgId, []);
    rowsByMsg.get(row.msgId).push(row);
    if (typeof row.t === 'number') pubTs.set(row.msgId, Math.min(pubTs.get(row.msgId) ?? Infinity, row.t));
  }
  for (const rm of rootMembers) { if (!rmByMsg.has(rm.msgId)) rmByMsg.set(rm.msgId, []); rmByMsg.get(rm.msgId).push(rm); }

  // proven edges per msgId: `${msgId}|${from}|${to}` for a forwarded-and-arrived hop
  const provenEdge = new Set();
  for (const [id, tx] of hopTx) { if (!hopRx.has(id) || !tx.from || !tx.to) continue; for (const m of tx.msgIds) provenEdge.add(`${m}|${tx.from}|${tx.to}`); }

  // R_app per msgId (deduped per (msg,sub); duplicates counted)
  const appByMsg = new Map(); let dupReceipts = 0, unmappedReceipts = 0; const seen = new Set(); const lat = [];
  for (const a of appRecv) {
    const node = idMap.get(a.key);
    if (!node) { unmappedReceipts++; continue; }
    const rk = `${a.msgId}|${node}`;
    if (seen.has(rk)) { dupReceipts++; continue; }
    seen.add(rk); if (!appByMsg.has(a.msgId)) appByMsg.set(a.msgId, new Set()); appByMsg.get(a.msgId).add(node);
    const pt = pubTs.get(a.msgId); if (typeof pt === 'number' && typeof a.t === 'number') lat.push(a.t - pt);
  }

  const agg = { publishes: 0, voided: 0, censored: 0, belief: 0, delivered: 0,
    forwardingDrop: 0, callbackDrop: 0, unresolved: 0, beliefNotApp: 0, unobservedBelief: 0, dupReceipts, unmappedReceipts };
  const voidReasons = {}; const rows = [];

  for (const [msgId, mrows] of rowsByMsg) {
    const pt = pubTs.get(msgId);
    if (typeof pt === 'number' && pt > censorCutoff) { agg.censored++; continue; }
    const tree = buildTree(mrows, rmByMsg.get(msgId));
    if (tree.void) { agg.voided++; voidReasons[tree.void] = (voidReasons[tree.void] || 0) + 1; continue; }
    agg.publishes++;
    // D_intent \ D_belief = UNOBSERVED_BELIEF: a subscriber the harness REQUIRED at
    // this publish (its active interval on the msg's topic contained publishTs) whose
    // resolved nodeId the kernel belief tree never listed. UNVERIFIED intent/belief
    // divergence — reported, never counted as delivery loss (Aster #1).
    if (intent && typeof pt === 'number') {
      const topicId = pfx(mrows.find((r) => r.topicId)?.topicId);
      for (const iv of intent.intervals) {
        if (iv.topicId !== topicId) continue;
        if (!(pt >= iv.from && pt < iv.to)) continue;
        const node = intent.selfAt(iv.peer, pt);
        if (node && !tree.belief.has(node)) agg.unobservedBelief++;
      }
    }
    const app = appByMsg.get(msgId) || new Set();
    const missed = setDiff(tree.belief, app);
    let fwd = 0, cb = 0, unres = 0;
    for (const s of missed) {
      const edges = pathEdges(s, tree.parentOf, tree.root);
      if (edges === null) { unres++; continue; }                 // unreconstructable path
      // earliest unproven edge is the forwarding boundary
      let boundary = -1;
      for (let i = 0; i < edges.length; i++) { const [a, b] = edges[i]; if (!provenEdge.has(`${msgId}|${a}|${b}`)) { boundary = i; break; } }
      if (boundary === -1) cb++;                                 // every network edge proven, app absent -> final-hop/callback
      else fwd++;                                                // earliest unproven edge -> forwarding boundary
    }
    agg.belief += tree.belief.size; agg.delivered += tree.belief.size - missed.length;
    agg.beliefNotApp += missed.length; agg.forwardingDrop += fwd; agg.callbackDrop += cb; agg.unresolved += unres;
    rows.push({ msgId, belief: tree.belief.size, delivered: tree.belief.size - missed.length, forwardingDrop: fwd, callbackDrop: cb, unresolved: unres });
  }

  lat.sort((a, b) => a - b);
  const p = (q) => lat.length ? lat[Math.min(lat.length - 1, Math.floor(q / 100 * lat.length))] : null;
  return { agg, rows, voidReasons, latP50: p(50), latP95: p(95), censorMs, hasIntent: !!intent,
    completeness: agg.belief ? +(100 * agg.delivered / agg.belief).toFixed(2) : null };
}

function report(res, note) {
  const a = res.agg;
  console.log('\n===== DELIVERY RECONCILIATION (path-aware; fanout-ledger 73b705d; rules 722f8464/89ab0777) =====');
  if (note) console.log(note);
  console.log(`publishes scored: ${a.publishes}  (VOID: ${a.voided}, censored: ${a.censored})`);
  console.log(`receipts: dup per (msg,sub)=${a.dupReceipts}, unmapped (no peer→node)=${a.unmappedReceipts}`);
  console.log(`expected app obligations (D_belief): ${a.belief}   delivered: ${a.delivered}   completeness=${res.completeness}%`);
  console.log('miss localization — path-aware, partitions the miss set exactly:');
  console.log(`  FORWARDING_DROP (earliest unproven edge on the root→sub path): ${a.forwardingDrop}`);
  console.log(`  FINAL_HOP_CALLBACK (every edge proven, app receipt absent):    ${a.callbackDrop}`);
  console.log(`  UNRESOLVED (path unreconstructable — not forced into a bucket): ${a.unresolved}`);
  console.log(`  = D_belief \\ R_app total: ${a.beliefNotApp}  (fwd+callback+unresolved must equal this)`);
  if (res.hasIntent) console.log(`  D_intent \\ D_belief: UNOBSERVED_BELIEF = ${a.unobservedBelief}  (required at publishTs, absent from belief tree — UNVERIFIED divergence, not loss)`);
  else console.log(`  D_intent \\ D_belief: UNOBSERVED_BELIEF pending — no lifecycle ledger in this data (Aster #1)`);
  if (a.voided) console.log(`VOID publishes by reason (never scored as loss): ${JSON.stringify(res.voidReasons)}`);
  console.log(`app-receipt latency: p50=${res.latP50}ms p95=${res.latP95}ms   censor window=${res.censorMs}ms`);
  console.log('=================================================================================================\n');
}

// ---- selftest --------------------------------------------------------
function selftest() {
  const R = 'aa'.repeat(6), A = 'a1'.repeat(6), B = 'b2'.repeat(6), C = 'c3'.repeat(6), D = 'd4'.repeat(6);
  const L = (o) => ({ stage: 'fanout-ledger', t: 1000, ...o });
  const idMap = new Map([['0|h', pfx(R)], ['1|h', pfx(A)], ['2|h', pfx(B)], ['3|h', pfx(C)], ['4|h', pfx(D)]]);
  const mkHop = (id, from, to, msg) => [id, { msgIds: [msg], from: pfx(from), to: pfx(to) }];
  const results = [];

  // Base fixture M1: root(dual-role R) -> leaf A, leaf B, child-relay C; C -> leaf D.
  // A,D,R deliver; B & C miss. Edge R->B NOT proven (forwarding); C is proven-reached
  // (R->C proven) but C's app absent -> callback.
  const base = {
    ledgerRows: [
      L({ msgId: 'M1', node: R, isRoot: 1, localDelivery: 1, parent: null, n: 3, recips: [{ sub: A, child: 0 }, { sub: B, child: 0 }, { sub: C, child: 1 }] }),
      L({ msgId: 'M1', node: C, localDelivery: 1, parent: R, n: 1, recips: [{ sub: D, child: 0 }] }),
    ],
    rootMembers: [], hopTx: new Map([mkHop('h1', R, A, 'M1'), mkHop('h3', R, C, 'M1'), mkHop('h4', C, D, 'M1')]),
    hopRx: new Set(['h1', 'h3', 'h4']),
    appRecv: [{ msgId: 'M1', key: '0|h', t: 1010 }, { msgId: 'M1', key: '1|h', t: 1012 }, { msgId: 'M1', key: '4|h', t: 1020 }, { msgId: 'M1', key: '1|h', t: 1013 }],
    idMap, maxT: 1e6,
  };
  const r1 = reconcile(base, { censorMs: 0 }); results.push(r1);
  const a1 = r1.agg;

  // (a) three-hop path failing at hop 2: R->X->Y->Z (Z the leaf sub); R->X proven,
  //     X->Y NOT proven -> forwarding boundary at depth 1 (intermediate), NOT callback.
  const X = 'e5'.repeat(6), Y = 'f6'.repeat(6), Z = '17'.repeat(6);
  const three = { ledgerRows: [
      L({ msgId: 'M3', node: R, isRoot: 1, parent: null, n: 1, recips: [{ sub: X, child: 1 }] }),
      L({ msgId: 'M3', node: X, parent: R, n: 1, recips: [{ sub: Y, child: 1 }] }),
      L({ msgId: 'M3', node: Y, parent: X, n: 1, recips: [{ sub: Z, child: 0 }] }),
    ], rootMembers: [], hopTx: new Map([mkHop('t1', R, X, 'M3')]), hopRx: new Set(['t1']), appRecv: [], idMap, maxT: 1e6 };
  const r3 = reconcile(three, { censorMs: 0 });

  // (b) dual-path/reattachment ambiguity: Z claimed by two parents -> CONFLICTING_PARENT -> VOID
  const dual = { ledgerRows: [
      L({ msgId: 'M4', node: R, isRoot: 1, parent: null, n: 2, recips: [{ sub: X, child: 1 }, { sub: Y, child: 1 }] }),
      L({ msgId: 'M4', node: X, parent: R, n: 1, recips: [{ sub: Z, child: 0 }] }),
      L({ msgId: 'M4', node: Y, parent: R, n: 1, recips: [{ sub: Z, child: 0 }] }),
    ], rootMembers: [], hopTx: new Map(), hopRx: new Set(), appRecv: [], idMap, maxT: 1e6 };
  const r4 = reconcile(dual, { censorMs: 0 });

  // (c) wholly omitted subtree: root names child-relay C but C never emitted a row.
  //     C is referenced as a child (child=1) with no row -> C unreachable? C's parent
  //     is R (claimed), C reaches root, but C emitted no row so its subtree is invisible.
  //     Detected via root-members overlap: root-members says n=2, root row recips=1 -> MISMATCH VOID.
  const omit = { ledgerRows: [
      L({ msgId: 'M5', node: R, isRoot: 1, parent: null, n: 1, recips: [{ sub: C, child: 1 }] }),
    ], rootMembers: [{ ev: 'root-members', msgId: 'M5', n: 2 }], hopTx: new Map(), hopRx: new Set(), appRecv: [], idMap, maxT: 1e6 };
  const r5 = reconcile(omit, { censorMs: 0 });

  // (d) conflicting parent ROWS: node X emits parent=R in one row and parent=Y in another.
  const conf = { ledgerRows: [
      L({ msgId: 'M6', node: R, isRoot: 1, parent: null, n: 1, recips: [{ sub: X, child: 1 }] }),
      L({ msgId: 'M6', node: X, parent: R, n: 0, recips: [] }),
      L({ msgId: 'M6', node: X, parent: Y, n: 0, recips: [] }),
    ], rootMembers: [], hopTx: new Map(), hopRx: new Set(), appRecv: [], idMap, maxT: 1e6 };
  const r6 = reconcile(conf, { censorMs: 0 });

  // (e) fully proven path, missing deliver:app -> FINAL_HOP_CALLBACK.
  const full = { ledgerRows: [
      L({ msgId: 'M7', node: R, isRoot: 1, parent: null, n: 1, recips: [{ sub: A, child: 0 }] }),
    ], rootMembers: [], hopTx: new Map([mkHop('f1', R, A, 'M7')]), hopRx: new Set(['f1']), appRecv: [], idMap, maxT: 1e6 };
  const r7 = reconcile(full, { censorMs: 0 });

  // (f) D_intent: the harness REQUIRED subscriber P (peerIdx 5, nodeId G) on topic
  //     TT, active before the publish, but the kernel belief tree listed only A ->
  //     G is UNOBSERVED_BELIEF (required, unlisted). A itself delivers.
  const G = '28'.repeat(6), TT = 'aabbccddeeff001122';
  const intentFix = {
    ledgerRows: [L({ msgId: 'M8', node: R, isRoot: 1, parent: null, topicId: TT, n: 1, recips: [{ sub: A, child: 0 }] })],
    rootMembers: [], hopTx: new Map([mkHop('g1', R, A, 'M8')]), hopRx: new Set(['g1']),
    appRecv: [{ msgId: 'M8', key: '1|h', t: 1010 }],
    idMap, selfTL: [{ peerIdx: 5, host: 'h', self: pfx(G), wall: 100 }],
    lifecycle: [{ kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 5, host: 'h', wallMs: 500 }],
    maxT: 1e6,
  };
  const r8 = reconcile(intentFix, { censorMs: 0 });

  const checks = [
    ['M1 D_belief==5 (dual-role C once, no leafhood drop)', a1.belief === 5],
    ['M1 delivered==3', a1.delivered === 3],
    ['M1 partition exact: fwd+cb+unresolved==beliefNotApp', a1.forwardingDrop + a1.callbackDrop + a1.unresolved === a1.beliefNotApp],
    ['M1 forwardingDrop==1 (B not forwarded)', a1.forwardingDrop === 1],
    ['M1 callbackDrop==1 (C reached, app absent)', a1.callbackDrop === 1],
    ['M1 dupReceipts==1', a1.dupReceipts === 1],
    ['(a) 3-hop fail@hop2 -> forwardingDrop==1 (intermediate boundary), callback==0', r3.agg.forwardingDrop === 1 && r3.agg.callbackDrop === 0],
    ['(b) dual-parent ambiguity -> VOID (0 scored)', r4.agg.voided === 1 && r4.agg.publishes === 0],
    ['(c) omitted subtree -> VOID via root-members mismatch', r5.agg.voided === 1 && !!r5.voidReasons.ROOTMEMBERS_MISMATCH],
    ['(d) conflicting parent rows -> VOID', r6.agg.voided === 1 && !!r6.voidReasons.CONFLICTING_PARENT],
    ['(e) fully proven path, no app -> callbackDrop==1', r7.agg.callbackDrop === 1 && r7.agg.forwardingDrop === 0],
    ['(f) D_intent: required-but-unlisted sub -> UNOBSERVED_BELIEF==1, A delivered', r8.hasIntent && r8.agg.unobservedBelief === 1 && r8.agg.delivered === 1],
  ];
  let ok = true;
  console.log('\n----- SELFTEST (path-aware reconciliation on synthetic fixtures) -----');
  for (const [label, pass] of checks) { console.log(`  ${pass ? '✓' : '✗ FAIL'}  ${label}`); if (!pass) ok = false; }
  report(r1, 'M1 base fixture: root(dual)+A+B+child-relay C+D; B not forwarded, C reached-no-app');
  console.log(ok ? 'SELFTEST PASS' : 'SELFTEST FAIL');
  return ok;
}

// ---- main ------------------------------------------------------------
if (SELFTEST) {
  process.exit(selftest() ? 0 : 1);
} else {
  const parsed = parse(DIR);
  if (!parsed.ledgerRows.length) {
    console.log(`\nNo fanout-ledger rows in ${DIR}. This analyzer scores a LAT_TRACE=1 arm on kernel >= 73b705d.`);
    console.log(`Parsed from existing signals: idMap=${parsed.idMap.size} (peerIdx,host)->node, deliver:app=${parsed.appRecv.length}, hop_tx=${parsed.hopTx.size}, hop_rx=${parsed.hopRx.size}, root-members=${parsed.rootMembers.length}.`);
    console.log(`Run with --selftest to verify the path-aware reconciliation on synthetic fixtures.\n`);
    process.exit(0);
  }
  report(reconcile(parsed));
}
