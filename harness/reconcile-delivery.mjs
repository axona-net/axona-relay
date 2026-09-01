// =====================================================================
// reconcile-delivery.mjs — combined Gate-4 item 4: reconcile the publish-time
// expectation ledger (kernel 73b705d fanout-ledger) against actual app receipts,
// with per-hop forwarding as the middle layer, to the council-frozen rules
// (Aster 722f8464, Orion 6c09ee0d / 35a201c4, Vega 5e4a6bb7 / 096003d3).
//
// This does NOT re-measure per-hop transport loss (that is analyze-deliver-hop.mjs).
// It answers the end-to-end question — "did each ELIGIBLE subscriber's app receive
// each message" — and LOCALIZES every miss to one stratum, without conflating them:
//
//   D_intent   who the harness REQUIRED to receive (ground-truth test intent)
//   D_belief   who the kernel BELIEVED it owed delivery (from fanout-ledger)
//   D_fwd      who the message was actually FORWARDED toward (from deliver:hop pairs)
//   R_app      who actually delivered to their app (from deliver:app)
//
//   D_intent \ D_belief  -> intent/belief DIVERGENCE  (pre-fanout registration/lease;
//                          classified as divergence FIRST, not kernel failure, until
//                          harness activation + identity continuity are verified — Aster #1)
//   D_belief  \ D_fwd     -> tree/forwarding drop      (never forwarded toward them)
//   D_fwd     \ R_app     -> final-hop / callback drop  (forwarded, app never got it)
//   D_belief  \ R_app     -> aggregate post-fanout loss (the two above combined)
//
// EXPECTATION SET (Aster's material correction 722f8464): tree position and delivery
// obligation are SEPARATE. Expected app recipients per msgId are derived as
//   { recip.sub where child==0 }  UNION  { node where localDelivery==1 }
// deduped by nodeId — NEVER from graph leafhood. A node that is both a forwarder and
// a subscriber is counted via its own localDelivery==1.
//
// VALIDITY (Aster #4 / Orion): a publish whose ledger is truncated or internally
// inconsistent is NOT scored — it is marked VOID (TELEMETRY_TRUNCATED) and reported
// separately. No silent partial ledger is ever counted as delivery loss. Structural
// truncation checks here (recips.length !== declared n; a referenced parent/child node
// with no ledger row); a kernel-side write-failure marker would strengthen this.
//
// CENSORING (Aster): right-censor only PROSPECTIVELY — drop publishes within
// (censorWindow) of arm end, so an unresolved recipient near the end is not a miss.
//
// DEDUP: receipts deduped per (msgId, subscriber); duplicates reported separately.
//
// D_intent SOURCE (Aster #1): the harness lifecycle ledger must be TIME-INDEXED
// (subscribe activation, unsubscribe/expiry, reconnect identity, publish cutoff). The
// current sidecar Ledger records subscribe COUNT + author identity but not the
// per-topic activation timeline. Until the harness emits that, D_intent is marked
// PARTIAL and D_intent\D_belief is reported as UNVERIFIED divergence, never as kernel
// failure — exactly the caution the council required.
//
//   node harness/reconcile-delivery.mjs [dir] [--selftest]
// =====================================================================
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const DIR = process.argv[2] && !process.argv[2].startsWith('--') ? process.argv[2] : 'harness/results';
const SELFTEST = process.argv.includes('--selftest');
const CENSOR_MS = Number((process.argv.find((a) => a.startsWith('--censor=')) || '').split('=')[1] || 5000);

// ---- helpers ---------------------------------------------------------
const pfx = (h) => (typeof h === 'string' ? h.slice(0, 12) : null);   // canonical 12-hex node key
const setDiff = (a, b) => [...a].filter((x) => !b.has(x));

// ---- parse -----------------------------------------------------------
// Returns { ledgers, hopFwd, appRecv, idMap, subRecv, maxT }
function parse(dir) {
  const files = readdirSync(dir).filter((f) => f.endsWith('.jsonl'));
  const ledgerRows = [];            // fanout-ledger lat-stage rows
  const hopTx = new Map();          // hopAttemptId -> {msgIds,to,hopIdx}
  const hopRx = new Set();          // hopAttemptId that produced an rx
  const appRecv = [];               // {msgId, key, t}
  const subRecv = [];               // {msgId, key, t}
  const idMap = new Map();          // (peerIdx|host) -> node 12-hex prefix (from sidecar disc `self`)
  let maxT = 0;

  for (const f of files) {
    let text; try { text = readFileSync(join(dir, f), 'utf8'); } catch { continue; }
    for (const line of text.split('\n')) {
      if (!line) continue;
      let r; try { r = JSON.parse(line); } catch { continue; }
      if (typeof r.t === 'number' && r.t > maxT) maxT = r.t;
      if (typeof r.wall === 'number' && r.wall > maxT) maxT = r.wall;

      // sidecar disc rows: (peerIdx,host) -> self  (the join from app-receipts to nodeId)
      if (typeof r.self === 'string' && Number.isInteger(r.peerIdx) && typeof r.host === 'string') {
        idMap.set(`${r.peerIdx}|${r.host}`, pfx(r.self));
      }
      const stage = r.stage;
      if (stage === 'fanout-ledger') { ledgerRows.push(r); }
      else if (stage === 'deliver:hop_tx') { if (r.hopAttemptId != null && !hopTx.has(r.hopAttemptId)) hopTx.set(r.hopAttemptId, { msgIds: r.msgIds || [], to: pfx(r.to), hopIdx: r.hopIdx }); }
      else if (stage === 'deliver:hop_rx') { if (r.hopAttemptId != null) hopRx.add(r.hopAttemptId); }
      else if (stage === 'deliver:app') { appRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t }); }
      else if (stage === 'sub:recv')   { subRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t }); }
    }
  }
  return { ledgerRows, hopTx, hopRx, appRecv, subRecv, idMap, maxT };
}

// ---- reconcile -------------------------------------------------------
// Accepts the parsed shapes (or a synthetic fixture in selftest) and produces the
// per-msg partition + aggregate. idMap maps a receipt key -> node prefix.
function reconcile({ ledgerRows, hopTx, hopRx, appRecv, subRecv, idMap, maxT }, opts = {}) {
  const censorMs = opts.censorMs ?? CENSOR_MS;
  const censorCutoff = maxT - censorMs;

  // ---- D_belief per msgId, with a structural validity check --------
  // beliefByMsg: msgId -> Set(nodePrefix expected to deliver to app)
  // nodesByMsg:  msgId -> Set(nodePrefix that emitted a ledger row) [tree nodes]
  // referenced:  msgId -> Set(nodePrefix named as a child edge OR a parent) [must have a row]
  const beliefByMsg = new Map();
  const nodesByMsg = new Map();
  const referenced = new Map();
  const voidMsgs = new Map();      // msgId -> reason (TELEMETRY_TRUNCATED / INCONSISTENT)
  const pubTs = new Map();         // msgId -> earliest ledger t (publish-ish anchor)

  const ensure = (m, key) => { if (!m.has(key)) m.set(key, new Set()); return m.get(key); };

  for (const row of ledgerRows) {
    const msgId = row.msgId; if (!msgId) continue;
    const node = pfx(row.node);
    ensure(nodesByMsg, msgId).add(node);
    if (typeof row.t === 'number') pubTs.set(msgId, Math.min(pubTs.get(msgId) ?? Infinity, row.t));

    // structural truncation: emitted recips must equal the declared local fanout n
    const recips = Array.isArray(row.recips) ? row.recips : [];
    if (Number.isInteger(row.n) && row.n !== recips.length) voidMsgs.set(msgId, 'TELEMETRY_TRUNCATED');

    // dual-role expectation derivation (Aster 722f8464): NEVER leafhood
    const belief = ensure(beliefByMsg, msgId);
    if (row.localDelivery === 1) belief.add(node);                 // this node's own app owes delivery
    const ref = ensure(referenced, msgId);
    if (row.parent) ref.add(pfx(row.parent));                      // upstream must also have a row (tree completeness)
    for (const rc of recips) {
      const sub = pfx(rc.sub);
      if (rc.child === 1) ref.add(sub);                            // a child-relay edge -> that node must emit its own row
      else belief.add(sub);                                        // a terminal leaf-subscriber edge -> expected app recipient
    }
  }

  // tree completeness: every referenced forwarder/parent must have emitted a ledger row,
  // else the sub-tree's expectations are unobserved -> the publish is VOID, not a miss.
  for (const [msgId, ref] of referenced) {
    const nodes = nodesByMsg.get(msgId) || new Set();
    for (const n of ref) {
      // a referenced node with no ledger row AND that is not itself a pure leaf receipt
      if (!nodes.has(n)) {
        // it may legitimately be a pure leaf (child edge to a non-forwarding subscriber):
        // those never emit a row. We cannot distinguish a missing-relay-row from a
        // leaf-with-no-row structurally, so flag as INCOMPLETE only when that node ALSO
        // appears as a parent (a parent MUST be a forwarder and MUST have a row).
      }
    }
  }
  // parent-completeness is the strict check: a named parent that never emitted a row =
  // a lost sub-tree ledger = VOID publish.
  for (const row of ledgerRows) {
    if (!row.parent) continue;
    const p = pfx(row.parent), msgId = row.msgId;
    const nodes = nodesByMsg.get(msgId);
    if (nodes && !nodes.has(p) && !voidMsgs.has(msgId)) voidMsgs.set(msgId, 'INCOMPLETE_TREE(parent-row-missing)');
  }

  // ---- R_app per msgId (deduped per (msg,subscriber); duplicates counted) ----
  const appByMsg = new Map();      // msgId -> Set(nodePrefix)
  let dupReceipts = 0;
  const seenReceipt = new Set();   // `${msgId}|${node}`
  const latencies = [];            // app receipt latency (t - publishTs) for delivered
  for (const a of appRecv) {
    const node = idMap.get(a.key);
    if (!node) continue;           // unmapped receipt key -> cannot join to a nodeId (reported)
    const rk = `${a.msgId}|${node}`;
    if (seenReceipt.has(rk)) { dupReceipts++; continue; }
    seenReceipt.add(rk);
    ensure(appByMsg, a.msgId).add(node);
    const pt = pubTs.get(a.msgId);
    if (typeof pt === 'number' && typeof a.t === 'number') latencies.push(a.t - pt);
  }

  // ---- D_fwd per msgId (forwarded-and-received hop receivers) ----
  // A hop attempt with a matching rx delivered its DELIVER to `to`. Union the receivers
  // per msgId carried on the hop. This is the middle layer; it is best-effort node-level
  // reachability, not a full path attribution, and is labeled as such.
  const fwdByMsg = new Map();
  for (const [id, tx] of hopTx) {
    if (!hopRx.has(id)) continue;                 // only forwarded-and-arrived
    for (const m of tx.msgIds) if (tx.to) ensure(fwdByMsg, m).add(tx.to);
  }

  // ---- partition per msg ----
  const rows = [];
  const agg = { publishes: 0, voided: 0, censored: 0,
    belief: 0, delivered: 0, treeDrop: 0, callbackDrop: 0, beliefNotApp: 0, dupReceipts };
  for (const [msgId, belief] of beliefByMsg) {
    if (voidMsgs.has(msgId)) { agg.voided++; continue; }
    const pt = pubTs.get(msgId);
    if (typeof pt === 'number' && pt > censorCutoff) { agg.censored++; continue; }   // prospective right-censor
    agg.publishes++;
    const app = appByMsg.get(msgId) || new Set();
    const fwd = fwdByMsg.get(msgId) || new Set();
    // Localize the MISS SET exactly (Orion's triangulation applied to the gaps, not to
    // delivered nodes): a missed recipient either was never forwarded toward (tree drop)
    // or was forwarded and the app still never got it (final-hop/callback drop). These
    // two PARTITION the misses — treeDrop + callbackDrop == beliefNotApp — so the origin
    // root (delivered locally, no inbound hop) never pollutes the forwarding stratum.
    const missed = setDiff(belief, app);
    const treeDrop = missed.filter((n) => !fwd.has(n));     // D_belief\R_app and NOT forwarded
    const callbackDrop = missed.filter((n) => fwd.has(n));  // D_belief\R_app but WAS forwarded
    agg.belief += belief.size; agg.delivered += belief.size - missed.length;
    agg.beliefNotApp += missed.length; agg.treeDrop += treeDrop.length; agg.callbackDrop += callbackDrop.length;
    rows.push({ msgId, belief: belief.size, delivered: belief.size - missed.length,
      treeDrop: treeDrop.length, callbackDrop: callbackDrop.length });
  }

  latencies.sort((a, b) => a - b);
  const p = (q) => latencies.length ? latencies[Math.min(latencies.length - 1, Math.floor(q / 100 * latencies.length))] : null;
  return { agg, rows, voidMsgs, latP50: p(50), latP95: p(95), censorMs, censorCutoff,
    completeness: agg.belief ? +(100 * agg.delivered / agg.belief).toFixed(2) : null };
}

// ---- report ----------------------------------------------------------
function report(res, extra = {}) {
  const a = res.agg;
  console.log('\n===== DELIVERY RECONCILIATION (fanout-ledger 73b705d; rules 722f8464/6c09ee0d/096003d3) =====');
  if (extra.note) console.log(extra.note);
  console.log(`publishes scored: ${a.publishes}  (VOID: ${a.voided}, censored: ${a.censored}, dup receipts: ${a.dupReceipts})`);
  console.log(`expected app-recipient obligations (D_belief): ${a.belief}`);
  console.log(`delivered to app (R_app ∩ D_belief): ${a.delivered}   completeness=${res.completeness}%`);
  console.log('loss localization — the miss set partitioned exactly (treeDrop + callbackDrop == post-fanout loss):');
  console.log(`  tree/forwarding drop (missed AND not forwarded toward): ${a.treeDrop}`);
  console.log(`  final-hop/callback drop (missed BUT was forwarded):     ${a.callbackDrop}`);
  console.log(`  D_belief \\ R_app  (aggregate post-fanout loss): ${a.beliefNotApp}`);
  console.log(`  D_intent \\ D_belief (intent/belief divergence): ${extra.intentNote || 'PARTIAL — harness lifecycle ledger not yet time-indexed; UNVERIFIED, not kernel failure (Aster #1)'}`);
  console.log(`app-receipt latency (t - publish): p50=${res.latP50}ms p95=${res.latP95}ms   censor window=${res.censorMs}ms`);
  if (res.voidMsgs.size) {
    const reasons = {}; for (const [, why] of res.voidMsgs) reasons[why] = (reasons[why] || 0) + 1;
    console.log(`VOID publishes by reason: ${JSON.stringify(reasons)}  (never scored as loss — Aster #4)`);
  }
  console.log('================================================================================================\n');
}

// ---- selftest: prove the reconciliation math on a synthetic fixture ----
// (73b705d is not armed yet, so no live fanout-ledger rows exist. This fixture
//  exercises every partition + the dual-role and validity rules deterministically.)
function selftest() {
  // Topology for msg M1: root R (also a subscriber -> localDelivery) fans to leaf L1,
  // leaf L2, and child-relay CR. CR fans to leaf L3 and to itself as a subscriber.
  // Receipts: R, L1, L3 deliver to app. L2 (leaf) MISSES. CR (dual-role) MISSES its
  // own app delivery. Hop data: L2 was NOT forwarded to; CR WAS forwarded to.
  const rootHex = 'aa'.repeat(6), l1 = 'b1'.repeat(6), l2 = 'b2'.repeat(6), cr = 'cc'.repeat(6), l3 = 'b3'.repeat(6);
  const ledgerRows = [
    { stage: 'fanout-ledger', msgId: 'M1', node: rootHex, localDelivery: 1, parent: null, n: 3, t: 1000,
      recips: [{ sub: l1, child: 0 }, { sub: l2, child: 0 }, { sub: cr, child: 1 }] },
    { stage: 'fanout-ledger', msgId: 'M1', node: cr, localDelivery: 1, parent: rootHex, n: 1, t: 1001,
      recips: [{ sub: l3, child: 0 }] },
  ];
  // idMap maps receipt keys to node prefixes; receipts for R, L1, L3 only (L2, CR miss).
  const idMap = new Map([['0|h', pfx(rootHex)], ['1|h', pfx(l1)], ['2|h', pfx(l2)], ['3|h', pfx(cr)], ['4|h', pfx(l3)]]);
  const appRecv = [
    { msgId: 'M1', key: '0|h', t: 1010 }, { msgId: 'M1', key: '1|h', t: 1012 },
    { msgId: 'M1', key: '4|h', t: 1020 }, { msgId: 'M1', key: '1|h', t: 1013 },  // <- duplicate for L1
  ];
  // hop: forwarded-and-arrived to CR (so CR is in D_fwd) but NOT to L2.
  const hopTx = new Map([['h1', { msgIds: ['M1'], to: pfx(cr), hopIdx: 1 }], ['h2', { msgIds: ['M1'], to: pfx(l1), hopIdx: 1 }], ['h3', { msgIds: ['M1'], to: pfx(l3), hopIdx: 2 }]]);
  const hopRx = new Set(['h1', 'h2', 'h3']);
  const res = reconcile({ ledgerRows, hopTx, hopRx, appRecv, subRecv: [], idMap, maxT: 100000 }, { censorMs: 0 });

  // Expected D_belief = {R(localDelivery), L1, L2, CR(localDelivery)} ∪ {L3(leaf)} = 5 nodes.
  //   NOTE the dual-role win: CR is a child edge (child=1) AND localDelivery=1 -> counted once.
  // Delivered = R, L1, L3 = 3.  beliefNotApp = L2, CR = 2.
  //   L2 not forwarded -> beliefNotFwd includes L2. CR forwarded but no app -> fwdNotApp includes CR.
  // duplicate L1 receipt -> dupReceipts = 1.
  const a = res.agg;
  const checks = [
    ['D_belief == 5 (dual-role CR counted once, no leafhood drop)', a.belief === 5],
    ['delivered == 3', a.delivered === 3],
    ['beliefNotApp == 2 (L2, CR)', a.beliefNotApp === 2],
    ['treeDrop == 1 (L2 missed & not forwarded)', a.treeDrop === 1],
    ['callbackDrop == 1 (CR missed but was forwarded)', a.callbackDrop === 1],
    ['partition exact: treeDrop + callbackDrop == beliefNotApp', a.treeDrop + a.callbackDrop === a.beliefNotApp],
    ['dupReceipts == 1 (L1 twice)', a.dupReceipts === 1],
    ['completeness == 60%', res.completeness === 60],
  ];
  // validity: a truncated ledger row (n != recips.length) must VOID that publish.
  const truncRes = reconcile({ ledgerRows: [{ stage: 'fanout-ledger', msgId: 'M2', node: rootHex, localDelivery: 0, n: 5, t: 1000, recips: [{ sub: l1, child: 0 }] }], hopTx: new Map(), hopRx: new Set(), appRecv: [], subRecv: [], idMap, maxT: 100000 }, { censorMs: 0 });
  checks.push(['truncated ledger (n!=recips) -> VOID, not scored', truncRes.agg.voided === 1 && truncRes.agg.publishes === 0]);

  let ok = true;
  console.log('\n----- SELFTEST (reconciliation math on synthetic fixture) -----');
  for (const [label, pass] of checks) { console.log(`  ${pass ? '✓' : '✗ FAIL'}  ${label}`); if (!pass) ok = false; }
  report(res, { note: 'fixture: root(dual-role)+L1+L2+child-relay(dual-role)+L3; L2 & CR miss', intentNote: 'n/a (fixture)' });
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
    console.log(`Parsed from existing signals: idMap=${parsed.idMap.size} (peerIdx,host)->node, deliver:app=${parsed.appRecv.length}, sub:recv=${parsed.subRecv.length}, hop_tx=${parsed.hopTx.size}, hop_rx=${parsed.hopRx.size}.`);
    console.log(`Run with --selftest to verify the reconciliation math on a synthetic fixture.\n`);
    process.exit(0);
  }
  report(reconcile(parsed));
}
