// reconstruct-transitions.mjs — path reconstruction from the Part B transition ledger
// (level-isolation spec v2). Joins the two independent one-way records the kernel emits
// per hop — a SENDER row (tx-ledger: enqueue/send-attempt + classified outcome) and a
// RECEIVER row (rx-ledger: arrival) — on (msgId, edgeAttemptId), and rebuilds each
// message's path as a graph. Because both endpoints are ours and log independently, an
// edge is CROSSED only when BOTH rows exist; a sender-"accepted" with no arrival is a
// real transport-or-coverage loss, never inferred from the request/reply resolution.
//
// Per-edge classification for (msgId, edgeAttemptId):
//   CROSSED          tx + rx present            -> the hop provably crossed
//   NOT_ATTEMPTED    tx.disposition=not-attempted (never left the scheduler)
//   ATTEMPTED_FAILED tx.disposition=attempted-failed (transport write threw)
//   ACCEPTED_NO_RX   tx.disposition=accepted, no rx (wrote, no arrival: transit loss
//                    OR an un-instrumented receiver — the fleet must be closed to read this)
//   RX_ORPHAN        rx present, no tx (sender-side coverage gap)
//
// The decisive per-hop transport rate is CROSSED / (CROSSED + ACCEPTED_NO_RX): of edges
// the sender actually put on the wire, the fraction the receiver independently confirmed.
// Reported with a Wilson 95% CI. Local failures (NOT_ATTEMPTED, ATTEMPTED_FAILED) are
// separated out — they are scheduler/write faults, not transit loss.
//
// Usage: [RUN_ID=..] [EPOCH=..] node harness/reconstruct-transitions.mjs <results-dir>
//        node harness/reconstruct-transitions.mjs --selftest

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const pfx = (h) => (typeof h === 'string' ? h.toLowerCase().slice(0, 12) : null);

// Wilson score interval (same convention as reconcile-delivery.mjs).
function wilson(k, n, z = 1.96) {
  if (!n) return { lo: 0, hi: 0 };
  const p = k / n, z2 = z * z;
  const c = (p + z2 / (2 * n)) / (1 + z2 / n);
  const h = (z * Math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)) / (1 + z2 / n);
  return { lo: +(100 * Math.max(0, c - h)).toFixed(2), hi: +(100 * Math.min(1, c + h)).toFixed(2) };
}

function parse(dir) {
  const tx = [], rx = [], manifests = [];
  for (const f of readdirSync(dir).filter((x) => x.endsWith('.jsonl'))) {
    let text; try { text = readFileSync(join(dir, f), 'utf8'); } catch { continue; }
    for (const line of text.split('\n')) {
      if (!line) continue;
      let r; try { r = JSON.parse(line); } catch { continue; }
      if (r.stage === 'tx-ledger') tx.push(r);
      else if (r.stage === 'rx-ledger') rx.push(r);
      else if (r.stage === 'ledger-manifest') manifests.push(r);
    }
  }
  return { tx, rx, manifests };
}

function reconstruct({ tx, rx, manifests }, opts = {}) {
  const runId = opts.runId ?? null, epoch = opts.epoch ?? null;
  const scoped = (r) => (runId == null || r.runId === runId) && (epoch == null || String(r.epoch) === String(epoch));
  tx = tx.filter(scoped); rx = rx.filter(scoped);

  // Index rx by (msgId, edgeAttemptId).
  const rxByEdge = new Map();
  for (const r of rx) { if (!r.msgId || !r.edgeAttemptId) continue; rxByEdge.set(`${r.msgId}|${r.edgeAttemptId}`, r); }
  const txSeen = new Set();

  const tally = { CROSSED: 0, NOT_ATTEMPTED: 0, ATTEMPTED_FAILED: 0, ACCEPTED_NO_RX: 0, RX_ORPHAN: 0 };
  const edgesByMsg = new Map();     // msgId -> [{from,to,klass,outcome}]
  const addEdge = (msgId, e) => { if (!edgesByMsg.has(msgId)) edgesByMsg.set(msgId, []); edgesByMsg.get(msgId).push(e); };

  for (const t of tx) {
    if (!t.msgId || !t.edgeAttemptId) continue;
    const key = `${t.msgId}|${t.edgeAttemptId}`;
    txSeen.add(key);
    const r = rxByEdge.get(key);
    let klass;
    if (r) klass = 'CROSSED';
    else if (t.disposition === 'not-attempted') klass = 'NOT_ATTEMPTED';
    else if (t.disposition === 'attempted-failed') klass = 'ATTEMPTED_FAILED';
    else klass = 'ACCEPTED_NO_RX';
    tally[klass]++;
    addEdge(t.msgId, { from: pfx(t.from), to: pfx(t.to), hopIdx: t.hopIdx ?? null, klass, outcome: t.outcome ?? null });
  }
  // rx rows with no matching tx = sender-side coverage gap
  for (const r of rx) {
    if (!r.msgId || !r.edgeAttemptId) continue;
    const key = `${r.msgId}|${r.edgeAttemptId}`;
    if (txSeen.has(key)) continue;
    tally.RX_ORPHAN++;
    addEdge(r.msgId, { from: pfx(r.from), to: pfx(r.to), hopIdx: null, klass: 'RX_ORPHAN', outcome: null });
  }

  // Per-message path shape: reachable node set from the origin (hopIdx===1 from) over
  // CROSSED edges, plus depth.
  const perMsg = [];
  for (const [msgId, edges] of edgesByMsg) {
    const crossed = edges.filter((e) => e.klass === 'CROSSED');
    const nodes = new Set(); for (const e of crossed) { if (e.from) nodes.add(e.from); if (e.to) nodes.add(e.to); }
    const origin = (edges.find((e) => e.hopIdx === 1) || {}).from || null;
    // BFS depth from origin over crossed edges
    const adj = new Map(); for (const e of crossed) { if (!adj.has(e.from)) adj.set(e.from, []); adj.get(e.from).push(e.to); }
    let depth = 0; if (origin) { let frontier = [origin], seen = new Set([origin]); while (frontier.length) { const nx = []; for (const n of frontier) for (const c of (adj.get(n) || [])) if (!seen.has(c)) { seen.add(c); nx.push(c); } if (nx.length) depth++; frontier = nx; } }
    perMsg.push({ msgId, edges: edges.length, crossed: crossed.length, nodesReached: nodes.size, depth, origin });
  }

  // Manifest completeness per process: rows observed for a proc vs its declared count.
  const observedByProc = new Map();
  for (const t of tx) observedByProc.set(t.proc, (observedByProc.get(t.proc) || 0) + 1);
  for (const r of rx) observedByProc.set(r.proc, (observedByProc.get(r.proc) || 0) + 1);
  const completeness = manifests.filter(scoped).map((m) => {
    const declared = m.count | 0;
    return { proc: m.proc, declared, note: declared === 0 ? 'empty' : 'ok' };
  });

  const onWire = tally.CROSSED + tally.ACCEPTED_NO_RX;
  const perHopRate = onWire ? +(100 * tally.CROSSED / onWire).toFixed(2) : null;
  const perHopCI = wilson(tally.CROSSED, onWire);

  return { tally, perMsg, completeness, onWire, perHopRate, perHopCI,
    manifests: manifests.filter(scoped).length, msgs: edgesByMsg.size, runId, epoch };
}

function report(res) {
  console.log('\n===== TRANSITION-LEDGER PATH RECONSTRUCTION (level-isolation Part B) =====');
  console.log(`run=${res.runId ?? '(all)'} epoch=${res.epoch ?? '(all)'}   messages=${res.msgs}   manifests=${res.manifests}`);
  console.log('edge classification (join on msgId+edgeAttemptId, both endpoints logged independently):');
  for (const k of ['CROSSED', 'NOT_ATTEMPTED', 'ATTEMPTED_FAILED', 'ACCEPTED_NO_RX', 'RX_ORPHAN'])
    console.log(`  ${k.padEnd(16)} ${res.tally[k]}`);
  console.log(`PER-HOP TRANSPORT RATE = CROSSED / (CROSSED + ACCEPTED_NO_RX) = ${res.tally.CROSSED}/${res.onWire} = ${res.perHopRate}% [${res.perHopCI.lo}-${res.perHopCI.hi}]`);
  console.log('  NOT_ATTEMPTED = scheduler never sent (local); ATTEMPTED_FAILED = transport write threw (local).');
  console.log('  ACCEPTED_NO_RX = sender wrote, receiver logged no arrival — transit loss (VALID only on a closed, fully-instrumented fleet).');
  console.log('  RX_ORPHAN = arrival with no sender row (sender-side coverage gap).');
  const empt = res.completeness.filter((c) => c.note === 'empty').length;
  console.log(`manifest completeness: ${res.completeness.length} process manifests, ${empt} empty. A process that logged rows but emitted no manifest = truncated harvest (investigate).`);
  console.log('==========================================================================\n');
}

// ---- selftest ----
function selftest() {
  const R = 'aa'.repeat(6), A = 'a1'.repeat(6), B = 'b2'.repeat(6), C = 'c3'.repeat(6);
  const TX = (msgId, eid, from, to, hopIdx, disposition) => ({ stage: 'tx-ledger', runId: 'r1', epoch: '1', proc: from.slice(-6), msgId, edgeAttemptId: eid, from, to, hopIdx, disposition, outcome: disposition });
  const RX = (msgId, eid, from, to) => ({ stage: 'rx-ledger', runId: 'r1', epoch: '1', proc: to.slice(-6), msgId, edgeAttemptId: eid, from, to });
  const MAN = (proc, count) => ({ stage: 'ledger-manifest', runId: 'r1', epoch: '1', proc, count });

  // Path for M1: R->A crossed, A->B crossed, B->C accepted-no-rx (lost); plus R->X not-attempted.
  const parsed = {
    tx: [ TX('M1', 'e1', R, A, 1, 'accepted'), TX('M1', 'e2', A, B, 2, 'accepted'),
          TX('M1', 'e3', B, C, 3, 'accepted'), TX('M1', 'e4', R, C, 1, 'not-attempted') ],
    rx: [ RX('M1', 'e1', R, A), RX('M1', 'e2', A, B),
          RX('M1', 'e9', A, C) ],   // e9 = rx with no tx -> RX_ORPHAN
    manifests: [ MAN(R.slice(-6), 2), MAN(A.slice(-6), 3), MAN(B.slice(-6), 1) ],
  };
  const res = reconstruct(parsed, { runId: 'r1', epoch: '1' });
  const checks = [
    ['CROSSED==2', res.tally.CROSSED === 2],
    ['NOT_ATTEMPTED==1', res.tally.NOT_ATTEMPTED === 1],
    ['ACCEPTED_NO_RX==1 (B->C lost)', res.tally.ACCEPTED_NO_RX === 1],
    ['RX_ORPHAN==1', res.tally.RX_ORPHAN === 1],
    ['on-wire==3 (2 crossed + 1 accepted-no-rx)', res.onWire === 3],
    ['per-hop rate==66.67%', res.perHopRate === 66.67],
    ['msgs==1', res.msgs === 1],
    ['epoch filter excludes other runs', reconstruct({ ...parsed, tx: [...parsed.tx, TX('M2','z1',R,A,1,'accepted')].map((r,i)=> i===parsed.tx.length ? {...r, epoch:'2'} : r) }, { runId:'r1', epoch:'1' }).msgs === 1],
  ];
  let pass = 0; for (const [name, ok] of checks) { console.log(`  ${ok ? '✓' : '✗'}  ${name}`); if (ok) pass++; }
  report(res);
  console.log(pass === checks.length ? `SELFTEST PASS (${pass}/${checks.length})` : `SELFTEST FAIL (${pass}/${checks.length})`);
  return pass === checks.length;
}

const arg = process.argv[2];
if (arg === '--selftest') { process.exit(selftest() ? 0 : 1); }
else if (arg) { report(reconstruct(parse(arg), { runId: process.env.RUN_ID || null, epoch: process.env.EPOCH || null })); }
else { console.error('usage: [RUN_ID=..] [EPOCH=..] node harness/reconstruct-transitions.mjs <results-dir> | --selftest'); process.exit(2); }
