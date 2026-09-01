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
// Clock basis (b): measured cross-host wall offsets (ms, m4=0 basis; same values the
// soak analyzers use) + a pre-declared uncertainty BAND. Sidecar event walls are
// offset-corrected; the residual (the kernel ledger row carries no host tag, so the
// relay's own offset is unknown up to max|offset|) is absorbed by the band, and a
// publish within band of a required reader's activation/end boundary is right-censored.
const OFFSETS = { m4: 0, m1: 136, 'axona-linux': 182, 'axona-win': 89 };
const CLOCK_BAND = Number((process.argv.find((a) => a.startsWith('--band=')) || '').split('=')[1]
  || Math.max(0, ...Object.values({ m4: 0, m1: 136, 'axona-linux': 182, 'axona-win': 89 })));  // = 182ms default

const pfx = (h) => (typeof h === 'string' ? h.slice(0, 12) : null);
const setDiff = (a, b) => [...a].filter((x) => !b.has(x));

// ---- parse -----------------------------------------------------------
// Collection provenance (Aster 2a2778b2): a kernel-emitted row (fanout-ledger, hop)
// carries no host field, but the relay-log FILENAME the collector wrote does. Recover
// it so every event's timestamp can be corrected with its OWN host's offset.
function hostFromFile(f) {
  if (/^relay-disc-axona-linux-/.test(f)) return 'axona-linux';
  if (/^relay-disc-win/.test(f)) return 'axona-win';
  if (/^relay-disc-m1-/.test(f)) return 'm1';
  if (/^relay-disc-m4-/.test(f)) return 'm4';
  return null;   // sidecar files carry host in-row
}

// Load the pre/post clock probes (clock-<seed>-{pre,post}.json). Returns per-host
// {offset, uncertainty, drift} + the validity band, or null if absent.
function loadClock(dir, seed) {
  const read = (phase) => { try { return JSON.parse(readFileSync(join(dir, `clock-${seed}-${phase}.json`), 'utf8')).hosts; } catch { return null; } };
  const pre = read('pre'), post = read('post');
  if (!pre && !post) return null;
  const hosts = {}; let worstUnc = 0, worstDrift = 0, anyFailed = false;
  const names = new Set([...Object.keys(pre || {}), ...Object.keys(post || {})]);
  for (const h of names) {
    const a = pre?.[h], b = post?.[h];
    if ((a && a.failed) || (b && b.failed)) anyFailed = true;
    const oPre = a?.offset, oPost = b?.offset;
    const offset = Number.isFinite(oPre) && Number.isFinite(oPost) ? Math.round((oPre + oPost) / 2) : (oPre ?? oPost ?? null);
    const drift = Number.isFinite(oPre) && Number.isFinite(oPost) ? Math.abs(oPost - oPre) : null;
    const uncertainty = Math.max(a?.uncertainty ?? 0, b?.uncertainty ?? 0);
    hosts[h] = { offset, uncertainty, drift };
    if (Number.isFinite(uncertainty)) worstUnc = Math.max(worstUnc, uncertainty);
    if (Number.isFinite(drift)) worstDrift = Math.max(worstDrift, drift);
  }
  return { hosts, band: worstUnc + worstDrift, worstUnc, worstDrift, anyFailed, hasPre: !!pre, hasPost: !!post };
}

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
    const fileHost = hostFromFile(f);
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
      // srcHost = the row's own host (sidecar rows) or the collection filename's host
      // (kernel rows). null => provenance missing => timing analysis VOID for it.
      const srcHost = (typeof r.host === 'string') ? r.host : fileHost;
      if (stage === 'fanout-ledger') ledgerRows.push({ ...r, srcHost });
      else if (stage === 'deliver:hop_tx') { if (r.hopAttemptId != null && !hopTx.has(r.hopAttemptId)) hopTx.set(r.hopAttemptId, { msgIds: r.msgIds || [], from: pfx(r.from), to: pfx(r.to), srcHost }); }
      else if (stage === 'deliver:hop_rx') { if (r.hopAttemptId != null) hopRx.add(r.hopAttemptId); }
      else if (stage === 'deliver:app') appRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t });
      else if (stage === 'sub:recv') subRecv.push({ msgId: r.msgId, key: `${r.peerIdx}|${r.host}`, t: r.t });
    }
  }
  return { ledgerRows, rootMembers, hopTx, hopRx, appRecv, subRecv, idMap, selfTL, lifecycle, maxT, clock: loadClock(dir, process.env.SEED || '0') };
}

// ---- three-set model from the harness lifecycle ledger (Aster 54d03977) ------
// Separates REQUIRED (the frozen plan's obligation, independent of sub success),
// ACTIVE (a resolved activation interval bound to the node identity at activation),
// and lets the caller compare against BELIEF (kernel ledger). Clock basis (b):
// sidecar event walls are offset-corrected to a common basis; the residual relay
// offset is absorbed by a pre-declared BAND, and a publish within BAND of any
// required reader's activation/end boundary is boundary-ambiguous → right-censored.
// Returns null if no lifecycle ledger is present (sets then stay 'pending').
function offOf(offsets, host) {   // handles measured-table {host:{offset}} OR static {host:number}
  const v = offsets && offsets[host];
  if (v == null) return 0;
  return Number.isFinite(v) ? v : (Number.isFinite(v.offset) ? v.offset : 0);
}
function buildSets(lifecycle, selfTL, offsets, band) {
  const hasLifecycle = lifecycle.some((e) => e.kind === 'sub-required' || e.kind === 'sub-activate');
  if (!hasLifecycle) return null;
  const off = (host) => offOf(offsets, host);
  const corr = (wallMs, host) => (wallMs == null ? null : wallMs - off(host));  // -> common (m4) basis

  // GLOBAL name->topicId (a topic's id is identical for every peer; a peer whose
  // sub FAILED still maps via another peer's successful topicmap for that name).
  const name2topic = new Map();
  for (const e of lifecycle) if (e.kind === 'topicmap' && e.detail?.topicId && e.detail?.name) name2topic.set(e.detail.name, pfx(e.detail.topicId));

  // reconnect-aware self timeline (corrected), per peer
  const tl = selfTL.map((s) => ({ peer: `${s.peerIdx}|${s.host}`, self: s.self, wall: corr(s.wall, s.host) }))
    .filter((s) => s.wall != null).sort((a, b) => a.wall - b.wall);
  const selfAt = (peer, ts) => { let best = null; for (const s of tl) { if (s.peer !== peer) continue; if (s.wall <= ts) best = s.self; else break; } return best; };
  const nextSelfChangeAfter = (peer, node, from) => { let out = Infinity; for (const s of tl) { if (s.peer !== peer) continue; if (s.wall > from && s.self !== node) { out = s.wall; break; } } return out; };

  // D_required: topicId -> Set(peer)
  const requiredByTopic = new Map();
  for (const e of lifecycle) if (e.kind === 'sub-required') { const tp = name2topic.get(e.detail?.topic); if (tp) { if (!requiredByTopic.has(tp)) requiredByTopic.set(tp, new Set()); requiredByTopic.get(tp).add(`${e.peerIdx}|${e.host}`); } }

  // window end per peer (fallback interval close)
  const endByPeer = new Map();
  for (const e of lifecycle) if (e.kind === 'end' && e.wallMs != null) endByPeer.set(`${e.peerIdx}|${e.host}`, corr(e.wallMs, e.host));

  // D_active intervals, keyed (peer|topicId), bound to the node at activation and
  // clamped to the next identity change (reconnect rule).
  const intervals = new Map();
  for (const e of lifecycle) {
    if (e.kind !== 'sub-activate') continue;
    const tp = pfx(e.detail?.topicId); if (!tp || e.wallMs == null) continue;
    const peer = `${e.peerIdx}|${e.host}`, from = corr(e.wallMs, e.host);
    const node = selfAt(peer, from);
    const clamp = nextSelfChangeAfter(peer, node, from);           // identity change closes the interval
    intervals.set(`${peer}|${tp}`, { peer, topicId: tp, from, to: clamp, node });   // to refined by sub-end below
  }
  for (const e of lifecycle) {
    if (e.kind !== 'sub-end') continue;
    const tp = name2topic.get(e.detail?.topic), k = `${e.peerIdx}|${e.host}|${tp}`;
    if (tp && intervals.has(k) && e.wallMs != null) { const iv = intervals.get(k); iv.to = Math.min(iv.to, corr(e.wallMs, e.host)); }
  }
  for (const iv of intervals.values()) if (iv.to === Infinity) { const en = endByPeer.get(iv.peer); if (en != null) iv.to = en; }
  const ivByPeerTopic = new Map(); for (const iv of intervals.values()) ivByPeerTopic.set(`${iv.peer}|${iv.topicId}`, iv);

  // activePeer at (peer, topicId, ts): active iff an interval contains ts AND the
  // identity is stable (selfAt(ts) === interval.node). boundary-ambiguous iff ts is
  // within band of a boundary.
  const activePeer = (peer, topicId, ts) => {
    const iv = ivByPeerTopic.get(`${peer}|${topicId}`);
    if (!iv) return { active: false, node: null, boundary: false };
    const inside = ts >= iv.from && ts < iv.to;
    const idStable = selfAt(peer, ts) === iv.node;
    const boundary = Math.abs(ts - iv.from) < band || (Number.isFinite(iv.to) && Math.abs(ts - iv.to) < band);
    return { active: inside && idStable, node: iv.node, boundary };
  };
  return { requiredByTopic, activePeer, band };
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
  const { ledgerRows, rootMembers, hopTx, hopRx, appRecv, idMap, selfTL = [], lifecycle = [], maxT, clock = null } = parsed;
  const censorMs = opts.censorMs ?? CENSOR_MS;
  const censorCutoff = maxT - censorMs;
  const FROZEN_DRIFT_BOUND = opts.driftBound ?? 500;    // pre-declared: drift beyond this VOIDs timing
  // Clock basis (b), Aster 2a2778b2: prefer the MEASURED pre/post probe; a measured
  // offset is a correction, the band is uncertainty + observed drift. Fall back to the
  // static offsets only with clockValidated=false + a conservative band, and VOID timing
  // when sampling failed or drift exceeds the frozen bound.
  const clk = opts.clock !== undefined ? opts.clock : clock;
  let offsets, band, clockValidated, timingVoid, clockNote;
  if (clk && clk.hosts) {
    offsets = clk.hosts; band = Math.max(clk.band || 0, 1);
    timingVoid = clk.anyFailed || !clk.hasPre || !clk.hasPost || (clk.worstDrift ?? 0) > FROZEN_DRIFT_BOUND;
    clockValidated = !timingVoid;
    clockNote = `measured (pre=${clk.hasPre} post=${clk.hasPost} worstUnc=${clk.worstUnc}ms drift=${clk.worstDrift}ms band=${band}ms${timingVoid ? ' → TIMING VOID' : ''})`;
  } else {
    offsets = opts.offsets ?? OFFSETS; band = Math.max(...Object.values(OFFSETS)) + (opts.bandMargin ?? 200);
    clockValidated = false; timingVoid = true;
    clockNote = `NO probe — static offsets, conservative band=${band}ms, timing UNVALIDATED (Aster 2a2778b2: VOID boundary calls)`;
  }
  const sets = buildSets(lifecycle, selfTL, offsets, band);   // null if no lifecycle ledger

  // group ledger rows + root-members by msgId
  const rowsByMsg = new Map(), rmByMsg = new Map(), pubTs = new Map(), provMissing = new Map();
  for (const row of ledgerRows) {
    if (!row.msgId) continue;
    if (!rowsByMsg.has(row.msgId)) rowsByMsg.set(row.msgId, []);
    rowsByMsg.get(row.msgId).push(row);
    // publishTs corrected to the common basis by the row's OWN source host (Aster 2a2778b2).
    if (typeof row.t === 'number') {
      const ct = row.t - offOf(offsets, row.srcHost);
      pubTs.set(row.msgId, Math.min(pubTs.get(row.msgId) ?? Infinity, ct));
    }
    if (!row.srcHost) provMissing.set(row.msgId, true);   // no host provenance -> timing unreliable for this publish
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

  const agg = { publishes: 0, voided: 0, censored: 0, boundaryAmbiguous: 0, provenanceVoid: 0, belief: 0, delivered: 0,
    forwardingDrop: 0, callbackDrop: 0, unresolved: 0, beliefNotApp: 0,
    required: 0, reqDelivered: 0, activationFailure: 0, beliefDivergence: 0, dupReceipts, unmappedReceipts };
  const voidReasons = {}; const rows = [];

  for (const [msgId, mrows] of rowsByMsg) {
    const pt = pubTs.get(msgId);
    if (typeof pt === 'number' && pt > censorCutoff) { agg.censored++; continue; }
    const tree = buildTree(mrows, rmByMsg.get(msgId));
    if (tree.void) { agg.voided++; voidReasons[tree.void] = (voidReasons[tree.void] || 0) + 1; continue; }
    const app = appByMsg.get(msgId) || new Set();

    // THREE-SET breakdown (Aster 54d03977), when a lifecycle ledger exists. REQUIRED
    // is the frozen-plan obligation (independent of sub success); ACTIVE is a resolved,
    // identity-stable interval containing publishTs; BELIEF is the kernel ledger. A
    // publish within band of any required reader's activation/end boundary is
    // boundary-ambiguous and right-censored (clock basis b). Divergences are reported,
    // NEVER counted as transport loss.
    if (sets && typeof pt === 'number') {
      // provenance/timing gates (Aster 2a2778b2): a publish whose fanout rows lack
      // host provenance, or any timing-void run (failed/absent probe or drift beyond
      // the frozen bound), cannot have its activation/end boundary trusted → censor
      // it rather than force a boundary call. Set membership stays identity-keyed.
      if (provMissing.get(msgId)) { agg.provenanceVoid++; continue; }
      const topicId = pfx(mrows.find((r) => r.topicId)?.topicId);
      const required = sets.requiredByTopic.get(topicId) || new Set();
      let boundary = false; const states = [];
      for (const peer of required) { const st = sets.activePeer(peer, topicId, pt); if (st.boundary) boundary = true; states.push({ peer, st }); }
      if (boundary || timingVoid) { agg.boundaryAmbiguous++; continue; }   // right-censor boundary-ambiguous / timing-void
      agg.publishes++;
      agg.required += required.size;
      for (const { st } of states) {
        if (!st.active) { agg.activationFailure++; continue; }    // D_required \ D_active
        if (st.node && !tree.belief.has(st.node)) agg.beliefDivergence++;   // D_active \ D_belief
        if (st.node && app.has(st.node)) agg.reqDelivered++;      // required AND delivered (service completeness)
      }
    } else {
      agg.publishes++;
    }

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
  return { agg, rows, voidReasons, latP50: p(50), latP95: p(95), censorMs, band, hasSets: !!sets,
    clockValidated, timingVoid, clockNote, clockHosts: (clk && clk.hosts) || null,
    completeness: agg.belief ? +(100 * agg.delivered / agg.belief).toFixed(2) : null,
    serviceCompleteness: agg.required ? +(100 * agg.reqDelivered / agg.required).toFixed(2) : null };
}

function report(res, note) {
  const a = res.agg;
  console.log('\n===== DELIVERY RECONCILIATION (path-aware; fanout-ledger 73b705d; rules 722f8464/89ab0777) =====');
  if (note) console.log(note);
  console.log(`CLOCK: ${res.clockNote}   validated=${res.clockValidated}${res.timingVoid ? '  [timing-dependent boundary calls VOID/censored]' : ''}`);
  if (res.clockHosts) for (const [h, c] of Object.entries(res.clockHosts)) console.log(`  ${h}: offset=${c.offset}ms uncertainty=${c.uncertainty}ms drift=${c.drift}ms`);
  console.log(`publishes scored: ${a.publishes}  (VOID: ${a.voided}, censored@end: ${a.censored}, boundary/timing censored: ${a.boundaryAmbiguous}, provenance-void: ${a.provenanceVoid})`);
  console.log(`receipts: dup per (msg,sub)=${a.dupReceipts}, unmapped (no peer→node)=${a.unmappedReceipts}`);
  if (res.hasSets) {
    console.log('THREE SETS (Aster 54d03977) — service completeness uses D_required as denominator:');
    console.log(`  D_required obligations: ${a.required}   delivered: ${a.reqDelivered}   SERVICE COMPLETENESS=${res.serviceCompleteness}%`);
    console.log(`  D_required \\ D_active  (subscription activation/continuity failure): ${a.activationFailure}`);
    console.log(`  D_active   \\ D_belief  (active-intent / kernel-belief divergence):    ${a.beliefDivergence}`);
    console.log(`  (both divergences reported, NEVER counted as transport loss)`);
  } else {
    console.log(`D_required/D_active pending — no lifecycle ledger in this data (Aster 54d03977)`);
  }
  console.log(`kernel-belief tree obligations (D_belief): ${a.belief}   delivered: ${a.delivered}   belief-completeness=${res.completeness}%`);
  console.log('D_belief \\ R_app miss localization — path-aware, partitions the miss set exactly:');
  console.log(`  FORWARDING_DROP (earliest unproven edge on the root→sub path): ${a.forwardingDrop}`);
  console.log(`  FINAL_HOP_CALLBACK (every edge proven, app receipt absent):    ${a.callbackDrop}`);
  console.log(`  UNRESOLVED (path unreconstructable — not forced into a bucket): ${a.unresolved}`);
  console.log(`  = D_belief \\ R_app total: ${a.beliefNotApp}  (fwd+callback+unresolved must equal this)`);
  if (a.voided) console.log(`VOID publishes by reason (never scored as loss): ${JSON.stringify(res.voidReasons)}`);
  console.log(`app-receipt latency: p50=${res.latP50}ms p95=${res.latP95}ms   censor window=${res.censorMs}ms   clock band=${res.band}ms`);
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

  // three-set lifecycle fixtures (Aster 54d03977). Host 'h' has offset 0; band 182.
  const G = '28'.repeat(6), H1 = '31'.repeat(6), H2 = '32'.repeat(6), I1 = '41'.repeat(6), I2 = '42'.repeat(6), J = '5a'.repeat(6), K = '6b'.repeat(6), TT = 'aabbccddeeff001122';
  const tmap = { kind: 'topicmap', detail: { name: 'T', topicId: TT }, peerIdx: 0, host: 'h' };
  // CK: a MEASURED clock table (validated: pre+post, no fail). unc=uncertainty(ms),
  // drift(ms), offset(ms). band = unc + drift. host 'h' is the only sidecar host here.
  const CK = (unc = 5, drift = 0, offset = 0) => ({ hosts: { h: { offset, uncertainty: unc, drift } }, band: unc + drift, worstUnc: unc, worstDrift: drift, anyFailed: false, hasPre: true, hasPost: true });
  const beliefTree = (msgId, leaves, srcHost = 'h') => [L({ msgId, node: R, isRoot: 1, parent: null, topicId: TT, n: leaves.length, srcHost, recips: leaves.map((s) => ({ sub: s, child: 0 })) })];
  const RC = (o, clk = CK()) => reconcile(o, { censorMs: 0, clock: clk });

  // (f) required + active, but node absent from belief -> D_active\D_belief divergence.
  const r8 = RC({ ledgerRows: beliefTree('M8', [A]), rootMembers: [], hopTx: new Map([mkHop('g1', R, A, 'M8')]), hopRx: new Set(['g1']),
    appRecv: [{ msgId: 'M8', key: '1|h', t: 1010 }], idMap, selfTL: [{ peerIdx: 5, host: 'h', self: pfx(G), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 5, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 5, host: 'h', wallMs: 100 }], maxT: 1e6 });

  // (g) required, sub NEVER resolves (no sub-activate) -> D_required\D_active activation failure.
  const r9 = RC({ ledgerRows: beliefTree('M9', [A]), rootMembers: [], hopTx: new Map([mkHop('g2', R, A, 'M9')]), hopRx: new Set(['g2']),
    appRecv: [], idMap, selfTL: [], lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 6, host: 'h' }], maxT: 1e6 });

  // (h) disconnect before publish, reconnect (new activate) AFTER publish -> inactive at publishTs.
  const r10 = RC({ ledgerRows: beliefTree('MA', [A]), rootMembers: [], hopTx: new Map([mkHop('g3', R, A, 'MA')]), hopRx: new Set(['g3']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 7, host: 'h', self: pfx(H1), wall: 50 }, { peerIdx: 7, host: 'h', self: pfx(H2), wall: 1200 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 7, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 7, host: 'h', wallMs: 1200 }], maxT: 1e6 });

  // (i) identity replaced mid-interval, NO reactivation -> interval clamped, inactive at publishTs.
  const r11 = RC({ ledgerRows: beliefTree('MB', [A]), rootMembers: [], hopTx: new Map([mkHop('g4', R, A, 'MB')]), hopRx: new Set(['g4']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 8, host: 'h', self: pfx(I1), wall: 50 }, { peerIdx: 8, host: 'h', self: pfx(I2), wall: 500 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 8, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 8, host: 'h', wallMs: 100 }], maxT: 1e6 });

  // (j) repeated idempotent activation -> ONE interval; active+in-belief+delivered.
  const r12 = RC({ ledgerRows: beliefTree('MC', [A, J]), rootMembers: [], hopTx: new Map([mkHop('g5', R, A, 'MC'), mkHop('g6', R, J, 'MC')]), hopRx: new Set(['g5', 'g6']),
    appRecv: [{ msgId: 'MC', key: '9|h', t: 1010 }], idMap: new Map([...idMap, ['9|h', pfx(J)]]), selfTL: [{ peerIdx: 9, host: 'h', self: pfx(J), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 9, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 9, host: 'h', wallMs: 100 }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 9, host: 'h', wallMs: 120 }], maxT: 1e6 });

  // (k) publish inside the clock band of activation (band 150 > |1000-900|) -> boundary-ambiguous censored.
  const r13 = RC({ ledgerRows: beliefTree('MD', [A]), rootMembers: [], hopTx: new Map([mkHop('g7', R, A, 'MD')]), hopRx: new Set(['g7']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 10, host: 'h', self: pfx(K), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 10, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 10, host: 'h', wallMs: 900 }], maxT: 1e6 }, CK(150));

  // (l) LARGE but PRECISE offset (5000ms, uncertainty 5) -> correct classification, NOT
  //     spuriously censored. Raw walls are in the host-5000ms-ahead basis; corrected:
  //     activation 5100->100, publish 6000->1000, self 5050->50. active+delivered.
  const r14 = RC({ ledgerRows: beliefTree('ME', [A], 'h').map((r) => ({ ...r, t: 6000 })), rootMembers: [], hopTx: new Map([mkHop('g8', R, A, 'ME')]), hopRx: new Set(['g8']),
    appRecv: [{ msgId: 'ME', key: '11|h', t: 6010 }], idMap: new Map([...idMap, ['11|h', pfx(G)]]), selfTL: [{ peerIdx: 11, host: 'h', self: pfx(G), wall: 5050 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 11, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 11, host: 'h', wallMs: 5100 }],
    maxT: 1e7 }, CK(5, 0, 5000));

  // (m) small offset, HIGH uncertainty (band 150) -> publish 100ms from activation is
  //     boundary-ambiguous, censored (the wide band, not the offset, drives it).
  const r15 = RC({ ledgerRows: beliefTree('MF', [A]), rootMembers: [], hopTx: new Map([mkHop('g9', R, A, 'MF')]), hopRx: new Set(['g9']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 12, host: 'h', self: pfx(K), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 12, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 12, host: 'h', wallMs: 900 }], maxT: 1e6 }, CK(150, 0, 0));

  // (n) MISSING source-host provenance on the fanout row -> timing unreliable -> VOID
  //     (provenance), not a forced boundary call.
  const noProv = beliefTree('MG', [A]).map((r) => { const { srcHost, ...rest } = r; return rest; });
  const r16 = RC({ ledgerRows: noProv, rootMembers: [], hopTx: new Map([mkHop('ga', R, A, 'MG')]), hopRx: new Set(['ga']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 13, host: 'h', self: pfx(K), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 13, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 13, host: 'h', wallMs: 100 }], maxT: 1e6 });

  // (o) DRIFT beyond the frozen bound (800 > 500) -> timing VOID; every boundary call
  //     censored, clockValidated=false.
  const r17 = RC({ ledgerRows: beliefTree('MH', [A]), rootMembers: [], hopTx: new Map([mkHop('gb', R, A, 'MH')]), hopRx: new Set(['gb']),
    appRecv: [], idMap, selfTL: [{ peerIdx: 14, host: 'h', self: pfx(K), wall: 50 }],
    lifecycle: [tmap, { kind: 'sub-required', detail: { topic: 'T' }, peerIdx: 14, host: 'h' }, { kind: 'sub-activate', detail: { topic: 'T', topicId: TT }, peerIdx: 14, host: 'h', wallMs: 100 }], maxT: 1e6 }, CK(5, 800, 0));

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
    ['(f) required+active, node unlisted -> D_active\\D_belief divergence==1', r8.hasSets && r8.agg.beliefDivergence === 1 && r8.agg.activationFailure === 0],
    ['(g) required, sub never resolves -> activationFailure==1', r9.hasSets && r9.agg.activationFailure === 1 && r9.agg.required === 1],
    ['(h) disconnect, reconnect after publish -> inactive (activationFailure==1)', r10.agg.activationFailure === 1],
    ['(i) identity replaced, no reactivation -> interval clamped, activationFailure==1', r11.agg.activationFailure === 1],
    ['(j) repeated idempotent activation -> ONE interval, reqDelivered==1, no failure', r12.agg.required === 1 && r12.agg.reqDelivered === 1 && r12.agg.activationFailure === 0],
    ['(k) publish in clock band -> boundary-ambiguous censored (0 scored)', r13.agg.boundaryAmbiguous === 1 && r13.agg.publishes === 0],
    ['(l) large-but-precise offset -> classified, NOT censored (reqDelivered==1)', r14.clockValidated && r14.agg.reqDelivered === 1 && r14.agg.publishes === 1],
    ['(m) small offset + high uncertainty -> wide band censors (0 scored)', r15.agg.boundaryAmbiguous === 1 && r15.agg.publishes === 0],
    ['(n) missing host provenance -> provenanceVoid==1 (0 scored)', r16.agg.provenanceVoid === 1 && r16.agg.publishes === 0],
    ['(o) drift beyond bound -> timing VOID, clockValidated=false, 0 scored', r17.timingVoid === true && r17.clockValidated === false && r17.agg.publishes === 0],
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
