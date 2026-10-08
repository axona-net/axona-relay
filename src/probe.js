// =============================================================================
// probe.js — the relay-side probe facility (0.150.0, David 2026-10-08).
//
// THE QUESTION. Can every node reach every other node, and what does a message
// cost? The first matrix (team-updates/P2P-RTT-Matrix-2026-10-08) had to use a
// newborn vantage node on each host as the sender, because a relay had no way
// to be asked. This file is that way.
//
// HOW IT IS ASKED. A request file dropped into RELAY_PROBE_DIR (default
// `<cwd>/probe-requests`): `<run>.json` = { targets: [{ id: <66-hex>, label }],
// samples?, gapMs?, warmupMs? }. Every relay sharing that directory picks it up
// on its next poll (RELAY_PROBE_POLL_MS, default 10 s), runs the matrix row from
// ITS OWN seat, and writes `<run>.<self12>.jsonl` beside it plus
// `<run>.<self12>.done`. A relay runs one request at a time and never the same
// run twice (the .done file is the memory across restarts). No network surface:
// the only way to ask is to write a file on the host. RELAY_PROBE=0 disables.
//
// WHAT IS MEASURED (same rows as ops/p2p-rtt-matrix.mjs, so the fold is shared):
//   ROUTED  peer.routeMessage(target, '__tunneled_direct__' → local_probe):
//           the kernel's routed envelope for a direct request, forwarded greedily
//           hop by hop; the target runs local_probe and reports consumed. The
//           round trip is the whole chain. Outcomes: ok / misrouted / terminal /
//           exhausted / unconsumed / error, with the hop count.
//   DIRECT  transport.send(target, 'local_probe') where a channel exists.
//
// The relay keeps serving while it probes: 40 ms between samples, one target at
// a time, at most a few hundred small requests per run.
// =============================================================================
import { readdirSync, readFileSync, existsSync, appendFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

export const PROBE_DEFAULTS = Object.freeze({ samples: 3, gapMs: 40, warmupMs: 0 });

const hex = (b) => (typeof b === 'bigint' ? b.toString(16).padStart(66, '0') : String(b));
const short = (b) => hex(b).slice(0, 12);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const safe = (o) => JSON.stringify(o, (k, v) => (typeof v === 'bigint' ? v.toString(16) : v));

/** Classify a routeMessage result against the intended target (hex). Pure. */
export function classifyRouted(r, targetHex) {
  if (!r || typeof r !== 'object') return { outcome: 'unconsumed', hops: null, atNode: null };
  const at = r.atNode != null ? hex(r.atNode) : null;
  const hops = r.hops ?? null;
  if (r.consumed && at === targetHex) return { outcome: 'ok', hops, atNode: short(at) };
  if (r.consumed) return { outcome: 'misrouted', hops, atNode: at ? short(at) : null };
  if (r.terminal) return { outcome: 'terminal', hops, atNode: at ? short(at) : null };
  if (r.exhausted) return { outcome: 'exhausted', hops, atNode: at ? short(at) : null };
  return { outcome: 'unconsumed', hops, atNode: at ? short(at) : null };
}

/**
 * Run one matrix row: this node → every target. `emit(row)` receives each row.
 * Pure of I/O apart from the probes themselves. Returns a summary.
 */
export async function runProbeMatrix({ peer, transport, selfId, targets, samples = PROBE_DEFAULTS.samples, gapMs = PROBE_DEFAULTS.gapMs, from = 'relay', emit = () => {} , now = () => performance.now() }) {
  const self = hex(selfId);
  const isConn = (id) => (typeof transport?.isConnected === 'function' ? !!transport.isConnected(id) : false);
  const list = targets.filter((t) => t && typeof t.id === 'string' && /^[0-9a-f]{66}$/.test(t.id) && t.id !== self);
  const summary = { from, self: short(self), targets: list.length, routedOk: 0, routedFail: 0, directOk: 0, directFail: 0 };
  for (const t of list) {
    const target = BigInt('0x' + t.id);
    const connected = isConn(target);
    for (let i = 0; i < samples; i++) {
      const t0 = now();
      let cls = { outcome: 'error', hops: null, atNode: null }, err = null;
      try {
        const r = await peer.routeMessage(target, '__tunneled_direct__', { targetId: t.id, innerType: 'local_probe', innerPayload: {} });
        cls = classifyRouted(r, t.id);
      } catch (e) { err = String(e?.message || e).slice(0, 100); }
      const ms = Math.round((now() - t0) * 100) / 100;
      if (cls.outcome === 'ok') summary.routedOk++; else summary.routedFail++;
      emit({ kind: 'pair', mode: 'routed', from, to: t.label ?? short(t.id), toId: short(t.id), ms, outcome: cls.outcome, hops: cls.hops, atNode: cls.atNode, err, connected });
      if (connected) {
        const d0 = now();
        try {
          await transport.send(target, 'local_probe', {});
          summary.directOk++;
          emit({ kind: 'pair', mode: 'direct', from, to: t.label ?? short(t.id), toId: short(t.id), ms: Math.round((now() - d0) * 100) / 100, outcome: 'ok' });
        } catch (e) {
          summary.directFail++;
          emit({ kind: 'pair', mode: 'direct', from, to: t.label ?? short(t.id), toId: short(t.id), ms: Math.round(now() - d0), outcome: 'error', err: String(e?.message || e).slice(0, 100) });
        }
      }
      if (gapMs > 0) await sleep(gapMs);
    }
  }
  return summary;
}

/** Parse a request file's JSON. Returns null when it is not a probe request. */
export function parseRequest(text) {
  let j; try { j = JSON.parse(text); } catch { return null; }
  if (!j || !Array.isArray(j.targets)) return null;
  const targets = j.targets.map((t) => (typeof t === 'string' ? { id: t } : t)).filter((t) => t && typeof t.id === 'string');
  const num = (v, d) => (Number.isFinite(v) && v >= 0 ? v : d);
  return { targets, samples: num(j.samples, PROBE_DEFAULTS.samples), gapMs: num(j.gapMs, PROBE_DEFAULTS.gapMs), warmupMs: num(j.warmupMs, PROBE_DEFAULTS.warmupMs) };
}

/**
 * The file-drop worker. Polls `dir` for `<run>.json`; for each run not yet
 * done by THIS node (no `<run>.<self12>.done`), runs the matrix and writes
 * `<run>.<self12>.jsonl` + `.done`. One run at a time. Returns a stopper.
 */
export function startProbeWorker({ peer, transport, selfId, dir, pollMs = 10000, log = () => {}, label = null }) {
  const self12 = short(selfId);
  let busy = false, stopped = false;
  const seen = new Set();
  try { mkdirSync(dir, { recursive: true }); } catch { /* read-only checkout: the poll will just find nothing */ }
  const poll = async () => {
    if (busy || stopped) return;
    let names = [];
    try { names = readdirSync(dir).filter((n) => n.endsWith('.json')).sort(); } catch { return; }
    for (const name of names) {
      const run = name.slice(0, -5);
      const done = join(dir, `${run}.${self12}.done`);
      if (seen.has(run) || existsSync(done)) { seen.add(run); continue; }
      let req = null;
      try { req = parseRequest(readFileSync(join(dir, name), 'utf8')); } catch { /* fall through */ }
      if (!req) { seen.add(run); log('warn', 'probe-request-ignored', { run }); continue; }
      busy = true;
      const out = join(dir, `${run}.${self12}.jsonl`);
      try {
        writeFileSync(out, '');
        if (req.warmupMs > 0) await sleep(req.warmupMs);
        const rec = (row) => appendFileSync(out, safe({ t: Date.now(), ...row }) + '\n');
        const syn = (() => { try { return [...(peer._node ?? peer.node).synaptome.values()].length; } catch { return null; } })();
        rec({ kind: 'run', vantage: label ?? self12, self: self12, synaptome: syn, targetsListed: req.targets.length, samples: req.samples, run });
        log('info', 'probe-run-start', { run, targets: req.targets.length, samples: req.samples });
        const summary = await runProbeMatrix({ peer, transport, selfId, targets: req.targets, samples: req.samples, gapMs: req.gapMs, from: label ?? self12, emit: rec });
        rec({ kind: 'done', ...summary, run });
        writeFileSync(done, JSON.stringify({ run, self: self12, finishedAt: new Date().toISOString(), ...summary }) + '\n');
        log('info', 'probe-run-done', { run, ...summary });
      } catch (e) {
        log('warn', 'probe-run-failed', { run, err: String(e?.message || e).slice(0, 120) });
        try { writeFileSync(done, JSON.stringify({ run, self: self12, failed: String(e?.message || e).slice(0, 120) }) + '\n'); } catch { /* */ }
      } finally {
        seen.add(run); busy = false;
      }
      break;   // one run per poll; the next poll picks up the next file
    }
  };
  const timer = setInterval(() => { poll().catch((e) => log('warn', 'probe-poll-failed', { err: String(e?.message || e) })); }, pollMs);
  timer.unref?.();
  return { stop: () => { stopped = true; clearInterval(timer); }, poll };
}
