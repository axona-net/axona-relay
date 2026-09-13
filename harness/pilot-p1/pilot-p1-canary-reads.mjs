#!/usr/bin/env node
// =============================================================================
// ops/pilot-p1-canary-reads.mjs — AX-PILOT-P1 Linux canary READ tool (offline
// implementation per David-approved decision AX-Pilot-Canary-Reads-Offline-Decision-
// 2026-09-13.md 5431979d…, whose six corrections override the base spec f9023429…).
//
// A separate, read-only plan for the axona-linux canary: three explicitly selected
// phases — B2 (baseline or candidate host pre-checks), DURING (bounded tails of this
// run's five generation logs + ps on the launch pids + df) and A1 (census, ps,
// inventory diff, one bounded read per bound capture, five tails, df). It reuses the
// accepted collector's transport and target table (ops/pilot-p1-collector.mjs, hash
// e00c88f3…) through its exports only; that file and the accepted recorder commit
// 29003c23… are not modified.
//
// It NEVER writes to a host: no roll, no stop-file write or removal, no collection
// copy. Its only "verdict" is an advisory string; acting on it is a separately
// approved human command.
//
// Guarantees (each with an offline test in pilot-p1-canary-reads.test.mjs):
//   C1 Every operator input and every host-derived name is validated BEFORE any
//      command string exists: --gen, --launch-pids, --expect-sha, --min-free-kb,
//      inventory rows, and capture basenames (exact recorder grammar
//      disc-relay-<pid>-<id>.jsonl; no path, no traversal, no shell characters, no
//      symlinks — the listing is `find -maxdepth 1 -type f`). Historic and foreign
//      captures are excluded from the candidate set before any read. Launch
//      completeness is operator-supplied unless corroborated by the census; an
//      inconsistent or --binding-incomplete binding fails closed (HOLD).
//   C2 The armed row is the first COMPLETE line of an 8 KiB head (not a 512-byte
//      prefix); armed and terminal pid/captureId must equal the filename's; the
//      terminal reason must be a known one; the advisory `stopfile-removal-permitted`
//      needs a nonvacuous, complete, one-file-per-launch-pid set. A tail's leading
//      partial line is reported as `tailFragment`, never as a torn-line count.
//   C3 One shell member per command; a missing file fails its own command; `cd`
//      failure stops the command (`&&`); nothing is masked by an `echo` or a later
//      member. Tests execute the GENERATED command strings against owned temp
//      fixtures through a local shell transport.
//   C4 Per-command caps fit their outputs (tails 128 KiB, small reads 4–64 KiB);
//      phase budgets 2 MiB retained / 5 min wall; every command is admitted against
//      the remaining budget before it runs; a truncated or clipped read is a HOLD,
//      never "absence". These bound retained output and dispatch, not remote work.
//   C5 --baseline (before checkout: PRE sha; recorder files may be absent) and
//      --candidate (after checkout: CANARY sha and the four accepted hashes) are
//      distinct; census vs requested pids is compared and reported, never reconciled
//      by assumption. Target `lin` only.
//   C6 Raw output stays in a 0700 results directory; the console and council see an
//      allowlisted summary (numbers, classes, booleans, validated names, digests).
//   NET Importing this module spawns nothing. --live requires PILOT_P1_AUTHORIZED=yes
//      AND a separate operational approval that this file cannot grant.
// =============================================================================
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync, lstatSync, chmodSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { homedir } from 'node:os';
import { realTransport, TARGETS, CLASSES } from './pilot-p1-collector.mjs';

// ---------------- constants ----------------
export const PHASES = Object.freeze(['B2', 'DURING', 'A1']);
export const TERMINAL_REASONS = Object.freeze(['capped', 'stopped', 'window-elapsed', 'closed']);
export const ACCEPTED_HASHES = Object.freeze({
  'src/trace-recorder.js':        '46306b2abb8d8e334be51f89a3f56ebcad0c4d3ab14dddde6bd463457e7c4a77',
  'test/trace_recorder.test.mjs': '09a20405df50abb780a266fd06fc7b22374c4f3e7fdd21f099f8d1a232be7a4f',
  'src/index.js':                 '3b18a8e892dfd09677a9204b500ec9a055163ed6468cc16728f82637de48dcea',
  'package.json':                 '7d83ed69cb766c533e4bceb97cbb9549f793c765948c5d1c797041e87fd9fe76',
});
// Retained-output / dispatch limits (C4). Not remote-work or memory guarantees.
export const CAPS = Object.freeze({
  phaseBytes: 2 * 1024 * 1024, phaseMs: 5 * 60_000, perCommandMs: 60_000,
  tailBytes: 131072, tailCap: 131072 + 4096,        // 128 KiB tail + envelope
  headBytes: 8192, captureCap: 8192 + 4096 + 2048,  // head + 4 KiB tail + ls/sha lines
  smallCap: 4096, listCap: 65536,
});
const RE_GEN = /^\d{8}-\d{6}$/;
const RE_SHA = /^[0-9a-f]{40}$/;
const RE_PID = /^[1-9]\d{0,7}$/;
const RE_CAPTURE = /^disc-relay-([1-9]\d{0,7})-([A-Za-z0-9]{1,32})\.jsonl$/;   // recorder basename grammar
const RE_NAME_SAFE = /^[A-Za-z0-9._-]{1,128}$/;
const RE_RUN = /^[A-Za-z0-9._-]{1,64}$/;                                   // operator-supplied run id, B2 candidate → A1
const RE_SHELL = /[\s'"`$\\|&;<>(){}\[\]*?~!#\n\r\0]/;

const sha256 = (s) => createHash('sha256').update(s).digest('hex');

// ---------------- validation (C1, C5) ----------------
export function validateOptions(o = {}) {
  const errs = [];
  if (o.target !== 'lin') errs.push('target must be lin');
  if (!PHASES.includes(o.phase)) errs.push('phase must be B2 | DURING | A1');
  if (o.phase === 'B2') {
    if (o.baseline === o.candidate) errs.push('B2 needs exactly one of --baseline / --candidate');
    if (!RE_SHA.test(o.expectSha || '')) errs.push('expectSha must be 40 hex');
    if (!Number.isInteger(o.minFreeKb) || o.minFreeKb <= 0) errs.push('minFreeKb must be a positive integer');
    if (o.candidate && !RE_RUN.test(o.runId || '')) errs.push('B2 --candidate needs --run-id (operator-supplied run identifier carried to A1)');
  }
  if (o.phase === 'DURING' || o.phase === 'A1') {
    if (!RE_GEN.test(o.gen || '')) errs.push('gen must match ^\\d{8}-\\d{6}$');
    const pids = Array.isArray(o.launchPids) ? o.launchPids : [];
    if (pids.length < 1 || pids.length > 5) errs.push('launchPids must list 1..5 pids');
    if (!pids.every((p) => RE_PID.test(String(p)))) errs.push('launchPids must be positive integers');
    if (new Set(pids.map(String)).size !== pids.length) errs.push('launchPids must be unique');
  }
  if (o.phase === 'A1') {
    // Contract C1: the inventory must be bound to THIS host and THIS run, and the operator must
    // AFFIRM launch completeness. Nothing here defaults to "complete"; absence is a HOLD.
    if (o.bindingIncomplete) errs.push('binding incomplete: A1 refuses (HOLD)');
    if (o.launchComplete !== true) errs.push('A1 needs --launch-complete (explicit operator assertion that launchPids is the full attempted set)');
    if (!RE_SHA.test(o.expectSha || '')) errs.push('A1 needs --expect-sha (the candidate sha the inventory was taken under)');
    if (!RE_RUN.test(o.runId || '')) errs.push('A1 needs --run-id (the same operator-supplied run identifier given to B2 --candidate)');
    if (!o.inventory || typeof o.inventory !== 'object') errs.push('A1 needs the B2 candidate inventory');
    else {
      const inv = o.inventory;
      if (inv.host !== 'lin' || inv.phase !== 'B2' || inv.mode !== 'candidate') errs.push('inventory must come from a B2 --candidate run on lin');
      if (inv.complete !== true || !Array.isArray(inv.names)) errs.push('inventory must be complete (its listing commands were complete)');
      else for (const n of inv.names) if (!RE_NAME_SAFE.test(n?.name || '')) errs.push(`inventory carries an unsafe name: ${JSON.stringify(String(n?.name).slice(0, 40))}`);
      if (!Number.isFinite(inv.takenAt) || inv.takenAt <= 0) errs.push('inventory lacks a numeric takenAt (run binding)');
      if (!RE_SHA.test(inv.expectSha || '')) errs.push('inventory lacks its expectSha (version binding)');
      else if (RE_SHA.test(o.expectSha || '') && inv.expectSha !== o.expectSha) errs.push('inventory expectSha differs from --expect-sha (different version)');
      // SAME RUN (Aster 1b84c148): host + time + version do not identify a run; the explicit
      // operator-supplied run id given to B2 --candidate must be carried and matched here.
      if (!RE_RUN.test(inv.runId || '')) errs.push('inventory lacks its runId (run binding)');
      else if (RE_RUN.test(o.runId || '') && inv.runId !== o.runId) errs.push('inventory runId differs from --run-id (WRONG RUN)');
    }
  }
  return errs;
}

/**
 * Parse a portable `find relay-logs -maxdepth 1 -name "disc-relay-*" -type f -print` listing
 * (one `relay-logs/<name>` per line; GNU and BSD find agree on this form) into validated rows.
 * `type` is 'f' for the regular-file listing and 'l' for the symlink listing; unsafe names are
 * reported by count and a 40-char excerpt, never used in any later command.
 */
export function parseListing(stdout, type = 'f') {
  const rows = [], rejected = [];
  for (const line of stdout.split('\n')) {
    if (!line) continue;
    const name = line.startsWith('relay-logs/') ? line.slice('relay-logs/'.length) : line;
    if (name.includes('/') || !RE_NAME_SAFE.test(name) || RE_SHELL.test(name)) { rejected.push(String(name).slice(0, 40)); continue; }
    rows.push({ name, type });
  }
  return { rows, rejected };
}

/** This run's capture candidates: grammar-valid regular files, absent from the inventory, pid ∈ launch pids. */
export function selectCandidates(listing, inventoryNames, launchPids) {
  const inv = new Set(inventoryNames.map((n) => n.name));
  const pids = new Set(launchPids.map(String));
  const out = { candidates: [], historic: [], foreign: [], notRegular: [], ambiguous: [] };
  const perPid = new Map();
  for (const r of listing.rows) {
    const m = RE_CAPTURE.exec(r.name);
    if (!m) continue;                                   // not a capture name (old disc-relay-<pid>.jsonl etc.)
    if (r.type !== 'f') { out.notRegular.push(r.name); continue; }   // symlinks and others never read
    if (inv.has(r.name)) { out.historic.push(r.name); continue; }   // present before the roll: never read
    if (!pids.has(m[1])) { out.foreign.push(r.name); continue; }
    const c = { name: r.name, pid: m[1], captureId: m[2] };
    out.candidates.push(c); perPid.set(m[1], (perPid.get(m[1]) || 0) + 1);
  }
  for (const [pid, n] of perPid) if (n > 1) out.ambiguous.push(pid);
  return out;
}

// ---------------- command construction (C3) — literal, one member per command ----------------
export function buildCommands(opts, checkout) {
  const errs = validateOptions(opts); if (errs.length) throw new RangeError(errs.join('; '));
  if (!/^[A-Za-z0-9_/.-]+$/.test(checkout)) throw new RangeError('unsafe checkout path');
  const L = (s) => `timeout -k 5 60 bash -c '${s}'`;    // no single quotes inside any member (asserted below)
  const cmds = [];
  const add = (step, inner, cap, pin = null) => { if (inner.includes("'")) throw new RangeError('quote in command'); cmds.push({ step, remote: L(inner), cap, pin }); };
  const co = checkout;
  if (opts.phase === 'B2') {
    add('head-sha', `git -C ${co} rev-parse HEAD`, CAPS.smallCap);
    if (opts.candidate) add('hashes', `cd ${co} && shasum -a 256 src/trace-recorder.js src/index.js test/trace_recorder.test.mjs package.json`, CAPS.smallCap);
    add('census', `cd ${co} && bash relay-census.sh count`, CAPS.smallCap);
    add('pids', `cd ${co} && bash relay-census.sh --pids`, CAPS.smallCap);
    add('df', `df -k /`, CAPS.smallCap);
    add('inventory', `cd ${co} && find relay-logs -maxdepth 1 -name "disc-relay-*" -type f -print`, CAPS.listCap);
    add('inventory-links', `cd ${co} && find relay-logs -maxdepth 1 -name "disc-relay-*" -type l -print`, CAPS.smallCap);
    add('stopfile', `cd ${co} && find relay-logs -maxdepth 1 -name .trace-stop -print`, CAPS.smallCap);
    add('envstub', `cd ${co} && LAT_TRACE=1 LAT_TRACE_MAX_MS=900000 node -p "JSON.stringify({a:process.env.LAT_TRACE,b:process.env.LAT_TRACE_MAX_MS})"`, CAPS.smallCap);
  }
  if (opts.phase === 'DURING' || opts.phase === 'A1') {
    const P = opts.launchPids.map(String).join(',');
    if (opts.phase === 'A1') {
      add('census-pids', `cd ${co} && bash relay-census.sh --pids`, CAPS.smallCap);
      add('ps', `ps -o pid=,lstart=,etime=,rss=,%cpu= -p ${P}`, CAPS.smallCap);
      add('listing', `cd ${co} && find relay-logs -maxdepth 1 -name "disc-relay-*" -type f -print`, CAPS.listCap);
      add('listing-links', `cd ${co} && find relay-logs -maxdepth 1 -name "disc-relay-*" -type l -print`, CAPS.smallCap);
      // per-capture reads are added AFTER the listing is classified (see runPhase): names must be validated first
    } else {
      add('ps', `ps -o pid=,lstart=,etime=,rss=,%cpu= -p ${P}`, CAPS.smallCap);
    }
    for (let slot = 1; slot <= 5; slot++) add(`tail-${slot}`, `cd ${co} && tail -c ${CAPS.tailBytes} relay-logs/relay-${opts.gen}-${slot}.log`, CAPS.tailCap, { slot });
    add('df', `df -k /`, CAPS.smallCap);
  }
  return cmds;
}
export function captureReadCommand(checkout, name) {
  if (!RE_CAPTURE.test(name) || RE_SHELL.test(name)) throw new RangeError('capture name failed validation');
  // one member each; a failing member fails the command (set -e semantics via &&)
  return { step: `capture:${name}`, remote: `timeout -k 5 60 bash -c 'cd ${checkout} && ls -la relay-logs/${name} && sha256sum relay-logs/${name} && head -c ${CAPS.headBytes} relay-logs/${name} && printf "\\n==TAIL==\\n" && tail -c 4096 relay-logs/${name}'`, cap: CAPS.captureCap, pin: { name } };
}

// ---------------- derivations (C2) ----------------
export function parseArmedHead(text) {
  const nl = text.indexOf('\n');
  if (nl < 0) return { ok: false, why: 'no complete first line within head bound' };
  let r; try { r = JSON.parse(text.slice(0, nl)); } catch { return { ok: false, why: 'first line is not JSON' }; }
  if (r?.ev !== 'armed') return { ok: false, why: 'first line is not an armed row' };
  return { ok: true, pid: String(r.pid), captureId: String(r.captureId ?? ''), kv: r.kv ?? null, settings: r.settings ?? null };
}
export function parseTail(text) {
  const lines = text.split('\n');
  // The first line of a byte-bounded tail usually starts mid-row. It is reported as a
  // tail-only observation (`tailFragment`), never as a torn-line count for the file.
  let fragment = false; if (lines[0]) { try { JSON.parse(lines[0]); } catch { fragment = true; } }
  let last = null; for (let i = lines.length - 1; i >= 0; i--) if (lines[i]) { last = lines[i]; break; }
  let terminal = null;
  if (last) { try { const r = JSON.parse(last); if (r?.ev === 'terminal') terminal = r; } catch { /* the last line is not a parsable terminal */ } }
  return { tailFragment: fragment, tailLines: lines.filter(Boolean).length, terminal };
}
const nonNeg = (v) => (typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : null);
/**
 * Structural parse of one capture read (C2, Aster ca2e604e #2/#4): line 0 must be the `ls -la`
 * line for the expected name, line 1 the `sha256sum` line for it, and line 2 — the FIRST LINE OF
 * THE FILE — must be the armed row. Nothing is scanned for; a non-JSON first file row fails the
 * identity. Terminal numeric fields are validated (finite, >= 0) or reported invalid; only
 * validated values reach the summary.
 */
export function parseCaptureRead(stdout, expected) {
  const cut = stdout.indexOf('\n==TAIL==\n');
  if (cut < 0) return { ok: false, why: 'tail marker missing (command incomplete)' };
  const headPart = stdout.slice(0, cut), tailPart = stdout.slice(cut + 10);
  const lines = headPart.split('\n');
  const name = `relay-logs/${expected.name ?? `disc-relay-${expected.pid}-${expected.captureId}.jsonl`}`;
  const lsM = /^-\S+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+.*\s(\S+)$/.exec(lines[0] || '');
  const lsOk = !!lsM && lsM[2] === name;
  const shaM = /^([0-9a-f]{64})\s+(\S+)$/.exec(lines[1] || '');
  const shaOk = !!shaM && shaM[2] === name;
  const armed = lines.length >= 3 ? parseArmedHead(lines.slice(2).join('\n')) : { ok: false, why: 'file head missing' };   // first FILE line only
  const tail = parseTail(tailPart);
  const idOk = lsOk && shaOk && armed.ok && armed.pid === expected.pid && armed.captureId === expected.captureId;
  const t = tail.terminal;
  const termIdOk = !!t && String(t.pid) === expected.pid && String(t.captureId) === expected.captureId && TERMINAL_REASONS.includes(t.reason);
  const termFields = t ? { reason: TERMINAL_REASONS.includes(t.reason) ? t.reason : null, elapsedMs: nonNeg(t.elapsedMs), rowsWritten: nonNeg(t.rowsWritten), bytesWritten: nonNeg(t.bytesWritten) } : null;
  const termFieldsValid = !t || (termFields.reason !== null && termFields.elapsedMs !== null && termFields.rowsWritten !== null && termFields.bytesWritten !== null);
  return { ok: true, lsOk, shaOk, sha256: shaOk ? shaM[1] : null, size: lsOk ? Number(lsM[1]) : null, armed, armedIdentityOk: idOk, terminal: termFields, terminalIdentityOk: termIdOk && termFieldsValid, terminalFieldsValid: termFieldsValid, tailFragment: tail.tailFragment, tailLines: tail.tailLines };
}

// ---------------- the phase runner (C4 admission, C3 classing, C6 summary) ----------------
export async function runPhase({ opts, checkout, transport, caps = CAPS, clock = () => performance.now(), wall = Date.now, log = () => {} }) {
  const target = TARGETS.find((t) => t.id === 'lin');
  const start = clock(); let retained = 0;
  const rows = [];
  const admit = (cap) => (clock() - start < caps.phaseMs) && (retained + cap <= caps.phaseBytes);
  const runOne = async (c) => {
    const row = { step: c.step, command: c.remote, cap: c.cap, cls: 'skipped', bytes: 0, start: null, end: null, code: null, stdout: '', stderr: '' };
    if (!admit(c.cap)) { row.reason = 'phase budget'; rows.push(row); return row; }
    const t0 = wall();
    const r = await transport.run(target, c.remote, { ms: Math.min(caps.perCommandMs, caps.phaseMs - (clock() - start)), bytes: c.cap });
    Object.assign(row, { cls: CLASSES.includes(r.cls) ? r.cls : 'error', bytes: r.bytes ?? Buffer.byteLength(r.stdout), start: t0, end: wall(), code: r.code, stdout: r.stdout, stderr: (r.stderr || '').slice(0, 2048) });
    retained += row.bytes; rows.push(row); log(`${c.step} ${row.cls} ${row.bytes}B`); return row;
  };
  const cmds = buildCommands(opts, checkout);
  let listing = null;
  for (const c of cmds) {
    const row = await runOne(c);
    if (opts.phase === 'A1' && c.step === 'listing' && row.cls === 'complete') listing = parseListing(row.stdout, 'f');
    if (opts.phase === 'A1' && c.step === 'listing-links' && listing && row.cls === 'complete') {
      const links = parseListing(row.stdout, 'l');
      listing = { rows: [...listing.rows, ...links.rows], rejected: [...listing.rejected, ...links.rejected] };
      const sel = selectCandidates(listing, opts.inventory.names, opts.launchPids);
      if (listing.rejected.length === 0 && sel.ambiguous.length === 0) {
        for (const cand of sel.candidates.slice(0, 5)) await runOne({ ...captureReadCommand(checkout, cand.name), pin: cand });
      }
      rows.__selection = sel; rows.__listing = listing;
    }
  }
  const summary = summarize({ opts, rows, start, clock, elapsedMs: clock() - start, retained, caps });
  return { summary, rows };
}

/** Allowlisted summary (C6): numbers, classes, booleans, validated names, digests, reasons — never raw host text. */
export function summarize({ opts, rows, elapsedMs, retained, caps }) {
  const worst = rows.reduce((w, r) => (order(r.cls) > order(w) ? r.cls : w), 'complete');
  const s = { plan: 'canary', target: 'lin', phase: opts.phase, mode: opts.phase === 'B2' ? (opts.baseline ? 'baseline' : 'candidate') : null, gen: opts.gen ?? null, launchPids: (opts.launchPids || []).map(String), budgets: { phaseBytes: caps.phaseBytes, phaseMs: caps.phaseMs, perCommandMs: caps.perCommandMs }, elapsedMs: Math.round(elapsedMs), retainedBytes: retained, commands: rows.map((r) => ({ step: r.step, cls: r.cls, bytes: r.bytes, cap: r.cap, code: r.code, reason: r.reason ?? null })), phaseClass: worst, complete: worst === 'complete', holds: [] };
  const byStep = Object.fromEntries(rows.map((r) => [r.step, r]));
  if (opts.phase === 'B2') {
    const head = (byStep['head-sha']?.stdout || '').trim();
    s.headSha = RE_SHA.test(head) ? head : null; s.headShaMatches = s.headSha === opts.expectSha;
    if (!s.headShaMatches) s.holds.push('HEAD sha does not match expectSha');
    if (opts.candidate) {
      const got = Object.fromEntries((byStep.hashes?.stdout || '').split('\n').map((l) => l.trim().split(/\s+/)).filter((a) => a.length === 2).map(([h, f]) => [f, h]));
      s.acceptedHashesMatch = Object.entries(ACCEPTED_HASHES).every(([f, h]) => got[f] === h);
      if (!s.acceptedHashesMatch) s.holds.push('accepted file hashes do not match on host');
    }
    s.census = Number((byStep.census?.stdout || '').trim()) || null; if (s.census !== 5) s.holds.push(`census ${s.census} != 5`);
    const free = Number(((byStep.df?.stdout || '').split('\n')[1] || '').split(/\s+/)[3]); s.freeKb = Number.isFinite(free) ? free : null;
    if (!(s.freeKb >= opts.minFreeKb)) s.holds.push('free space below tripwire or unreadable');
    const inv = byStep.inventory, invL = byStep['inventory-links'];
    const listing = inv?.cls === 'complete' && invL?.cls === 'complete' ? (() => { const f = parseListing(inv.stdout, 'f'), l = parseListing(invL.stdout, 'l'); return { rows: [...f.rows, ...l.rows], rejected: [...f.rejected, ...l.rejected] }; })() : null;
    s.inventory = { host: 'lin', phase: 'B2', mode: s.mode, runId: opts.runId ?? null, complete: !!listing && listing.rejected.length === 0, names: listing ? listing.rows : [], rejected: listing?.rejected.length ?? null, takenAt: rows.find((r) => r.step === 'inventory')?.start ?? null, expectSha: opts.expectSha };
    if (!s.inventory.complete) s.holds.push('inventory not complete');
    if ((byStep.stopfile?.stdout || '').includes('.trace-stop')) s.holds.push('pre-existing .trace-stop (admission failure)');
    s.envStubOk = (byStep.envstub?.stdout || '').includes('{"a":"1","b":"900000"}'); if (!s.envStubOk) s.holds.push('env propagation stub failed');
  }
  if (opts.phase === 'DURING' || opts.phase === 'A1') {
    s.tails = [1, 2, 3, 4, 5].map((i) => { const r = byStep[`tail-${i}`]; return { slot: i, cls: r?.cls ?? 'skipped', bytes: r?.bytes ?? 0, abortKeys: r?.cls === 'complete' ? countKeys(r.stdout) : null }; });
    const ps = byStep.ps; s.psRows = ps?.cls === 'complete' ? ps.stdout.split('\n').filter(Boolean).length : null;
  }
  if (opts.phase === 'A1') {
    const sel = rows.__selection, listing = rows.__listing;
    // Census, explicitly (Aster ca2e604e #3, 1b84c148): every line must be a pid; empty, malformed
    // or duplicate output is a HOLD; a launch pid absent from the census is a HOLD with no waiver.
    // The terminal reason of its bound capture (`closed` included) is reported beside the hold and
    // never lifts it: it proves recorder closure, not process exit or launch provenance.
    const cRow = byStep['census-pids'];
    const cLines = (cRow?.stdout || '').split('\n').map((x) => x.trim()).filter((x) => x.length);
    const census = cLines.filter((x) => RE_PID.test(x));
    const launch = opts.launchPids.map(String);
    s.censusPids = census.length; s.censusMalformedLines = cLines.length - census.length; s.censusDuplicates = census.length - new Set(census).size;
    s.launchPidsInCensus = launch.filter((p) => census.includes(p)).length; s.censusPidsNotLaunched = census.filter((p) => !launch.includes(p)).length;
    if (cRow?.cls !== 'complete') s.holds.push('census read not complete');
    else if (census.length === 0) s.holds.push('census empty');
    if (s.censusMalformedLines > 0) s.holds.push('census has malformed lines');
    if (s.censusDuplicates > 0) s.holds.push('census has duplicate pids');
    if (s.censusPidsNotLaunched > 0) s.holds.push('census has pids outside the launch set');
    s.launchPidsAbsentFromCensus = launch.filter((p) => !census.includes(p));
    s.selection = sel ? { candidates: sel.candidates.length, historic: sel.historic.length, foreign: sel.foreign.length, notRegular: sel.notRegular.length, ambiguousPids: sel.ambiguous.length, rejectedNames: listing?.rejected.length ?? 0 } : null;
    if (!sel) s.holds.push('listing not complete');
    else {
      if (listing.rejected.length) s.holds.push('unsafe names in listing');
      if (sel.ambiguous.length) s.holds.push('more than one capture for a launch pid');
      if (sel.candidates.length === 0) s.holds.push('no captures bound (empty set)');
      s.captures = sel.candidates.map((c) => {
        const r = byStep[`capture:${c.name}`];
        if (!r || r.cls !== 'complete') return { name: c.name, pid: c.pid, captureId: c.captureId, cls: r?.cls ?? 'skipped', bound: false };
        const p = parseCaptureRead(r.stdout, c);
        if (!p.ok) return { name: c.name, pid: c.pid, captureId: c.captureId, cls: 'error', bound: false };
        return { name: c.name, pid: c.pid, captureId: c.captureId, cls: r.cls, size: p.size, sha256: p.sha256, armedIdentityOk: p.armedIdentityOk, terminalPresent: !!p.terminal, terminalReason: p.terminal?.reason ?? null, terminalIdentityOk: p.terminalIdentityOk, terminalFieldsValid: p.terminalFieldsValid, elapsedMs: p.terminal?.elapsedMs ?? null, rowsWritten: p.terminal?.rowsWritten ?? null, bytesWritten: p.terminal?.bytesWritten ?? null, tailFragment: p.tailFragment, tailLines: p.tailLines, bound: p.armedIdentityOk };
      });
      for (const c of s.captures) {
        if (!c.bound) s.holds.push(`capture ${c.name} identity not bound`);
        else if (!c.terminalIdentityOk) s.holds.push(`capture ${c.name} lacks a matching terminal row`);
        if (c.terminalPresent && c.terminalFieldsValid === false) s.holds.push(`capture ${c.name} terminal fields invalid`);
      }
      const boundPids = new Set(s.captures.filter((c) => c.bound).map((c) => c.pid));
      for (const p of launch) if (!boundPids.has(p)) s.holds.push(`launch pid ${p} has no bound capture`);
      // A launch pid absent from the census is a HOLD in this patch, whatever its capture says: a
      // `closed` terminal proves recorder closure, not process exit or launch provenance
      // (Aster 1b84c148). The reason observed is reported beside it, never used as a waiver.
      s.launchPidsAbsentDetail = s.launchPidsAbsentFromCensus.map((p) => ({ pid: p, terminalReason: s.captures.find((c) => c.pid === p && c.bound)?.terminalReason ?? null }));
      for (const p of s.launchPidsAbsentFromCensus) s.holds.push(`launch pid ${p} absent from census`);
    }
    s.advisory = (s.complete && s.holds.length === 0 && s.captures && s.captures.length === launch.length && s.captures.every((c) => c.bound && c.terminalIdentityOk) && s.censusPids > 0) ? 'stopfile-removal-permitted' : 'HOLD: unresolved';
    s.advisoryNote = 'advisory only; this tool never removes or writes anything';
  }
  if (!s.complete) s.holds.unshift(`phase class ${worst} (worst member): NOT OBSERVED`);
  return s;
}
const order = (c) => ({ complete: 0, truncated: 2, clipped: 2, timeout: 3, error: 3, skipped: 1 })[c] ?? 3;
const ABORT_KEYS = ['trace-write-failed', 'trace-terminal-write-failed', 'trace-recorder-disabled'];
// The summary's `lastState` is a CLOSED vocabulary (Aster 4e9f9554 #1): the kernel's bridgeState
// machine (vendor/axona-protocol/src/transport/web/index.js setBridgeState) plus the tui's `down`
// fallback. A `state=` token outside it becomes the literal 'unrecognized'; no host text is copied.
export const BRIDGE_STATES = Object.freeze(['connecting', 'open', 'stale', 'disconnected', 'upgrade-required', 'graduated', 'down']);
function countKeys(text) {
  const out = {}; for (const k of ABORT_KEYS) out[k] = (text.match(new RegExp(k, 'g')) || []).length;
  const st = text.match(/state=([A-Za-z0-9_-]+)/g);
  out.lastState = st ? (BRIDGE_STATES.includes(st.at(-1).slice(6)) ? st.at(-1).slice(6) : 'unrecognized') : null;
  return out;
}

// ---------------- results ----------------
// The results directory is created EXCLUSIVELY and fresh (Aster 4e9f9554 #2): anything already at
// `dir` — a directory, a file, a symlink (dangling or not) — is a refusal, so a pre-existing mode
// is never inherited and no file is ever overwritten. Files are opened 'wx' at 0600.
export function writePhaseResults(dir, summary, rows) {
  let pre = null; try { pre = lstatSync(dir); } catch (e) { if (e.code !== 'ENOENT') throw e; }
  if (pre) throw new Error(`refusing: output path exists (${pre.isSymbolicLink() ? 'symlink' : pre.isDirectory() ? 'directory' : 'file'}): ${dir}`);
  mkdirSync(dirname(dir), { recursive: true, mode: 0o700 });
  mkdirSync(dir, { mode: 0o700 });                    // not recursive: EEXIST if anything raced in
  chmodSync(dir, 0o700);                              // umask can only strip bits; pin the mode
  const st = lstatSync(dir); if (!st.isDirectory() || st.isSymbolicLink()) throw new Error(`refusing: output path is not a fresh directory: ${dir}`);
  const put = (name, data) => writeFileSync(join(dir, name), data, { mode: 0o600, flag: 'wx' });
  put('summary.json', JSON.stringify(summary, null, 2));
  put('manifest.json', JSON.stringify({ budgets: summary.budgets, rows: rows.map(({ stdout, stderr, ...m }) => m) }, null, 2));
  for (const r of rows) if (r.cls !== 'skipped') put(`${r.step.replace(/[^A-Za-z0-9_.-]/g, '_')}.txt`, r.stdout);
  if (summary.inventory) put('inventory.json', JSON.stringify(summary.inventory, null, 2));
}

// ---------------- CLI (dry-run by default; live doubly gated) ----------------
function parseArgv(argv) {
  const o = { target: null, phase: null, baseline: false, candidate: false, launchPids: [], live: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i], v = argv[i + 1];
    if (a === '--plan') { if (v !== 'canary') throw new RangeError('only --plan canary'); i++; }
    else if (a === '--target') { o.target = v; i++; }
    else if (a === '--phase') { o.phase = v; i++; }
    else if (a === '--baseline') o.baseline = true;
    else if (a === '--candidate') o.candidate = true;
    else if (a === '--expect-sha') { o.expectSha = v; i++; }
    else if (a === '--min-free-kb') { o.minFreeKb = Number(v); i++; }
    else if (a === '--gen') { o.gen = v; i++; }
    else if (a === '--launch-pids') { o.launchPids = String(v).split(',').map((x) => x.trim()).filter(Boolean); i++; }
    else if (a === '--inventory') { o.inventory = JSON.parse(readFileSync(v, 'utf8')); i++; }
    else if (a === '--binding-incomplete') o.bindingIncomplete = true;
    else if (a === '--launch-complete') o.launchComplete = true;
    else if (a === '--run-id') { o.runId = v; i++; }
    else if (a === '--out') { o.out = v; i++; }
    else if (a === '--live') o.live = true;
    else throw new RangeError(`unknown argument ${a}`);
  }
  return o;
}
async function main() {
  const o = parseArgv(process.argv.slice(2));
  const errs = validateOptions(o); if (errs.length) { console.error('refusing: ' + errs.join('; ')); process.exit(2); }
  const checkout = TARGETS.find((t) => t.id === 'lin').checkout;
  const cmds = buildCommands(o, checkout);
  if (!o.live) { for (const c of cmds) console.log(`${c.step.padEnd(12)} cap=${c.cap}  ${c.remote}`); console.log(`\ndry run: ${cmds.length} commands (A1 adds ≤5 per-capture reads after the listing), nothing spawned. caps=${JSON.stringify(CAPS)}`); return; }
  if (process.env.PILOT_P1_AUTHORIZED !== 'yes') { console.error('refusing --live: PILOT_P1_AUTHORIZED=yes not set. Interlock only; a separate operational approval is still required.'); process.exit(2); }
  const out = o.out || join(homedir(), 'Documents/claude/axona-relay/harness/results/pilot-p1-canary', `${o.phase}-${new Date().toISOString().replace(/[:.]/g, '-')}`);
  const { summary, rows } = await runPhase({ opts: o, checkout, transport: realTransport({ budget: { perCommandMs: CAPS.perCommandMs, killGraceMs: 5000, confirmMs: 1000, wallMs: CAPS.phaseMs, perCommandBytes: CAPS.tailCap, stderrBytes: 2048, totalBytes: CAPS.phaseBytes } }), log: (l) => console.error(l) });
  writePhaseResults(out, summary, rows);
  console.log(JSON.stringify(summary, null, 2));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main().catch((e) => { console.error(e?.message || e); process.exit(1); });
