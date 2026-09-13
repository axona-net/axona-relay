#!/usr/bin/env node
// =============================================================================
// ops/pilot-p1-collector.mjs — AX-PILOT-P1 read-only, METADATA-ONLY fleet preflight
// collector. First pass per Aster 7f70bc01: clock/hostname, bounded systemd unit
// properties, safe per-pid resource fields, df, full checkout commit. Nothing else.
// Revision 2 after Aster be4c5b03 (F1 cleanup escalation, F2 stderr bound, F3 in-flight
// run budgets, F4 order-independent unit parsing).
//
// NOT collected in this pass: logs, journals, environment, unit files, process
// arguments or command lines, git status, attestation files. Windows node counts
// are a PROXY (relay-census.sh counts every node.exe) and are labelled so.
//
// Safety properties (each has an offline test in pilot-p1-collector.test.mjs; the
// tests exercise the collector's own local process handling and parsing — they are
// not evidence about remote hosts):
//   P1 Every remote command is a constant from plan(); nothing returned by a host
//      is ever interpolated into a later command.
//   P2 One wall budget and one output budget for the whole run, enforced BEFORE and
//      DURING every command (the transport receives the remaining budget); per-command
//      60 s and 64 KiB counted over stdout+stderr; stderr capped at ingestion.
//   P3 The collector signals ONLY the process group it spawned: SIGTERM at timeout,
//      SIGKILL after the grace period UNCONDITIONALLY (leader exit does not cancel
//      it), then confirms the group is gone; a row records cleanup confirmed or not.
//      This is local cleanup. It proves nothing about processes on a remote host.
//      Linux/droplet commands carry a remote-side `timeout -k 5 60`; mac commands are
//      single short reads and no remote watchdog is claimed.
//   P4 Windows commands contain none of  | > & ( ) $  (cmd.exe eats them before
//      bash sees them); unsupported platform reads are OMITTED, not improvised.
//   P5 Each manifest row carries CLASS in {complete, truncated, timeout, clipped,
//      error, skipped}; `clipped` = cut by the RUN budget, not the per-command one.
//      Only `complete` rows are observations.
//   P6 Live mode refuses to run unless PILOT_P1_AUTHORIZED=yes is set. This is an
//      interlock against accidental dispatch, not a record of David's approval.
//
//   node ops/pilot-p1-collector.mjs --dry-run            # print the exact plan
//   PILOT_P1_AUTHORIZED=yes node ops/pilot-p1-collector.mjs --live [--out DIR]
//   node --test ops/pilot-p1-collector.test.mjs          # offline, fake transport
// =============================================================================
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { homedir } from 'node:os';

const HOME = homedir();
const SAFE_PATH = /^[A-Za-z0-9_/:.\-]+$/;          // checkout paths + unit names, validated at load
const SAFE_UNIT = /^[a-z0-9]+$/;

export const TARGETS = Object.freeze([
  { id: 'air',  flavour: 'mac',     ssh: ['air'],         checkout: '/Users/davidasmith/Documents/claude/axona-relay' },
  { id: 'm1',   flavour: 'mac',     ssh: ['m1'],          checkout: '/Users/david/Documents/claude/axona-relay' },
  { id: 'lin',  flavour: 'linux',   ssh: ['axona-linux'], checkout: '/home/mintlinux/Documents/claude/axona-relay' },
  { id: 'win',  flavour: 'windows', ssh: ['axona-win'],   checkout: '/c/Users/david/github/axona-relay' },
  { id: 'sfo3', flavour: 'droplet', ssh: ['-i', `${HOME}/.ssh/id_ed25519_axona`, 'root@143.110.224.247'], checkout: '/opt/axona-relay', units: ['useast', 'uswest', 'grizzly1'] },
  { id: 'nyc3', flavour: 'droplet', ssh: ['-i', `${HOME}/.ssh/id_ed25519_axona`, 'root@167.71.106.63'],  checkout: '/opt/axona-relay', units: ['useast', 'uswest', 'grizzly1'] },
  { id: 'tor1', flavour: 'droplet', ssh: ['-i', `${HOME}/.ssh/id_ed25519_axona`, 'root@159.203.46.28'],  checkout: '/opt/axona-relay', units: ['useast', 'uswest', 'grizzly1'] },
].map(Object.freeze));
for (const t of TARGETS) {
  if (!SAFE_PATH.test(t.checkout)) throw new Error(`unsafe checkout path for ${t.id}`);
  for (const u of t.units ?? []) if (!SAFE_UNIT.test(u)) throw new Error(`unsafe unit name for ${t.id}`);
}

export const SSH_FLAGS = Object.freeze(['-n', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2']);
export const BUDGET = Object.freeze({ perCommandMs: 60_000, killGraceMs: 5_000, confirmMs: 1_000, wallMs: 15 * 60_000, perCommandBytes: 65_536, stderrBytes: 4_096, totalBytes: 4 * 1024 * 1024 });
export const CLASSES = Object.freeze(['complete', 'truncated', 'timeout', 'clipped', 'error', 'skipped']);
const WINBASH = '"C:\\Program Files\\Git\\bin\\bash.exe"';
const WIN_FORBIDDEN = /[|>&()$]/;
const mono = () => performance.now();               // monotonic; wall clock is never used for deadlines

// ---- the plan: literal command strings, built only from allowlisted constants ----
const PS_FIELDS = 'pid=,lstart=,etime=,rss=,%cpu=';    // NO args column — process identity is not collected
function unixSteps(co) {
  return [
    { step: 'clock',  remote: 'hostname; uname -sr; uptime; date -u +%s' },
    { step: 'census', remote: `cd ${co} && bash relay-census.sh count` },
    { step: 'procs',  remote: `cd ${co} && P=$(bash relay-census.sh --pids | paste -sd, -) && [ -n "$P" ] && ps -o ${PS_FIELDS} -p "$P"` },
    { step: 'df',     remote: `df -k ${co}` },
    { step: 'commit', remote: `git -C ${co} rev-parse HEAD` },
  ];
}
const linuxWrap = (s) => `timeout -k 5 60 bash -c '${s}'`;   // catalogue strings contain no single quotes

export function plan(target) {
  const co = target.checkout;
  let steps;
  switch (target.flavour) {
    case 'mac':
      steps = unixSteps(co);                              // no remote watchdog claimed on mac (P3)
      break;
    case 'linux':
      steps = unixSteps(co).map((s) => ({ ...s, remote: linuxWrap(s.remote) }));
      break;
    case 'droplet': {
      const units = target.units;
      const props = 'Id,MainPID,ActiveState,SubState,NRestarts,ExecMainStartTimestamp,MemoryCurrent,CPUUsageNSec';
      steps = [
        { step: 'clock',  remote: 'hostname; uname -sr; uptime; nproc; date -u +%s; date -u +%s.%N' },
        { step: 'units',  remote: 'systemctl list-units axona-relay@* --all --no-legend --no-pager' },
        // one blank line between unit records so the parser never depends on field order (F4)
        { step: 'unitprops', remote: units.map((u) => `systemctl show axona-relay@${u} -p ${props} --no-pager; echo`).join('; ') },
        { step: 'census', remote: `cd ${co} && bash relay-census.sh count` },
        { step: 'procs',  remote: `cd ${co} && P=$(bash relay-census.sh --pids | paste -sd, -) && [ -n "$P" ] && ps -o ${PS_FIELDS} -p "$P"` },
        { step: 'df',     remote: `df -k ${co} /var/log/journal` },
        { step: 'commit', remote: `git -C ${co} rev-parse HEAD` },
      ].map((s) => ({ ...s, remote: linuxWrap(s.remote) }));
      break;
    }
    case 'windows': {
      // Login shell is cmd.exe: no pipes, redirects, ampersands, parens or $(). Unsupported
      // reads (process table with fields, ps) are omitted. Count is a proxy for every node.exe.
      const inner = [
        { step: 'clock',  cmd: 'hostname; date -u +%s' },
        { step: 'census-proxy', cmd: `cd ${co}; timeout 60 bash relay-census.sh count` },
        { step: 'df',     cmd: `timeout 60 df -k ${co}` },
        { step: 'commit', cmd: `timeout 60 git -C ${co} rev-parse HEAD` },
      ];
      for (const s of inner) if (WIN_FORBIDDEN.test(s.cmd) || s.cmd.includes("'")) throw new Error(`windows command not transport-safe: ${s.step}`);
      steps = inner.map((s) => ({ step: s.step, remote: `${WINBASH} -lc '${s.cmd}'` }));
      break;
    }
    default: throw new Error(`unknown flavour ${target.flavour}`);
  }
  for (const s of steps) if (s.remote.includes('\n')) throw new Error(`multi-line command: ${s.step}`);
  return steps.map(Object.freeze);
}

export function sshArgs(target, remote) { return [...SSH_FLAGS, ...target.ssh, remote]; }

// ---- transport: the only place a process is spawned ----
// run(target, remote, limits) — limits = { ms, bytes } are the EFFECTIVE limits for this
// command: min(per-command, remaining run budget). The transport reports which one bit.
export function realTransport({ spawnFn = spawn, budget = BUDGET, sleep = (ms) => new Promise((r) => setTimeout(r, ms)) } = {}) {
  const live = new Set();                                   // groups we own and have not confirmed gone
  const signalGroup = (pid, sig) => { try { process.kill(-pid, sig); return true; } catch (e) { return e?.code === 'ESRCH' ? false : true; } };
  const groupAlive = (pid) => { try { process.kill(-pid, 0); return true; } catch (e) { return e?.code !== 'ESRCH'; } };
  const killAll = () => { for (const pid of live) signalGroup(pid, 'SIGKILL'); };
  process.once('exit', killAll);
  process.once('SIGINT', () => { killAll(); process.exit(130); });

  // F1: unconditional escalation. TERM at deadline, KILL after grace no matter what the
  // leader did, then confirm the group is gone (ESRCH) within confirmMs. The pgid stays in
  // `live` until confirmed, so process-exit cleanup re-signals it.
  async function escalate(pid) {
    signalGroup(pid, 'SIGTERM');
    await sleep(budget.killGraceMs);
    signalGroup(pid, 'SIGKILL');
    const until = mono() + budget.confirmMs;
    while (mono() < until) { if (!groupAlive(pid)) { live.delete(pid); return 'confirmed'; } await sleep(20); }
    if (!groupAlive(pid)) { live.delete(pid); return 'confirmed'; }
    return 'unconfirmed';                                   // stays in `live`; reported, not hidden
  }

  return {
    run(target, remote, limits = {}) {
      const ms = Math.min(budget.perCommandMs, limits.ms ?? Infinity);
      const bytes = Math.min(budget.perCommandBytes, limits.bytes ?? Infinity);
      const clippedByRun = { ms: ms < budget.perCommandMs, bytes: bytes < budget.perCommandBytes };
      return new Promise((resolve) => {
        const start = mono(), startWall = Date.now();
        // detached => the child leads its own process group; a timeout signals that group only.
        const child = spawnFn('ssh', sshArgs(target, remote), { stdio: ['ignore', 'pipe', 'pipe'], detached: true });
        const pid = child.pid;
        if (pid) live.add(pid);
        let out = Buffer.alloc(0), err = Buffer.alloc(0), overflow = false, timedOut = false, settled = false, cleanup = 'n/a';
        const total = () => out.length + err.length;
        const ingest = (which, b) => {                       // F2: cap at ingestion, stdout+stderr together
          const cap = which === 'err' ? Math.min(budget.stderrBytes, bytes - out.length) : bytes - err.length;
          const cur = which === 'err' ? err : out;
          const room = Math.max(0, cap - cur.length);
          if (b.length > room) { overflow = true; b = b.subarray(0, room); }
          if (which === 'err') err = Buffer.concat([err, b]); else out = Buffer.concat([out, b]);
          if (total() >= bytes) overflow = true;
        };
        const finish = (code, signal) => {
          if (settled) return; settled = true;
          clearTimeout(t1);
          if (pid && !timedOut) live.delete(pid);           // clean exit: group gone with the leader (no descendants under ssh)
          const cls = timedOut ? (clippedByRun.ms ? 'clipped' : 'timeout')
            : overflow ? (clippedByRun.bytes ? 'clipped' : 'truncated')
            : code === 0 ? 'complete' : 'error';
          resolve({ stdout: out.toString('utf8'), stderr: err.toString('utf8'), code, signal, cls, cleanup, start: startWall, end: Date.now(), elapsedMs: mono() - start, bytes: total() });
        };
        child.stdout.on('data', (b) => ingest('out', b));
        child.stderr.on('data', (b) => ingest('err', b));
        child.on('error', (e) => { ingest('err', Buffer.from(String(e?.message ?? e))); finish(-1, null); });
        child.on('close', (code, signal) => { if (!timedOut) finish(code, signal); });
        const t1 = setTimeout(async () => {
          timedOut = true;
          cleanup = pid ? await escalate(pid) : 'no-pid';    // resolves only after escalation completed
          finish(child.exitCode, child.signalCode);
        }, ms);
      });
    },
    _live: live,
  };
}

// ---- derivations: pure functions over saved stdout ----
export function deriveClock(stdout, t0, t1) {
  const m = stdout.match(/^(\d{10})$/m);
  if (!m) return { cls: 'unknown' };
  const S = Number(m[1]);
  return { remoteSec: S, offsetLo: S - t1 / 1000, offsetHi: S + 1 - t0 / 1000, note: 'seconds, relative to M4 wall clock; M4 clock uncertainty not measured' };
}
export function deriveCount(stdout) { const m = stdout.trim().match(/^(\d+)$/); return m ? Number(m[1]) : null; }
export function deriveProcs(stdout) {
  return stdout.split('\n').map((l) => l.trim()).filter(Boolean).map((l) => {
    const m = l.match(/^(\d+)\s+(.+?)\s+(\S+)\s+(\d+)\s+([\d.]+)$/);
    return m ? { pid: Number(m[1]), lstart: m[2], etime: m[3], rssKiB: Number(m[4]), cpuPct: Number(m[5]) } : { unparsed: l.slice(0, 200) };
  });
}
// F4: blank-line-delimited records, any property order, required fields checked.
export const UNIT_REQUIRED = Object.freeze(['Id', 'MainPID', 'ActiveState']);
export function deriveUnitProps(stdout) {
  return stdout.split(/\n\s*\n/).map((block) => {
    const rec = {};
    for (const l of block.split('\n')) { const m = l.match(/^([A-Za-z]+)=(.*)$/); if (m) rec[m[1]] = m[2]; }
    return rec;
  }).filter((rec) => Object.keys(rec).length > 0).map((rec) => {
    const missing = UNIT_REQUIRED.filter((k) => !(k in rec));
    return missing.length ? { ...rec, incomplete: missing } : rec;
  });
}
export function deriveCommit(stdout) { const m = stdout.trim().match(/^[0-9a-f]{40}$/); return m ? m[0] : null; }

// ---- the run loop: P1, P2, P5 ----
export async function collect({ targets = TARGETS, transport, budget = BUDGET, clock = mono, wall = Date.now, log = () => {} }) {
  const runStart = clock(), runStartWall = wall();
  let totalBytes = 0;
  const rows = [], derived = {};
  for (const target of targets) {
    derived[target.id] = { flavour: target.flavour };
    for (const { step, remote } of plan(target)) {           // commands fixed before any output exists (P1)
      const row = { target: target.id, step, command: remote, cls: 'skipped', start: null, end: null, code: null, bytes: 0, sha256: null, cleanup: 'n/a', stdout: '' };
      const remainingMs = budget.wallMs - (clock() - runStart);
      const remainingBytes = budget.totalBytes - totalBytes;
      if (remainingMs <= 0) { row.reason = 'wall budget'; rows.push(row); continue; }
      if (remainingBytes <= 0) { row.reason = 'output budget'; rows.push(row); continue; }
      const t0 = wall();
      const r = await transport.run(target, remote, { ms: remainingMs, bytes: remainingBytes });   // F3: enforced in flight
      const t1 = wall();
      Object.assign(row, { cls: CLASSES.includes(r.cls) ? r.cls : 'error', start: t0, end: t1, code: r.code, bytes: r.bytes ?? Buffer.byteLength(r.stdout) + Buffer.byteLength(r.stderr ?? ''), sha256: createHash('sha256').update(r.stdout).digest('hex'), cleanup: r.cleanup ?? 'n/a', stdout: r.stdout, stderr: r.stderr });
      totalBytes += row.bytes;
      rows.push(row);
      log(`${target.id} ${step} ${row.cls} ${row.bytes}B ${t1 - t0}ms cleanup=${row.cleanup}`);
      if (r.cls !== 'complete') continue;                    // P5: only complete rows are observations
      const d = derived[target.id];
      if (step === 'clock') d.clock = deriveClock(r.stdout, t0, t1);
      else if (step === 'census' || step === 'census-proxy') d[step] = deriveCount(r.stdout);
      else if (step === 'procs') d.procs = deriveProcs(r.stdout);
      else if (step === 'unitprops') d.units = deriveUnitProps(r.stdout);
      else if (step === 'commit') d.commit = deriveCommit(r.stdout);
      else if (step === 'df' || step === 'units') d[step] = r.stdout.trim().slice(0, 4096);
    }
  }
  return { runStart: runStartWall, runEnd: wall(), elapsedMs: clock() - runStart, rows, derived };
}

export function writeResults(dir, result) {
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const manifest = result.rows.map(({ stdout, stderr, ...m }) => m);
  writeFileSync(join(dir, 'manifest.json'), JSON.stringify({ runStart: result.runStart, runEnd: result.runEnd, elapsedMs: result.elapsedMs, budget: BUDGET, rows: manifest }, null, 2), { mode: 0o600 });
  for (const r of result.rows) if (r.cls !== 'skipped') writeFileSync(join(dir, `${r.target}-${r.step}.txt`), r.stdout, { mode: 0o600 });
  writeFileSync(join(dir, 'derived.json'), JSON.stringify(result.derived, null, 2), { mode: 0o600 });
}

async function main() {
  const argv = process.argv.slice(2);
  const live = argv.includes('--live');
  const outIdx = argv.indexOf('--out');
  const out = outIdx >= 0 ? argv[outIdx + 1] : join(HOME, 'Documents/claude/axona-relay/harness/results/pilot-p1-preflight', new Date().toISOString().replace(/[:.]/g, '-'));
  if (!live) {
    for (const t of TARGETS) for (const s of plan(t)) console.log(`${t.id.padEnd(5)} ${s.step.padEnd(13)} ssh ${sshArgs(t, s.remote).map((a) => (/\s/.test(a) ? JSON.stringify(a) : a)).join(' ')}`);
    console.log(`\ndry run: ${TARGETS.length} targets, ${TARGETS.reduce((n, t) => n + plan(t).length, 0)} commands, nothing spawned. budget=${JSON.stringify(BUDGET)}`);
    return;
  }
  if (process.env.PILOT_P1_AUTHORIZED !== 'yes') { console.error('refusing --live: PILOT_P1_AUTHORIZED=yes not set. This flag is an interlock against accidental dispatch; it does not stand in for David\'s authorization, which is recorded on council.'); process.exit(2); }
  const result = await collect({ transport: realTransport(), log: (l) => console.error(l) });
  writeResults(out, result);
  console.log(`wrote ${out} (${result.rows.length} rows, ${result.rows.filter((r) => r.cls === 'complete').length} complete)`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main().catch((e) => { console.error(e); process.exit(1); });
