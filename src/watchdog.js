// watchdog.js — a second thread that turns a FROZEN relay into a DEAD one.
//
// WHY. Three Windows relays froze (2026-10-09 ×2, 2026-10-12 ×1): the event
// loop stopped inside a native call, CPU time stopped climbing, the process
// kept its sockets and its service state read Running — for 5 h and 16 h on
// 2026-10-09, and 19 min on 2026-10-12 until a roll's gate tripped over it.
// Nothing in the process could notice, because the only thread that logs,
// ticks and pings was the one that had stopped. The 4.108.0 deferred close
// did not remove the freeze (the third one happened with it in place), and
// the mechanism is still unidentified (node-datachannel 0.33.1 /
// libdatachannel 0.24.3 on win32). When the cause is unknown, BOUND the
// failure: a frozen relay must become a dead relay within WATCHDOG stallMs,
// so the service manager (relaysvc.exe → SCM failure actions; launchd;
// systemd) restarts it, and the evidence of the freeze is kept.
//
// HOW. A worker thread runs its own event loop. The main thread posts a beat
// every beatMs; the worker counts the beats it did NOT receive. After
// ceil(stallMs / beatMs) consecutive misses it writes `event-loop-stalled`
// straight to fd 2 (NOT process.stderr — a worker's process.stderr is a pipe
// through the main thread and would never reach the log while that thread is
// frozen), on win32 writes a minidump of the process beside the logs (thread
// stacks, via the comsvcs.dll MiniDump export that ships with Windows), and
// kills the process. Counting beats rather than reading a wall clock keeps a
// system sleep from reading as a stall: both threads pause together.
//
// Verified 2026-10-12 on the Mac (fence_watchdog.mjs): a main thread blocked
// in Atomics.wait is killed by its worker within the budget; a live loop is
// never killed. The minidump command was run by hand on axona-win against a
// live relay the same day: 1.0 s, a dump file, the relay unaffected.
//
//   RELAY_WATCHDOG_MS   stall budget in ms (default 30000); 0 = off
//
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { spawnSync } from 'node:child_process';
import { writeSync, mkdirSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

export const WATCHDOG_DEFAULT_MS = 30_000;
export const WATCHDOG_BEAT_MS    = 1_000;

/** The stall budget from the environment: default, 0 = off, anything else a non-negative integer. */
export function watchdogFromEnv(env = process.env) {
  const raw = env.RELAY_WATCHDOG_MS;
  if (raw == null || raw === '') return WATCHDOG_DEFAULT_MS;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) throw new Error(`RELAY_WATCHDOG_MS must be a non-negative integer (ms), got "${raw}"`);
  return n;
}

/** Where a dump goes: relay-logs/ beside the checkout (anchored on the code, not the cwd). */
export function defaultDumpDir() {
  return join(dirname(fileURLToPath(import.meta.url)), '..', 'relay-logs');
}

const ts = () => new Date().toISOString().slice(0, 19).replace('T', ' ');

/**
 * Arm the watchdog. Returns null when stallMs is 0 (off); otherwise
 * { stallMs, beatMs, need, stop() }. The worker and the beat timer are
 * unref'd: they never keep a relay alive that is otherwise done.
 */
export function startWatchdog({ stallMs = WATCHDOG_DEFAULT_MS, beatMs = WATCHDOG_BEAT_MS, dumpDir = defaultDumpDir(), kill = true } = {}) {
  if (!(stallMs > 0)) return null;
  const need = Math.max(2, Math.ceil(stallMs / beatMs));
  const worker = new Worker(fileURLToPath(import.meta.url), {
    workerData: { stallMs, beatMs, need, dumpDir, kill, pid: process.pid, platform: process.platform },
  });
  worker.unref();
  const beat = setInterval(() => { try { worker.postMessage(1); } catch { /* the worker is gone; nothing to feed */ } }, beatMs);
  beat.unref();
  return {
    stallMs, beatMs, need,
    stop() { clearInterval(beat); return worker.terminate(); },
  };
}

// ── the worker ──────────────────────────────────────────────────────────────
if (!isMainThread && workerData && workerData.stallMs) {
  const { beatMs, need, dumpDir, kill, pid, platform, stallMs } = workerData;
  let missed = 0;
  const say = (event, ctx) => { try { writeSync(2, `[${ts()}] ${event} ${JSON.stringify(ctx)}\n`); } catch { /* fd 2 is gone; the kill still happens */ } };
  parentPort.on('message', () => {
    // A late beat after a long gap is evidence too: it says how close the loop came.
    if (missed >= Math.ceil(need / 2)) say('event-loop-stall-recovered', { pid, missedBeats: missed, beatMs, budgetBeats: need });
    missed = 0;
  });
  const dump = () => {
    if (platform !== 'win32' || !dumpDir) return null;
    try {
      mkdirSync(dumpDir, { recursive: true });
      const file = join(dumpDir, `stall-${pid}-${new Date().toISOString().replace(/[-:]/g, '').slice(0, 15)}Z.dmp`);
      // rundll32 comsvcs.dll, MiniDump <pid> <file> full — ships with Windows, no tool to install.
      const r = spawnSync('rundll32.exe', ['C:\\Windows\\System32\\comsvcs.dll,', 'MiniDump', String(pid), file, 'full'],
        { timeout: 60_000, windowsHide: true, stdio: 'ignore' });
      return existsSync(file) ? { file, status: r.status } : { file: null, status: r.status, error: r.error?.message ?? null };
    } catch (e) { return { file: null, error: e?.message ?? String(e) }; }
  };
  const t = setInterval(() => {
    missed++;
    if (missed < need) return;
    clearInterval(t);
    const d = dump();
    say('event-loop-stalled', { pid, stallMs, beatMs, missedBeats: missed, dump: d, action: kill ? 'kill' : 'report-only' });
    if (kill) {
      try { process.kill(pid, 'SIGKILL'); } catch (e) { say('event-loop-stalled-kill-failed', { pid, err: e?.message ?? String(e) }); }
    }
  }, beatMs);
}
