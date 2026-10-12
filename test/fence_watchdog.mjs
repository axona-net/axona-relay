// =====================================================================
// fence_watchdog.mjs — a frozen relay becomes a dead relay (0.155.0).
//
// WHY. Three Windows relays froze with their event loop stopped inside a
// native call (2026-10-09 ×2, 2026-10-12 ×1); each read Running for hours,
// because the only thread that could have noticed was the frozen one. The
// watchdog is a worker thread that counts the main thread's missed beats and
// kills the process once the budget is spent, writing the evidence to fd 2
// first (a worker's process.stderr would route through the frozen thread).
//
// Real child processes, real worker threads, a main thread REALLY blocked in
// Atomics.wait (the closest a test can come to a native call that never
// returns). Budget 2 s / beat 200 ms (need = 10 beats):
//   1  a blocked main thread is killed within the budget plus two beats; the
//      kill is SIGKILL; `event-loop-stalled` is on stderr with pid, missed
//      beats and (off win32) dump:null; the main thread's "unblocked" line
//      never appears
//   2  a live main thread running past the budget is NOT killed and exits 0
//      by itself; the unref'd worker holds nothing open
//   3  RELAY_WATCHDOG_MS=0 arms nothing: startWatchdog returns null and a
//      blocked main thread survives to print "unblocked"
//   4  a stall shorter than the budget (1.2 s of 2 s) is not killed and leaves
//      `event-loop-stall-recovered` with the missed count
//   5  watchdogFromEnv: default 30000; "0" → 0; "abc" / "-1" / "1.5" throw
//
// Mutant (run by hand 2026-10-12): delete the process.kill line → 1 fails
// (the child prints "unblocked" and exits 0 after 15 s instead of dying).
//
// Run: node test/fence_watchdog.mjs
// =====================================================================
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { startWatchdog, watchdogFromEnv, WATCHDOG_DEFAULT_MS } from '../src/watchdog.js';

const SELF = fileURLToPath(import.meta.url);
const STALL_MS = 2000, BEAT_MS = 200;

// ── child modes ───────────────────────────────────────────────────────────────
if (process.argv[2] === '--child') {
  const mode = process.argv[3];
  const block = (ms) => { const a = new Int32Array(new SharedArrayBuffer(4)); Atomics.wait(a, 0, 0, ms); };
  if (mode === 'stall') {
    startWatchdog({ stallMs: STALL_MS, beatMs: BEAT_MS, dumpDir: null });
    block(15_000);                        // a native call that never returns, as far as the loop can tell
    console.log('unblocked');             // must never print
    process.exit(0);
  } else if (mode === 'alive') {
    startWatchdog({ stallMs: STALL_MS, beatMs: BEAT_MS, dumpDir: null });
    setTimeout(() => { console.log('alive-done'); process.exit(0); }, STALL_MS + 1500);
  } else if (mode === 'off') {
    const wd = startWatchdog({ stallMs: 0, beatMs: BEAT_MS, dumpDir: null });
    console.log(`armed=${wd === null ? 'null' : 'object'}`);
    block(STALL_MS + 1000);
    console.log('unblocked');
    process.exit(0);
  } else if (mode === 'brief') {
    startWatchdog({ stallMs: STALL_MS, beatMs: BEAT_MS, dumpDir: null });
    setTimeout(() => { block(1200); setTimeout(() => { console.log('brief-done'); process.exit(0); }, 600); }, 400);
  }
} else {
  // ── the fence ───────────────────────────────────────────────────────────────
  let passed = 0, failed = 0;
  const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

  function run(mode, killAfterMs) {
    return new Promise((resolve) => {
      const t0 = Date.now();
      const ch = spawn(process.execPath, [SELF, '--child', mode], { stdio: ['ignore', 'pipe', 'pipe'] });
      let out = '', err = '', timedOut = false;
      ch.stdout.on('data', (d) => { out += d; });
      ch.stderr.on('data', (d) => { err += d; });
      const guard = setTimeout(() => { timedOut = true; try { ch.kill('SIGKILL'); } catch { /* */ } }, killAfterMs);
      ch.on('exit', (code, signal) => { clearTimeout(guard); resolve({ code, signal, out, err, ms: Date.now() - t0, timedOut }); });
    });
  }
  const parse = (err, event) => {
    const line = err.split('\n').find((l) => l.includes(` ${event} `));
    if (!line) return null;
    try { return JSON.parse(line.slice(line.indexOf('{'))); } catch { return null; }
  };

  (async () => {
    console.log('fence_watchdog: a frozen relay becomes a dead relay within the budget\n');

    const s = await run('stall', 12_000);
    const ev = parse(s.err, 'event-loop-stalled');
    check('1 a blocked main thread is killed within budget + two beats',
      !s.timedOut && s.signal === 'SIGKILL' && s.ms < STALL_MS + 2 * BEAT_MS + 1500, `signal=${s.signal} code=${s.code} ms=${s.ms} timedOut=${s.timedOut}`);
    check('1 event-loop-stalled is on fd 2 with pid, the budget and the missed beats',
      !!ev && ev.pid > 0 && ev.stallMs === STALL_MS && ev.missedBeats >= 10 && ev.action === 'kill', JSON.stringify(ev) || s.err.slice(-300));
    check('1 off win32 the dump is null, never a path', !!ev && (process.platform === 'win32' ? ev.dump !== null : ev.dump === null), JSON.stringify(ev?.dump));
    check('1 the main thread never resumed', !s.out.includes('unblocked'), JSON.stringify(s.out));

    const a = await run('alive', 12_000);
    check('2 a live main thread past the budget is not killed and exits 0 by itself',
      !a.timedOut && a.code === 0 && a.signal === null && a.out.includes('alive-done') && !a.err.includes('event-loop-stalled'), `code=${a.code} signal=${a.signal} ms=${a.ms} err=${a.err.slice(-200)}`);

    const o = await run('off', 12_000);
    check('3 stallMs 0 arms nothing and a blocked main thread survives',
      !o.timedOut && o.code === 0 && o.out.includes('armed=null') && o.out.includes('unblocked') && !o.err.includes('event-loop-stalled'), `code=${o.code} out=${JSON.stringify(o.out)}`);

    const b = await run('brief', 12_000);
    const rec = parse(b.err, 'event-loop-stall-recovered');
    check('4 a stall shorter than the budget is not killed',
      !b.timedOut && b.code === 0 && b.out.includes('brief-done') && !b.err.includes('event-loop-stalled'), `code=${b.code} signal=${b.signal} err=${b.err.slice(-200)}`);
    check('4 ...and leaves event-loop-stall-recovered with the missed count',
      !!rec && rec.missedBeats >= 5 && rec.missedBeats < 10 && rec.budgetBeats === 10, JSON.stringify(rec) || b.err.slice(-200));

    check('5 watchdogFromEnv default', watchdogFromEnv({}) === WATCHDOG_DEFAULT_MS && WATCHDOG_DEFAULT_MS === 30_000);
    check('5 watchdogFromEnv "0" is off', watchdogFromEnv({ RELAY_WATCHDOG_MS: '0' }) === 0);
    check('5 watchdogFromEnv "45000"', watchdogFromEnv({ RELAY_WATCHDOG_MS: '45000' }) === 45_000);
    const throws = (v) => { try { watchdogFromEnv({ RELAY_WATCHDOG_MS: v }); return false; } catch { return true; } };
    check('5 watchdogFromEnv refuses abc / -1 / 1.5', throws('abc') && throws('-1') && throws('1.5'));

    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed ? 1 : 0);
  })();
}
