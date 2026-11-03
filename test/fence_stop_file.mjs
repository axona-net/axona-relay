#!/usr/bin/env node
// =====================================================================
// fence_stop_file.mjs — the Windows wrapper's graceful-stop request (0.193.0,
// Closer-Born v0.6 Part 6a; David 2026-11-03 "go ahead with 6a").
//
// relaysvc.cs used to terminate the job on every service stop, so a Windows
// relay never ran shutdown() and handed nothing off. Now it writes
// RELAY_STOP_FILE and waits; the relay's watchStopFile calls shutdown() once.
// Real files, real child processes:
//   1  a file that appears fires onStop once, with 'stop-file', and is removed
//   2  a stale file present at start is removed and does NOT fire
//   3  no path: nothing is armed (null), nothing fires
//   4  a child process with only the watcher armed exits by itself (unref'd),
//      and a child told to stop through the file runs its shutdown and exits 0
//      within 2 s
// =====================================================================
import { mkdtempSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { watchStopFile } from '../src/stopfile.js';

let pass = 0, fail = 0;
const ok = (m, c, x = '') => { if (c) { pass++; console.log(`  ✓ ${m}`); } else { fail++; console.log(`  ✗ ${m} ${x}`); } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dir = mkdtempSync(join(tmpdir(), 'stopfile-'));
const here = dirname(fileURLToPath(import.meta.url));

try {
  { const f = join(dir, 'a.stop'); const calls = [];
    const w = watchStopFile(f, (why) => calls.push(why), { intervalMs: 50 });
    await sleep(120); writeFileSync(f, 'stop'); await sleep(250);
    ok('1 a file that appears fires onStop once with stop-file and is removed', calls.length === 1 && calls[0] === 'stop-file' && !existsSync(f), JSON.stringify(calls));
    writeFileSync(f, 'again'); await sleep(200);
    ok('1b it fires only once', calls.length === 1, JSON.stringify(calls)); w.stop(); }
  { const f = join(dir, 'b.stop'); writeFileSync(f, 'stale'); const calls = [];
    const w = watchStopFile(f, (why) => calls.push(why), { intervalMs: 50 });
    await sleep(250);
    ok('2 a stale file at start is removed and does not fire', calls.length === 0 && !existsSync(f), JSON.stringify(calls)); w.stop(); }
  { const calls = [];
    ok('3 no path: nothing armed', watchStopFile(undefined, () => calls.push(1)) === null && watchStopFile('', () => calls.push(1)) === null && calls.length === 0); }
  { const mod = join(here, '..', 'src', 'stopfile.js').replace(/\\/g, '/');
    const idle = spawn(process.execPath, ['--input-type=module', '-e', `import { watchStopFile } from '${mod}'; watchStopFile('${join(dir, 'idle.stop')}', () => {});`]);
    const idleExit = await Promise.race([new Promise((r) => idle.on('exit', (c) => r(c))), sleep(3000).then(() => 'hung')]);
    const f = join(dir, 'c.stop');
    const child = spawn(process.execPath, ['--input-type=module', '-e', `import { watchStopFile } from '${mod}'; watchStopFile('${f}', (why) => { console.log('shutdown ' + why); process.exit(0); }); setInterval(() => {}, 1000);`], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = ''; child.stdout.on('data', (d) => { out += d; });
    await sleep(300); const t0 = Date.now(); writeFileSync(f, 'stop');
    const code = await Promise.race([new Promise((r) => child.on('exit', (c) => r(c))), sleep(4000).then(() => 'hung')]);
    if (code === 'hung') child.kill('SIGKILL');
    ok('4 the watcher alone holds no process open; a stop request runs shutdown and exits 0 within 2 s',
      idleExit === 0 && code === 0 && /shutdown stop-file/.test(out) && Date.now() - t0 < 2000, JSON.stringify({ idleExit, code, out: out.trim(), ms: Date.now() - t0 })); }
} finally { rmSync(dir, { recursive: true, force: true }); }
console.log(`\nfence_stop_file: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
