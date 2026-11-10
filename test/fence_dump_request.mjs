#!/usr/bin/env node
// =====================================================================
// fence_dump_request.mjs — the request-file trigger for the SIGUSR1 dumps
// (0.198.0). Windows has no SIGUSR1, so the 20 services on axona-win could
// never be asked for a health dump. watchDumpRequest polls ONE file's mtime;
// each relay dumps once every time that mtime advances. Nothing is deleted,
// so one rewrite reaches every relay that shares the directory.
// Real files, real timers, one real child process:
//   1  no file at arm: nothing fires; the first write fires once
//   2  a file already there at arm is an OLD request: it does not fire
//   3  each later rewrite fires once more, and never twice for one rewrite
//   4  two watchers on one file (two relays, one checkout) both fire
//   5  the file is left in place (the watcher deletes nothing)
//   6  a throwing callback does not end the watch
//   7  stop() ends it
//   8  bad arguments arm nothing (null); an unusable directory never throws
//   9  the timer is unref'd: a child with only the watcher armed exits
// Run: node test/fence_dump_request.mjs
// =====================================================================
import { mkdtempSync, rmSync, writeFileSync, existsSync, utimesSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { watchDumpRequest, DUMP_REQUEST_FILE, DUMP_REQUEST_POLL_MS } from '../src/dumprequest.js';

let pass = 0, fail = 0;
const ok = (m, c, x = '') => { if (c) { pass++; console.log(`  ✓ ${m}`); } else { fail++; console.log(`  ✗ ${m} ${x}`); } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const root = mkdtempSync(join(tmpdir(), 'dumpreq-'));
const here = dirname(fileURLToPath(import.meta.url));
// A rewrite always advances the mtime here: stamp it explicitly, one second on
// each time, so the check never depends on the filesystem's timestamp grain.
let stamp = Math.floor(Date.now() / 1000) + 10;
const request = (dir) => { const f = join(dir, DUMP_REQUEST_FILE); writeFileSync(f, new Date().toISOString()); stamp += 1; utimesSync(f, stamp, stamp); };

try {
  ok('0 the poll period is 2 s and the file is named request', DUMP_REQUEST_POLL_MS === 2000 && DUMP_REQUEST_FILE === 'request');

  { const dir = join(root, 'a'); const calls = [];
    const w = watchDumpRequest(dir, (why) => calls.push(why), { intervalMs: 40 });
    ok('1 arming creates the directory and names the file', w && existsSync(dir) && w.path === join(dir, DUMP_REQUEST_FILE));
    await sleep(150);
    ok('1b no file: nothing fires', calls.length === 0, JSON.stringify(calls));
    request(dir); await sleep(200);
    ok('1c the first write fires once, with request-file', calls.length === 1 && calls[0] === 'request-file', JSON.stringify(calls));
    await sleep(200);
    ok('3 one rewrite never fires twice', calls.length === 1, JSON.stringify(calls));
    request(dir); await sleep(200); request(dir); await sleep(200);
    ok('3b each later rewrite fires once more', calls.length === 3, JSON.stringify(calls));
    ok('5 the request file is left in place', existsSync(w.path));
    w.stop(); request(dir); await sleep(200);
    ok('7 stop() ends the watch', calls.length === 3, JSON.stringify(calls)); }

  { const dir = join(root, 'a'); const calls = [];   // the file from the block above is still there
    const w = watchDumpRequest(dir, () => calls.push(1), { intervalMs: 40 });
    await sleep(200);
    ok('2 a file already there at arm does not fire', calls.length === 0, JSON.stringify(calls));
    request(dir); await sleep(200);
    ok('2b the next rewrite does', calls.length === 1, JSON.stringify(calls)); w.stop(); }

  { const dir = join(root, 'b'); const a = [], b = [];
    const w1 = watchDumpRequest(dir, () => a.push(1), { intervalMs: 40 });
    const w2 = watchDumpRequest(dir, () => b.push(1), { intervalMs: 55 });
    await sleep(100); request(dir); await sleep(250);
    ok('4 two watchers on one file both fire once for one rewrite', a.length === 1 && b.length === 1, JSON.stringify([a, b]));
    w1.stop(); w2.stop(); }

  { const dir = join(root, 'c'); let n = 0;
    const w = watchDumpRequest(dir, () => { n++; throw new Error('boom'); }, { intervalMs: 40 });
    request(dir); await sleep(200); request(dir); await sleep(200);
    ok('6 a throwing callback does not end the watch', n === 2, String(n)); w.stop(); }

  { ok('8 no directory, an empty one, a non-string, a missing callback: null',
      [watchDumpRequest(null, () => {}), watchDumpRequest('', () => {}), watchDumpRequest(42, () => {}), watchDumpRequest(join(root, 'd'))].every((v) => v === null));
    const blocker = join(root, 'file-not-dir'); writeFileSync(blocker, 'x');
    let w = null, threw = null, n = 0;
    try { w = watchDumpRequest(join(blocker, 'under-a-file'), () => n++, { intervalMs: 40 }); await sleep(150); } catch (e) { threw = e; }
    ok('8b a directory that cannot exist: nothing thrown, nothing fires', !threw && n === 0, String(threw));
    w?.stop(); }

  { const dir = join(root, 'e');
    const src = `import { watchDumpRequest } from ${JSON.stringify(pathToFileURL(join(here, '..', 'src', 'dumprequest.js')).href)}; watchDumpRequest(${JSON.stringify(dir)}, () => {});`;
    const child = spawn(process.execPath, ['--input-type=module', '-e', src], { stdio: 'ignore' });
    const code = await Promise.race([new Promise((r) => child.on('exit', r)), sleep(3000).then(() => 'timeout')]);
    if (code === 'timeout') child.kill('SIGKILL');
    ok('9 a child with only the watcher armed exits by itself (code 0)', code === 0, String(code)); }
} finally {
  rmSync(root, { recursive: true, force: true });
}

console.log(`\nfence_dump_request: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
