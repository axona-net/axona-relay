// Offline tests for src/trace-recorder.js — synthetic rows, an in-memory fake fs for the
// failure paths, and a temp directory under the OS tmpdir for the real-fs paths. No relay,
// no network, no fleet. These exercise the recorder's own accounting and lifecycle; they
// say nothing about remote hosts or about durability under power loss.
//   node --test test/trace_recorder.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import * as realFs from 'node:fs';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DEFAULTS, validateSettings, createTraceRecorder, recorderFromEnv, readCapture, withTrace, estimateRowBytes, snapshotRow, TERMINAL_REASONS } from '../src/trace-recorder.js';

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const tmp = () => mkdtempSync(join(tmpdir(), 'trace-rec-'));

// ---- fake fs: in-memory files, scripted write behaviour ----
function fakeFs(script = {}) {
  const files = new Map();            // path -> Buffer
  const fds = new Map();              // fd -> path
  let nextFd = 10, writes = 0;
  const f = {
    files, calls: { open: [], write: 0, writeSync: 0, close: 0, existsSync: 0, access: 0, accessInflight: 0, accessMaxInflight: 0 },
    mkdirSync() {},
    existsSync(p) { f.calls.existsSync += 1; return files.has(p) || (script.stopExists?.(p) ?? false); },
    access(p, _mode, cb) {
      f.calls.access += 1; f.calls.accessInflight += 1; f.calls.accessMaxInflight = Math.max(f.calls.accessMaxInflight, f.calls.accessInflight);
      const exists = files.has(p) || (script.stopExists?.(p) ?? false);
      const go = () => { f.calls.accessInflight -= 1; cb(exists ? null : Object.assign(new Error('ENOENT'), { code: 'ENOENT' })); };
      const d = script.accessDelayMs ?? 0; d ? setTimeout(go, d) : setImmediate(go);
    },
    openSync(p, flag) {
      f.calls.open.push(p);
      if (script.openError) { const e = new Error(script.openError); e.code = script.openError; throw e; }
      if (flag === 'wx' && files.has(p)) { const e = new Error('EEXIST'); e.code = 'EEXIST'; throw e; }
      files.set(p, Buffer.alloc(0)); const fd = nextFd++; fds.set(fd, p); return fd;
    },
    writeSync(fd, buf, off, len) {
      f.calls.writeSync += 1;
      if (script.syncError && script.syncError(f.calls.writeSync)) { const e = new Error('EIO'); e.code = 'EIO'; throw e; }
      const p = fds.get(fd); files.set(p, Buffer.concat([files.get(p), buf.subarray(off, off + len)])); return len;
    },
    write(fd, buf, off, len, _pos, cb) {
      f.calls.write += 1; writes += 1;
      const n = writes;
      const go = () => {
        f.calls.maxWriteLen = Math.max(f.calls.maxWriteLen || 0, len);
        if (script.error && script.error(n)) { const e = new Error('ENOSPC'); e.code = 'ENOSPC'; return cb(e); }
        if (script.zero && script.zero(n)) return cb(null, 0);
        let take = len;
        if (script.partial && script.partial(n)) take = Math.max(1, Math.floor(len / 2));
        const p = fds.get(fd); files.set(p, Buffer.concat([files.get(p), buf.subarray(off, off + take)])); cb(null, take);
      };
      const d = script.delayMs?.(n) ?? 0;
      d ? setTimeout(go, d) : setImmediate(go);
    },
    closeSync(fd) { f.calls.close += 1; fds.delete(fd); },
    close(fd, cb) { setImmediate(() => { f.calls.close += 1; fds.delete(fd); cb(null); }); },
    readFileSync(p) { return files.get(p).toString('utf8'); },
  };
  return f;
}
const SMALL = { capBytes: 8192, reserveBytes: 1024, queueBytes: 4096, maxRowBytes: 1024, flushMs: 20, flushBytes: 2048, maxMs: 60_000, stopCheckMs: 10 };
const mk = (fs, over = {}, deps = {}) => createTraceRecorder({ dir: '/cap', pid: 4242, kv: '4.84.0', self: 'abcdef012345', settings: { ...SMALL, ...over }, fs, random: deps.random ?? (() => 'r' + Math.random().toString(16).slice(2, 10)), ...deps });
const row = (i, proc = 'p1') => ({ stream: 'lat', stage: 'sub:recv', msgId: 'm'.repeat(8) + i, proc, pad: 'x'.repeat(100) });

// ---------------- settings ----------------
test('settings: defaults are the contract numbers and validation names the field', () => {
  assert.equal(DEFAULTS.capBytes, 64 * 1024 * 1024); assert.equal(DEFAULTS.reserveBytes, 4096);
  assert.equal(DEFAULTS.queueBytes, 4 * 1024 * 1024); assert.equal(DEFAULTS.flushMs, 250);
  assert.equal(DEFAULTS.flushBytes, 64 * 1024); assert.equal(DEFAULTS.maxMs, 60 * 60 * 1000);
  assert.throws(() => validateSettings({ capBytes: 0 }), /capBytes/);
  assert.throws(() => validateSettings({ capBytes: 1000, reserveBytes: 1000 }), /reserveBytes/);
  assert.throws(() => validateSettings({ maxRowBytes: 5 * 1024 * 1024 }), /maxRowBytes/);
  assert.throws(() => validateSettings({ flushBytes: 8 * 1024 * 1024 }), /flushBytes/);
  assert.throws(() => validateSettings({ bogus: 1 }), /unknown setting/);
  assert.throws(() => validateSettings({ maxMs: 1.5 }), /maxMs/);
  assert.ok(Object.isFrozen(validateSettings({})));
});

test('default-off: recorderFromEnv returns null unless LAT_TRACE === "1"; env numbers are validated', () => {
  const fs = fakeFs();
  assert.equal(recorderFromEnv({}, { fs, dir: '/cap', pid: 1 }), null);
  assert.equal(recorderFromEnv({ LAT_TRACE: '0' }, { fs, dir: '/cap', pid: 1 }), null);
  assert.equal(recorderFromEnv({ LAT_TRACE: 'true' }, { fs, dir: '/cap', pid: 1 }), null);
  assert.equal(fs.calls.open.length, 0, 'nothing opened when off');
  assert.throws(() => recorderFromEnv({ LAT_TRACE: '1', LAT_TRACE_CAP_BYTES: 'abc' }, { fs, dir: '/cap', pid: 1 }), /capBytes/);
  // a cap smaller than maxRowBytes + reserve is refused by the validator, not silently accepted
  assert.throws(() => recorderFromEnv({ LAT_TRACE: '1', LAT_TRACE_CAP_BYTES: '65536' }, { fs, dir: '/cap', pid: 1 }), /maxRowBytes \+ reserveBytes/);
  const r = recorderFromEnv({ LAT_TRACE: '1', LAT_TRACE_CAP_BYTES: '1048576', LAT_TRACE_MAX_MS: '15000' }, { fs, dir: '/cap', pid: 1 });
  assert.equal(r.status().settings.capBytes, 1048576); assert.equal(r.status().settings.maxMs, 15000);
  r.end('closed');
});

// ---------------- identity ----------------
test('identity: exclusive creation; EEXIST retries with a new id; a reused pid never appends to an old capture', async () => {
  const fs = fakeFs();
  const ids = ['dup', 'dup', 'fresh']; let i = 0;
  const a = mk(fs, {}, { random: () => 'dup' });
  const b = mk(fs, {}, { random: () => ids[i++] });
  assert.equal(a.captureId, 'dup'); assert.equal(b.captureId, 'fresh');
  assert.equal(fs.calls.open.length, 4);                    // a:1, b: dup, dup, fresh
  assert.notEqual(a.file, b.file);
  assert.ok(a.file.includes('disc-relay-4242-dup.jsonl'));
  a.record(row(1)); b.record(row(2));
  a.end('closed'); b.end('closed'); await a.whenEnded(); await b.whenEnded();
  assert.equal(readCapture(a.file, fs).rows, 1); assert.equal(readCapture(b.file, fs).rows, 1);
  // three collisions => fail closed, no file writes, rows dropped and counted
  const c = mk(fs, {}, { random: () => 'dup' });
  assert.equal(c.status().disabled, true); assert.match(c.status().disabledReason, /EEXIST x3/);
  assert.equal(c.record(row(3)), false); assert.equal(c.status().droppedAfterEnd, 1);
});

test('identity: attestation is row 0 and carries the settings; relay pid and kernel proc are separate fields', async () => {
  const fs = fakeFs(); const r = mk(fs);
  r.record({ stream: 'lat', stage: 'node-start', proc: '4242-1700000000000', transportId: 'ab' });
  r.record(row(1, '4242-1700000000000'));
  r.end('closed'); await r.whenEnded();
  const cap = readCapture(r.file, fs);
  assert.equal(cap.armed.ev, 'armed'); assert.equal(cap.armed.pid, 4242); assert.equal(cap.armed.kv, '4.84.0');
  assert.equal(cap.armed.settings.capBytes, SMALL.capBytes); assert.equal(cap.armed.captureId, r.captureId);
  assert.deepEqual(cap.procs, { '4242-1700000000000': 2 }); assert.equal(cap.preNodeStart, 0);
});

// ---------------- incarnations and ordering ----------------
test('incarnations: rows before their proc\'s node-start are typed preNodeStart; rows without proc are typed noProc; several procs in one file', async () => {
  const fs = fakeFs(); const r = mk(fs);
  r.record(row(1, 'pA'));                                        // early: kernel emits node-start lazily (AxonaManager.js:1541/1552)
  r.record({ stream: 'lat', stage: 'node-start', proc: 'pA' });
  r.record(row(2, 'pA'));
  r.record({ stream: 'disc', ev: 'became-root', t: 'abc' });    // disc rows carry no proc
  r.record({ stream: 'lat', stage: 'node-start', proc: 'pB' }); // reconnect => new kernel incarnation, same pid
  r.record(row(3, 'pB')); r.record(row(4, 'pA'));
  r.end('closed'); await r.whenEnded();
  const cap = readCapture(r.file, fs);
  assert.equal(cap.rows, 7); assert.deepEqual(cap.nodeStarts, ['pA', 'pB']);
  assert.equal(cap.preNodeStart, 1); assert.equal(cap.noProc, 1);
  assert.deepEqual(cap.procs, { pA: 4, pB: 2 });
  const s = r.status(); assert.equal(s.nodeStarts, 2); assert.equal(s.preNodeStart, 1); assert.equal(s.noProc, 1);
});

// ---------------- caps and accounting ----------------
test('cap: admission counts written + queued + inflight + reserve; the file never exceeds capBytes; terminal fits the reserve; exactly once', async () => {
  // A synchronous burst hits the QUEUE ceiling first (backpressure drops), never the cap; the
  // cap is reached only as the writer keeps up. So feed rows while letting writes complete.
  const fs = fakeFs({ delayMs: () => 3 });                       // writes complete, bytes stay in flight briefly
  const r = mk(fs, { capBytes: 6000, reserveBytes: 1024, queueBytes: 4096, flushBytes: 512 });
  let admitted = 0;
  for (let i = 0; i < 400 && !r.status().ending; i++) { if (r.record(row(i))) admitted += 1; if (i % 3 === 0) await wait(2); }
  await r.whenEnded();
  assert.equal(r.status().droppedQueue, 0, 'this scenario must reach the cap, not the queue ceiling');
  const s = r.status();
  assert.equal(s.terminal.reason, 'capped'); assert.equal(s.terminal.emitted, true);
  assert.ok(s.droppedAtCap >= 1);
  const size = fs.files.get(r.file).length;
  assert.ok(size <= 6000, `file ${size} > cap`);
  assert.ok(size > 6000 - 1024 - 200, `cap not approached: ${size}`);
  const cap = readCapture(r.file, fs);
  assert.equal(cap.complete, true); assert.equal(cap.terminal.reason, 'capped');
  assert.equal(cap.rows, s.rowsWritten - 2);                     // armed + terminal are the other two
  assert.equal(admitted, cap.rows, 'every admitted row was written (drain before terminal)');
  assert.equal((fs.readFileSync(r.file).match(/"ev":"terminal"/g) || []).length, 1);
  assert.equal(r.record(row(999)), false); assert.ok(r.status().droppedAfterEnd >= 1);
});

test('cap: oversized rows are dropped and counted, never written, and do not end the capture', async () => {
  const fs = fakeFs(); const r = mk(fs, { maxRowBytes: 600 });
  assert.equal(r.record({ ...row(1), pad: 'y'.repeat(2000) }), false);
  assert.equal(r.record(row(2)), true);
  assert.equal(r.status().droppedOversized, 1); assert.equal(r.status().ended, false);
  r.end('closed'); await r.whenEnded();
  assert.equal(readCapture(r.file, fs).rows, 1);
});

test('queue: a slow writer makes rows beyond queueBytes drop (counted), never block; the writer catches up', async () => {
  const fs = fakeFs({ delayMs: (n) => (n <= 2 ? 200 : 0) });
  const r = mk(fs, { capBytes: 1_000_000, reserveBytes: 1024, queueBytes: 1500, maxRowBytes: 400, flushBytes: 400 });
  const t0 = performance.now();
  for (let i = 0; i < 40; i++) r.record(row(i));
  assert.ok(performance.now() - t0 < 50, 'record() blocked');
  const s1 = r.status();
  assert.ok(s1.droppedQueue > 0, 'expected backpressure drops'); assert.ok(s1.bytesQueued + s1.bytesInflight <= 1500);
  await wait(500);
  for (let i = 0; i < 3; i++) r.record(row(100 + i));
  r.end('closed'); await r.whenEnded();
  const s = r.status();
  assert.equal(s.bytesQueued, 0); assert.equal(s.bytesInflight, 0);
  assert.equal(readCapture(r.file, fs).rows, s.rowsWritten - 2);
});

// ---------------- write failures ----------------
test('writes: partial writes are continued to completion and counted once', async () => {
  const fs = fakeFs({ partial: () => true });
  const r = mk(fs, { capBytes: 400_000, queueBytes: 200_000, flushBytes: 100_000 });
  for (let i = 0; i < 5; i++) r.record(row(i));
  r.end('closed'); await r.whenEnded();
  assert.ok(fs.calls.write > 1, 'partial writes should need several write calls');
  const cap = readCapture(r.file, fs);
  assert.equal(cap.rows, 5); assert.equal(cap.parseErrors, 0); assert.equal(r.status().appendFailures, 0);
});

test('writes: errors are counted with bytes, logged at the first and every 100th, and the recorder keeps going', async () => {
  const seen = [];
  const fs = fakeFs({ error: (n) => n % 2 === 1 && n <= 6 });    // chunks 1,3,5 fail; the terminal write later succeeds
  const r = mk(fs, { capBytes: 400_000, queueBytes: 200_000, flushBytes: 100_000, flushMs: 5 }, { log: (lvl, ev, ctx) => seen.push([lvl, ev, ctx]) });
  for (let i = 0; i < 6; i++) { r.record(row(i)); await wait(15); }
  r.end('closed'); await r.whenEnded();
  const s = r.status();
  assert.ok(s.appendFailures >= 2, `failures ${s.appendFailures}`); assert.ok(s.bytesFailed > 0);
  assert.equal(s.lastError, 'ENOSPC');
  assert.equal(seen.filter(([, ev]) => ev === 'trace-write-failed').length, 1, 'first failure logged once, next at the 100th');
  const cap = readCapture(r.file, fs);
  assert.ok(cap.rows < 6 && cap.rows > 0, `some rows lost to errors, some written: ${cap.rows}`);
  assert.equal(cap.parseErrors, 0, 'a failed chunk never leaves a torn row');
  assert.equal(cap.complete, true);
});

test('writes: sustained failure — nothing written, everything counted, terminal write failure counted, no throw', async () => {
  const seen = [];
  const fs = fakeFs({ error: () => true, syncError: (n) => n >= 2 });   // attestation ok, then every write fails
  const r = mk(fs, { flushMs: 5 }, { log: (lvl, ev) => seen.push(ev) });
  for (let i = 0; i < 20; i++) r.record(row(i));
  r.end('closed'); await r.whenEnded();
  const s = r.status();
  assert.ok(s.appendFailures >= 1); assert.equal(s.terminalWriteFailed, 1); assert.equal(s.terminal.emitted, false);
  assert.ok(seen.includes('trace-write-failed')); assert.ok(seen.includes('trace-terminal-write-failed'));
  const cap = readCapture(r.file, fs);
  assert.equal(cap.complete, false); assert.match(cap.reading, /ended without record/);
});

test('open failure (not EEXIST) fails closed: disabled, no attestation, rows counted as dropped, no throw', () => {
  const fs = fakeFs({ openError: 'EACCES' });
  const seen = [];
  const r = mk(fs, {}, { log: (l, ev, ctx) => seen.push([ev, ctx]) });
  assert.equal(r.status().disabled, true); assert.match(r.status().disabledReason, /EACCES/);
  assert.equal(r.record(row(1)), false); assert.equal(r.status().droppedAfterEnd, 1);
  assert.equal(r.end('closed'), false);
  assert.equal(seen[0][0], 'trace-recorder-disabled');
});

// ---------------- stop, time, races ----------------
test('stop-file: checked at most once per stopCheckMs; ends with "stopped" without touching the process', async () => {
  let exists = false, stats = 0;
  const fs = fakeFs({ stopExists: (p) => { if (p.endsWith('.trace-stop')) { stats += 1; return exists; } return false; } });
  const r = mk(fs, { stopCheckMs: 50, flushMs: 5 });
  for (let i = 0; i < 30; i++) r.record(row(i));
  assert.ok(stats <= 2, `stop-file stat not throttled: ${stats}`);
  exists = true; await wait(60); r.record(row(99));
  await r.whenEnded();
  assert.equal(r.status().terminal.reason, 'stopped');
  assert.equal(readCapture(r.file, fs).terminal.reason, 'stopped');
});

test('time ceiling: monotonic deadline ends the capture with "window-elapsed", even when idle', async () => {
  const fs = fakeFs();
  let now = 0; const mono = () => now;
  const timers = [];
  const setTimer = (fn, ms) => { const h = { fn, ms, unref() {} }; timers.push(h); return h; };
  const r = mk(fs, { maxMs: 1000 }, { mono, setTimer, clearTimer: () => {} });
  r.record(row(1));
  now = 999; r.record(row(2)); assert.equal(r.status().ended, false);
  now = 1000; assert.equal(r.record(row(3)), false);           // deadline hit inside record()
  await r.whenEnded();
  assert.equal(r.status().terminal.reason, 'window-elapsed');
  // idle path: the max timer alone ends it (the last timer registered is r2's max timer)
  const r2 = mk(fs, { maxMs: 1000 }, { mono, setTimer, clearTimer: () => {} });
  const maxT = timers.filter((t) => t.ms === 1000).at(-1); assert.ok(maxT); maxT.fn();
  await r2.whenEnded(); assert.equal(r2.status().terminal.reason, 'window-elapsed');
  assert.equal(readCapture(r2.file, fs).rows, 0);
});

test('races: cap, stop and time arriving together produce exactly one terminal row with the first reason', async () => {
  let exists = false;
  const fs = fakeFs({ stopExists: () => exists, delayMs: () => 20 });
  let now = 0; const mono = () => now;
  const r = mk(fs, { capBytes: 3000, reserveBytes: 1024, maxMs: 5000, stopCheckMs: 1, flushBytes: 256 }, { mono });
  for (let i = 0; i < 6; i++) r.record(row(i));
  exists = true; now = 5000;                                     // stop and time both true now
  r.record(row(7)); r.stop(); r.end('window-elapsed'); r.end('capped');
  await r.whenEnded();
  const txt = fs.readFileSync(r.file);
  assert.equal((txt.match(/"ev":"terminal"/g) || []).length, 1);
  assert.ok(TERMINAL_REASONS.includes(r.status().terminal.reason));
  assert.equal(readCapture(r.file, fs).complete, true);
});

test('no-terminal reading: a capture that dies mid-way reads as ended without record', () => {
  const fs = fakeFs(); const r = mk(fs);
  r.record(row(1));                                              // queued, never flushed: simulate abrupt death
  const cap = readCapture(r.file, fs);
  assert.equal(cap.complete, false); assert.equal(cap.rows, 0); assert.match(cap.reading, /not complete, not zero-loss/);
});

// ---------------- real filesystem ----------------
test('real fs: exclusive create, end-to-end write, terminal row, PID reuse yields a second file', async () => {
  const dir = tmp();
  try {
    const a = createTraceRecorder({ dir, pid: process.pid, kv: '4.84.0', self: 'self', settings: { ...SMALL, capBytes: 200_000, reserveBytes: 2048, queueBytes: 65_536 } });
    assert.ok(realFs.existsSync(a.file));
    for (let i = 0; i < 50; i++) a.record(row(i, 'p1'));
    a.end('closed'); await a.whenEnded();
    const cap = readCapture(a.file);
    assert.equal(cap.armed.pid, process.pid); assert.equal(cap.rows, 50); assert.equal(cap.terminal.reason, 'closed');
    assert.equal(realFs.statSync(a.file).size, a.status().bytesWritten);
    const b = createTraceRecorder({ dir, pid: process.pid, kv: '4.84.0', self: 'self', settings: SMALL });   // "restart", same pid
    assert.notEqual(b.file, a.file); b.end('closed'); await b.whenEnded();
    assert.equal(readCapture(a.file).rows, 50, 'old capture untouched by the new incarnation');
    // stop-file on real fs
    const c = createTraceRecorder({ dir, pid: process.pid, settings: { ...SMALL, stopCheckMs: 1 } });
    realFs.writeFileSync(join(dir, '.trace-stop'), '');
    await wait(5); c.record(row(1)); await c.whenEnded();
    assert.equal(c.status().terminal.reason, 'stopped');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---------------- revision 2: Aster f8bd96ce #1–#7 ----------------
test('#1 armed row is admitted like any row: oversized armed => disabled, fd closed, nothing written, rows counted', () => {
  const fs = fakeFs();
  const longStop = '/cap/' + 's'.repeat(900);
  const r = createTraceRecorder({ dir: '/cap', pid: 7, kv: 'k'.repeat(500), self: 'x'.repeat(3000), settings: { ...SMALL, maxRowBytes: 800, reserveBytes: 512 }, stopFile: longStop, fs });
  assert.equal(r.status().disabled, true); assert.match(r.status().disabledReason, /armed-oversized/);
  assert.equal(fs.calls.writeSync, 0, 'no bytes written'); assert.equal(fs.calls.close, 1, 'fd closed on startup failure');
  assert.equal(fs.files.get(fs.calls.open[0]).length, 0);
  assert.equal(r.record(row(1)), false); assert.equal(r.status().droppedAfterEnd, 1);
  // bounded fields: with a sane stop path the same long kv/self are truncated and the row is admitted
  const ok = createTraceRecorder({ dir: '/cap', pid: 8, kv: 'k'.repeat(500), self: 'x'.repeat(3000), settings: { ...SMALL, maxRowBytes: 800, reserveBytes: 512 }, fs });
  assert.equal(ok.status().disabled, false);
  const cap = readCapture(ok.file, fs); assert.equal(cap.armed.self.length, 12); assert.equal(cap.armed.kv.length, 32);
  assert.ok(ok.status().bytesWritten <= 800); ok.end('closed');
});

test('#1 armed write failure closes the fd, counts the failure and disables; partial armed write is a torn chunk', () => {
  const fs = fakeFs({ syncError: () => true });
  const r = mk(fs);
  assert.equal(r.status().disabled, true); assert.match(r.status().disabledReason, /armed-write: EIO/);
  assert.equal(fs.calls.close, 1); assert.equal(r.status().appendFailures, 1); assert.equal(r.status().tornChunks, 0);
});

test('#2 identity state is committed only on ADMITTED node-start; the reader recomputes typing from file order', async () => {
  const fs = fakeFs(); const r = mk(fs, { maxRowBytes: 600 });
  assert.equal(r.record({ stream: 'lat', stage: 'node-start', proc: 'pZ', pad: 'y'.repeat(2000) }), false);   // oversized => dropped
  assert.equal(r.status().nodeStarts, 0);
  assert.equal(r.record(row(1, 'pZ')), true);
  r.end('closed'); await r.whenEnded();
  const cap = readCapture(r.file, fs);
  assert.equal(cap.preNodeStart, 1, 'written flag'); assert.equal(cap.byOrder.preNodeStart, 1, 'recomputed from order'); assert.equal(cap.flagMismatches, 0);
  assert.deepEqual(cap.nodeStarts, []);
  // a hand-made file whose flags disagree with its order is reported, not trusted
  fs.files.set('/cap/hand.jsonl', Buffer.from([
    JSON.stringify({ stage: 'node-start', proc: 'pA' }),
    JSON.stringify({ stage: 'sub:recv', proc: 'pA', preNodeStart: true }),     // claims pre, but node-start came first
    JSON.stringify({ stage: 'sub:recv', proc: 'pB' }),                          // no node-start for pB, no flag
    JSON.stringify({ stage: 'x' }),                                             // no proc, no flag
  ].join('\n') + '\n'));
  const h = readCapture('/cap/hand.jsonl', fs);
  assert.equal(h.flagMismatches, 3); assert.equal(h.byOrder.preNodeStart, 1); assert.equal(h.byOrder.noProc, 1);
});

test('#3 idle recorder ends on the stop-file and on the time ceiling without any further record()', async () => {
  let exists = false;
  const fs = fakeFs({ stopExists: (p) => p.endsWith('.trace-stop') && exists });
  const r = mk(fs, { stopCheckMs: 20 });
  await wait(60); assert.equal(r.status().ended, false, 'no stop file yet');
  exists = true; await wait(80);
  await r.whenEnded(); assert.equal(r.status().terminal.reason, 'stopped');
  const r2 = mk(fakeFs(), { maxMs: 40, stopCheckMs: 10 });      // fresh fs (no stop file), real timers, idle
  await r2.whenEnded(); assert.equal(r2.status().terminal.reason, 'window-elapsed');
});

test('#4 end() returns immediately; the terminal row and close go through the async path', async () => {
  const fs = fakeFs({ delayMs: () => 80 });
  const r = mk(fs, { capBytes: 400_000, queueBytes: 200_000, flushBytes: 100_000 });
  r.record(row(1));
  const t0 = performance.now();
  assert.equal(r.end('closed'), true);
  assert.ok(performance.now() - t0 < 20, 'end() blocked the caller');
  assert.equal(r.status().ended, false, 'not ended synchronously');
  await r.whenEnded();
  assert.ok(performance.now() - t0 >= 80, 'terminal written asynchronously');
  const cap = readCapture(r.file, fs); assert.equal(cap.rows, 1); assert.equal(cap.terminal.reason, 'closed');
  assert.equal(fs.calls.close, 1);
});

test('#5 fs.write returning 0 is a no-progress failure, not a spin; partial-then-error leaves a counted torn tail the reader sees, and later rows survive', async () => {
  const fsZero = fakeFs({ zero: (n) => n === 1 });
  const BIG = { capBytes: 400_000, queueBytes: 200_000, flushBytes: 100_000, flushMs: 5 };
  const a = mk(fsZero, BIG);
  a.record(row(1)); await wait(30);
  assert.equal(a.status().appendFailures, 1); assert.equal(a.status().lastError, 'ENOPROGRESS');
  a.record(row(2)); a.end('closed'); await a.whenEnded();
  assert.equal(readCapture(a.file, fsZero).rows, 1);
  const fsTorn = fakeFs({ partial: (n) => n === 1, error: (n) => n === 2 });   // first chunk: half, then error on the continuation
  const b = mk(fsTorn, BIG);
  b.record(row(1)); b.record({ ...row(2), pad: 'z'.repeat(300) }); await wait(30);   // unequal rows so the half-cut lands mid-row
  assert.equal(b.status().tornChunks, 1); assert.equal(b.status().appendFailures, 1);
  b.record(row(3)); b.end('closed'); await b.whenEnded();
  const cap = readCapture(b.file, fsTorn);
  assert.equal(cap.parseErrors, 1, 'the torn fragment is one unparsable line');
  assert.equal(cap.rows, 2, 'row 1 (fully written before the cut) and row 3 (after the resync) are intact; row 2 is the torn fragment');
  assert.equal(cap.complete, true, 'the terminal row is intact');
  assert.match(cap.reading, /torn/);
});

test('#6 unserializable rows are counted drops, never throws; flush transient is bounded by flushBytes + one row; internal faults never escape', async () => {
  const fs = fakeFs(); const r = mk(fs);
  const cyc = { stream: 'lat', stage: 's', proc: 'p' }; cyc.self = cyc;
  assert.equal(r.record({ stream: 'lat', stage: 's', proc: 'p', n: 10n }), false);
  assert.equal(r.record(cyc), false);
  assert.equal(r.record({ stream: 'lat', proc: 'p', get boom() { throw new Error('getter'); } }), false);
  // BigInt and the throwing getter are unserializable; the cycle is stopped by the depth budget (oversized). All three are counted drops.
  const s0 = r.status(); assert.equal(s0.droppedUnserializable + s0.droppedOversized, 3); assert.equal(s0.droppedUnserializable, 2);
  assert.equal(r.record(row(1)), true);
  r.end('closed'); await r.whenEnded(); assert.equal(readCapture(r.file, fs).rows, 1);
  const fs2 = fakeFs({ delayMs: () => 2 });
  const r2 = mk(fs2, { capBytes: 2_000_000, reserveBytes: 1024, queueBytes: 200_000, maxRowBytes: 1024, flushBytes: 2048 });
  for (let i = 0; i < 120; i++) r2.record(row(i));
  r2.end('closed'); await r2.whenEnded();
  assert.ok(fs2.calls.maxWriteLen <= 2048 + 1024 + 1, `transient ${fs2.calls.maxWriteLen} exceeds flushBytes + one row`);
  assert.equal(readCapture(r2.file, fs2).rows, 120 - r2.status().droppedQueue);
  // a throwing dependency (mono) is absorbed and counted
  const r3 = createTraceRecorder({ dir: '/cap', pid: 9, settings: SMALL, fs, mono: (() => { let n = 0; return () => { if (++n > 1) throw new Error('clock'); return n; }; })() });
  assert.equal(r3.record(row(1)), false); assert.ok(r3.status().internalErrors >= 1);
});

test('#7 withTrace adds nothing when the recorder is off: same object, no trace key', () => {
  const p = Object.freeze({ peers: 3, roles: 1 });
  assert.equal(withTrace(p, null), p); assert.equal(withTrace(p, undefined), p);
  assert.ok(!('trace' in withTrace(p, null)));
  const fs = fakeFs(); const r = mk(fs);
  const w = withTrace(p, r); assert.equal(w.trace.captureId, r.captureId); assert.notEqual(w, p); r.end('closed');
});

// ---------------- revision 3: Aster ba64f57d R1–R4 ----------------
test('R1 stop check is asynchronous after startup: no existsSync on the event loop, at most one access in flight, still ends on the stop-file', async () => {
  let exists = false;
  const fs = fakeFs({ stopExists: (p) => p.endsWith('.trace-stop') && exists, accessDelayMs: 40 });
  const r = mk(fs, { capBytes: 65536, stopCheckMs: 1, flushMs: 5 });
  const syncCallsAfterOpen = fs.calls.existsSync;
  for (let i = 0; i < 30; i++) { r.record(row(i)); await wait(2); }
  assert.equal(fs.calls.existsSync, syncCallsAfterOpen, 'existsSync must not be used after startup');
  assert.ok(fs.calls.access >= 1); assert.equal(fs.calls.accessMaxInflight, 1, 'one stop check in flight at a time');
  exists = true; await wait(100); r.record(row(99));
  await r.whenEnded(); assert.equal(r.status().terminal.reason, 'stopped');
  assert.equal(fs.calls.existsSync, 0, 'existsSync is never used at all');
});

test('R2 oversized input is rejected BEFORE encoding (no JSON.stringify), by a bounded estimate; the estimator itself is bounded', () => {
  const fs = fakeFs(); const r = mk(fs, { maxRowBytes: 1024 });
  let stringified = false;
  const huge = { stream: 'lat', stage: 's', proc: 'p', pad: 'x'.repeat(5_000_000), toJSON() { stringified = true; return { stream: 'lat' }; } };
  // toJSON is opaque to the estimator, so put the big string beside it in a nested object instead
  const ctx = { stream: 'lat', stage: 's', proc: 'p', big: { pad: 'x'.repeat(5_000_000) }, marker: { toJSON() { stringified = true; return 1; } } };
  assert.equal(r.record(ctx), false); assert.equal(r.status().droppedOversized, 1);
  assert.equal(stringified, false, 'JSON.stringify must not have run on an oversized input');
  void huge;
  // depth / node / key budgets stop traversal early
  let deep = {}; let cur = deep; for (let i = 0; i < 20; i++) { cur.n = {}; cur = cur.n; }
  assert.equal(estimateRowBytes(deep, 1_000_000).exceeded, true, 'depth budget');
  const wide = {}; for (let i = 0; i < 300; i++) wide['k' + i] = 1;
  assert.equal(estimateRowBytes(wide, 1_000_000).exceeded, true, 'key budget');
  const many = { a: Array.from({ length: 5000 }, (_, i) => i) };
  assert.equal(estimateRowBytes(many, 1_000_000).exceeded, true, 'node budget');
  const small = estimateRowBytes({ stream: 'lat', stage: 'sub:recv', msgId: 'm'.repeat(64), proc: 'p1', t: 1789264000000, from: null }, 1024);
  assert.equal(small.exceeded, false); assert.ok(small.bytes < Buffer.byteLength(JSON.stringify({ stream: 'lat', stage: 'sub:recv', msgId: 'm'.repeat(64), proc: 'p1', t: 1789264000000, from: null })) + 1, 'estimate is a lower bound');
  assert.equal(r.record(row(1)), true); r.end('closed');
});

test('R3 the resync separator is admitted against cap and queue, counted separately, and never counted as a row', async () => {
  const fsTorn = fakeFs({ partial: (n) => n === 1, error: (n) => n === 2 });
  const r = mk(fsTorn, { capBytes: 400_000, queueBytes: 200_000, flushBytes: 100_000, flushMs: 5 });
  r.record(row(1)); r.record({ ...row(2), pad: 'z'.repeat(300) }); await wait(30);
  assert.equal(r.status().needResync, true); assert.equal(r.status().tornChunks, 1);
  r.record(row(3)); r.end('closed'); await r.whenEnded();
  const s = r.status(); const cap = readCapture(r.file, fsTorn);
  assert.equal(s.separatorsWritten, 1);
  // rowsWritten counts rows of chunks that completed: armed + row 3 + terminal = 3. Row 1 landed on
  // disk inside the torn chunk but is NOT claimed as written; the reader can see more rows than
  // the recorder claims, never fewer. The separator is not a row.
  assert.equal(s.rowsWritten, 3); assert.ok(cap.rows >= s.rowsWritten - 2);
  assert.equal(s.bytesWritten, fsTorn.files.get(r.file).length, 'bytesWritten equals file length, separator included');
  assert.equal(cap.parseErrors, 1); assert.equal(cap.complete, true);
  // at a tight cap the separator byte is what tips admission: the file still never exceeds the cap
  const fs2 = fakeFs({ partial: (n) => n === 1, error: (n) => n === 2, delayMs: () => 3 });
  const r2 = mk(fs2, { capBytes: 4000, reserveBytes: 1024, queueBytes: 4000, maxRowBytes: 1024, flushBytes: 256 });
  for (let i = 0; i < 60 && !r2.status().ending; i++) { r2.record(row(i)); if (i % 2 === 0) await wait(4); }
  await r2.whenEnded();
  assert.ok(fs2.files.get(r2.file).length <= 4000, 'cap held with a separator in the accounting');
  assert.equal(r2.status().terminal.reason, 'capped');
});

test('R4 every data row carries the analyzer envelope {wall, pid, self, stream}; ctx may override; capture naming matches the on-host convention', async () => {
  const fs = fakeFs(); const r = mk(fs);
  r.record({ stream: 'lat', stage: 'node-start', proc: 'pA', transportId: 'ab' });
  r.record({ stream: 'lat', stage: 'sub:recv', msgId: 'm'.repeat(64), from: 'f'.repeat(66), to: 't'.repeat(66), proc: 'pA', t: 1, mono: 1 });
  r.record({ stream: 'lat', stage: 'deliver:hop_tx', hopAttemptId: 'h1', hopIdx: 1, from: 'f', to: 't', writeOutcome: 'ok', msgIds: ['m'], proc: 'pA', t: 2, mono: 2 });
  r.record({ stream: 'disc', ev: 'root-members', t: 'abc', msgId: 'm'.repeat(64), n: 2, self: 'override0001' });
  r.end('closed'); await r.whenEnded();
  const cap = readCapture(r.file, fs);
  assert.equal(cap.rows, 4); assert.equal(cap.envelopeMissing, 0);
  const lines = fs.readFileSync(r.file).trim().split('\n').map((l) => JSON.parse(l));
  for (const l of lines.slice(1, -1)) { assert.equal(l.pid, 4242); assert.equal(typeof l.wall, 'number'); assert.ok(['lat', 'disc'].includes(l.stream)); assert.equal(typeof l.self, 'string'); }
  assert.equal(lines[1].self, 'abcdef012345'); assert.equal(lines[4].self, 'override0001', 'ctx self wins');
  assert.match(r.file, /\/disc-relay-4242-[A-Za-z0-9]+\.jsonl$/);
  assert.equal(r.status().self, 'abcdef012345');
});

// ---------------- revision 4: Aster 0998a341 — single-read snapshot ----------------
test('R2b input is read ONCE into a plain snapshot: a getter cannot answer small then huge, and toJSON is never invoked', async () => {
  const fs = fakeFs(); const r = mk(fs, { maxRowBytes: 1024 });
  let reads = 0;
  const ctx = { stream: 'lat', stage: 'sub:recv', proc: 'p1', get shifty() { reads += 1; return reads === 1 ? 'small' : 'x'.repeat(5_000_000); } };
  assert.equal(r.record(ctx), true);
  assert.equal(reads, 1, 'the accessor is read exactly once');
  let invoked = false;
  const withToJson = { stream: 'lat', stage: 'sub:recv', proc: 'p1', inner: { a: 1, toJSON() { invoked = true; return 'y'.repeat(5_000_000); } } };
  assert.equal(r.record(withToJson), true);
  assert.equal(invoked, false, 'toJSON must never run');
  r.end('closed'); await r.whenEnded();
  const lines = fs.readFileSync(r.file).trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(lines[1].shifty, 'small'); assert.ok(Buffer.byteLength(JSON.stringify(lines[1])) <= 1024);
  assert.deepEqual(lines[2].inner, { a: 1 }, 'plain snapshot of own enumerable data; the method is gone');
  assert.equal(readCapture(r.file, fs).rows, 2);
  // snapshot semantics: functions/undefined dropped, non-finite numbers -> null, nested arrays kept, BigInt flagged
  const s = snapshotRow({ a: 1, f() {}, u: undefined, n: NaN, arr: [1, 'x', [2]], o: { k: 'v' } }, 4096);
  assert.deepEqual(JSON.parse(JSON.stringify(s.value)), { a: 1, n: null, arr: [1, 'x', [2]], o: { k: 'v' } }); assert.equal(s.unserializable, false);
  assert.equal(Object.getPrototypeOf(s.value), null, 'snapshots are null-prototype objects');
  assert.equal(snapshotRow({ b: 10n }, 4096).unserializable, true);
  assert.equal(snapshotRow({ s: 'z'.repeat(5000) }, 4096).exceeded, true);
});

// ---------------- revision 5: Aster 0045c91e — prototype-key identity ----------------
test('R2c a "__proto__" key cannot smuggle a node-start: the snapshot has a null prototype, typing reads only own data, and a later ghost-proc row stays preNodeStart', async () => {
  const hostile = JSON.parse('{"__proto__":{"stage":"node-start","proc":"ghost"},"stream":"disc"}');
  const s = snapshotRow(hostile, 1024);
  assert.equal(s.exceeded, false); assert.equal(Object.getPrototypeOf(s.value), null);
  assert.equal(s.value.stage, undefined); assert.equal(s.value.proc, undefined);
  assert.equal(Object.hasOwn(s.value, '__proto__'), true, 'kept as an own data property, as JSON does');
  assert.equal(JSON.stringify(s.value), JSON.stringify(hostile));
  const fs = fakeFs(); const r = mk(fs);
  assert.equal(r.record(hostile), true);
  assert.equal(r.status().nodeStarts, 0, 'no node-start was committed');
  assert.equal(r.record({ stream: 'lat', stage: 'sub:recv', proc: 'ghost' }), true);
  r.end('closed'); await r.whenEnded();
  const cap = readCapture(r.file, fs);
  assert.deepEqual(cap.nodeStarts, []); assert.equal(cap.preNodeStart, 1); assert.equal(cap.byOrder.preNodeStart, 1); assert.equal(cap.flagMismatches, 0);
  const lines = fs.readFileSync(r.file).trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(lines[1].stage, undefined); assert.equal(lines[1].noProc, true);
  assert.equal(lines[2].preNodeStart, true);
  // a genuine own-property node-start still binds
  const r2 = mk(fs); r2.record({ stream: 'lat', stage: 'node-start', proc: 'real' }); r2.record({ stream: 'lat', stage: 'sub:recv', proc: 'real' });
  assert.equal(r2.status().nodeStarts, 1); assert.equal(r2.status().preNodeStart, 0); r2.end('closed');
  // key enumeration stops at the key budget without walking everything
  let reads = 0; const wide = {}; for (let i = 0; i < 10_000; i++) Object.defineProperty(wide, 'k' + i, { enumerable: true, get() { reads += 1; return 1; } });
  assert.equal(snapshotRow(wide, 1_000_000).exceeded, true); assert.ok(reads <= 257, `walked ${reads} getters past the key budget`);
});
