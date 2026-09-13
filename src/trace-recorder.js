// =============================================================================
// trace-recorder.js — bounded, non-blocking recorder for the relay's LAT_TRACE
// diagnostic rows (AX-PILOT-P1 offline contract, Aster 022c275b / 26336fb5 /
// 854b2dc4, David-approved 2026-09-13). Revision 5 after Aster f8bd96ce + ba64f57d + 0998a341 + 0045c91e.
//
// Replaces the per-row appendFileSync to relay-logs/disc-relay-<pid>.jsonl
// (index.js, 2026-08-31 arm) which was synchronous, unbounded, and swallowed
// write failures. This one:
//
//   • cannot grow its file past capBytes: EVERY row, the armed row included, and
//     every resync separator is admitted on bytesWritten + bytesQueued +
//     bytesInflight + pendingSeparator + rowBytes + reserveBytes <= capBytes, and on
//     rowBytes <= maxRowBytes — with the size ESTIMATED before anything is encoded,
//     so an oversized input never costs an oversized transient;
//   • cannot grow its memory past queueBytes: rows beyond the queue ceiling are
//     DROPPED and counted, never blocked on; a flush moves at most flushBytes + one
//     row (+ one separator byte) through a transient buffer;
//   • performs no synchronous filesystem call after startup: the armed row is the
//     only synchronous write (before any traffic); stop-file checks, the terminal
//     row and the close are asynchronous and serialized;
//   • ends at most once, with one terminal row inside the reserve, for exactly one
//     of: capped | stopped | window-elapsed | closed; the idle case is covered by a
//     periodic check, not by the next record();
//   • names its file by an exclusive random capture id (open flag 'wx'), never by
//     pid + wall clock, so a reused pid cannot append to an older capture;
//   • keeps the row envelope the analyzers read: {wall, pid, self, stream, ...ctx}
//     plus captureId and the typing flags;
//   • commits identity state (which kernel procs have a node-start row) only when
//     the node-start row was ADMITTED; rows carrying a proc with no admitted
//     node-start get preNodeStart:true; rows without proc get noProc:true; the
//     reader recomputes both from file order and reports disagreements;
//   • never throws into the relay: unserializable input and internal errors are
//     counted drops.
//
// What the input bounds ARE and ARE NOT: the budgets bound the snapshot the recorder
// retains and encodes (bytes, depth, node and key counts) and stop traversal at the
// first breach. They do not bound the caller's input itself (an enormous object still
// occupies the caller's memory), nor the work a caller-provided accessor performs when
// it is read once, nor the encoded row beyond the snapshot's own size. Queue and row
// bounds are bounds on retained bytes, not a total memory or callback-time bound.
//
// What it does NOT claim: durability. Flush cadence is a scheduling target. An
// abrupt death loses whatever is queued or in flight plus whatever the OS has not
// written; the counters die with the process; a capture with no terminal row is
// "ended without record", never "complete". A write that fails after partial
// progress can leave a torn last row on disk: it is counted (tornChunks), the next
// write starts with a separator so the torn fragment stays one unparsable line,
// and the reader reports it as a parse error. Failure lines go to the ordinary
// relay log, which may share the failing disk, so they are best-effort.
//
// Startup hook order, verified in the vendored kernel (4.84.0): the kernel emits
// its `node-start` row lazily from the FIRST tx-ledger / rx-ledger emission
// (AxonaManager.js:1541, 1552), not at construction. sub:recv, fanout-ledger,
// root:origin and the hop rows do not trigger it. So rows CAN precede node-start
// for their proc; that is why preNodeStart exists, and why no kernel change is
// proposed here. The relay attaches its capture handler after createRelay and
// before startRelay (index.js), so nothing emitted during start is missed.
//
// Default OFF: index.js only creates a recorder when LAT_TRACE === '1'. With it
// unset the relay is byte-identical to before (recorderFromEnv returns null, and
// withTrace() adds no field).
// =============================================================================
import * as nodeFs from 'node:fs';
import { randomBytes } from 'node:crypto';
import { join } from 'node:path';

export const DEFAULTS = Object.freeze({
  capBytes:     64 * 1024 * 1024,   // per capture file, INCLUDING the terminal reserve
  reserveBytes: 4096,               // kept free for exactly one terminal row (+ one separator)
  queueBytes:   4 * 1024 * 1024,    // queued + in-flight ceiling
  maxRowBytes:  64 * 1024,          // one encoded row; larger rows are dropped and counted
  flushMs:      250,                // scheduling target, not a durability bound
  flushBytes:   64 * 1024,          // flush early once this much is queued; also the transient chunk bound
  maxMs:        60 * 60 * 1000,     // capture ceiling; ends with window-elapsed
  stopCheckMs:  1000,               // stop-file check throttle AND the idle check period
});

export const TERMINAL_REASONS = Object.freeze(['capped', 'stopped', 'window-elapsed', 'closed']);
export const INPUT_LIMITS = Object.freeze({ maxDepth: 6, maxNodes: 4096, maxKeys: 256 });
const MAX_SELF = 12, MAX_KV = 32, MAX_PATH = 1024;

const isPosInt = (v) => Number.isInteger(v) && v > 0;

/** Validate and freeze settings. Throws RangeError naming the offending field. */
export function validateSettings(input = {}) {
  const s = { ...DEFAULTS, ...input };
  for (const k of Object.keys(s)) if (!(k in DEFAULTS)) throw new RangeError(`trace-recorder: unknown setting ${k}`);
  for (const k of Object.keys(DEFAULTS)) if (!isPosInt(s[k])) throw new RangeError(`trace-recorder: ${k} must be a positive integer, got ${String(s[k])}`);
  if (s.reserveBytes >= s.capBytes) throw new RangeError('trace-recorder: reserveBytes must be smaller than capBytes');
  if (s.maxRowBytes + s.reserveBytes > s.capBytes) throw new RangeError('trace-recorder: maxRowBytes + reserveBytes must fit in capBytes');
  if (s.maxRowBytes > s.queueBytes) throw new RangeError('trace-recorder: maxRowBytes must fit in queueBytes');
  if (s.flushBytes > s.queueBytes) throw new RangeError('trace-recorder: flushBytes must fit in queueBytes');
  if (s.reserveBytes < 512) throw new RangeError('trace-recorder: reserveBytes must leave room for a terminal row (>= 512)');
  return Object.freeze(s);
}

/**
 * Lower-bound estimate of the encoded size of a value WITHOUT encoding it, with a
 * traversal budget. Strings count their length (JSON adds quotes and escapes, never
 * removes characters), numbers/booleans/null their printed length, keys their length.
 * Getters are invoked (that is what JSON.stringify would do too) but inside the
 * caller's try. Returns {bytes, nodes, exceeded} — exceeded when the depth, node
 * or byte budget is passed; traversal stops early so the cost is bounded.
 */
export function estimateRowBytes(value, limit, limits = INPUT_LIMITS) {
  const s = snapshotRow(value, limit, limits);
  return { bytes: s.bytes, nodes: s.nodes, exceeded: s.exceeded };
}

/**
 * ONE bounded pass over the input that produces a PLAIN snapshot (primitives, strings,
 * arrays, plain objects — no accessors, no toJSON, no functions, no symbols) and its
 * size estimate. Only the snapshot is ever encoded, so nothing in the input is read
 * twice: a getter is read once here, and toJSON is never invoked by anyone (the snapshot
 * has none). Returns {value, bytes, nodes, exceeded, unserializable}; `unserializable`
 * is set for BigInt/symbol values or a throwing accessor (the caller counts the row as
 * dropped-unserializable); `exceeded` when the depth, node, key or byte budget is passed
 * (dropped-oversized). Traversal stops at the first budget breach.
 */
export function snapshotRow(value, limit, limits = INPUT_LIMITS) {
  let bytes = 0, nodes = 0, exceeded = false, unserializable = false;
  const walk = (v, depth) => {
    if (exceeded || unserializable) return undefined;
    if (++nodes > limits.maxNodes || bytes > limit) { exceeded = true; return undefined; }
    if (v === null) { bytes += 4; return null; }
    if (v === undefined) return undefined;                                               // dropped from objects, null in arrays — as JSON does
    const t = typeof v;
    if (t === 'string') { bytes += v.length + 2; if (bytes > limit) exceeded = true; return v; }
    if (t === 'number') { bytes += String(v).length; return Number.isFinite(v) ? v : null; }
    if (t === 'boolean') { bytes += v ? 4 : 5; return v; }
    if (t === 'bigint' || t === 'symbol') { unserializable = true; return undefined; }
    if (t === 'function') { return undefined; }                                          // dropped, like JSON.stringify does
    if (depth >= limits.maxDepth) { exceeded = true; return undefined; }
    if (Array.isArray(v)) {
      bytes += 2; const out = [];
      for (let i = 0; i < v.length; i++) { if (i) bytes += 1; const x = walk(v[i], depth + 1); if (exceeded || unserializable) return undefined; out.push(x === undefined ? null : x); }
      return out;
    }
    if (t === 'object') {
      // Null-prototype output: a key literally named "__proto__" becomes an OWN data property
      // (as JSON.parse and JSON.stringify treat it), never the snapshot's prototype — so what
      // admit() reads (plain.stage / plain.proc) is exactly what gets encoded. for-in with
      // hasOwn stops at maxKeys without materializing every key first.
      bytes += 2; const out = Object.create(null); let keys = 0;
      for (const k in v) {
        if (!Object.hasOwn(v, k)) continue;                                              // own enumerable only; accessors read ONCE here
        if (++keys > limits.maxKeys) { exceeded = true; return undefined; }
        if (keys > 1) bytes += 1; bytes += k.length + 3;
        const x = walk(v[k], depth + 1); if (exceeded || unserializable) return undefined;
        if (x !== undefined) Object.defineProperty(out, k, { value: x, enumerable: true, writable: true, configurable: true });
      }
      return out;
    }
    return undefined;
  };
  let out;
  try { out = walk(value, 0); } catch { unserializable = true; out = undefined; }
  if (bytes > limit) exceeded = true;
  return { value: out, bytes, nodes, exceeded, unserializable };
}

/** Encode a row; returns null (never throws) when the value cannot be serialized. */
function encode(obj) {
  try { return Buffer.from(JSON.stringify(obj) + '\n'); } catch { return null; }
}

/**
 * Create a recorder. Opens the capture file immediately (exclusive) and writes the
 * attestation row synchronously — the one synchronous write, at startup, before any
 * traffic, and the one write that is admitted under the same cap and row bounds as
 * every other. Everything after is queued and flushed asynchronously.
 *
 * deps (all injectable for tests): fs, wall, mono, random, log, setTimer, clearTimer.
 * fs must provide openSync, writeSync, closeSync (startup) and write, access, close (after).
 */
export function createTraceRecorder({
  dir, pid, kv, self, settings, stopFile,
  fs = nodeFs, wall = Date.now, mono = () => performance.now(),
  random = () => randomBytes(8).toString('hex'), log = () => {},
  setTimer = setTimeout, clearTimer = clearTimeout,
} = {}) {
  if (typeof dir !== 'string' || !dir || dir.length > MAX_PATH) throw new TypeError('trace-recorder: dir required (<= 1024 chars)');
  if (!isPosInt(pid)) throw new TypeError('trace-recorder: pid required');
  const S = validateSettings(settings);
  const stopPath = String(stopFile ?? join(dir, '.trace-stop')).slice(0, MAX_PATH);
  const selfId = self == null ? null : String(self).slice(0, MAX_SELF);
  const kvId = kv == null ? null : String(kv).slice(0, MAX_KV);
  const startedWall = wall(), startedMono = mono();
  const safeLog = (lvl, ev, ctx) => { try { log(lvl, ev, ctx); } catch { /* the relay's logger is not our fault to propagate */ } };
  const safeMono = () => { try { return mono(); } catch { return null; } };

  const c = {                                   // counters — in memory only
    rowsWritten: 0, bytesWritten: 0, rowsQueued: 0, bytesQueued: 0, bytesInflight: 0, separatorsWritten: 0,
    droppedOversized: 0, droppedAtCap: 0, droppedQueue: 0, droppedAfterEnd: 0, droppedUnserializable: 0,
    appendFailures: 0, bytesFailed: 0, tornChunks: 0, lastError: null, terminalWriteFailed: 0,
    internalErrors: 0, preNodeStart: 0, noProc: 0, nodeStarts: 0,
  };
  let fd = null, file = null, captureId = null, disabled = false, disabledReason = null;
  let queue = [], flushing = false, timer = null, tick = null, maxTimer = null;
  let ending = false, ended = false, terminal = { emitted: false, reason: null };
  let lastStopCheck = -Infinity, stopCheckInflight = false, needResync = false;
  const NL = Buffer.from('\n');
  const procsSeen = new Set();
  const deadline = startedMono + S.maxMs;
  const pendingDone = [];
  const pendingSep = () => (needResync ? 1 : 0);

  // ---- open: exclusive random identity, three tries, then fail closed ----
  try { fs.mkdirSync(dir, { recursive: true }); } catch { /* opening will tell us */ }
  for (let i = 0; i < 3 && fd === null; i++) {
    const id = String(random()).replace(/[^A-Za-z0-9]/g, '').slice(0, 32);
    const path = join(dir, `disc-relay-${pid}-${id}.jsonl`);
    try { fd = fs.openSync(path, 'wx'); file = path; captureId = id; }
    catch (e) { if (e?.code !== 'EEXIST') { disabled = true; disabledReason = `open: ${e?.code || e?.message || e}`; break; } }
  }
  if (fd === null && !disabled) { disabled = true; disabledReason = 'open: EEXIST x3'; }

  // ---- armed row: bounded fields, then admitted like any other row ----
  if (!disabled) {
    const armed = { ev: 'armed', wall: startedWall, pid, captureId, kv: kvId, latTrace: 1, self: selfId, settings: S, inputLimits: INPUT_LIMITS, stopFile: stopPath };
    const row = encode(armed);
    if (!row) fail('armed-unserializable');
    else if (row.length > S.maxRowBytes) fail(`armed-oversized:${row.length}`);
    else if (row.length + S.reserveBytes > S.capBytes) fail(`armed-over-cap:${row.length}`);
    else {
      let off = 0;
      try {
        while (off < row.length) { const n = fs.writeSync(fd, row, off, row.length - off, null); if (!(n > 0)) throw Object.assign(new Error('no progress'), { code: 'ENOPROGRESS' }); off += n; c.bytesWritten += n; }
        c.rowsWritten += 1;
      } catch (e) {
        c.appendFailures += 1; c.bytesFailed += row.length - off; c.lastError = String(e?.code || e?.message || e);
        if (off > 0) c.tornChunks += 1;
        fail(`armed-write: ${c.lastError}`);
      }
    }
  }
  function fail(reason) {
    disabled = true; disabledReason = reason;
    if (fd !== null) { try { fs.closeSync(fd); } catch { /* */ } fd = null; }
    safeLog('warn', 'trace-recorder-disabled', { reason, file });
  }
  if (disabled && (disabledReason || '').startsWith('open')) safeLog('warn', 'trace-recorder-disabled', { reason: disabledReason });
  if (!disabled) {
    maxTimer = setTimer(() => end('window-elapsed'), S.maxMs); maxTimer?.unref?.();
    tick = setTimer(onTick, S.stopCheckMs); tick?.unref?.();     // idle stop/time check
  }

  // ---- helpers ----
  function status() {
    const now = safeMono();
    return {
      file, captureId, pid, self: selfId, disabled, disabledReason, ended, ending, terminal: { ...terminal },
      settings: S, inputLimits: INPUT_LIMITS, startedWall, elapsedMs: now === null ? null : now - startedMono, needResync, ...c,
    };
  }
  /** Async, throttled, at most one in flight: never a synchronous fs call on the event loop (R1). */
  function checkStop() {
    const now = mono();
    if (stopCheckInflight || now - lastStopCheck < S.stopCheckMs) return;
    lastStopCheck = now; stopCheckInflight = true;
    try {
      fs.access(stopPath, 0, (err) => { stopCheckInflight = false; if (!err && !ended && !ending) end('stopped'); });
    } catch { stopCheckInflight = false; }
  }
  function checkTime() { if (mono() >= deadline) end('window-elapsed'); }
  function onTick() {
    tick = null;
    if (ended || ending || disabled) return;
    try { checkTime(); if (!ending) checkStop(); } catch (e) { c.internalErrors += 1; c.lastError = String(e?.message || e); }
    if (!ended && !ending) { tick = setTimer(onTick, S.stopCheckMs); tick?.unref?.(); }
  }

  /** Queue one diagnostic row. Returns true if admitted to the queue. Never throws. */
  function record(ctx) {
    try { return admit(ctx); } catch (e) { c.internalErrors += 1; c.lastError = String(e?.message || e); return false; }
  }
  function admit(ctx) {
    if (disabled || ended || ending) { c.droppedAfterEnd += 1; return false; }
    checkTime(); if (!ending) checkStop();
    if (ending) { c.droppedAfterEnd += 1; return false; }
    // Typing is DECIDED here but identity state is COMMITTED only on admission.
    // Reading ctx can itself throw (a getter); that is an unserializable row, not our fault.
    let isNodeStart, proc, flags, buf;
    try {
      if (!ctx || typeof ctx !== 'object') ctx = { value: ctx };
      // R2: ONE bounded pass produces a plain snapshot; only the snapshot is encoded. The input
      // is never read twice, so accessors cannot answer small here and huge later, and toJSON
      // is never invoked. An oversized input never costs an oversized transient.
      const snap = snapshotRow(ctx, S.maxRowBytes);
      if (snap.unserializable) { buf = null; }
      else if (snap.exceeded || !snap.value || typeof snap.value !== 'object') { c.droppedOversized += 1; return false; }
      else {
        const plain = snap.value;
        isNodeStart = plain.stage === 'node-start' && plain.proc != null;
        proc = plain.proc != null ? String(plain.proc) : null;
        flags = {};
        if (proc === null) flags.noProc = true;
        else if (!isNodeStart && !procsSeen.has(proc)) flags.preNodeStart = true;
        // envelope the analyzers read: wall, pid, self, stream come first; the row may override stream/self
        buf = encode({ wall: wall(), pid, self: selfId, captureId, ...plain, ...flags });
      }
    } catch { buf = null; }
    if (!buf) { c.droppedUnserializable += 1; if (c.droppedUnserializable === 1) safeLog('warn', 'trace-row-unserializable', {}); return false; }
    const size = buf.length;
    if (size > S.maxRowBytes) { c.droppedOversized += 1; return false; }
    if (c.bytesWritten + c.bytesQueued + c.bytesInflight + pendingSep() + size + S.reserveBytes > S.capBytes) { c.droppedAtCap += 1; end('capped'); return false; }
    if (c.bytesQueued + c.bytesInflight + pendingSep() + size > S.queueBytes) { c.droppedQueue += 1; return false; }
    if (isNodeStart) { procsSeen.add(proc); c.nodeStarts += 1; }
    if (flags.preNodeStart) c.preNodeStart += 1;
    if (flags.noProc) c.noProc += 1;
    queue.push(buf); c.bytesQueued += size; c.rowsQueued += 1;
    if (c.bytesQueued >= S.flushBytes) flush();
    else if (!timer) { timer = setTimer(onTimer, S.flushMs); timer?.unref?.(); }
    return true;
  }
  function onTimer() { timer = null; if (!ended && !ending) { try { checkTime(); if (!ending) checkStop(); } catch { c.internalErrors += 1; } } flush(); }

  /** Write one buffer fully through fs.write: partial writes continue, n<=0 is a no-progress error. */
  function writeBuf(buf, cb) {
    let off = 0;
    const step = () => {
      let called = false;
      const onDone = (err, n) => {
        if (called) return; called = true;
        if (err || !(n > 0)) return cb(err ?? Object.assign(new Error('no progress'), { code: 'ENOPROGRESS' }), off);
        off += n;
        if (off < buf.length) return step();
        cb(null, off);
      };
      try { fs.write(fd, buf, off, buf.length - off, null, onDone); } catch (e) { onDone(e); }
    };
    step();
  }

  /** Serialize writes: one bounded chunk in flight at a time (<= flushBytes + one row + one separator). */
  function flush(done) {
    if (done) pendingDone.push(done);
    if (flushing || disabled || fd === null) return;
    if (queue.length === 0) { const cbs = pendingDone.splice(0); for (const cb of cbs) cb(); return; }
    if (timer) { clearTimer(timer); timer = null; }
    const take = []; let rowBytes = 0;
    while (queue.length && (rowBytes === 0 || rowBytes + queue[0].length <= S.flushBytes)) { const b = queue.shift(); take.push(b); rowBytes += b.length; }
    const rows = take.length;
    // R3: the separator is accounted (pendingSep was in every admission since the tear) and is
    // never counted as a row.
    const sep = needResync ? 1 : 0;
    if (sep) { take.unshift(NL); needResync = false; }
    const buf = take.length === 1 ? take[0] : Buffer.concat(take, rowBytes + sep);
    c.bytesQueued -= rowBytes; c.rowsQueued -= rows; c.bytesInflight = rowBytes + sep; flushing = true;
    writeBuf(buf, (err, written) => {
      c.bytesWritten += written;
      if (err) {
        c.appendFailures += 1; c.bytesFailed += buf.length - written; c.lastError = String(err?.code || err?.message || err);
        if (written > 0) { c.tornChunks += 1; needResync = true; if (sep) c.separatorsWritten += 1; }
        else if (sep) needResync = true;                              // separator not written: still owed
        if (c.appendFailures === 1 || c.appendFailures % 100 === 0) safeLog('warn', 'trace-write-failed', { n: c.appendFailures, err: c.lastError, bytesFailed: c.bytesFailed, tornChunks: c.tornChunks });
      } else { c.rowsWritten += rows; if (sep) c.separatorsWritten += 1; }
      c.bytesInflight = 0; flushing = false;
      if (queue.length) return flush();
      const cbs = pendingDone.splice(0); for (const cb of cbs) cb();
    });
  }

  /** End exactly once: drain, write the terminal row (async, inside the reserve), close (async). */
  function end(reason) {
    if (disabled || ending || ended) return false;
    if (!TERMINAL_REASONS.includes(reason)) reason = 'closed';
    ending = true; terminal.reason = reason;
    if (timer) { clearTimer(timer); timer = null; }
    if (maxTimer) { clearTimer(maxTimer); maxTimer = null; }
    if (tick) { clearTimer(tick); tick = null; }
    flush(finalize);
    return true;
    function finalize() {
      if (ended) return;
      const t = { ev: 'terminal', reason, wall: wall(), pid, captureId, elapsedMs: (safeMono() ?? startedMono) - startedMono, ...counterSnapshot() };
      const sep = needResync ? 1 : 0;
      let row = encode(t);
      if (!row || row.length + sep > S.reserveBytes) row = encode({ ev: 'terminal', reason, wall: t.wall, pid, captureId, truncated: true });
      const closeOut = () => {
        const h = fd; fd = null;
        const done = () => { ended = true; };
        try { if (typeof fs.close === 'function') fs.close(h, () => done()); else { fs.closeSync(h); done(); } } catch { done(); }
      };
      if (fd === null || !row) { ended = true; return; }
      if (sep) { row = Buffer.concat([NL, row]); needResync = false; }
      writeBuf(row, (err, written) => {
        c.bytesWritten += written;
        if (err) { c.terminalWriteFailed += 1; c.bytesFailed += row.length - written; c.lastError = String(err?.code || err?.message || err); if (written > 0) c.tornChunks += 1; safeLog('warn', 'trace-terminal-write-failed', { reason, err: c.lastError }); }
        else { c.rowsWritten += 1; if (sep) c.separatorsWritten += 1; terminal.emitted = true; }
        closeOut();
      });
    }
  }
  function counterSnapshot() {
    const { rowsWritten, bytesWritten, separatorsWritten, droppedOversized, droppedAtCap, droppedQueue, droppedAfterEnd, droppedUnserializable, appendFailures, bytesFailed, tornChunks, terminalWriteFailed, internalErrors, preNodeStart, noProc, nodeStarts } = c;
    return { rowsWritten, bytesWritten, separatorsWritten, droppedOversized, droppedAtCap, droppedQueue, droppedAfterEnd, droppedUnserializable, appendFailures, bytesFailed, tornChunks, terminalWriteFailed, internalErrors, preNodeStart, noProc, nodeStarts };
  }
  /** Wait until end() has fully completed (tests, orderly shutdown). Real timer on purpose. */
  function whenEnded() { return new Promise((res) => { const poll = () => (ended || disabled ? res() : setTimeout(poll, 5)); poll(); }); }

  return Object.freeze({ record, end, stop: () => end('stopped'), status, whenEnded, get file() { return file; }, get captureId() { return captureId; } });
}

/** Build a recorder from the relay's environment; null unless LAT_TRACE === '1' (default OFF). */
export function recorderFromEnv(env, deps = {}) {
  if (!env || env.LAT_TRACE !== '1') return null;
  const num = (k) => (env[k] == null || env[k] === '' ? undefined : Number(env[k]));
  const settings = {};
  const m = { LAT_TRACE_CAP_BYTES: 'capBytes', LAT_TRACE_QUEUE_BYTES: 'queueBytes', LAT_TRACE_MAX_MS: 'maxMs', LAT_TRACE_FLUSH_MS: 'flushMs', LAT_TRACE_MAX_ROW_BYTES: 'maxRowBytes' };
  for (const [k, f] of Object.entries(m)) { const v = num(k); if (v !== undefined) settings[f] = v; }
  return createTraceRecorder({ ...deps, settings, stopFile: env.LAT_TRACE_STOP_FILE || undefined });
}

/** Health-dump helper: adds a `trace` field only when a recorder exists, so default-off stays byte-identical. */
export function withTrace(payload, recorder) {
  return recorder ? { ...payload, trace: recorder.status() } : payload;
}

/**
 * Read a capture file back. Never infers completeness: `complete` is true only when a
 * terminal row is present. Groups rows by kernel proc, and INDEPENDENTLY recomputes the
 * preNodeStart / noProc typing from file order, reporting rows whose written flag
 * disagrees with the recomputation (the reader validates the sequence itself).
 */
export function readCapture(path, fs = nodeFs) {
  const text = fs.readFileSync(path, 'utf8');
  const out = { armed: null, terminal: null, complete: false, rows: 0, parseErrors: 0, procs: {}, preNodeStart: 0, noProc: 0, nodeStarts: [], byOrder: { preNodeStart: 0, noProc: 0 }, flagMismatches: 0, envelopeMissing: 0 };
  const seen = new Set();
  for (const line of text.split('\n')) {
    if (!line) continue;
    let r; try { r = JSON.parse(line); } catch { out.parseErrors += 1; continue; }
    if (r.ev === 'armed') { out.armed = r; continue; }
    if (r.ev === 'terminal') { out.terminal = r; continue; }
    out.rows += 1;
    if (!('wall' in r) || !('pid' in r) || !('self' in r) || !('stream' in r)) out.envelopeMissing += 1;
    const proc = r.proc != null ? String(r.proc) : null;
    const isNodeStart = r.stage === 'node-start' && proc !== null;
    let expectPre = false, expectNo = false;
    if (proc === null) expectNo = true; else if (!isNodeStart && !seen.has(proc)) expectPre = true;
    if (isNodeStart) { seen.add(proc); out.nodeStarts.push(proc); }
    if (proc !== null) out.procs[proc] = (out.procs[proc] || 0) + 1;
    if (r.preNodeStart) out.preNodeStart += 1;
    if (r.noProc) out.noProc += 1;
    if (expectPre) out.byOrder.preNodeStart += 1;
    if (expectNo) out.byOrder.noProc += 1;
    if (!!r.preNodeStart !== expectPre || !!r.noProc !== expectNo) out.flagMismatches += 1;
  }
  out.complete = !!out.terminal;
  out.reading = out.complete ? `ended: ${out.terminal.reason}` : 'ended without record — not complete, not zero-loss';
  if (out.parseErrors) out.reading += `; ${out.parseErrors} torn/unparsable line(s)`;
  return out;
}
