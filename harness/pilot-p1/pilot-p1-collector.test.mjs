// Offline tests for ops/pilot-p1-collector.mjs — fake transport, no ssh, no network.
// These exercise the collector's OWN local process handling, budgets and parsing. They are
// not evidence about any remote host.
//   node --test ops/pilot-p1-collector.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { TARGETS, BUDGET, CLASSES, plan, sshArgs, collect, realTransport, deriveClock, deriveProcs, deriveUnitProps, deriveCommit, deriveCount } from './pilot-p1-collector.mjs';

const HOSTILE = '$(touch /tmp/pwned); `id`; \'; rm -rf /; 12345\n$P\n';
const pidAlive = (pid) => { try { process.kill(pid, 0); return true; } catch (e) { return e.code !== 'ESRCH'; } };
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function fakeTransport(script = {}) {
  const issued = [];
  return {
    issued,
    async run(target, remote, limits) {
      issued.push({ target: target.id, remote, limits });
      const s = script[target.id] ?? script['*'] ?? {};
      if (s.delayMs) await wait(s.delayMs);
      const cls = s.cls ?? 'complete';
      const stdout = s.stdout ?? HOSTILE;
      return { stdout, stderr: '', code: cls === 'complete' ? 0 : 1, signal: null, cls, cleanup: 'n/a', start: Date.now(), end: Date.now(), bytes: Buffer.byteLength(stdout) };
    },
  };
}

// ---------------- plan ----------------
test('plan: every target has commands and none is empty or multi-line', () => {
  for (const t of TARGETS) {
    const p = plan(t);
    assert.ok(p.length >= 4, t.id);
    for (const s of p) { assert.ok(s.remote.length > 0); assert.ok(!s.remote.includes('\n')); }
  }
});

test('plan: no args/command-line/env/log/journal/git-status reads in the first pass', () => {
  const banned = [/args/, /CommandLine/, /environ/, /\bps -E/, /journalctl/, /relay-logs/, /git status/, /systemctl cat/, /cat /, /tail /, /head /, /--selftest/, /--kernels/, /find /, /du /];
  for (const t of TARGETS) for (const s of plan(t)) for (const b of banned) assert.ok(!b.test(s.remote), `${t.id}/${s.step} contains ${b}`);
});

test('plan: windows commands carry none of | > & ( ) $ and use the Git-bash wrapper; unsupported reads omitted', () => {
  const win = TARGETS.find((t) => t.id === 'win');
  for (const s of plan(win)) {
    const inner = s.remote.replace(/^"C:\\Program Files\\Git\\bin\\bash.exe" -lc '/, '').replace(/'$/, '');
    assert.ok(!/[|>&()$]/.test(inner), `${s.step}: ${inner}`);
    assert.ok(s.remote.startsWith('"C:\\Program Files\\Git\\bin\\bash.exe" -lc \''));
  }
  assert.ok(!plan(win).some((s) => s.step === 'procs'));
  assert.ok(plan(win).some((s) => s.step === 'census-proxy'));
});

test('plan: linux and droplet commands carry a remote-side timeout; mac ones do not claim one', () => {
  for (const t of TARGETS) for (const s of plan(t)) {
    if (t.flavour === 'linux' || t.flavour === 'droplet') assert.ok(s.remote.startsWith('timeout -k 5 60 bash -c \''), `${t.id}/${s.step}`);
    if (t.flavour === 'mac') assert.ok(!s.remote.includes('timeout'), `${t.id}/${s.step}`);
  }
});

test('plan: droplet unitprops separates unit records with a blank line (F4)', () => {
  const s = plan(TARGETS.find((t) => t.id === 'sfo3')).find((x) => x.step === 'unitprops');
  assert.equal((s.remote.match(/; echo/g) || []).length, 3);
});

test('sshArgs: flags first, then the allowlisted target, then exactly one remote string', () => {
  const t = TARGETS.find((x) => x.id === 'sfo3');
  const a = sshArgs(t, 'X');
  assert.deepEqual(a.slice(0, 10), ['-n', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', '-i']);
  assert.equal(a.at(-1), 'X');
  assert.equal(a.at(-2), 'root@143.110.224.247');
});

// ---------------- P1 / P5 ----------------
test('P1: hostile output never reaches a later command; issued commands equal the plan exactly', async () => {
  const tr = fakeTransport({ '*': { stdout: HOSTILE } });
  await collect({ transport: tr });
  const expected = TARGETS.flatMap((t) => plan(t).map((s) => ({ target: t.id, remote: s.remote })));
  assert.deepEqual(tr.issued.map(({ target, remote }) => ({ target, remote })), expected);
  for (const i of tr.issued) { assert.ok(!i.remote.includes('pwned')); assert.ok(!i.remote.includes('rm -rf')); }
});

test('P5: non-complete rows are recorded, classes are from the fixed set, and derive nothing', async () => {
  const tr = fakeTransport({ air: { cls: 'timeout', stdout: '1789264000\n' }, m1: { cls: 'error' }, lin: { cls: 'bogus' }, '*': { stdout: '1789264000\n' } });
  const r = await collect({ transport: tr });
  assert.ok(r.rows.filter((x) => x.target === 'air').every((x) => x.cls === 'timeout'));
  assert.equal(r.derived.air.clock, undefined);
  assert.ok(r.rows.filter((x) => x.target === 'm1').every((x) => x.cls === 'error'));
  assert.ok(r.rows.filter((x) => x.target === 'lin').every((x) => x.cls === 'error'), 'unknown class must not pass as an observation');
  assert.ok(r.rows.every((x) => CLASSES.includes(x.cls)));
});

// ---------------- P2 budgets ----------------
test('P2: wall budget exhausted => remaining rows skipped, transport not called (monotonic fake clock)', async () => {
  let t = 0;
  const clock = () => t;
  const tr = { issued: [], async run(target) { tr.issued.push(target.id); t += 100_000; return { stdout: '1789264000\n', stderr: '', code: 0, cls: 'complete', bytes: 11 }; } };
  const r = await collect({ transport: tr, budget: { ...BUDGET, wallMs: 250_000 }, clock });
  assert.equal(r.rows.filter((x) => x.cls !== 'skipped').length, 3);
  assert.equal(tr.issued.length, 3);
  assert.ok(r.rows.slice(3).every((x) => x.cls === 'skipped' && x.reason === 'wall budget'));
});

test('P2: output budget exhausted => remaining rows skipped', async () => {
  const big = 'x'.repeat(60_000);
  const tr = fakeTransport({ '*': { stdout: big } });
  const r = await collect({ transport: tr, budget: { ...BUDGET, totalBytes: 120_000 } });
  assert.equal(r.rows.filter((x) => x.cls !== 'skipped').length, 2);
  assert.ok(r.rows.slice(2).every((x) => x.cls === 'skipped' && x.reason === 'output budget'));
});

test('F3: the REMAINING run budget is handed to the transport for every command', async () => {
  let t = 0;
  const clock = () => t;
  const tr = { issued: [], async run(target, remote, limits) { tr.issued.push(limits); t += 1000; return { stdout: 'abcde', stderr: '', code: 0, cls: 'complete', bytes: 5 }; } };
  await collect({ transport: tr, budget: { ...BUDGET, wallMs: 10_000, totalBytes: 100 }, clock });
  assert.deepEqual(tr.issued[0], { ms: 10_000, bytes: 100 });
  assert.deepEqual(tr.issued[1], { ms: 9_000, bytes: 95 });
  assert.deepEqual(tr.issued[2], { ms: 8_000, bytes: 90 });
});

test('F3: realTransport enforces the run budget IN FLIGHT and classes the cut as clipped, not complete', async () => {
  // Aster\'s replay: wallMs 10 / totalBytes 3 must not yield a complete row.
  const byBytes = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'echo 12345'], o) });
  const r1 = await byBytes.run(TARGETS[0], 'x', { ms: 60_000, bytes: 3 });
  assert.equal(r1.cls, 'clipped'); assert.equal(Buffer.byteLength(r1.stdout), 3);
  const byTime = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'sleep 5; echo late'], o), budget: { ...BUDGET, killGraceMs: 100, confirmMs: 300 } });
  const r2 = await byTime.run(TARGETS[0], 'x', { ms: 10, bytes: 1000 });
  assert.equal(r2.cls, 'clipped'); assert.ok(!r2.stdout.includes('late'));
  assert.ok(r2.elapsedMs < 2000);
  // per-command limits, by contrast, class as truncated / timeout
  const perCmd = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'head -c 200000 /dev/zero | tr "\\0" y'], o), budget: { ...BUDGET, perCommandBytes: 1000 } });
  const r3 = await perCmd.run(TARGETS[0], 'x', { ms: 60_000, bytes: 1_000_000 });
  assert.equal(r3.cls, 'truncated'); assert.equal(r3.bytes, 1000);
});

test('F2: stderr is capped at ingestion and counted with stdout against the per-command bytes', async () => {
  const tr = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'head -c 100000 /dev/zero | tr "\\0" e 1>&2; echo out'], o), budget: { ...BUDGET, perCommandBytes: 65_536, stderrBytes: 4096 } });
  const r = await tr.run(TARGETS[0], 'x');
  assert.ok(Buffer.byteLength(r.stderr) <= 4096, `stderr ${Buffer.byteLength(r.stderr)}`);
  assert.equal(r.cls, 'truncated');                        // stderr overflow is not silent
  assert.ok(r.bytes <= 65_536);
  const small = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'echo warn 1>&2; echo out'], o) });
  const s = await small.run(TARGETS[0], 'x');
  assert.equal(s.cls, 'complete'); assert.equal(s.bytes, 9);
});

// ---------------- P3 cleanup ----------------
test('P3/F1: escalation reaches a TERM-resistant descendant; leader exit does not cancel SIGKILL; bystander untouched', async () => {
  // Leader sh exits at once; grandchild node ignores SIGTERM and would live 30 s. Only SIGKILL
  // to the GROUP ends it. Identified by its own pid (printed), never by scanning process names.
  const token = randomBytes(6).toString('hex');
  const grandchild = `node -e "process.on('SIGTERM',()=>{});console.log('PID='+process.pid);setTimeout(()=>{},30000)" ${token}`;
  const spawnFn = (_c, _a, o) => spawn('sh', ['-c', `${grandchild} & echo LEADER_DONE`], o);
  const tr = realTransport({ spawnFn, budget: { ...BUDGET, perCommandMs: 400, killGraceMs: 300, confirmMs: 500 } });
  const bystander = spawn('sleep', ['10']);
  const r = await tr.run(TARGETS[0], plan(TARGETS[0])[0].remote);
  const m = r.stdout.match(/PID=(\d+)/);
  assert.ok(m, `grandchild pid not seen in stdout: ${JSON.stringify(r.stdout)}`);
  const gpid = Number(m[1]);
  assert.equal(r.cls, 'timeout');
  assert.equal(r.cleanup, 'confirmed');
  assert.equal(tr._live.size, 0);
  assert.ok(r.elapsedMs < 3000, `took ${r.elapsedMs} ms`);
  await wait(100);
  assert.equal(pidAlive(gpid), false, 'TERM-resistant grandchild survived the group SIGKILL');
  assert.equal(bystander.exitCode, null, 'bystander was killed');
  bystander.kill('SIGKILL');
});

test('P3: an ordinary pipeline is also ended promptly on timeout', async () => {
  const spawnFn = (_c, _a, o) => spawn('sh', ['-c', 'sleep 30; echo late'], o);
  const tr = realTransport({ spawnFn, budget: { ...BUDGET, perCommandMs: 300, killGraceMs: 200, confirmMs: 300 } });
  const r = await tr.run(TARGETS[0], 'x');
  assert.equal(r.cls, 'timeout'); assert.equal(r.cleanup, 'confirmed'); assert.ok(r.elapsedMs < 2000); assert.ok(!r.stdout.includes('late'));
});

test('P3: clean exit is complete, nonzero exit is error, and neither leaves a live group', async () => {
  const ok = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'echo 1789264000'], o) });
  assert.equal((await ok.run(TARGETS[0], 'x')).cls, 'complete'); assert.equal(ok._live.size, 0);
  const bad = realTransport({ spawnFn: (_c, _a, o) => spawn('sh', ['-c', 'exit 3'], o) });
  const r = await bad.run(TARGETS[0], 'x');
  assert.equal(r.cls, 'error'); assert.equal(r.code, 3); assert.equal(bad._live.size, 0);
});

// ---------------- derivations ----------------
test('derive: clock is an interval containing quantization and the whole ssh round trip', () => {
  const t0 = 1_789_264_000_400, t1 = 1_789_264_002_900;   // 2.5 s round trip
  const d = deriveClock('host\nDarwin 25.6.0\n 1:00 up 7 days\n1789264001\n', t0, t1);
  assert.equal(d.remoteSec, 1789264001);
  assert.ok(d.offsetLo <= d.offsetHi);
  assert.ok(Math.abs(d.offsetLo - (1789264001 - 1789264002.9)) < 1e-6);
  assert.ok(Math.abs(d.offsetHi - (1789264002 - 1789264000.4)) < 1e-6);
  assert.equal(deriveClock('garbage', t0, t1).cls, 'unknown');
});

test('derive: procs rows carry only pid/lstart/etime/rss/cpu; unparsable lines are kept opaque', () => {
  const rows = deriveProcs('  123 Sat Sep 12 21:10:03 2026  1-02:03:04 45678 12.5\nweird line\n');
  assert.deepEqual(rows[0], { pid: 123, lstart: 'Sat Sep 12 21:10:03 2026', etime: '1-02:03:04', rssKiB: 45678, cpuPct: 12.5 });
  assert.deepEqual(rows[1], { unparsed: 'weird line' });
});

test('F4: unit records parse in any property order, blank-line delimited, with required fields checked', () => {
  const idFirst = 'Id=axona-relay@useast.service\nMainPID=42\nActiveState=active\n\nId=axona-relay@uswest.service\nMainPID=0\nActiveState=inactive\n';
  const idLast  = 'MainPID=42\nActiveState=active\nId=axona-relay@useast.service\n\nActiveState=inactive\nMainPID=0\nId=axona-relay@uswest.service\n\n';
  const a = deriveUnitProps(idFirst), b = deriveUnitProps(idLast);
  assert.equal(a.length, 2); assert.equal(b.length, 2);
  assert.deepEqual(b[0], { MainPID: '42', ActiveState: 'active', Id: 'axona-relay@useast.service' });
  assert.equal(b[1].ActiveState, 'inactive');
  const partial = deriveUnitProps('MainPID=7\nSubState=running\n');
  assert.deepEqual(partial[0].incomplete, ['Id', 'ActiveState']);
  assert.deepEqual(deriveUnitProps('\n\n'), []);
});

test('derive: commit and count', () => {
  assert.equal(deriveCommit('c3f2aa0'.padEnd(40, '0') + '\n'), 'c3f2aa0'.padEnd(40, '0'));
  assert.equal(deriveCommit('not a sha'), null);
  assert.equal(deriveCount(' 7\n'), 7); assert.equal(deriveCount(HOSTILE), null);
});
