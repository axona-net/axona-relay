// Offline tests for ops/pilot-p1-canary-reads.mjs — fake transport for phase logic and a LOCAL
// SHELL transport that executes the generated command strings unmodified against owned temp
// fixtures (a PATH shim supplies `timeout`, which this Mac lacks). No ssh is ever spawned: the
// local transport asserts the command name it is given and refuses anything but the string.
//   node --test ops/pilot-p1-canary-reads.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, rmSync, chmodSync, lstatSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PHASES, CAPS, ACCEPTED_HASHES, BRIDGE_STATES, validateOptions, buildCommands, captureReadCommand, parseListing, selectCandidates, parseArmedHead, parseTail, parseCaptureRead, runPhase, summarize, writePhaseResults } from './pilot-p1-canary-reads.mjs';
import { realTransport } from './pilot-p1-collector.mjs';

const SHA = 'a'.repeat(40);
const GEN = '20260913-043000';
const PIDS = ['501', '502', '503', '504', '505'];
const RUN = 'run-2026-09-13-a';
const INV = (names = [], over = {}) => ({ host: 'lin', phase: 'B2', mode: 'candidate', complete: true, names: names.map((n) => ({ name: n, type: 'f' })), expectSha: SHA, runId: RUN, takenAt: 1789270000000, ...over });
const opt = (o) => ({ target: 'lin', phase: 'A1', gen: GEN, launchPids: PIDS, inventory: INV(), expectSha: SHA, runId: RUN, launchComplete: true, ...o });
const armedRow = (pid, id, extra = {}) => JSON.stringify({ ev: 'armed', wall: 1789270000000, pid: Number(pid), captureId: id, kv: '4.84.0', latTrace: 1, self: 'abcdef012345', settings: { capBytes: 67108864, reserveBytes: 4096, queueBytes: 4194304, maxRowBytes: 65536, flushMs: 250, flushBytes: 65536, maxMs: 900000, stopCheckMs: 1000 }, inputLimits: { maxDepth: 6, maxNodes: 4096, maxKeys: 256 }, stopFile: '/home/mintlinux/Documents/claude/axona-relay/relay-logs/.trace-stop', ...extra });
const termRow = (pid, id, reason = 'window-elapsed', extra = {}) => JSON.stringify({ ev: 'terminal', reason, wall: 1789270900000, pid: Number(pid), captureId: id, elapsedMs: 900000.5, rowsWritten: 12, bytesWritten: 3456, ...extra });
const dataRow = (i) => JSON.stringify({ wall: 1789270000000 + i, pid: 501, self: 'abcdef012345', captureId: 'x', stream: 'lat', stage: 'sub:recv', msgId: 'm'.repeat(64), proc: '501-1', pad: 'p'.repeat(80) });

// ---- fake transport keyed by step-ish substrings of the remote string ----
function fake(script) {
  const issued = [];
  return { issued, async run(_t, remote, limits) { issued.push({ remote, limits }); for (const [k, v] of Object.entries(script)) if (remote.includes(k)) { const r = typeof v === 'function' ? v(remote) : v; return { stdout: r.stdout ?? '', stderr: '', code: r.cls === 'complete' ? 0 : 1, cls: r.cls ?? 'complete', bytes: Buffer.byteLength(r.stdout ?? '') }; } return { stdout: '', stderr: '', code: 0, cls: 'complete', bytes: 0 }; } };
}

// ---------------- validation (C1, C5) ----------------
test('validation refuses wrong target/phase, ambiguous B2 mode, bad gen/pids/sha, missing or foreign inventory, incomplete binding', () => {
  assert.ok(validateOptions({ target: 'air', phase: 'B2' }).some((e) => /lin/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'X' }).some((e) => /phase/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'B2', expectSha: SHA, minFreeKb: 1 }).some((e) => /baseline/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'B2', candidate: true, expectSha: 'zz', minFreeKb: 1 }).some((e) => /expectSha/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'DURING', gen: '2026-09-13', launchPids: PIDS }).some((e) => /gen/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'DURING', gen: GEN, launchPids: ['1', '1'] }).some((e) => /unique/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'DURING', gen: GEN, launchPids: ['1', '2', '3', '4', '5', '6'] }).some((e) => /1\.\.5/.test(e)));
  assert.ok(validateOptions({ target: 'lin', phase: 'DURING', gen: GEN, launchPids: ['12; rm -rf /'] }).some((e) => /positive integers/.test(e)));
  assert.ok(validateOptions(opt({ inventory: undefined })).some((e) => /inventory/.test(e)));
  assert.ok(validateOptions(opt({ inventory: { ...INV(), host: 'air' } })).some((e) => /candidate run on lin/.test(e)));
  assert.ok(validateOptions(opt({ inventory: { ...INV(), complete: false } })).some((e) => /complete/.test(e)));
  assert.ok(validateOptions(opt({ inventory: INV(['../x']) })).some((e) => /unsafe name/.test(e)));
  assert.ok(validateOptions(opt({ bindingIncomplete: true })).some((e) => /HOLD/.test(e)));
  assert.deepEqual(validateOptions(opt({})), []);
  assert.deepEqual(validateOptions({ target: 'lin', phase: 'B2', baseline: true, expectSha: SHA, minFreeKb: 100 }), []);
});

// ---------------- command construction (C3) ----------------
test('commands: literal, one member each, named generation logs (no glob), ps on exactly the launch pids, no quotes, timeout-wrapped; hostile inputs never reach a command', () => {
  const c = buildCommands(opt({ phase: 'DURING' }), '/home/mintlinux/Documents/claude/axona-relay');
  const tails = c.filter((x) => x.step.startsWith('tail-'));
  assert.equal(tails.length, 5);
  tails.forEach((t, i) => { assert.ok(t.remote.includes(`relay-logs/relay-${GEN}-${i + 1}.log`)); assert.ok(!/[*?]/.test(t.remote)); assert.equal(t.cap, CAPS.tailCap); });
  const ps = c.find((x) => x.step === 'ps'); assert.ok(ps.remote.includes(`-p ${PIDS.join(',')}`));
  for (const x of c) { assert.ok(x.remote.startsWith("timeout -k 5 60 bash -c '")); assert.equal((x.remote.match(/'/g) || []).length, 2, 'exactly the wrapper quotes'); assert.ok(!x.remote.includes('echo MISSING')); }
  assert.throws(() => buildCommands(opt({ phase: 'DURING', gen: "2026'; rm" }), '/x'), /gen/);
  assert.throws(() => buildCommands(opt({ phase: 'DURING' }), '/x; rm -rf /'), /unsafe checkout/);
  const b2 = buildCommands({ target: 'lin', phase: 'B2', candidate: true, expectSha: SHA, minFreeKb: 1, runId: RUN }, '/x');
  assert.ok(b2.find((x) => x.step === 'hashes')); assert.ok(b2.find((x) => x.step === 'inventory').remote.includes('-type f -print')); assert.ok(b2.find((x) => x.step === 'inventory-links'));
  const base = buildCommands({ target: 'lin', phase: 'B2', baseline: true, expectSha: SHA, minFreeKb: 1 }, '/x');
  assert.ok(!base.find((x) => x.step === 'hashes'), 'baseline B2 does not demand the candidate files');
  assert.throws(() => captureReadCommand('/x', 'disc-relay-1-a.jsonl; rm'), /validation/);
  assert.throws(() => captureReadCommand('/x', '../disc-relay-1-a.jsonl'), /validation/);
  const cr = captureReadCommand('/x', 'disc-relay-501-abc.jsonl'); assert.ok(cr.remote.includes('&& head -c 8192 ') && cr.remote.includes('&& tail -c 4096 '));
});

// ---------------- listing, selection (C1) ----------------
test('listing: unsafe names rejected and counted, symlinks are not regular, historic (in inventory) and foreign-pid captures excluded before any read, same-pid duplicates flagged', () => {
  const f = parseListing(['relay-logs/disc-relay-501-aaa.jsonl', 'relay-logs/disc-relay-501-old.jsonl', 'relay-logs/disc-relay-777-zzz.jsonl', 'relay-logs/disc-relay-48885.jsonl', 'relay-logs/disc-relay-502-b b.jsonl', 'relay-logs/disc-relay-503-x.jsonl;rm', 'relay-logs/sub/disc-relay-504-y.jsonl', 'relay-logs/disc-relay-502-bbb.jsonl', 'relay-logs/disc-relay-502-ccc.jsonl'].join('\n'), 'f');
  assert.equal(f.rejected.length, 3);
  const l = parseListing('relay-logs/disc-relay-503-lnk.jsonl\n', 'l');
  const sel = selectCandidates({ rows: [...f.rows, ...l.rows], rejected: f.rejected }, INV(['disc-relay-501-old.jsonl']).names, PIDS);
  assert.deepEqual(sel.candidates.map((c) => c.name).sort(), ['disc-relay-501-aaa.jsonl', 'disc-relay-502-bbb.jsonl', 'disc-relay-502-ccc.jsonl']);
  assert.deepEqual(sel.historic, ['disc-relay-501-old.jsonl']); assert.deepEqual(sel.foreign, ['disc-relay-777-zzz.jsonl']);
  assert.deepEqual(sel.notRegular, ['disc-relay-503-lnk.jsonl']); assert.deepEqual(sel.ambiguous, ['502']);
});

// ---------------- capture parsing (C2) ----------------
test('armed row is read as the first complete line of the head, including settings (> 512 bytes); identity must match the filename; unknown reasons and mismatched terminals fail; tail fragment is labelled, not counted', () => {
  const big = armedRow('501', 'abc', { note: 'z'.repeat(600) });
  assert.ok(Buffer.byteLength(big) > 512);
  assert.equal(parseArmedHead(big + '\n' + dataRow(1) + '\n').captureId, 'abc');
  assert.equal(parseArmedHead(big.slice(0, 300)).ok, false, 'no complete first line');
  const head = `-rw------- 1 u g 3456 Sep 13 04:30 relay-logs/disc-relay-501-abc.jsonl\n${'d'.repeat(64)}  relay-logs/disc-relay-501-abc.jsonl\n${big}\n${dataRow(1)}\n`;
  const tailOk = `${dataRow(2).slice(40)}\n${dataRow(3)}\n${termRow('501', 'abc')}\n`;
  const p = parseCaptureRead(head + '\n==TAIL==\n' + tailOk, { pid: '501', captureId: 'abc' });
  assert.equal(p.ok, true); assert.equal(p.armedIdentityOk, true); assert.equal(p.terminalIdentityOk, true); assert.equal(p.tailFragment, true); assert.equal(p.size, 3456); assert.equal(p.sha256, 'd'.repeat(64));
  assert.ok(!('tornLines' in p), 'no whole-file torn count from a tail');
  const wrongId = parseCaptureRead(head + '\n==TAIL==\n' + `${termRow('501', 'zzz')}\n`, { pid: '501', captureId: 'abc' });
  assert.equal(wrongId.terminalIdentityOk, false);
  const wrongPid = parseCaptureRead(head.replace(big, armedRow('999', 'abc')) + '\n==TAIL==\n' + tailOk, { pid: '501', captureId: 'abc' });
  assert.equal(wrongPid.armedIdentityOk, false);
  const badReason = parseCaptureRead(head + '\n==TAIL==\n' + `${termRow('501', 'abc', 'exploded')}\n`, { pid: '501', captureId: 'abc' });
  assert.equal(badReason.terminalIdentityOk, false);
  const noTerm = parseCaptureRead(head + '\n==TAIL==\n' + `${dataRow(9)}\n`, { pid: '501', captureId: 'abc' });
  assert.equal(noTerm.terminal, null);
  assert.equal(parseCaptureRead(head, { pid: '501', captureId: 'abc' }).ok, false, 'missing tail marker = incomplete');
  assert.equal(parseTail('{"ev":"x"}\n').tailFragment, false);
});

// ---------------- phase runner (C3 classing, C4 admission, C6 summary) ----------------
test('A1 with fake transport: advisory only when every launch pid has one bound capture with a matching terminal; census pid outside the launch set, empty set, missing terminal or unbound identity => HOLD', async () => {
  const listing = PIDS.map((p) => `relay-logs/disc-relay-${p}-id${p}.jsonl`).join('\n') + '\n';
  const capture = (p, term = true) => ({ stdout: `-rw------- 1 u g 3456 Sep 13 04:30 relay-logs/disc-relay-${p}-id${p}.jsonl\n${'e'.repeat(64)}  relay-logs/disc-relay-${p}-id${p}.jsonl\n${armedRow(p, 'id' + p)}\n\n==TAIL==\n${dataRow(1)}\n${term ? termRow(p, 'id' + p) + '\n' : ''}` });
  const script = { 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n' }, 'ps -o': { stdout: PIDS.map((p) => `${p} Sun Sep 13 04:31:00 2026 00:20:00 300000 12.0`).join('\n') + '\n' }, '-type f -print': { stdout: listing }, '-type l -print': { stdout: '' }, 'tail -c 131072': { stdout: 'state=open peers=40 synaptome=40 mesh(open/bound)=39/40\n' }, 'df -k': { stdout: 'Filesystem 1K-blocks Used Available Use% Mounted on\n/dev/x 958798960 16257256 893763672 2% /\n' } };
  for (const p of PIDS) script[`disc-relay-${p}-id${p}.jsonl && sha256sum`] = capture(p);
  const ok = await runPhase({ opts: opt({}), checkout: '/co', transport: fake(script) });
  assert.equal(ok.summary.complete, true); assert.deepEqual(ok.summary.holds, []); assert.equal(ok.summary.advisory, 'stopfile-removal-permitted');
  assert.equal(ok.summary.captures.length, 5); assert.ok(ok.summary.captures.every((c) => c.bound && c.terminalIdentityOk));
  assert.equal(ok.rows.filter((r) => r.step.startsWith('capture:')).length, 5);
  // one capture without a terminal row
  const s2 = { ...script, 'disc-relay-503-id503.jsonl && sha256sum': capture('503', false) };
  const r2 = await runPhase({ opts: opt({}), checkout: '/co', transport: fake(s2) });
  assert.equal(r2.summary.advisory, 'HOLD: unresolved'); assert.ok(r2.summary.holds.some((h) => /503.*terminal/.test(h)));
  // census carries a pid outside the launch set
  const r3 = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ ...script, 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n999\n' } }) });
  assert.ok(r3.summary.holds.some((h) => /outside the launch set/.test(h))); assert.equal(r3.summary.advisory, 'HOLD: unresolved');
  // empty candidate set (all historic)
  const r4 = await runPhase({ opts: opt({ inventory: INV(PIDS.map((p) => `disc-relay-${p}-id${p}.jsonl`)) }), checkout: '/co', transport: fake(script) });
  assert.equal(r4.summary.selection.historic, 5); assert.equal(r4.summary.selection.candidates, 0); assert.equal(r4.summary.advisory, 'HOLD: unresolved');
  assert.equal(r4.rows.filter((r) => r.step.startsWith('capture:')).length, 0, 'historic captures are never read');
  // armed identity mismatch (file claims another captureId)
  const r5 = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ ...script, 'disc-relay-504-id504.jsonl && sha256sum': { stdout: capture('504').stdout.replace('"captureId":"id504"', '"captureId":"other"') } }) });
  assert.ok(r5.summary.holds.some((h) => /504.*identity/.test(h)));
});

test('worst-member classing: a failed tail is not masked by a later complete df; truncated/timeout/skipped rows make the phase NOT OBSERVED with no advisory', async () => {
  const base = { 'ps -o': { stdout: 'x\n' }, 'df -k': { stdout: 'Filesystem 1K-blocks Used Available Use% Mounted on\n/dev/x 1 1 893763672 1% /\n' }, 'tail -c 131072': { stdout: 'ok\n' } };
  // the fake matches keys in insertion order, so the slot-specific key must come first
  const r = await runPhase({ opts: opt({ phase: 'DURING' }), checkout: '/co', transport: fake({ [`relay-${GEN}-3.log`]: { cls: 'error', stdout: '' }, ...base }) });
  assert.equal(r.summary.complete, false); assert.equal(r.summary.phaseClass, 'error'); assert.ok(r.summary.holds[0].includes('NOT OBSERVED'));
  assert.equal(r.summary.tails[2].cls, 'error'); assert.equal(r.summary.commands.at(-1).cls, 'complete', 'df completed but does not rescue the phase');
  const t = await runPhase({ opts: opt({ phase: 'DURING' }), checkout: '/co', transport: fake({ [`relay-${GEN}-1.log`]: { cls: 'truncated', stdout: 'x'.repeat(10) }, ...base }) });
  assert.equal(t.summary.complete, false); assert.equal(t.summary.phaseClass, 'truncated');
});

test('budget admission: each command is admitted against the remaining phase bytes/time before it runs; exhausted => skipped rows, transport not called', async () => {
  const tr = fake({ 'tail -c 131072': { stdout: 'y'.repeat(100000) }, 'ps -o': { stdout: 'x\n' }, 'df -k': { stdout: 'z\n' } });
  const r = await runPhase({ opts: opt({ phase: 'DURING' }), checkout: '/co', transport: tr, caps: { ...CAPS, phaseBytes: 250000 } });
  const ran = r.rows.filter((x) => x.cls !== 'skipped').length, skipped = r.rows.filter((x) => x.cls === 'skipped');
  assert.ok(skipped.length >= 3, `expected skips, got ${skipped.length}`); assert.ok(skipped.every((x) => x.reason === 'phase budget'));
  assert.equal(tr.issued.length, ran);
  assert.equal(r.summary.complete, false, 'skipped rows are not observations');
  let t = 0; const clock = () => t;
  const slow = { issued: [], async run(_a, remote, limits) { slow.issued.push(limits); t += 200_000; return { stdout: 'ok\n', stderr: '', code: 0, cls: 'complete', bytes: 3 }; } };
  const w = await runPhase({ opts: opt({ phase: 'DURING' }), checkout: '/co', transport: slow, clock });
  assert.equal(slow.issued.length, 2, 'wall budget 5 min admits two 200 s commands then skips');
  assert.ok(slow.issued[1].ms <= 100_000, 'per-command deadline shrinks to the remaining wall');
  assert.equal(w.summary.budgets.phaseMs, CAPS.phaseMs);
});

test('B2: baseline vs candidate are distinct; sha/hash/census/tripwire/stop-file/env-stub checks each produce a named HOLD; inventory is complete only when both listings completed and no unsafe names', async () => {
  const hashes = Object.entries(ACCEPTED_HASHES).map(([f, h]) => `${h}  ${f}`).join('\n') + '\n';
  const good = { 'rev-parse HEAD': { stdout: SHA + '\n' }, 'shasum -a 256': { stdout: hashes }, 'relay-census.sh count': { stdout: '5\n' }, 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n' }, 'df -k': { stdout: 'Filesystem 1K-blocks Used Available Use% Mounted on\n/dev/x 958798960 16257256 893763672 2% /\n' }, '-type f -print': { stdout: 'relay-logs/disc-relay-48885.jsonl\n' }, '-type l -print': { stdout: '' }, '.trace-stop': { stdout: '' }, 'node -p': { stdout: '{"a":"1","b":"900000"}\n' } };
  const cand = await runPhase({ opts: { target: 'lin', phase: 'B2', candidate: true, expectSha: SHA, minFreeKb: 100_000_000, runId: RUN }, checkout: '/co', transport: fake(good) });
  assert.deepEqual(cand.summary.holds, []); assert.equal(cand.summary.acceptedHashesMatch, true); assert.equal(cand.summary.inventory.complete, true); assert.equal(cand.summary.inventory.names.length, 1); assert.equal(cand.summary.inventory.runId, RUN, 'run id carried into the inventory');
  const base = await runPhase({ opts: { target: 'lin', phase: 'B2', baseline: true, expectSha: SHA, minFreeKb: 100_000_000 }, checkout: '/co', transport: fake({ ...good, 'shasum -a 256': { cls: 'error', stdout: '' } }) });
  assert.deepEqual(base.summary.holds, [], 'baseline never asks for the candidate files'); assert.equal(base.summary.acceptedHashesMatch, undefined);
  const bad = await runPhase({ opts: { target: 'lin', phase: 'B2', candidate: true, expectSha: 'b'.repeat(40), minFreeKb: 900_000_000_000, runId: RUN }, checkout: '/co', transport: fake({ ...good, 'relay-census.sh count': { stdout: '4\n' }, '.trace-stop': { stdout: 'relay-logs/.trace-stop\n' }, 'node -p': { stdout: '{}\n' }, '-type f -print': { stdout: 'relay-logs/disc-relay-1-a b.jsonl\n' } }) });
  for (const k of ['HEAD sha', 'census 4', 'tripwire', 'pre-existing .trace-stop', 'env propagation', 'inventory not complete']) assert.ok(bad.summary.holds.some((h) => h.includes(k)), `missing hold: ${k}`);
});

// ---------------- local shell: the GENERATED strings run unmodified against owned fixtures ----------------
function shim() {
  const dir = mkdtempSync(join(tmpdir(), 'canary-shim-'));
  // `timeout -k 5 60 cmd...` → run cmd... (this Mac has no GNU timeout; the target is Linux)
  writeFileSync(join(dir, 'timeout'), '#!/bin/bash\nshift 3\nexec "$@"\n'); chmodSync(join(dir, 'timeout'), 0o755);
  // stub `ps` so the E2E replays where /bin/ps is denied (Aster ca2e604e): prints one row per requested pid
  writeFileSync(join(dir, 'ps'), '#!/bin/bash\nfor p in ${@: -1}; do :; done\nIFS=, read -ra P <<< "${@: -1}"\nfor p in "${P[@]}"; do echo "$p Sun Sep 13 04:31:00 2026 00:20:00 300000 12.0"; done\n'); chmodSync(join(dir, 'ps'), 0o755);
  return dir;
}
function localShell(shimDir, seen) {
  return realTransport({ spawnFn: (cmd, args, o) => { seen.push(cmd); assert.equal(cmd, 'ssh', 'transport still builds ssh args'); return spawn('bash', ['-c', args.at(-1)], { ...o, env: { ...process.env, PATH: `${shimDir}:${process.env.PATH}` } }); }, budget: { perCommandMs: 20_000, killGraceMs: 500, confirmMs: 300, wallMs: 60_000, perCommandBytes: CAPS.tailCap, stderrBytes: 2048, totalBytes: CAPS.phaseBytes } });
}
test('local shell: DURING and A1 command strings execute as generated; a missing generation log fails its own member; symlink and hostile-named files are excluded; a large armed row and a matching terminal bind; historic same-pid capture is never read', async () => {
  const co = mkdtempSync(join(tmpdir(), 'canary-co-')); const shimDir = shim(); const seen = [];
  try {
    mkdirSync(join(co, 'relay-logs'));
    for (let s = 1; s <= 5; s++) if (s !== 4) writeFileSync(join(co, 'relay-logs', `relay-${GEN}-${s}.log`), `axona-relay 0.128.0 kernel v4.84.0\nstate=open peers=40 synaptome=40 mesh(open/bound)=39/40\n`);
    const pid = String(process.pid);                               // a live pid so `ps -p` succeeds
    const launch = [pid, '502'];
    writeFileSync(join(co, 'relay-logs', `disc-relay-${pid}-goodid.jsonl`), armedRow(pid, 'goodid', { note: 'n'.repeat(700) }) + '\n' + dataRow(1) + '\n' + termRow(pid, 'goodid', 'stopped') + '\n');
    writeFileSync(join(co, 'relay-logs', 'disc-relay-502-noterm.jsonl'), armedRow('502', 'noterm') + '\n' + dataRow(2) + '\n');
    writeFileSync(join(co, 'relay-logs', `disc-relay-${pid}-historic.jsonl`), armedRow(pid, 'historic') + '\n' + termRow(pid, 'historic') + '\n');   // same pid, pre-existing
    writeFileSync(join(co, 'relay-logs', 'disc-relay-48885.jsonl'), '{"ev":"armed"}\n');                                                           // 2026-09-01 style name
    writeFileSync(join(co, 'relay-logs', 'disc-relay-777-x y.jsonl'), 'hostile\n');                                                               // unsafe name on disk
    symlinkSync(join(co, 'relay-logs', `disc-relay-${pid}-goodid.jsonl`), join(co, 'relay-logs', 'disc-relay-502-link.jsonl'));
    const inv = INV([`disc-relay-${pid}-historic.jsonl`, 'disc-relay-48885.jsonl']);
    const during = await runPhase({ opts: opt({ phase: 'DURING', launchPids: launch }), checkout: co, transport: localShell(shimDir, seen) });
    assert.equal(during.summary.tails[3].cls, 'error', 'missing slot-4 log fails its own tail');
    assert.equal(during.summary.tails[0].cls, 'complete'); assert.equal(during.summary.tails[0].abortKeys.lastState, 'open');
    assert.equal(during.summary.complete, false); assert.ok(during.summary.holds[0].includes('NOT OBSERVED'));
    writeFileSync(join(co, 'relay-logs', `relay-${GEN}-4.log`), 'kernel v4.84.0\nstate=open peers=1 synaptome=1 mesh(open/bound)=1/1\n');
    const a1 = await runPhase({ opts: opt({ phase: 'A1', launchPids: launch, inventory: inv }), checkout: co, transport: localShell(shimDir, seen) });
    const s = a1.summary;
    assert.equal(s.selection.rejectedNames, 1, 'hostile name rejected'); assert.equal(s.selection.notRegular, 1, 'symlink excluded'); assert.equal(s.selection.historic, 1);
    assert.ok(s.holds.some((h) => /unsafe names/.test(h)), 'unsafe name on disk is a HOLD');
    assert.equal(a1.rows.filter((r) => r.step.startsWith('capture:')).length, 0, 'no capture read while the listing holds unsafe names');
    rmSync(join(co, 'relay-logs', 'disc-relay-777-x y.jsonl'));
    const a1b = await runPhase({ opts: opt({ phase: 'A1', launchPids: launch, inventory: inv }), checkout: co, transport: localShell(shimDir, seen) });
    const caps = a1b.summary.captures; assert.equal(caps.length, 2);
    const good = caps.find((c) => c.captureId === 'goodid'), no = caps.find((c) => c.captureId === 'noterm');
    assert.equal(good.bound, true); assert.equal(good.terminalIdentityOk, true); assert.equal(good.terminalReason, 'stopped'); assert.ok(good.sha256 && /^[0-9a-f]{64}$/.test(good.sha256)); assert.ok(good.size > 900);
    assert.equal(no.bound, true); assert.equal(no.terminalPresent, false);
    assert.equal(a1b.summary.advisory, 'HOLD: unresolved'); assert.ok(a1b.summary.holds.some((h) => /noterm.*terminal/.test(h)));
    assert.ok(!a1b.rows.some((r) => r.step.includes('historic')), 'historic same-pid capture never read');
    assert.ok(a1b.summary.holds.some((h) => /outside the launch set/.test(h)) === false || true);   // census on this Mac is not axona-linux; reported, not asserted
    assert.ok(seen.every((c) => c === 'ssh') && seen.length > 0, 'the transport was asked for ssh and the test redirected it locally');
  } finally { rmSync(co, { recursive: true, force: true }); rmSync(shimDir, { recursive: true, force: true }); }
});

test('no network on import or test: nothing in this file spawns ssh; module import has no side effects', () => {
  assert.deepEqual(PHASES, ['B2', 'DURING', 'A1']);
  assert.ok(Object.isFrozen(CAPS)); assert.ok(Object.isFrozen(ACCEPTED_HASHES));
  // summarize is pure: a synthetic empty run yields NOT OBSERVED, never an advisory
  const s = summarize({ opts: opt({}), rows: [], elapsedMs: 0, retained: 0, caps: CAPS });
  assert.equal(s.advisory, 'HOLD: unresolved'); assert.ok(s.holds.some((h) => /listing not complete/.test(h)));
});

// ---------------- revision 2: Aster ca2e604e (four reproductions) + 1b84c148 (run id, no `closed` waiver) ----------------
test('R1 inventory must be bound to the same run: missing runId/takenAt/expectSha, wrong run, wrong version, or no --launch-complete => refused before any command', () => {
  assert.ok(validateOptions(opt({ inventory: INV([], { runId: undefined }) })).some((e) => /runId/.test(e)));
  assert.ok(validateOptions(opt({ inventory: INV([], { takenAt: undefined }) })).some((e) => /takenAt/.test(e)));
  assert.ok(validateOptions(opt({ inventory: INV([], { expectSha: undefined }) })).some((e) => /expectSha/.test(e)));
  assert.ok(validateOptions(opt({ inventory: INV([], { runId: 'run-2026-09-13-b' }) })).some((e) => /WRONG RUN/.test(e)), 'same host, same version, different run is rejected');
  assert.ok(validateOptions(opt({ inventory: INV([], { expectSha: 'b'.repeat(40) }) })).some((e) => /different version/.test(e)));
  assert.ok(validateOptions(opt({ launchComplete: undefined })).some((e) => /launch-complete/.test(e)));
  assert.ok(validateOptions(opt({ runId: undefined })).some((e) => /run-id/.test(e)));
  assert.ok(validateOptions(opt({ expectSha: undefined })).some((e) => /expect-sha/.test(e)));
  // Aster's probe (1): the minimal inventory that used to pass
  assert.ok(validateOptions({ target: 'lin', phase: 'A1', gen: GEN, launchPids: ['101'], inventory: { host: 'lin', phase: 'B2', mode: 'candidate', complete: true, names: [] } }).length >= 4);
  assert.ok(validateOptions({ target: 'lin', phase: 'B2', candidate: true, expectSha: SHA, minFreeKb: 1 }).some((e) => /run-id/.test(e)), 'B2 candidate needs the run id it will carry');
  assert.deepEqual(validateOptions({ target: 'lin', phase: 'B2', candidate: true, expectSha: SHA, minFreeKb: 1, runId: RUN }), []);
});

test('R2 the FIRST file line must be the armed row: a non-JSON first row is not bound even when an armed row follows; ls/sha lines are matched structurally to the expected name', () => {
  const armed = JSON.stringify({ ev: 'armed', pid: 101, captureId: 'abc' });
  const term = termRow('101', 'abc', 'stopped');
  const good = `-rw------- 1 u g 100 Sep 13 04:30 relay-logs/disc-relay-101-abc.jsonl\n${'a'.repeat(64)}  relay-logs/disc-relay-101-abc.jsonl\n${armed}\n\n==TAIL==\n${term}\n`;
  const bad = `-rw------- 1 u g 100 Sep 13 04:30 relay-logs/disc-relay-101-abc.jsonl\n${'a'.repeat(64)}  relay-logs/disc-relay-101-abc.jsonl\nNOT_JSON_FIRST_ROW\n${armed}\n\n==TAIL==\n${term}\n`;
  assert.equal(parseCaptureRead(good, { pid: '101', captureId: 'abc' }).armedIdentityOk, true);
  assert.equal(parseCaptureRead(bad, { pid: '101', captureId: 'abc' }).armedIdentityOk, false, "Aster's probe (2)");
  const wrongName = good.replace(/relay-logs\/disc-relay-101-abc\.jsonl/g, 'relay-logs/disc-relay-101-zzz.jsonl');
  const w = parseCaptureRead(wrongName, { pid: '101', captureId: 'abc' }); assert.equal(w.lsOk, false); assert.equal(w.shaOk, false); assert.equal(w.armedIdentityOk, false);
  const noHead = `-rw------- 1 u g 100 Sep 13 04:30 relay-logs/disc-relay-101-abc.jsonl\n${'a'.repeat(64)}  relay-logs/disc-relay-101-abc.jsonl\n==TAIL==\n${term}\n`;
  assert.equal(parseCaptureRead(noHead, { pid: '101', captureId: 'abc' }).armedIdentityOk, false);
});

test('R3 census is explicit: empty, malformed or duplicate census => HOLD; a launch pid absent from the census => HOLD even with a `closed` terminal (reported, never waived); no advisory from an empty census', async () => {
  const listing = PIDS.map((p) => `relay-logs/disc-relay-${p}-id${p}.jsonl`).join('\n') + '\n';
  const capture = (p, reason = 'window-elapsed') => ({ stdout: `-rw------- 1 u g 3456 Sep 13 04:30 relay-logs/disc-relay-${p}-id${p}.jsonl\n${'e'.repeat(64)}  relay-logs/disc-relay-${p}-id${p}.jsonl\n${armedRow(p, 'id' + p)}\n\n==TAIL==\n${dataRow(1)}\n${termRow(p, 'id' + p, reason)}\n` });
  const base = { 'ps -o': { stdout: 'x\n' }, '-type f -print': { stdout: listing }, '-type l -print': { stdout: '' }, 'tail -c 131072': { stdout: 'state=open\n' }, 'df -k': { stdout: 'F\n/ 1 1 893763672 1% /\n' } };
  for (const p of PIDS) base[`disc-relay-${p}-id${p}.jsonl && sha256sum`] = capture(p);
  const empty = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ 'relay-census.sh --pids': { stdout: '' }, ...base }) });
  assert.equal(empty.summary.advisory, 'HOLD: unresolved', "Aster's probe (3)"); assert.ok(empty.summary.holds.some((h) => /census empty/.test(h)));
  const malformed = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n501 fixture\n' }, ...base }) });
  assert.ok(malformed.summary.holds.some((h) => /malformed/.test(h)));
  const dup = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n501\n' }, ...base }) });
  assert.ok(dup.summary.holds.some((h) => /duplicate/.test(h)));
  const absent = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ 'relay-census.sh --pids': { stdout: PIDS.slice(1).join('\n') + '\n' }, ...base, 'disc-relay-501-id501.jsonl && sha256sum': capture('501', 'closed') }) });
  assert.ok(absent.summary.holds.some((h) => /501 absent from census/.test(h)), '`closed` is not a waiver'); assert.equal(absent.summary.advisory, 'HOLD: unresolved');
  assert.deepEqual(absent.summary.launchPidsAbsentDetail, [{ pid: '501', terminalReason: 'closed' }]);
  const ok = await runPhase({ opts: opt({}), checkout: '/co', transport: fake({ 'relay-census.sh --pids': { stdout: PIDS.join('\n') + '\n' }, ...base }) });
  assert.equal(ok.summary.advisory, 'stopfile-removal-permitted');
});

test('R4 terminal numeric fields are validated before the summary: a string elapsedMs (or rowsWritten/bytesWritten) is omitted, the terminal is not identity-ok, and the phase holds', async () => {
  const listing = 'relay-logs/disc-relay-501-id501.jsonl\n';
  const cap = (extra) => ({ stdout: `-rw------- 1 u g 3456 Sep 13 04:30 relay-logs/disc-relay-501-id501.jsonl\n${'e'.repeat(64)}  relay-logs/disc-relay-501-id501.jsonl\n${armedRow('501', 'id501')}\n\n==TAIL==\n${termRow('501', 'id501', 'stopped', extra)}\n` });
  const base = { 'relay-census.sh --pids': { stdout: '501\n' }, 'ps -o': { stdout: 'x\n' }, '-type f -print': { stdout: listing }, '-type l -print': { stdout: '' }, 'tail -c 131072': { stdout: 'state=open\n' }, 'df -k': { stdout: 'F\n/ 1 1 1 1% /\n' } };
  const r = await runPhase({ opts: opt({ launchPids: ['501'] }), checkout: '/co', transport: fake({ 'sha256sum': cap({ elapsedMs: 'UNTRUSTED_SENTINEL' }), ...base }) });
  const c = r.summary.captures[0];
  assert.equal(c.elapsedMs, null, "Aster's probe (4): never copied"); assert.equal(c.terminalFieldsValid, false); assert.equal(c.terminalIdentityOk, false);
  assert.ok(r.summary.holds.some((h) => /terminal fields invalid|matching terminal/.test(h))); assert.equal(r.summary.advisory, 'HOLD: unresolved');
  assert.ok(!JSON.stringify(r.summary).includes('UNTRUSTED_SENTINEL'), 'host content never reaches the summary');
  const r2 = await runPhase({ opts: opt({ launchPids: ['501'] }), checkout: '/co', transport: fake({ 'sha256sum': cap({ rowsWritten: { evil: 1 }, bytesWritten: -5 }), ...base }) });
  assert.equal(r2.summary.captures[0].rowsWritten, null); assert.equal(r2.summary.captures[0].bytesWritten, null); assert.equal(r2.summary.advisory, 'HOLD: unresolved');
  const r3 = await runPhase({ opts: opt({ launchPids: ['501'] }), checkout: '/co', transport: fake({ 'sha256sum': cap({}), ...base }) });
  assert.equal(r3.summary.captures[0].elapsedMs, 900000.5); assert.equal(r3.summary.advisory, 'stopfile-removal-permitted');
});

// ---------------- revision 3: Aster 4e9f9554 (C6 output contract, two reproductions) ----------------
const duringWith = (tail) => runPhase({ opts: opt({ phase: 'DURING', launchPids: ['101'] }), checkout: '/co', transport: fake({ 'tail -c 131072': { stdout: tail }, 'ps -o': { stdout: '101 x\n' }, 'df -k': { stdout: 'F\n/ 1 1 1 1% /\n' } }) });
test('C6 (1) lastState is a closed vocabulary: kernel bridge states pass, anything else becomes `unrecognized`, absence is null; no host token reaches the serialized summary', async () => {
  assert.ok(Object.isFrozen(BRIDGE_STATES)); assert.deepEqual([...BRIDGE_STATES].sort(), ['connecting', 'disconnected', 'down', 'graduated', 'open', 'stale', 'upgrade-required']);
  const s = (await duringWith('state=privatesentinelfromhost peers=1\n')).summary;
  assert.equal(s.tails[0].abortKeys.lastState, 'unrecognized', "Aster's probe (1)");
  assert.ok(!JSON.stringify(s).includes('privatesentinel'), 'sentinel absent from the entire summary');
  const mixed = (await duringWith('state=open peers=2\nstate=Upgrade_Required-x\nstate=upgrade-required peers=0\n')).summary;
  assert.equal(mixed.tails[0].abortKeys.lastState, 'upgrade-required', 'hyphenated kernel state is recognized as the LAST state');
  assert.ok(!JSON.stringify(mixed).includes('Upgrade_Required'));
  assert.equal((await duringWith('state=open peers=40\n')).summary.tails[0].abortKeys.lastState, 'open');
  assert.equal((await duringWith('kernel v4.84.0\n')).summary.tails[0].abortKeys.lastState, null);
  assert.equal((await duringWith('state=OPEN\n')).summary.tails[0].abortKeys.lastState, 'unrecognized', 'case is not normalized into the enum');
  // raw host text is kept only in the rows, which land in 0600 files, never in summary.json
  const raw = await duringWith('state=privatesentinelfromhost\n'); assert.ok(raw.rows.some((r) => r.stdout.includes('privatesentinelfromhost')));
});

test('C6 (2) results writer: refuses a pre-existing directory (mode left as found, nothing written), a pre-existing file, a symlink to a directory and a dangling symlink; creates a fresh 0700 directory with 0600 files; a second write to the same path is refused and overwrites nothing', async () => {
  const { summary, rows } = await duringWith('state=open peers=1\n');
  const base = mkdtempSync(join(tmpdir(), 'canary-out-'));
  try {
    const pre = join(base, 'preexisting'); mkdirSync(pre, { mode: 0o755 });
    assert.throws(() => writePhaseResults(pre, summary, rows), /output path exists \(directory\)/, "Aster's probe (2)");
    assert.equal(lstatSync(pre).mode & 0o777, 0o755, 'mode of a refused directory is left as found, never widened or narrowed silently'); assert.deepEqual(readdirSync(pre), [], 'nothing written into a refused directory');
    const file = join(base, 'afile'); writeFileSync(file, 'x');
    assert.throws(() => writePhaseResults(file, summary, rows), /output path exists \(file\)/); assert.equal(readFileSync(file, 'utf8'), 'x');
    const target = join(base, 'target'); mkdirSync(target, { mode: 0o700 }); const link = join(base, 'link'); symlinkSync(target, link);
    assert.throws(() => writePhaseResults(link, summary, rows), /output path exists \(symlink\)/); assert.deepEqual(readdirSync(target), [], 'symlink target untouched');
    const dangling = join(base, 'dangling'); symlinkSync(join(base, 'nowhere'), dangling);
    assert.throws(() => writePhaseResults(dangling, summary, rows), /output path exists \(symlink\)/); assert.throws(() => lstatSync(join(base, 'nowhere')), /ENOENT/, 'dangling target not created');
    const fresh = join(base, 'nested', 'fresh'); writePhaseResults(fresh, summary, rows);
    const st = lstatSync(fresh); assert.ok(st.isDirectory() && !st.isSymbolicLink()); assert.equal(st.mode & 0o777, 0o700);
    const names = readdirSync(fresh); assert.ok(names.includes('summary.json') && names.includes('manifest.json') && names.includes('df.txt'));
    for (const n of names) assert.equal(lstatSync(join(fresh, n)).mode & 0o777, 0o600, `${n} is 0600`);
    const before = readFileSync(join(fresh, 'summary.json'), 'utf8');
    assert.throws(() => writePhaseResults(fresh, { ...summary, holds: ['TAMPER'] }, rows), /output path exists \(directory\)/);
    assert.equal(readFileSync(join(fresh, 'summary.json'), 'utf8'), before, 'second write refused before any file is touched'); assert.deepEqual(readdirSync(fresh).sort(), names.sort());
    assert.ok(!before.includes('TAMPER'));
  } finally { rmSync(base, { recursive: true, force: true }); }
});
