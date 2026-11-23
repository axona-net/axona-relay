#!/usr/bin/env node
// =====================================================================
// fence_relay_dump.mjs — relay-dump.sh, the read-only dump reader for the
// mac and linux relay hosts (David 2026-11-09: the known-network collector
// should list each relay's connected nodes).
//
// THE RULE UNDER TEST (Aster cd4d626b, f3b80b9a; 2026-11-23): the script
// SIGNALS NO PROCESS. It asks through the request file every relay of the
// checkout watches (src/dumprequest.js, the REAL watcher, imported by the
// stand-ins), and uses the census only to find which logs to read.
//
// The script runs from a temp directory beside a STUB relay-census.sh that
// names only this test's stand-ins, so a run on a relay host touches nothing
// real. EVERY stand-in writes a marker file if it ever receives SIGUSR1; the
// markers' absence is the reading, and D-control proves a marker is written
// when a signal IS sent. Stand-ins:
//   both      a relay: watches the request file, answers health-dump + peer-dump
//   health    a relay that answers health-dump only
//   stale     holds an OLD pair of dump lines in its log and answers nothing
//   piped     a relay whose stdout is a pipe: its log cannot be resolved
//   nonrelay  NOT a relay, running in the SAME checkout directory, named by
//             the census, with a readable log. It watches nothing.
//   foreign   a node process the census names that runs from ANOTHER directory
//   gone      a census pid whose process has exited (a stale identity)
// Run: node test/fence_relay_dump.mjs
// =====================================================================
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, copyFileSync, openSync, closeSync, existsSync, readdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const EXPECT = 19;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

console.log('fence_relay_dump: relay-dump.sh asks through the request file, signals nothing, prints only what was logged after');
if (process.platform === 'win32') { console.log('  skipped on Windows (relayctl.ps1 dump is the reader there)'); passed = EXPECT; process.exit(0); }

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const dir = mkdtempSync(join(tmpdir(), 'relay-dump-fence-'));
const elsewhere = mkdtempSync(join(tmpdir(), 'relay-dump-foreign-'));
const marks = mkdtempSync(join(tmpdir(), 'relay-dump-marks-'));       // SIGUSR1 markers, outside the checkout
copyFileSync(join(root, 'relay-dump.sh'), join(dir, 'relay-dump.sh'));
writeFileSync(join(dir, 'relay-census.sh'), '#!/usr/bin/env bash\ncat "$(dirname "$0")/pids"\n');
writeFileSync(join(dir, 'standin.mjs'), `
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
const { watchDumpRequest } = await import(${JSON.stringify(pathToFileURL(join(root, 'src', 'dumprequest.js')).href)});
const mode = process.argv[2], markDir = process.argv[3]; let n = 0;
const ts = () => new Date().toISOString();
process.on('SIGUSR1', () => { writeFileSync(join(markDir, 'signalled-' + process.pid), mode); });
if (mode === 'stale') { console.log('[' + ts() + '] health-dump {"peers":9,"old":true}'); console.log('[' + ts() + '] peer-dump {"nodeId":"old","peers":[]}'); }
const answer = () => {
  n++;
  if (mode === 'piped') writeFileSync(join(markDir, 'piped-answered'), String(n));
  if (mode === 'both' || mode === 'health' || mode === 'piped') console.log('[' + ts() + '] health-dump {"peers":2,"n":' + n + '}');
  if (mode === 'both') console.log('[' + ts() + '] peer-dump {"nodeId":"aa","n":' + n + ',"peers":[{"id":"bb","link":"webrtc"}]}');
};
// A relay arms the watcher on ITS OWN checkout directory. nonrelay, stale and
// foreign arm nothing: they are not relays of this checkout.
if (mode === 'both' || mode === 'health' || mode === 'piped') watchDumpRequest(join(process.cwd(), 'dump-requests'), answer, { intervalMs: 100 });
console.log('[' + ts() + '] ready');
setInterval(() => {}, 1000);
`);
const kids = [];
const start = (mode, label, cwd = dir) => {
  const log = join(cwd, `${label}.log`);
  const fd = openSync(log, 'a');
  const k = spawn(process.execPath, [join(dir, 'standin.mjs'), mode, marks], { cwd, stdio: ['ignore', fd, fd] });
  closeSync(fd); kids.push(k);
  return { pid: k.pid, log, label, kid: k };
};
const both = start('both', 'relay-t-1');
const health = start('health', 'relay-t-2');
const stale = start('stale', 'relay-t-3');
const nonrelay = start('nonrelay', 'not-a-relay');
const foreign = start('foreign', 'other-server', elsewhere);
const pipedKid = spawn(process.execPath, [join(dir, 'standin.mjs'), 'piped', marks], { cwd: dir, stdio: ['ignore', 'pipe', 'ignore'] });
let pipedReady = false; pipedKid.stdout.on('data', () => { pipedReady = true; }); kids.push(pipedKid);
// A stale identity: a pid the census still names after its process has exited.
const goneKid = spawnSync(process.execPath, ['-e', 'process.stdout.write(String(process.pid))'], { encoding: 'utf8' });
const gonePid = Number(goneKid.stdout);
const cleanup = () => { for (const k of kids) { try { k.kill('SIGKILL'); } catch { /* */ } } for (const d of [dir, elsewhere, marks]) { try { rmSync(d, { recursive: true, force: true }); } catch { /* */ } } };
process.on('exit', cleanup);

for (let t = 0; t < 100; t++) {
  if (pipedReady && [both, health, stale, nonrelay, foreign].every((r) => readFileSync(r.log, 'utf8').includes('] ready'))) break;
  await sleep(50);
}
const ALL = [both.pid, health.pid, stale.pid, pipedKid.pid, nonrelay.pid, foreign.pid, gonePid];
writeFileSync(join(dir, 'pids'), ALL.join('\n') + '\n');
const run = (env = {}) => spawnSync('bash', [join(dir, 'relay-dump.sh')], { encoding: 'utf8', env: { ...process.env, DUMP_WAIT_SEC: '1', ...env } });
const signalled = () => readdirSync(marks).filter((f) => f.startsWith('signalled-'));
const filesBefore = readdirSync(dir).sort();

const r1 = run();
const lines = r1.stdout.split('\n').filter(Boolean);
const find = (prefix) => lines.filter((l) => l.startsWith(prefix));
check('D1 exit 0 for a completed read', r1.status === 0, `status ${r1.status} ${r1.stderr}`);
const bh = find('relay-t-1 health-dump '), bp = find('relay-t-1 peer-dump ');
let bhJ = null, bpJ = null;
try { bhJ = JSON.parse(bh[0].slice('relay-t-1 health-dump '.length)); bpJ = JSON.parse(bp[0].slice('relay-t-1 peer-dump '.length)); } catch { /* */ }
check('D2 POSITIVE CONTROL: the intended relay answers the request file: one health-dump line, whole JSON, labelled by its log', bh.length === 1 && bhJ?.peers === 2 && bhJ?.n === 1, JSON.stringify(bh));
check('D3 and one peer-dump line, whole JSON', bp.length === 1 && bpJ?.nodeId === 'aa' && bpJ?.peers?.[0]?.id === 'bb', JSON.stringify(bp));
check('D4 a relay that answers health-dump only: that line and no peer-dump', find('relay-t-2 health-dump ').length === 1 && find('relay-t-2 peer-dump').length === 0 && find('relay-t-2 no-dump').length === 0, JSON.stringify(find('relay-t-2')));
check('D5 an OLD dump pair in the log is not printed as current: no-dump', find('relay-t-3').length === 1 && find('relay-t-3')[0] === 'relay-t-3 no-dump' && !r1.stdout.includes('"old"'), JSON.stringify(find('relay-t-3')));
check('D6 a relay whose log cannot be resolved is named', find(`pid-${pipedKid.pid} no-dump (log unresolved)`).length === 1, JSON.stringify(find('pid-')));
check('D7 NO PROCESS WAS SIGNALLED: not the relays, not the non-relay in the same checkout, not the foreign process', signalled().length === 0, signalled().join(','));
check('D8 a NON-RELAY in the same checkout is not asked anything: it prints no-dump and received no signal', find('not-a-relay').length === 1 && find('not-a-relay')[0] === 'not-a-relay no-dump' && !existsSync(join(marks, `signalled-${nonrelay.pid}`)), JSON.stringify(find('not-a-relay')));
check('D9 a census pid running from another directory is named, skipped and not counted', find(`pid-${foreign.pid} skipped (not running from this checkout)`).length === 1 && !r1.stdout.includes('other-server'), JSON.stringify(find('pid-')));
check('D10 a STALE identity (a census pid whose process has exited) is named, skipped and not counted', Number.isInteger(gonePid) && find(`pid-${gonePid} skipped`).length === 1, JSON.stringify(find('pid-')));
check('D11 the summary counts this checkout\'s processes and the two skipped', lines[lines.length - 1] === 'SUMMARY dumps health=2/5 peer=1/5 waited=1s skipped=2', lines[lines.length - 1]);
const filesAfter = readdirSync(dir).sort();
check('D12 the script wrote ONE thing (the relays made the directory when they armed): dump-requests/request, holding a UTC time and nothing else', filesAfter.join(',') === filesBefore.join(',') && filesBefore.includes('dump-requests') && readdirSync(join(dir, 'dump-requests')).join(',') === 'request' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\n$/.test(readFileSync(join(dir, 'dump-requests', 'request'), 'utf8')), filesAfter.join(','));

const r2 = run();                                             // at once: inside the same second where the clock allows
const again = r2.stdout.split('\n').filter((l) => l.startsWith('relay-t-1 peer-dump '));
let n2 = null; try { n2 = JSON.parse(again[0].slice('relay-t-1 peer-dump '.length)).n; } catch { /* */ }
check('D13 a second run at once still reaches the relay and prints the SECOND answer only', again.length === 1 && n2 === 2, JSON.stringify(again));
check('D14 still nothing signalled after two runs', signalled().length === 0, signalled().join(','));

// CONTROL for D7/D8/D14: every stand-in does write its marker when it IS signalled.
for (const k of [both.kid, nonrelay.kid, foreign.kid]) k.kill('SIGUSR1');
for (let t = 0; t < 40 && signalled().length < 3; t++) await sleep(50);
check('D15 control: the relay, the non-relay and the foreign stand-in each mark a signal sent directly', signalled().length === 3, signalled().join(','));

writeFileSync(join(dir, 'pids'), `${foreign.pid}\n${gonePid}\nnot-a-pid\n`);
const reqBefore = statSync(join(dir, 'dump-requests', 'request')).mtimeMs;
await sleep(1100);
const onlyForeign = run();
check('D16 a census of foreign and stale pids only: 0/0, skipped=2, and no request is written', onlyForeign.status === 0 && onlyForeign.stdout.trim().split('\n').pop() === 'SUMMARY dumps health=0/0 peer=0/0 waited=0s skipped=2' && statSync(join(dir, 'dump-requests', 'request')).mtimeMs === reqBefore, onlyForeign.stdout);
const bad = run({ DUMP_WAIT_SEC: 'abc' });
check('D17 a wait that is not whole seconds is refused (exit 2)', bad.status === 2 && !bad.stdout.includes('dump'), `status ${bad.status}`);
writeFileSync(join(dir, 'pids'), '');
const none = run();
check('D18 no relays: the summary says 0/0 and exit 0', none.status === 0 && none.stdout.trim() === 'SUMMARY dumps health=0/0 peer=0/0 waited=0s', none.stdout);
const code = readFileSync(join(root, 'relay-dump.sh'), 'utf8').split('\n').filter((l) => !/^\s*#/.test(l)).join('\n');
check('D19 the script has no kill, pkill or pgrep of its own; its pid list is relay-census.sh --pids', !/\b(kill|pkill|killall|pgrep)\b/.test(code) && /bash relay-census\.sh --pids/.test(code));

console.log(`\nfence_relay_dump: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
