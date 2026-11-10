#!/usr/bin/env node
// =====================================================================
// fence_relay_dump.mjs — relay-dump.sh, the read-only dump reader for the
// mac and linux relay hosts (David 2026-11-09: the known-network collector
// should list each relay's connected nodes).
//
// The script is run from a temp directory beside a STUB relay-census.sh that
// names only this test's stand-in relays, so a run on a relay host signals
// nothing real. Stand-ins:
//   both     answers SIGUSR1 with a health-dump and a peer-dump line
//   health   answers with health-dump only (a relay older than 0.198.0)
//   stale    holds an OLD pair of dump lines in its log and answers nothing
//   piped    stdout is a pipe, so its log cannot be resolved
// Run: node test/fence_relay_dump.mjs
// =====================================================================
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, copyFileSync, openSync, closeSync, existsSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const EXPECT = 13;
process.on('exit', (code) => { if (code === 0 && passed + failed < EXPECT) { console.log(`  ✗ exited after ${passed + failed} of ${EXPECT} checks`); process.exitCode = 1; } });

console.log('fence_relay_dump: relay-dump.sh prints only what each relay logged after the signal');
if (process.platform === 'win32') { console.log('  skipped on Windows (no SIGUSR1; relayctl.ps1 dump is the reader there)'); passed = EXPECT; process.exit(0); }

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const dir = mkdtempSync(join(tmpdir(), 'relay-dump-fence-'));
copyFileSync(join(root, 'relay-dump.sh'), join(dir, 'relay-dump.sh'));
writeFileSync(join(dir, 'relay-census.sh'), '#!/usr/bin/env bash\ncat "$(dirname "$0")/pids"\n');
writeFileSync(join(dir, 'standin.mjs'), `
import { writeFileSync } from 'node:fs';
const mode = process.argv[2]; let n = 0;
const ts = () => new Date().toISOString();
if (mode === 'stale') { console.log('[' + ts() + '] health-dump {"peers":9,"old":true}'); console.log('[' + ts() + '] peer-dump {"nodeId":"old","peers":[]}'); }
process.on('SIGUSR1', () => {
  n++;
  if (mode === 'piped') writeFileSync(process.argv[3], 'signalled');
  if (mode === 'both' || mode === 'health') console.log('[' + ts() + '] health-dump {"peers":2,"n":' + n + '}');
  if (mode === 'both') console.log('[' + ts() + '] peer-dump {"nodeId":"aa","n":' + n + ',"peers":[{"id":"bb","link":"webrtc"}]}');
});
console.log('[' + ts() + '] ready');
setInterval(() => {}, 1000);
`);
const kids = [];
const start = (mode, label) => {
  const log = join(dir, `${label}.log`);
  const fd = openSync(log, 'a');
  const k = spawn(process.execPath, [join(dir, 'standin.mjs'), mode], { stdio: ['ignore', fd, fd] });
  closeSync(fd); kids.push(k);
  return { pid: k.pid, log, label };
};
const both = start('both', 'relay-t-1');
const health = start('health', 'relay-t-2');
const stale = start('stale', 'relay-t-3');
const marker = join(dir, 'piped-signalled');
const pipedKid = spawn(process.execPath, [join(dir, 'standin.mjs'), 'piped', marker], { stdio: ['ignore', 'pipe', 'ignore'] });
let pipedReady = false; pipedKid.stdout.on('data', () => { pipedReady = true; }); kids.push(pipedKid);
const cleanup = () => { for (const k of kids) { try { k.kill('SIGKILL'); } catch { /* */ } } try { rmSync(dir, { recursive: true, force: true }); } catch { /* */ } };
process.on('exit', cleanup);

for (let t = 0; t < 100; t++) {
  if (pipedReady && [both, health, stale].every((r) => readFileSync(r.log, 'utf8').includes('] ready'))) break;
  await sleep(50);
}
writeFileSync(join(dir, 'pids'), [both.pid, health.pid, stale.pid, pipedKid.pid].join('\n') + '\n');
const run = (env = {}) => spawnSync('bash', [join(dir, 'relay-dump.sh')], { encoding: 'utf8', env: { ...process.env, DUMP_WAIT_SEC: '1', ...env } });
const filesBefore = readdirSync(dir).sort().join(',');

const r1 = run();
const lines = r1.stdout.split('\n').filter(Boolean);
const find = (prefix) => lines.filter((l) => l.startsWith(prefix));
check('D1 exit 0 for a completed read', r1.status === 0, `status ${r1.status} ${r1.stderr}`);
const bh = find('relay-t-1 health-dump '), bp = find('relay-t-1 peer-dump ');
let bhJ = null, bpJ = null;
try { bhJ = JSON.parse(bh[0].slice('relay-t-1 health-dump '.length)); bpJ = JSON.parse(bp[0].slice('relay-t-1 peer-dump '.length)); } catch { /* */ }
check('D2 a relay that answers both: one health-dump line, whole JSON, labelled by its log', bh.length === 1 && bhJ?.peers === 2 && bhJ?.n === 1, JSON.stringify(bh));
check('D3 and one peer-dump line, whole JSON', bp.length === 1 && bpJ?.nodeId === 'aa' && bpJ?.peers?.[0]?.id === 'bb', JSON.stringify(bp));
check('D4 a relay that answers health-dump only: that line and no peer-dump', find('relay-t-2 health-dump ').length === 1 && find('relay-t-2 peer-dump').length === 0 && find('relay-t-2 no-dump').length === 0, JSON.stringify(find('relay-t-2')));
check('D5 an OLD dump pair in the log is not printed as current: no-dump', find('relay-t-3').length === 1 && find('relay-t-3')[0] === 'relay-t-3 no-dump' && !r1.stdout.includes('"old"'), JSON.stringify(find('relay-t-3')));
check('D6 a relay whose log cannot be resolved is named and NOT signalled', find(`pid-${pipedKid.pid} no-dump (log unresolved`).length === 1 && !existsSync(marker), JSON.stringify(find('pid-')));
check('D7 the summary counts what was printed', lines[lines.length - 1] === 'SUMMARY dumps health=2/4 peer=1/4 waited=1s', lines[lines.length - 1]);
check('D8 the script wrote no file', readdirSync(dir).sort().join(',') === filesBefore, readdirSync(dir).join(','));

const r2 = run();
const again = r2.stdout.split('\n').filter((l) => l.startsWith('relay-t-1 peer-dump '));
let n2 = null; try { n2 = JSON.parse(again[0].slice('relay-t-1 peer-dump '.length)).n; } catch { /* */ }
check('D9 a second run prints the SECOND answer only', again.length === 1 && n2 === 2, JSON.stringify(again));

// CONTROL for D6: the stand-in does write its marker when it is signalled, so
// the marker's absence above is a real reading.
pipedKid.kill('SIGUSR1');
for (let t = 0; t < 40 && !existsSync(marker); t++) await sleep(50);
check('D10 control: the piped stand-in marks a signal when one is sent', existsSync(marker));

const bad = run({ DUMP_WAIT_SEC: 'abc' });
check('D11 a wait that is not whole seconds is refused (exit 2), nothing signalled', bad.status === 2 && !bad.stdout.includes('dump'), `status ${bad.status}`);
writeFileSync(join(dir, 'pids'), '');
const none = run();
check('D12 no relays: the summary says 0/0 and exit 0', none.status === 0 && none.stdout.trim() === 'SUMMARY dumps health=0/0 peer=0/0 waited=0s', none.stdout);
const src = readFileSync(join(root, 'relay-dump.sh'), 'utf8');
check('D13 the pid list is relay-census.sh --pids and no pgrep of its own', /bash relay-census\.sh --pids/.test(src) && !/^[^#]*\bpgrep\b/m.test(src));

console.log(`\nfence_relay_dump: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
