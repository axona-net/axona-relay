// smoke_churn_guard.mjs — the churn driver's relay-churn guards, WITHOUT
// touching any relay. ARM_RELAY unset ⇒ every relay action must refuse and
// ledger the refusal; sidecar-restart stays available. This proves the fence
// that keeps relay churn inside an armed Arm A (ARM_RELAY=1), never a stray run.
// Run: node harness/test/smoke_churn_guard.mjs
import { execFileSync } from 'node:child_process';
import { readFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';

let passed = 0, failed = 0;
const check = (l, c) => { console.log(`  ${c ? '✓' : '✗'} ${l}`); c ? passed++ : failed++; };
console.log('churn guard smoke\n');

const DIR = `${tmpdir()}/churn-guard-${process.pid}`;
mkdirSync(DIR, { recursive: true });
const plan = JSON.stringify([
  { atMs: 0, kind: 'relay-kill', host: 'm4' },
  { atMs: 50, kind: 'relay-roll', host: 'm1' },
]);
// ARM_RELAY intentionally UNSET → both relay actions must refuse, no relay touched.
execFileSync('node', ['harness/churn.mjs'], {
  env: { ...process.env, SEED: '900', PLAN: plan, LEDGER_DIR: DIR, HOST: 'm4', ARM_RELAY: '' },
});
const rows = readFileSync(`${DIR}/sidecar-900-churn.jsonl`, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
rmSync(DIR, { recursive: true, force: true });

const refusals = rows.filter((r) => r.kind === 'churn:refused' && /ARM_RELAY/.test(r.detail?.note ?? ''));
check('relay-kill refused without ARM_RELAY', refusals.some((r) => r.detail));
check('both relay actions refused (2)', refusals.length === 2);
check('no relay-begin event emitted (nothing touched)',
  !rows.some((r) => String(r.kind).includes('-begin')));
check('driver still emits plan + done bookends',
  rows.some((r) => r.kind === 'churn:plan') && rows.some((r) => r.kind === 'churn:done'));

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
