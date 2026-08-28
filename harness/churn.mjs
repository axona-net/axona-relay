// =============================================================================
// harness/churn.mjs — the scripted churn driver (spec v0.3 §6).
//
// Executes a seeded action plan, ledgering every event so the analyzer can
// join detector windows to stimuli. Actions:
//   sidecar-restart  — kill a LOCAL sidecar process and relaunch it with the
//                      same env; its since:'all' resubscription then exercises
//                      the replay / eventual-delivery path. Validation-safe.
//   relay-kill / relay-roll — Arm A/B ONLY. Refused here by design: touching
//                      a relay is fleet churn under the §6 schedule and runs
//                      inside an arm with David's gate-3/4 sanction, never in
//                      a build validation.
//
//   SEED=7 PLAN='[{"atMs":300000,"kind":"sidecar-restart","peerIdx":0,"env":{...}}]' \
//     LEDGER_DIR=harness/results node harness/churn.mjs
// =============================================================================
import { Ledger } from './lib/ledger.mjs';
import { execSync, spawn } from 'node:child_process';

const SEED = Number(process.env.SEED);
const PLAN = JSON.parse(process.env.PLAN ?? '[]');
const LEDGER_DIR = process.env.LEDGER_DIR ?? 'harness/results';
const led = new Ledger(`${LEDGER_DIR}/sidecar-${SEED}-churn.jsonl`,
  { host: process.env.HOST ?? 'm4', os: process.platform, peerIdx: 999, author: 'churn-driver' });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const t0 = Date.now();
led.event({ kind: 'churn:plan', detail: { actions: PLAN.length } });

for (const a of PLAN.sort((x, y) => x.atMs - y.atMs)) {
  const wait = a.atMs - (Date.now() - t0);
  if (wait > 0) await sleep(wait);
  if (a.kind === 'sidecar-restart') {
    led.event({ kind: 'churn:sidecar-kill', detail: { peerIdx: a.peerIdx } });
    try {
      execSync(`pkill -f "PEER_IDX=${a.peerIdx} .*sidecar.mjs" 2>/dev/null || pkill -f "sidecar.mjs" -n 2>/dev/null || true`, { shell: '/bin/bash' });
    } catch { /* dead already is fine */ }
    await sleep(a.downMs ?? 10_000);
    const env = { ...process.env, ...a.env, PEER_IDX: String(a.peerIdx) };
    const child = spawn('node', ['harness/sidecar.mjs'], { env, detached: true,
      stdio: ['ignore', 'ignore', 'ignore'] });
    child.unref();
    led.event({ kind: 'churn:sidecar-restart', detail: { peerIdx: a.peerIdx, pid: child.pid, downMs: a.downMs ?? 10_000 } });
  } else {
    led.event({ kind: 'churn:refused', detail: { kind: a.kind,
      note: 'relay actions run only inside an arm under David gate-3/4 sanction' } });
  }
}
led.event({ kind: 'churn:done', detail: {} });
process.exit(0);
