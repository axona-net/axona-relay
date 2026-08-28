// =============================================================================
// harness/churn.mjs — the scripted churn driver (spec v0.3 §6).
//
// Executes a seeded action plan, ledgering every event so the analyzer can
// join detector windows to stimuli. Actions:
//   sidecar-restart  — kill a LOCAL sidecar process and relaunch it with the
//                      same env; its since:'all' resubscription then exercises
//                      the replay / eventual-delivery path. Validation-safe.
//   relay-roll  — §6 rolling restart: replace ONE relay on a host, heir-
//                 preserving (start-then-stop; census never drops). macOS/
//                 Linux via roll-fleet's one-slot form; Windows relays are
//                 windows-fleet's, respawned by schtasks. Gated: ARM_RELAY=1.
//   relay-kill  — §6 abrupt kill+restart of one relay per host, ≥3min heir
//                 window. Gated: ARM_RELAY=1.
// Both are REFUSED unless ARM_RELAY=1 (David's gate-3 Arm-A sanction), and
// BOTH verify the host census would stay ≥ the §6 floor (95%) before acting;
// a floor breach is refused-and-ledgered, never forced.
//
//   SEED=7 PLAN='[{"atMs":300000,"kind":"sidecar-restart","peerIdx":0,"env":{...}}]' \
//     LEDGER_DIR=harness/results node harness/churn.mjs
// =============================================================================
import { Ledger } from './lib/ledger.mjs';
import { execSync, spawn } from 'node:child_process';

const ARM_RELAY = process.env.ARM_RELAY === '1';
// Per-host relay census + one-relay roll/kill, dispatched by host kind. Each
// returns { ok, census, note }. NONE force past the §6 floor. Windows and the
// remote unix hosts are driven over ssh with the proven launch shapes.
const HOST_CENSUS = {
  m4:   'COUNT=0; for p in $(pgrep -f "src/index.js"); do [ "$(ps -o comm= -p $p | xargs basename 2>/dev/null)" = node ] && COUNT=$((COUNT+1)); done; echo $COUNT',
  // m1 is an Apple-silicon Mac: relays run under caffeinate, so a plain
  // pgrep|wc DOUBLE-counts (node + its caffeinate twin). Count comm=node only.
  m1:   'ssh -o ConnectTimeout=10 m1 \'c=0; for p in $(pgrep -f "src/index.js"); do [ "$(basename "$(ps -p $p -o comm=)")" = node ] && c=$((c+1)); done; echo $c\'',
  // linux has no caffeinate; node\'s comm reads as its thread name, so match
  // the exe via /proc/PID/exe rather than comm.
  'axona-linux': 'ssh -o ConnectTimeout=10 axona-linux \'n=0; for p in $(pgrep -f "src/index.js"); do case "$(readlink /proc/$p/exe 2>/dev/null)" in *node*) n=$((n+1));; esac; done; echo $n\'',
  // relay-filtered via the committed helper — never counts the harness sidecars
  // that also run as node.exe (a plain tasklist node.exe count would over-read).
  'axona-win':   'printf \'/c/Users/david/github/axona-relay/harness/win-relay.sh census\\n\' | ssh -o ConnectTimeout=15 axona-win \'"C:\\\\Program Files\\\\Git\\\\bin\\\\bash.exe" -s\'',
};
const censusOf = (host) => {
  try { return Number(execSync(HOST_CENSUS[host], { shell: '/bin/bash' }).toString().trim()); }
  catch { return NaN; }
};

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
    // EXACT-pattern kill on the argv peer tag; NO broad fallback — a missed
    // kill is ledgered and visible in the artifacts, while the previous
    // broad fallback killed every sidecar on the host and destroyed a run.
    let killed = false;
    try {
      execSync(`pkill -f "sidecar.mjs --peer ${a.peerIdx}$"`, { shell: '/bin/bash' });
      killed = true;
    } catch { /* no match — ledger it below */ }
    led.event({ kind: killed ? 'churn:killed' : 'churn:kill-missed', detail: { peerIdx: a.peerIdx } });
    await sleep(a.downMs ?? 10_000);
    const env = { ...process.env, ...a.env, PEER_IDX: String(a.peerIdx) };
    const child = spawn('node', ['harness/sidecar.mjs', '--peer', String(a.peerIdx)], { env, detached: true,
      stdio: ['ignore', 'ignore', 'ignore'] });
    child.unref();
    led.event({ kind: 'churn:sidecar-restart', detail: { peerIdx: a.peerIdx, pid: child.pid, downMs: a.downMs ?? 10_000 } });
  } else if (a.kind === 'relay-kill' || a.kind === 'relay-roll') {
    if (!ARM_RELAY) {
      led.event({ kind: 'churn:refused', detail: { kind: a.kind,
        note: 'relay churn needs ARM_RELAY=1 (David gate-3 Arm-A sanction)' } });
      continue;
    }
    const host = a.host ?? 'm4';
    const before = censusOf(host);
    const floor = Math.ceil((a.floorPct ?? 95) / 100 * (a.hostSize ?? before));
    // §6 floor guard: refuse if removing one would drop this host below the
    // 95% census floor. A relay-roll (start replacement FIRST) never dips, so
    // it is floor-safe by construction; a relay-kill (remove first) must have
    // headroom. Either way the census is measured, never assumed.
    if (!Number.isFinite(before)) {
      led.event({ kind: 'churn:relay-skip', detail: { host, kind: a.kind, note: 'census unreadable' } });
      continue;
    }
    if (a.kind === 'relay-kill' && before - 1 < floor) {
      led.event({ kind: 'churn:relay-refused-floor', detail: { host, before, floor, note: 'kill would breach §6 census floor' } });
      continue;
    }
    led.event({ kind: `churn:${a.kind}-begin`, detail: { host, census: before, floor, heirMs: a.heirMs ?? 180_000 } });
    // The live relay roll/kill mechanics per host live in harness/relay-churn.sh
    // (roll-fleet one-slot on macOS/linux; schtasks respawn on windows). It is
    // invoked ONLY here, ONLY under ARM_RELAY, and prints the post-action
    // census as its last line. This driver never SIGKILLs a relay directly —
    // it delegates to the ritual script, preserving fleet discipline.
    let after = before;
    try {
      const out = execSync(`bash harness/relay-churn.sh ${a.kind} ${host} ${a.heirMs ?? 180_000}`,
        { shell: '/bin/bash', timeout: (a.heirMs ?? 180_000) + 120_000 }).toString().trim();
      after = Number(out.split('\n').pop()) || before;
    } catch (e) {
      led.event({ kind: 'churn:relay-error', detail: { host, kind: a.kind, err: String(e.message).slice(0, 120) } });
    }
    led.event({ kind: `churn:${a.kind}-done`, detail: { host, censusBefore: before, censusAfter: after } });
  } else {
    led.event({ kind: 'churn:refused', detail: { kind: a.kind, note: 'unknown action' } });
  }
}
led.event({ kind: 'churn:done', detail: {} });
process.exit(0);
