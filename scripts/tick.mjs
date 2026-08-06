// scripts/tick.mjs — the clock that outlives the Claude session.
//
//   node scripts/tick.mjs            # foreground (debugging)
//   started detached by ops/axona-ops.sh restore
//
// WHY THIS EXISTS. Every timer we had lived inside the agent session:
// CronCreate jobs and ScheduleWakeup both die the moment the session ends, and
// nothing announces their death. The hourly #jokes chime stopped three times
// that way — each time the diagnosis was written down, and each time the same
// session-scoped mechanism was used to restart it. A clock whose lifetime is
// shorter than the task it schedules is not a clock.
//
// So the clock is a PROCESS. It survives the agent restarting, which is the
// failure that actually happens; it dies on reboot/logout, which is covered by
// the restore ritual. Same lifetime class as the local relay fleet.
//
// WHAT IT DOES NOT DO: the work. It has no model, no opinions, and never posts
// to a topic. When a task is due it appends one line to the SAME inbox file the
// MCP arrival sink writes to, so the agent has exactly ONE thing to watch for
// both "somebody said something" and "something is due". A tick that fires with
// no session running still writes its line, so the agent returns to a visible
// record of what it missed rather than to silence.
//
// Schedule (JSON, hot-reloaded each minute so edits need no restart):
//   ~/.axona/schedule.json
//   [ { "task": "jokes-chime", "everyMinutes": 60, "note": "post a new joke" } ]
//
// State (last fire per task, so a restart does not re-fire everything):
//   ~/.axona/schedule-state.json
import { readFileSync, writeFileSync, appendFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const HOME     = homedir();
const INBOX    = process.env.MCP_INBOX     || join(HOME, '.axona', 'mcp-inbox.jsonl');
const SCHEDULE = process.env.TICK_SCHEDULE || join(HOME, '.axona', 'schedule.json');
const STATE    = process.env.TICK_STATE    || join(HOME, '.axona', 'schedule-state.json');

mkdirSync(dirname(INBOX), { recursive: true });

const DEFAULT_SCHEDULE = [
  { task: 'jokes-chime',    everyMinutes: 60,
    note: 'Post a NEW joke to #jokes (eagle) and confirm it landed. Never repeat one — msgId is hash(publisher+message), so a duplicate dedups and reads as a delivery failure.' },
  { task: 'surface-sweep',  everyMinutes: 25,
    note: 'Drain #axona.dev, #axona.chat and #council; sweep GitHub issues for new comments and unlabelled issues.' },
];

const log = (...a) => console.log(new Date().toISOString(), ...a);

function readJson(path, fallback) {
  try { return JSON.parse(readFileSync(path, 'utf8')); }
  catch { return fallback; }
}

// Seed the schedule on first run so the file is discoverable and editable
// rather than an undocumented default buried in this source.
if (!existsSync(SCHEDULE)) {
  writeFileSync(SCHEDULE, `${JSON.stringify(DEFAULT_SCHEDULE, null, 2)}\n`);
  log(`seeded ${SCHEDULE}`);
}

function fire(entry, lateBy) {
  // `kind` separates a due-task from a message; anything watching the inbox can
  // filter on it. `lateBy` is reported rather than hidden: a tick that fires 90
  // minutes late because the laptop slept is a different event from one on time,
  // and pretending otherwise is how a missed window becomes invisible.
  appendFileSync(INBOX, `${JSON.stringify({
    at: new Date().toISOString(),
    kind: 'due',
    task: entry.task,
    everyMinutes: entry.everyMinutes,
    lateBySec: Math.round(lateBy / 1000),
    note: entry.note ?? null,
  })}\n`);
  log(`due: ${entry.task} (late ${Math.round(lateBy / 1000)}s)`);
}

function pass() {
  const schedule = readJson(SCHEDULE, DEFAULT_SCHEDULE);
  const state = readJson(STATE, {});
  const now = Date.now();
  let changed = false;

  for (const entry of Array.isArray(schedule) ? schedule : []) {
    const every = Number(entry.everyMinutes);
    if (!entry.task || !Number.isFinite(every) || every <= 0) continue;
    const last = Number(state[entry.task]) || 0;
    const dueAt = last + every * 60_000;
    // First sight of a task fires immediately: a newly added entry should not
    // wait a full period before anyone learns it exists.
    if (last === 0 || now >= dueAt) {
      fire(entry, last === 0 ? 0 : now - dueAt);
      state[entry.task] = now;
      changed = true;
    }
  }
  if (changed) { try { writeFileSync(STATE, `${JSON.stringify(state, null, 2)}\n`); } catch (e) { log('state write failed:', e.message); } }
}

// TICK_ONCE runs a single pass and exits — used by the tests and by
// `axona-ops.sh status`, which wants to know what is due without starting a
// second long-lived clock alongside the running one.
if (process.env.TICK_ONCE) {
  log(`tick single pass — schedule ${SCHEDULE} -> inbox ${INBOX}`);
  pass();
  process.exit(0);
}

log(`tick started — schedule ${SCHEDULE} -> inbox ${INBOX}`);
pass();
setInterval(() => { try { pass(); } catch (e) { log('pass failed:', e.message); } }, 60_000);
