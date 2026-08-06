// scripts/inbox.mjs — "what did I miss, and what do I still owe?"
//
//   node scripts/inbox.mjs                 # digest since my last read, then advance
//   node scripts/inbox.mjs --peek          # same, without advancing
//   node scripts/inbox.mjs --ack <task>    # close an outstanding responsibility
//   node scripts/inbox.mjs --who aster     # read/write a different agent's cursor
//
// WHY THIS EXISTS, and why it is separate from the wake.
//
// The three council minds are woken in three different ways and two of them
// cannot be woken on demand at all:
//
//   Claude  file Monitor          -> interrupted in seconds
//   Orion   parent-process cron   -> interrupted every 5 min (sub-second with a file watch)
//   Aster   host heartbeats only  -> CANNOT be interrupted; only inspects at turn start
//
// Chasing a single wake mechanism is therefore a dead end. But the wake is only
// half the problem, and it is the half we cannot standardise. The other half —
// "having woken, what happened and what do I owe?" — can be identical for
// everyone, and it is the half that actually causes work to be dropped.
//
// So this reads the append-only inbox (arrivals from the MCP sink, due-tasks
// from tick.mjs) and answers both questions against a per-agent cursor.
//
// THE PART THAT MATTERS MOST: a due task stays OUTSTANDING until it is acked.
// A notification you merely saw is not a notification you acted on, and the
// failure we keep having is not missing the alert — it is seeing it, meaning to
// act, and losing it. An unacked task therefore accumulates visible age instead
// of scrolling away. Nothing here can make an agent do the work; it can make
// not having done it impossible to overlook.
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const args = process.argv.slice(2);
const flag = (name) => { const i = args.indexOf(name); return i >= 0 ? (args[i + 1] ?? true) : null; };
const has = (name) => args.includes(name);

const WHO    = flag('--who') || process.env.MCP_HANDLE || 'agent';
const INBOX  = process.env.MCP_INBOX || join(homedir(), '.axona', 'mcp-inbox.jsonl');
// Per-agent cursor: several minds share this machine and one shared cursor
// would mean whoever read first hid the traffic from everybody else.
const CURSOR = process.env.INBOX_CURSOR || join(homedir(), '.axona', `inbox-cursor-${WHO}.json`);

const now = Date.now();
const ago = (ms) => {
  const s = Math.round(ms / 1000);
  if (s < 90) return `${s}s`;
  if (s < 5400) return `${Math.round(s / 60)}m`;
  return `${(s / 3600).toFixed(1)}h`;
};

if (!existsSync(INBOX)) {
  console.log(`inbox ABSENT: ${INBOX}`);
  console.log('Nothing is writing it. The MCP arrival sink needs a server restart to appear;');
  console.log('the clock (tick.mjs) writes it too — check with: bash ops/axona-ops.sh status');
  process.exit(0);
}

const state = existsSync(CURSOR)
  ? JSON.parse(readFileSync(CURSOR, 'utf8'))
  : { line: 0, acked: {} };
state.acked ??= {};

const lines = readFileSync(INBOX, 'utf8').split('\n').filter(Boolean);
const parsed = lines.map((l) => { try { return JSON.parse(l); } catch { return null; } });

// ── --ack closes a responsibility ───────────────────────────────────────────
const ack = flag('--ack');
if (ack && ack !== true) {
  state.acked[ack] = new Date().toISOString();
  mkdirSync(dirname(CURSOR), { recursive: true });
  writeFileSync(CURSOR, `${JSON.stringify(state, null, 2)}\n`);
  console.log(`acked: ${ack} (as ${WHO})`);
  process.exit(0);
}

// ── new since the cursor ────────────────────────────────────────────────────
const fresh = parsed.slice(state.line).filter(Boolean);
const messages = fresh.filter((e) => e.kind !== 'due' && e.topic);
const dues     = fresh.filter((e) => e.kind === 'due');

console.log(`inbox for ${WHO} — ${lines.length} lines total, ${fresh.length} new since last read\n`);

if (messages.length) {
  console.log(`MESSAGES (${messages.length})`);
  for (const m of messages) {
    const txt = (m.text ?? JSON.stringify(m.message ?? '')).replace(/\s+/g, ' ').slice(0, 160);
    console.log(`  [${m.topic}] ${m.signer ?? '?'} ${ago(now - Date.parse(m.at))} ago`);
    console.log(`    ${txt}`);
  }
  console.log('');
} else {
  console.log('MESSAGES: none new\n');
}

// ── outstanding responsibilities: EVERY due ever fired and not acked ────────
// Deliberately scans the whole file, not just the new slice. A task that fired
// while nobody was running is exactly the one at risk of being lost, and it
// would never appear in a "since last read" view once the cursor moved past it.
const outstanding = new Map();
for (const e of parsed) {
  if (!e || e.kind !== 'due') continue;
  const firedAt = Date.parse(e.at);
  const ackedAt = state.acked[e.task] ? Date.parse(state.acked[e.task]) : 0;
  if (firedAt > ackedAt) outstanding.set(e.task, e);   // keep the LATEST unacked firing
}

if (outstanding.size) {
  console.log(`OUTSTANDING (${outstanding.size}) — due and not acked`);
  for (const [task, e] of outstanding) {
    const age = now - Date.parse(e.at);
    const late = e.lateBySec > 60 ? `, fired ${ago(e.lateBySec * 1000)} late` : '';
    console.log(`  ${task}  (due ${ago(age)} ago${late})`);
    if (e.note) console.log(`    ${e.note}`);
    console.log(`    close with: node scripts/inbox.mjs --ack ${task}`);
  }
  console.log('');
} else {
  console.log('OUTSTANDING: nothing owed\n');
}

if (!has('--peek')) {
  state.line = lines.length;
  mkdirSync(dirname(CURSOR), { recursive: true });
  writeFileSync(CURSOR, `${JSON.stringify(state, null, 2)}\n`);
} else {
  console.log('(--peek: cursor not advanced)');
}
