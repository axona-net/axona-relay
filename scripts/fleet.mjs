#!/usr/bin/env node
// fleet.mjs — launch and MONITOR N Axona relays with one command.
// Cross-platform (pure Node, no bash) so it runs the same on Windows, macOS,
// and Linux. No global environment variables required — everything is a flag.
//
//   node scripts/fleet.mjs --region grizzly --count 10 --network testnet
//   node scripts/fleet.mjs --region eagle  --count 3                 # prod
//   node scripts/fleet.mjs --region grizzly --count 5 --bridge wss://my-bridge:8080
//
// Flags:
//   --region  <name|code>      region to host (e.g. grizzly / 0x80). default eagle
//   --count   <n>              how many relay processes to launch.       default 1
//   --network <prod|testnet>   which bridge network to bootstrap from.   default prod
//   --bridge  <wss-url>        explicit bridge URL (overrides --network).
//   --stagger <ms>             delay between launches (default 600) so N relays
//                              don't hit the bridge with N handshakes at once.
//   --raw                      stream each relay's raw log lines instead of the
//                              live dashboard (useful for debugging one relay).
//   --heal-delay <ms>          pause between relays on a rolling stop/restart so
//                              the mesh heals + new heirs settle (default 5000).
//   --leave-timeout <ms>       cap on waiting for one relay's graceful leave()
//                              before a hard kill (default 45000).
//
// By default the output is a LIVE DASHBOARD: one fixed table, one row per relay,
// redrawn in place — so you can watch fleet health at a glance instead of a wall
// of interleaved scrolling logs. Each relay is its own child process with an
// EPHEMERAL transport identity (re-minted every start), so N relays never collide.
//
// CONTROLS (dashboard, any OS incl. Windows):
//   r = ROLLING RESTART in place — drains + respawns each relay ONE at a time,
//       waiting for the mesh to heal between each, so the region's cache is never
//       held by zero nodes at once (a mass restart shreds the held history).
//   q / Ctrl-C = rolling STOP — drains the whole fleet one relay at a time.
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { writeFileSync } from 'node:fs';

const argv = process.argv.slice(2);
const opts = {};
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) continue;
  const key = a.slice(2);
  const next = argv[i + 1];
  if (next == null || next.startsWith('--')) opts[key] = true;   // bare flag
  else opts[key] = argv[++i];
}

if (opts.help || opts.h) {
  console.log('usage: node scripts/fleet.mjs --region <name> --count <n> ' +
    '[--network prod|testnet] [--bridge wss://url] [--stagger ms] [--raw] ' +
    '[--heal-delay ms] [--leave-timeout ms]\n' +
    'controls: r = rolling restart in place · q / Ctrl-C = rolling stop');
  process.exit(0);
}

const region  = typeof opts.region === 'string' ? opts.region : 'eagle';
const count   = Math.max(1, Number.parseInt(opts.count ?? '1', 10) || 1);
const network = typeof opts.network === 'string' ? opts.network : 'prod';
const bridge  = typeof opts.bridge === 'string' ? opts.bridge : null;
const staggerMs = Math.max(0, Number.parseInt(opts.stagger ?? '600', 10) || 0);
// Rolling restart/shutdown tuning. A relay's graceful leave() hands its cached
// roles to still-alive heirs; killing the WHOLE fleet at once leaves no heirs, so
// the region's held history dies with it. We instead drain ONE relay at a time:
// stop it, wait for its leave() to finish (process exit), pause so the mesh heals
// and the new heirs settle, then touch the next. `healDelayMs` is that heal pause;
// `leaveTimeoutMs` caps how long we wait for a wedged leaver before a hard kill.
const healDelayMs    = Math.max(0, Number.parseInt(opts['heal-delay']    ?? '5000',  10) || 0);
const leaveTimeoutMs = Math.max(1000, Number.parseInt(opts['leave-timeout'] ?? '45000', 10) || 45000);
// Dashboard needs a TTY to redraw in place; fall back to raw logs when piped
// or when --raw is passed.
const rawMode = Boolean(opts.raw) || !process.stdout.isTTY;

const here  = dirname(fileURLToPath(import.meta.url));
const entry = join(here, '..', 'src', 'index.js');

// ── Per-relay state, parsed from each relay's plain log stream ────────
// A relay prints, ~1×/s:
//   [HH:MM:SS] state=open peers=19 synaptome=19 mesh(open/bound)=15/18 roles=52 subs=0
// and, once at boot:
//   [HH:MM:SS] node 80416ebe…  /  axona-relay vX [EPHEMERAL] (kernel vY)
// Per relay we keep a live snapshot (state/peers/mesh…, refreshed ~1×/s) AND a
// map of TROUBLE EVENTS keyed by kind. `op` is a controller-set status (draining,
// re-meshing) that overrides the note during a rolling op.
const stats = Array.from({ length: count }, () => ({
  pid: null, node: '—', state: '…', peers: 0, meshO: 0, meshB: 0,
  roles: 0, subs: 0, exited: null, last: 0, events: {}, op: '',
}));

const STATE_RE = /state=(\S+)\s+peers=(\d+)\s+synaptome=\d+\s+mesh\(open\/bound\)=(\d+)\/(\d+)\s+roles=(\d+)\s+subs=(\d+)/;
const NODE_RE  = /\bnode\s+([0-9a-f]{8,})/i;
const KVER_RE  = /kernel v([0-9][0-9.]*)/i;

// Trouble-line taxonomy. First match wins, so the most specific/severe rule is
// listed first and the generic error/failed catch-all is last. severity drives
// both display ordering AND how long the note lingers after the last occurrence
// (a churn-race signal clears fast; a durability warning is held far longer), so
// the note decays back to blank as a relay returns to health.
//   sev 3 = durability / substantive   sev 2 = connectivity / credential   sev 1 = transient
const EVENT_RULES = [
  { kind: 'replicate', sev: 3, re: /replicate[-\w]*failed|replicate-all/i },
  { kind: 'degraded',  sev: 3, re: /degraded/i },
  { kind: 'turn/cred', sev: 2, re: /credential|\bTURN\b|\bturn:|\b401\b|unauthor/i },
  { kind: 'reconnect', sev: 2, re: /reconnect|disconnect|bridge[^\n]*(down|lost|closed)/i },
  { kind: 'auth',      sev: 2, re: /\bauth\b[^\n]*fail|verif[^\n]*fail|bad[- ]?signature/i },
  { kind: 'signal',    sev: 1, re: /signal-handler-failed|icecandidate|ice[- ]candidate|\bICE\b|\bsdp\b|setremote|setlocal/i },
  { kind: 'upgrade',   sev: 1, re: /upgrade/i },
  { kind: 'error',     sev: 1, re: /error|failed/i },
];
// How long an event of each severity stays in the note after its last sighting.
const EVENT_TTL_MS = { 1: 120000, 2: 300000, 3: 600000 };

function ingest(idx, line) {
  const s = stats[idx];
  const m = STATE_RE.exec(line);
  if (m) {
    s.state = m[1]; s.peers = +m[2]; s.meshO = +m[3]; s.meshB = +m[4];
    s.roles = +m[5]; s.subs = +m[6]; s.last = Date.now();
    return;
  }
  const n = NODE_RE.exec(line); if (n && s.node === '—') s.node = n[1].slice(0, 8);
  const k = KVER_RE.exec(line);  if (k) s.kernel = k[1];
  // Classify a trouble line into a COUNTED, decaying event — not a frozen,
  // mid-token-truncated string. We keep the running count, the last-seen time,
  // and the FULL text (recoverable with `d`), so the note reflects what is
  // happening now, not an exception frozen from hours ago.
  const clean = line.replace(/^\[[^\]]*\]\s*/, '').trim();
  if (!clean) return;
  for (const rule of EVENT_RULES) {
    if (rule.re.test(clean)) {
      const ev = s.events[rule.kind] || (s.events[rule.kind] = { count: 0, last: 0, sev: rule.sev, text: '' });
      ev.count++; ev.last = Date.now(); ev.text = clean;
      break;
    }
  }
}

// Compact "5s / 4m / 2h" age.
function fmtAge(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const mm = Math.round(s / 60);
  if (mm < 60) return `${mm}m`;
  return `${Math.round(mm / 60)}h`;
}

// The note cell: a live op status wins; otherwise the still-relevant trouble
// events (those seen within their severity TTL), most severe first, shown as
// `kind×count age`. Returns '' — a clean cell — once every event has aged out,
// which is what happens as a relay stays healthy.
function noteFor(s, now) {
  if (s.op) return s.op;
  const live = Object.entries(s.events)
    .map(([kind, ev]) => ({ kind, sev: ev.sev, count: ev.count, age: now - ev.last }))
    .filter((e) => e.age <= (EVENT_TTL_MS[e.sev] || 300000))
    .sort((a, b) => (b.sev - a.sev) || (a.age - b.age));
  if (!live.length) return '';
  const top = live[0];
  let out = `${top.kind}×${top.count} ${fmtAge(top.age)}`;
  if (live.length > 1) out += ` +${live.length - 1}`;
  return out;
}

// Rolling-op flags — declared BEFORE render() so the 1s render timer, which
// reads `restarting` in its footer, never hits the temporal dead zone (a `let`
// declared lower in the file is uninitialized until its line runs → the first
// render tick crashed with "Cannot access 'restarting' before initialization").
let draining = false;
let restarting = false;
// Footer note set by the [d] full-event dump (path of the written file), read by
// render()'s footer — declared here so the first render tick can't hit its TDZ.
let detailMsg = '';

// ── Dashboard renderer (in-place, no scrolling) ──────────────────────
let kernelVer = '';
function pad(v, w) { const s = String(v); return s.length >= w ? s.slice(0, w) : s + ' '.repeat(w - s.length); }
function padL(v, w) { const s = String(v); return s.length >= w ? s.slice(0, w) : ' '.repeat(w - s.length) + s; }

function render() {
  const now = Date.now();
  const up = stats.filter((s) => s.exited == null).length;
  const meshed = stats.filter((s) => s.state === 'open' && s.meshO > 0).length;
  const totRoles = stats.reduce((a, s) => a + s.roles, 0);
  const hdr = `Axona relay fleet · region=${region} · ${bridge ? bridge : network}` +
    (kernelVer ? ` · kernel v${kernelVer}` : '');
  const summary = `${up}/${count} up · ${meshed} meshed · ${totRoles} roles total` +
    `   (${new Date(now).toTimeString().slice(0, 8)})`;
  const cols = `${pad('#', 4)}${pad('node', 10)}${pad('state', 8)}${padL('peers', 6)}` +
    `${padL('mesh', 8)}${padL('roles', 7)}${padL('subs', 6)}  recent events`;
  const rows = stats.map((s, i) => {
    const stale = s.last && now - s.last > 8000;
    const state = s.exited != null ? `exit${s.exited}` : (stale ? `${s.state}?` : s.state);
    const mesh = `${s.meshO}/${s.meshB}`;
    return `${pad(i + 1, 4)}${pad(s.node, 10)}${pad(state, 8)}${padL(s.peers, 6)}` +
      `${padL(mesh, 8)}${padL(s.roles, 7)}${padL(s.subs, 6)}  ${noteFor(s, now)}`;
  });
  const busy = restarting ? '  ⟳ rolling restart in progress…' : '';
  const dmsg = detailMsg ? `  ·  ${detailMsg}` : '';
  const foot = `[r] rolling restart · [q]/Ctrl-C rolling stop · [d] dump full events · --raw for logs${busy}${dmsg}`;
  const frame = [hdr, summary, '', cols, ...rows, '', foot].join('\n');
  // Cursor home, clear to end of screen, write frame.
  process.stdout.write('\x1b[H\x1b[0J' + frame + '\n');
}

let renderTimer = null;
if (!rawMode) {
  process.stdout.write('\x1b[2J');                 // clear once at start
  renderTimer = setInterval(render, 1000);
  if (typeof renderTimer.unref === 'function') renderTimer.unref();
}

// ── Launch ───────────────────────────────────────────────────────────
console.log(`axona-relay fleet → ${count} relay(s) · region=${region} · ` +
  (bridge ? `bridge=${bridge}` : `network=${network}`) +
  (rawMode ? ' · raw logs' : ' · live dashboard'));

const children = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const carry = Array.from({ length: count }, () => '');   // partial-line buffers

function onData(idx, buf) {
  // Split into whole lines, buffering any partial trailing line.
  const text = carry[idx] + String(buf);
  const lines = text.split('\n');
  carry[idx] = lines.pop() ?? '';
  for (const line of lines) {
    if (!line) continue;
    ingest(idx, line);
    if (!stats[idx].kernel) { /* noop */ } else if (!kernelVer) kernelVer = stats[idx].kernel;
    if (rawMode) process.stdout.write(`[relay-${idx + 1}] ${line}\n`);
  }
}

// Spawn one relay into slot idx. Reused by the initial launch AND by the rolling
// restart, so a respawned slot is wired identically to a fresh one.
function spawnRelay(idx) {
  const env = { ...process.env, RELAY_REGION: region, RELAY_NETWORK: network, RELAY_TUI: '0' };
  if (bridge) env.BRIDGE_URL = bridge;
  const child = fork(entry, [], { env, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  const s = stats[idx];
  s.pid = child.pid; s.exited = null; s.node = '—'; s.state = '…';
  s.peers = 0; s.meshO = 0; s.meshB = 0; s.roles = 0; s.subs = 0; s.last = 0;
  s.events = {}; s.op = '';
  carry[idx] = '';
  child.stdout.on('data', (d) => onData(idx, d));
  child.stderr.on('data', (d) => onData(idx, d));
  child.on('exit', (code, sig) => { stats[idx].exited = code != null ? code : (sig || '?'); });
  children[idx] = child;
  return child;
}

// Stop the relay in slot idx and RESOLVE only once its process has exited — i.e.
// its graceful leave() (cache/role handoff to heirs) has run to completion. A
// leaver wedged past leaveTimeoutMs is hard-killed so the roll can proceed.
function stopOne(idx) {
  const child = children[idx];
  if (!child || stats[idx].exited != null) return Promise.resolve();
  return new Promise((resolve) => {
    let done = false;
    const finish = () => { if (done) return; done = true; clearTimeout(t); resolve(); };
    const t = setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* */ } finish(); }, leaveTimeoutMs);
    if (typeof t.unref === 'function') t.unref();
    child.once('exit', finish);
    try { child.kill('SIGTERM'); } catch { finish(); }   // already gone
  });
}

// ── Initial launch ────────────────────────────────────────────────────
for (let i = 1; i <= count; i++) {
  const idx = i - 1;
  spawnRelay(idx);
  if (i < count && staggerMs > 0) await sleep(staggerMs);   // spread the handshakes
}

// ── ROLLING drain: stop the whole fleet ONE relay at a time ──────────────
// Never a mass SIGTERM: that leaves every dying relay's heirs to be the OTHER
// dying relays (total-cohort teardown) and shreds the region's held history.
// (draining declared above, before render)
async function rollingShutdown() {
  if (draining) return; draining = true;
  if (renderTimer) clearInterval(renderTimer);
  process.stdout.write(`\nfleet: rolling stop — one at a time, ${healDelayMs}ms heal between…\n`);
  for (let idx = 0; idx < count; idx++) {
    if (stats[idx].exited != null) continue;
    process.stdout.write(`  · stopping relay-${idx + 1} (pid ${stats[idx].pid}); waiting for leave()…\n`);
    await stopOne(idx);
    if (idx < count - 1) await sleep(healDelayMs);   // let heirs settle before the next departure
  }
  process.stdout.write('fleet: all relays stopped cleanly.\n');
  process.exit(0);
}

// ── ROLLING restart IN PLACE: drain+respawn each slot in turn ────────────
// Keeps N-1 relays live throughout, so the region's cache never loses all its
// holders at once. After respawning a slot we wait for it to re-mesh (so heirs
// exist) before touching the next. This is the robust "restart the fleet" path.
// (restarting declared above, before render)
async function rollingRestart() {
  if (restarting || draining) return; restarting = true;
  if (rawMode) process.stdout.write(`[fleet] rolling restart of ${count} relay(s)…\n`);
  for (let idx = 0; idx < count; idx++) {
    stats[idx].op = 'draining…';
    if (rawMode) process.stdout.write(`[fleet] relay-${idx + 1}: draining (waiting for leave())…\n`);
    await stopOne(idx);
    await sleep(healDelayMs);                 // heal after the departure
    spawnRelay(idx);                          // fresh replacement in this slot (resets events + op)
    stats[idx].op = 're-meshing…';
    const deadline = Date.now() + 30000;      // wait for it to re-mesh before the next departure
    while (Date.now() < deadline && !(stats[idx].state === 'open' && stats[idx].meshO > 0)) await sleep(500);
    stats[idx].op = '';
    if (rawMode) process.stdout.write(`[fleet] relay-${idx + 1}: back (${stats[idx].state}, mesh ${stats[idx].meshO})\n`);
    await sleep(healDelayMs);                 // let it accrue roles before the next departure
  }
  restarting = false;
  if (rawMode) process.stdout.write('[fleet] rolling restart complete.\n');
}

// [d] — write every relay's FULL, untruncated latest event line per kind to a
// timestamped file. The dashboard column is deliberately compact; this is the
// escape hatch for root-causing one exception (e.g. the exact {from, kind, err}
// of a signal-handler-failed) without switching the whole fleet to --raw.
function dumpDetail() {
  const now = Date.now();
  const out = [`# axona fleet event detail · ${new Date(now).toISOString()} · region=${region}`];
  let any = false;
  stats.forEach((s, i) => {
    const evs = Object.entries(s.events).sort((a, b) => b[1].sev - a[1].sev || b[1].last - a[1].last);
    if (!evs.length) return;
    any = true;
    out.push(`relay-${i + 1}  node=${s.node}  state=${s.state}  peers=${s.peers}  mesh=${s.meshO}/${s.meshB}  roles=${s.roles}`);
    for (const [kind, ev] of evs) {
      out.push(`  ${kind}  ×${ev.count}  last=${fmtAge(now - ev.last)}  sev=${ev.sev}`);
      out.push(`    ${ev.text}`);
    }
  });
  if (!any) { detailMsg = 'no events to dump — fleet clean'; return; }
  const stamp = new Date(now).toISOString().replace(/[:.]/g, '-');
  const file = join(here, '..', `fleet-events-${stamp}.log`);
  try { writeFileSync(file, out.join('\n') + '\n'); detailMsg = `events → ${file}`; }
  catch (e) { detailMsg = `dump failed: ${e.message}`; }
}

process.on('SIGINT', rollingShutdown);
process.on('SIGTERM', rollingShutdown);

// ── Keyboard controls (cross-platform, Windows-safe) ─────────────────────
// "Restart in place" has no portable signal (Windows lacks SIGHUP/SIGUSR2), so
// drive it from a keypress: `r` rolling-restart, `d` dump full events, `q`/Ctrl-C stop.
if (process.stdin.isTTY) {
  try {
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (key) => {
      if (key === 'r' || key === 'R') rollingRestart();
      else if (key === 'd' || key === 'D') dumpDetail();
      else if (key === 'q' || key === 'Q' || key === '\u0003') rollingShutdown();   // q or Ctrl-C
    });
  } catch { /* raw mode unavailable — SIGINT still triggers rollingShutdown */ }
}
