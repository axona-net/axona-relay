#!/usr/bin/env node
// fleet.mjs — launch and MONITOR N Axona relays with one command.
// Cross-platform (pure Node, no bash) so it runs the same on Windows, macOS,
// and Linux. No global environment variables required — everything is a flag.
//
//   node scripts/fleet.mjs --region grizzly --count 10 --network testnet
//   node scripts/fleet.mjs --region useast  --count 3                 # prod
//   node scripts/fleet.mjs --region grizzly --count 5 --bridge wss://my-bridge:8080
//
// Flags:
//   --region  <name|code>      region to host (e.g. grizzly / 0x80). default useast
//   --count   <n>              how many relay processes to launch.       default 1
//   --network <prod|testnet>   which bridge network to bootstrap from.   default prod
//   --bridge  <wss-url>        explicit bridge URL (overrides --network).
//   --stagger <ms>             delay between launches (default 600) so N relays
//                              don't hit the bridge with N handshakes at once.
//   --raw                      stream each relay's raw log lines instead of the
//                              live dashboard (useful for debugging one relay).
//
// By default the output is a LIVE DASHBOARD: one fixed table, one row per relay,
// redrawn in place — so you can watch fleet health at a glance instead of a wall
// of interleaved scrolling logs. Each relay is its own child process with an
// EPHEMERAL transport identity (re-minted every start), so N relays never
// collide. Ctrl-C terminates the whole fleet gracefully.
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

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
    '[--network prod|testnet] [--bridge wss://url] [--stagger ms] [--raw]');
  process.exit(0);
}

const region  = typeof opts.region === 'string' ? opts.region : 'useast';
const count   = Math.max(1, Number.parseInt(opts.count ?? '1', 10) || 1);
const network = typeof opts.network === 'string' ? opts.network : 'prod';
const bridge  = typeof opts.bridge === 'string' ? opts.bridge : null;
const staggerMs = Math.max(0, Number.parseInt(opts.stagger ?? '600', 10) || 0);
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
const stats = Array.from({ length: count }, () => ({
  pid: null, node: '—', state: '…', peers: 0, meshO: 0, meshB: 0,
  roles: 0, subs: 0, exited: null, last: 0, note: '',
}));

const STATE_RE = /state=(\S+)\s+peers=(\d+)\s+synaptome=\d+\s+mesh\(open\/bound\)=(\d+)\/(\d+)\s+roles=(\d+)\s+subs=(\d+)/;
const NODE_RE  = /\bnode\s+([0-9a-f]{8,})/i;
const KVER_RE  = /kernel v([0-9][0-9.]*)/i;

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
  // Surface anything that looks like trouble as a short note.
  if (/error|degraded|reconnect|upgrade|failed|401|ICE/i.test(line)) {
    s.note = line.replace(/^\[[^\]]*\]\s*/, '').slice(0, 32);
  }
}

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
    `${padL('mesh', 8)}${padL('roles', 7)}${padL('subs', 6)}  note`;
  const rows = stats.map((s, i) => {
    const stale = s.last && now - s.last > 8000;
    const state = s.exited != null ? `exit${s.exited}` : (stale ? `${s.state}?` : s.state);
    const mesh = `${s.meshO}/${s.meshB}`;
    return `${pad(i + 1, 4)}${pad(s.node, 10)}${pad(state, 8)}${padL(s.peers, 6)}` +
      `${padL(mesh, 8)}${padL(s.roles, 7)}${padL(s.subs, 6)}  ${s.note}`;
  });
  const foot = 'Ctrl-C to stop the fleet · --raw for per-relay logs';
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

for (let i = 1; i <= count; i++) {
  const idx = i - 1;
  const env = { ...process.env, RELAY_REGION: region, RELAY_NETWORK: network, RELAY_TUI: '0' };
  if (bridge) env.BRIDGE_URL = bridge;
  const child = fork(entry, [], { env, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  stats[idx].pid = child.pid;
  child.stdout.on('data', (d) => onData(idx, d));
  child.stderr.on('data', (d) => onData(idx, d));
  child.on('exit', (code, sig) => { stats[idx].exited = code != null ? code : (sig || '?'); });
  children.push(child);
  if (i < count && staggerMs > 0) await sleep(staggerMs);   // spread the handshakes
}

let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  if (renderTimer) clearInterval(renderTimer);
  process.stdout.write('\nfleet: stopping all relays…\n');
  for (const c of children) { try { c.kill('SIGTERM'); } catch { /* already gone */ } }
  // Give each relay a moment to leave() gracefully, then exit.
  setTimeout(() => process.exit(0), 6000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
