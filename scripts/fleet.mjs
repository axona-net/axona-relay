#!/usr/bin/env node
// fleet.mjs — launch N Axona relays in one region with a single command.
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
//
// Each relay is its own child process with an EPHEMERAL transport identity
// (re-minted every start — see src/index.js), so N relays never collide on a
// shared identity file. Ctrl-C terminates the whole fleet gracefully.
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
  console.log('usage: node scripts/fleet.mjs --region <name> --count <n> [--network prod|testnet] [--bridge wss://url]');
  process.exit(0);
}

const region  = typeof opts.region === 'string' ? opts.region : 'useast';
const count   = Math.max(1, Number.parseInt(opts.count ?? '1', 10) || 1);
const network = typeof opts.network === 'string' ? opts.network : 'prod';
const bridge  = typeof opts.bridge === 'string' ? opts.bridge : null;

const here  = dirname(fileURLToPath(import.meta.url));
const entry = join(here, '..', 'src', 'index.js');

console.log(`axona-relay fleet → ${count} relay(s) · region=${region} · ` +
  (bridge ? `bridge=${bridge}` : `network=${network}`));

const children = [];
const tagOf = (i) => `[relay-${i}]`;
const emit = (tag, buf) => {
  for (const line of String(buf).split('\n')) if (line) process.stdout.write(`${tag} ${line}\n`);
};

for (let i = 1; i <= count; i++) {
  const env = { ...process.env, RELAY_REGION: region, RELAY_NETWORK: network, RELAY_TUI: '0' };
  if (bridge) env.BRIDGE_URL = bridge;
  const child = fork(entry, [], { env, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  const tag = tagOf(i);
  child.stdout.on('data', (d) => emit(tag, d));
  child.stderr.on('data', (d) => emit(tag, d));
  child.on('exit', (code, sig) => console.log(`${tag} exited (code=${code} signal=${sig})`));
  children.push(child);
}

let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\nfleet: stopping all relays…');
  for (const c of children) { try { c.kill('SIGTERM'); } catch { /* already gone */ } }
  // Give each relay a moment to leave() gracefully, then exit.
  setTimeout(() => process.exit(0), 6000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
