// ONE EXIT PATH: a closed stdin or a SIGHUP ENDS the MCP server and releases
// its author lock.
//
// Until 0.185.0 both only wrote a line to stderr. On 2026-11-01 an editor host
// restarted and dropped the stdio pipe without a signal; the old server stayed
// alive (sockets and timers keep the loop open), reparented to pid 1, holding
// the seat's author lock, and the replacement was REFUSED with "already held
// by pid" until a human killed the orphan (Orion f94337df, MCP-STDIN-ORPHAN).
//
// These tests spawn the real src/mcp.js with its own temporary author file and
// BRIDGE_URL pointed at a WebSocket server in THIS process on 127.0.0.1 that
// accepts the peer's connection and never answers: an open socket is what keeps
// the loop alive, which is the orphan's shape (a refused port would let the
// process fall out on its own and prove nothing). Nothing touches the network.
// Each test asserts the process EXITS (code 0) within a budget and the lock
// file is gone. With the stdin wiring in src/mcp.js removed, the first test
// hangs until its guard kills the child; with SIGHUP removed from the signal
// list in src/mcp-session.js, the second does.
import { test, before, after } from 'node:test';
import { WebSocketServer } from 'ws';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const MCP = fileURLToPath(new URL('../src/mcp.js', import.meta.url));
const EXIT_BUDGET_MS = 8000;

// A bridge that answers the TCP/WS handshake and then says nothing: the child's
// peer sits in its connect with a live socket, the way a real seat sits on
// its bridge socket, so an un-ended server stays alive.
let wss, bridgeUrl;
const held = new Set();
before(async () => {
  wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  wss.on('connection', (sock) => { held.add(sock); sock.on('close', () => held.delete(sock)); });
  await new Promise((r) => wss.once('listening', r));
  bridgeUrl = `ws://127.0.0.1:${wss.address().port}`;
});
after(async () => { for (const s of held) s.terminate(); await new Promise((r) => wss.close(r)); });

function start() {
  const dir = mkdtempSync(join(tmpdir(), 'axona-mcp-exit-'));
  const author = join(dir, 'id.json');
  const env = {};
  for (const [k, v] of Object.entries(process.env)) if (!k.startsWith('MCP_') && k !== 'BRIDGE_URL' && k !== 'RELAY_NETWORK') env[k] = v;
  Object.assign(env, {
    MCP_AUTHOR_PATH: author, MCP_HANDLE: 'exit-test', MCP_INBOX: 'off', MCP_STANDING_WATCHES: '',
    MCP_AUTO_CONNECT: '1', BRIDGE_URL: bridgeUrl,   // the silent loopback bridge above; never the network
  });
  const child = spawn(process.execPath, [MCP], { stdio: ['pipe', 'pipe', 'pipe'], env });
  let stderr = '';
  child.stderr.on('data', (b) => { stderr += b.toString(); });
  child.stdout.on('data', () => { /* JSON-RPC channel; nothing is sent */ });
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
  const until = async (pred, ms, label) => {
    const end = Date.now() + ms;
    while (!pred()) {
      if (Date.now() > end) throw new Error(`${label} timed out\n--- stderr ---\n${stderr}`);
      await new Promise((r) => setTimeout(r, 25));
    }
  };
  const exitWithin = async (ms, label) => {
    let timer;
    const guard = new Promise((_, rej) => { timer = setTimeout(() => rej(new Error(`${label}: no exit within ${ms} ms (the orphan)\n--- stderr ---\n${stderr}`)), ms); });
    try { return await Promise.race([exited, guard]); }
    finally { clearTimeout(timer); if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); }
  };
  return { child, lock: `${author}.lock`, stderr: () => stderr, until, exitWithin };
}

async function ready(s) {
  await s.until(() => /axona MCP server v\S+ ready/.test(s.stderr()), 15000, 'ready line');
  await s.until(() => existsSync(s.lock), 5000, 'author lock claimed');
}

test('closing stdin ends the server within the budget and releases the author lock', async () => {
  const s = start();
  await ready(s);
  s.child.stdin.end();
  const r = await s.exitWithin(EXIT_BUDGET_MS, 'stdin end');
  assert.equal(r.code, 0, `exit code (signal ${r.signal})`);
  assert.match(s.stderr(), /stdin ended \(host closed the MCP transport\)/);
  assert.match(s.stderr(), /\[mcp\] terminating: stdin ended/);
  assert.equal(existsSync(s.lock), false, 'author lock released');
});

test('SIGHUP ends the server within the budget and releases the author lock', async () => {
  const s = start();
  await ready(s);
  s.child.kill('SIGHUP');
  const r = await s.exitWithin(EXIT_BUDGET_MS, 'SIGHUP');
  assert.equal(r.code, 0, `exit code (signal ${r.signal})`);
  assert.match(s.stderr(), /SIGHUP received/);
  assert.match(s.stderr(), /\[mcp\] terminating: SIGHUP received/);
  assert.equal(existsSync(s.lock), false, 'author lock released');
});

test('a second terminate() is a no-op: one exit path, once', async () => {
  const s = start();
  await ready(s);
  s.child.kill('SIGHUP'); s.child.kill('SIGHUP');
  const r = await s.exitWithin(EXIT_BUDGET_MS, 'double SIGHUP');
  assert.equal(r.code, 0);
  assert.equal((s.stderr().match(/\[mcp\] terminating:/g) || []).length, 1, 'terminating logged once');
  assert.equal(existsSync(s.lock), false);
});
