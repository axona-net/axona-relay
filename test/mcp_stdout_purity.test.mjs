// STDOUT PURITY: under the MCP server, stdout carries JSON-RPC frames and
// nothing else, whatever any loaded module does with console.log.
//
// MCP-STDOUT-POLLUTION (Orion b91784d9, 2026-11-01): src/polyfill.js writes an
// "[ts] ice-pair {...}" line with console.log on every RTCPeerConnection state
// change, and console.log is process.stdout — the JSON-RPC wire. A tolerant
// host (Claude Code) logs "Ignoring non-JSON line on stdout" and continues; a
// strict one (Antigravity) drops the pipe. Since 0.189.0 src/mcp.js imports
// src/mcp-console.js FIRST, which rebinds console.log/info/debug/warn to stderr.
//
// This test spawns the real src/mcp.js with a --require preload that calls the
// console methods (log, info, debug, warn, and since 0.190.0 dir, table, count,
// group and assert) from a timer, AFTER the server has loaded: the same shape
// as the polyfill's state-change callback. It sends one MCP initialize request
// so stdout carries at least one real frame, closes stdin (the one exit path),
// and asserts that every stdout line parses as a JSON-RPC frame while the
// probe's text arrives on stderr. With the import of ./mcp-console.js removed
// from src/mcp.js, the probe line lands on stdout and the test fails.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const MCP = fileURLToPath(new URL('../src/mcp.js', import.meta.url));
const EXIT_BUDGET_MS = 8000;
const PROBE_DELAY_MS = 600;
const tempDirs = [];
after(() => { for (const d of tempDirs) rmSync(d, { recursive: true, force: true }); });

test('console.log/info/debug/warn from a loaded module never reach stdout under the MCP server', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'axona-mcp-purity-')); tempDirs.push(dir);
  const probe = join(dir, 'probe.cjs');
  writeFileSync(probe, `setTimeout(() => {
    console.log('[2026-11-01 18:19:43] ice-pair {"probe":"log"}');
    console.info('[probe] info');
    console.debug('[probe] debug');
    console.warn('[probe] warn');
    console.dir({ probe: 'dir' });
    console.table([{ probe: 'table' }]);
    console.count('[probe] count');
    console.group('[probe] group'); console.groupEnd();
    console.assert(false, '[probe] assert');
  }, ${PROBE_DELAY_MS});\n`);
  const env = {};
  for (const [k, v] of Object.entries(process.env)) if (!k.startsWith('MCP_') && k !== 'BRIDGE_URL' && k !== 'RELAY_NETWORK') env[k] = v;
  Object.assign(env, {
    MCP_AUTHOR_PATH: join(dir, 'id.json'), MCP_HANDLE: 'purity-test', MCP_INBOX: 'off', MCP_STANDING_WATCHES: '',
    MCP_AUTO_CONNECT: '0', BRIDGE_URL: 'ws://127.0.0.1:9',   // no startup connect; the port is a refusal anyway
  });
  const child = spawn(process.execPath, ['--require', probe, MCP], { stdio: ['pipe', 'pipe', 'pipe'], env });
  let stdout = '', stderr = '';
  child.stdout.on('data', (b) => { stdout += b.toString(); });
  child.stderr.on('data', (b) => { stderr += b.toString(); });
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })));
  // One real frame on the wire, so a clean stdout is not an empty stdout.
  child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize',
    params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'purity-test', version: '0' } } }) + '\n');
  // Wait past the probe timer, then end the transport (the one exit path).
  await new Promise((r) => setTimeout(r, PROBE_DELAY_MS + 900));
  child.stdin.end();
  let guard;
  const result = await Promise.race([exited, new Promise((_, rej) => { guard = setTimeout(() => rej(new Error(`exit timed out\n--- stderr ---\n${stderr}`)), EXIT_BUDGET_MS); })]);
  clearTimeout(guard);
  if (result.code !== 0) child.kill('SIGKILL');
  assert.equal(result.code, 0, `exit code ${result.code} signal ${result.signal}\n--- stderr ---\n${stderr}`);
  const lines = stdout.split('\n').filter((l) => l.trim() !== '');
  assert.ok(lines.length >= 1, `no frame on stdout at all\n--- stderr ---\n${stderr}`);
  for (const line of lines) {
    let frame;
    try { frame = JSON.parse(line); } catch { assert.fail(`non-JSON line on stdout: ${line.slice(0, 120)}`); }
    assert.equal(frame.jsonrpc, '2.0', `stdout line is not a JSON-RPC frame: ${line.slice(0, 120)}`);
  }
  assert.ok(lines.some((l) => JSON.parse(l).id === 1), 'initialize response missing from stdout');
  // Only after stdout is proven clean: the probe did fire, and it went to stderr.
  assert.match(stderr, /ice-pair \{"probe":"log"\}/, 'the probe never fired (or went nowhere); nothing was proven');
  assert.match(stderr, /\[probe\] info/); assert.match(stderr, /\[probe\] debug/); assert.match(stderr, /\[probe\] warn/);
  // 0.190.0: the methods that bypass a rebound console.log (dir on Node 24) and
  // the ones that route through it must all land on stderr too.
  assert.match(stderr, /probe: 'dir'/, 'console.dir did not reach stderr');
  assert.match(stderr, /table/); assert.match(stderr, /\[probe\] count: 1/); assert.match(stderr, /\[probe\] group/); assert.match(stderr, /\[probe\] assert/);
});
