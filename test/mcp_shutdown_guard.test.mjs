// ONCE SHUTDOWN HAS BEGUN, THIS PROCESS NEVER SIGNS AGAIN.
//
// 0.187.0 releases the author-key lock at the ENTRY of shutdown() so a
// replacement server can claim the seat during the dying one's teardown. That
// is only safe if the dying process cannot publish, declare a class, or open a
// session after that line. These tests call shutdown() in THIS process (no
// session exists, so it returns at once and touches no network) and assert
// the three signing entry points refuse afterwards, before any argument
// validation or connect attempt. With a guard removed, the matching assertion
// fails because the call proceeds to claim an identity and connect.
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// A private author file so an accidental claim could never touch a real seat.
const dir = mkdtempSync(join(tmpdir(), 'axona-mcp-guard-'));
after(() => rmSync(dir, { recursive: true, force: true }));
process.env.MCP_AUTHOR_PATH = join(dir, 'id.json');
process.env.MCP_HANDLE = 'guard-test';
process.env.BRIDGE_URL = 'ws://127.0.0.1:1';

const { shutdown, ensureSession, publish, setAuthorClass, sendFile, reconnect } = await import('../src/mcp-session.js');

test('after shutdown() begins, ensureSession/publish/setAuthorClass are refused', async () => {
  await shutdown();                                   // no session: returns immediately
  await assert.rejects(ensureSession(), /shutting down/);
  await assert.rejects(publish({ topic: 'council', message: 'never sent' }), /shutting down/);
  await assert.rejects(setAuthorClass(), /shutting down/);
});

test('the refusal names the call and the reason', async () => {
  await assert.rejects(publish({ topic: 't', message: 'm' }), (e) => /publish refused/.test(e.message) && /seat is released/.test(e.message));
});

// 0.188.0: the two remaining entry points that sign or tear down (Orion 97e3b896).
// Each refusal must name ITS call: with a guard removed the call falls through to
// ensureSession's refusal, and the wording assertion fails.
test('sendFile and reconnect are refused by name after shutdown() begins', async () => {
  await assert.rejects(sendFile({ path: '/nonexistent', topic: 't' }), (e) => /sendFile refused/.test(e.message));
  await assert.rejects(reconnect(), (e) => /reconnect refused/.test(e.message));
});
