// axona_pull BY msgId — found:true means THAT message, nothing else.
//
// 2026-10-31 20:03–20:09Z: the tool schema had no msgId field, so the MCP SDK
// stripped the id a caller passed and the session read the NEWEST message
// instead — then reported found:true for it. Twice in one evening a caller
// following the poll hint ("axona_pull a known msgId") got an unrelated post
// back as if it were the one asked for. This file pins the contract with a
// stub peer; nothing here touches the network.
//
// Mutants that must go red: a prefix forwarded to the peer; a non-matching
// envelope reported found:true; the no-id path sending anything but null.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import { pullMessage, MSG_ID_RE } from '../src/mcp-pull.js';

const ID_A = 'a'.repeat(64), ID_B = 'b'.repeat(64);
const DESC = { region: 'eagle', name: 'council' };

// A stub peer: records every call, answers with whatever `answer` says.
function stubPeer(answer) {
  const calls = [];
  const pull = async (id, opts) => { calls.push({ id, opts }); return typeof answer === 'function' ? answer(id) : answer; };
  return { pull, calls };
}

test('a PREFIX is refused before the peer is asked (ok:false, reason "invalid msgId")', async () => {
  const peer = stubPeer({ msgId: ID_A, message: 'newest' });
  const r = await pullMessage({ pull: peer.pull, msgId: ID_A.slice(0, 8), descriptor: DESC });
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'invalid msgId');
  assert.equal(r.found, null, 'nothing was read, so absence is not established');
  assert.equal(r.message, null);
  assert.equal(peer.calls.length, 0, 'the peer must not be contacted for a bad id');
});

test('uppercase hex, 63 chars, 65 chars and non-hex are all refused', async () => {
  for (const bad of [ID_A.toUpperCase(), ID_A.slice(0, 63), ID_A + 'a', 'g'.repeat(64), '']) {
    const peer = stubPeer({ msgId: ID_A, message: 'x' });
    const r = await pullMessage({ pull: peer.pull, msgId: bad, descriptor: DESC });
    assert.equal(r.ok, false, `expected refusal for ${JSON.stringify(bad)}`);
    assert.equal(r.reason, 'invalid msgId');
    assert.equal(peer.calls.length, 0);
  }
  assert.ok(MSG_ID_RE.test(ID_A));
});

test('a requested id that comes back as a DIFFERENT envelope is found:false with mismatch — never found:true', async () => {
  const peer = stubPeer({ msgId: ID_B, message: "someone else's newest post" });
  const r = await pullMessage({ pull: peer.pull, msgId: ID_A, descriptor: DESC });
  assert.equal(r.ok, true, 'the read itself completed');
  assert.equal(r.found, false);
  assert.equal(r.message, null, 'the unrelated message is not surfaced as the answer');
  assert.equal(r.msgId, null);
  assert.equal(r.requestedMsgId, ID_A);
  assert.deepEqual(r.mismatch, { msgId: ID_B, message: "someone else's newest post" });
  assert.match(r.reason, /different message/);
  assert.equal(peer.calls.length, 1);
  assert.equal(peer.calls[0].id, ID_A, 'the full id goes to the peer');
  assert.deepEqual(peer.calls[0].opts.topic, DESC);
});

test('a matching id returns that message as found:true', async () => {
  const peer = stubPeer((id) => ({ msgId: id, message: 'the one asked for' }));
  const r = await pullMessage({ pull: peer.pull, msgId: ID_A, descriptor: DESC, timeoutMs: 8000 });
  assert.equal(r.ok, true);
  assert.equal(r.found, true);
  assert.equal(r.msgId, ID_A);
  assert.equal(r.message, 'the one asked for');
  assert.equal(r.requestedMsgId, ID_A);
  assert.equal(r.mismatch, undefined);
  assert.equal(peer.calls[0].id, ID_A);
});

test('a requested id the holder does not have is found:false (no mismatch, nothing came back)', async () => {
  const peer = stubPeer(null);
  let t = 0; const now = () => t;
  const r = await pullMessage({ pull: peer.pull, msgId: ID_A, descriptor: DESC, timeoutMs: 8000, now });
  assert.equal(r.ok, true);
  assert.equal(r.found, false);
  assert.equal(r.mismatch, undefined);
  assert.equal(r.requestedMsgId, ID_A);
});

test('no msgId → the peer is asked for the NEWEST message (null id) and it is returned unchanged', async () => {
  const peer = stubPeer({ msgId: ID_B, message: 'newest' });
  const r = await pullMessage({ pull: peer.pull, descriptor: DESC, timeoutMs: 8000 });
  assert.equal(peer.calls.length, 1);
  assert.equal(peer.calls[0].id, null, 'the newest-message path passes null, as before');
  assert.deepEqual(peer.calls[0].opts, { topic: DESC, timeoutMs: 8000 });
  assert.equal(r.ok, true);
  assert.equal(r.found, true);
  assert.equal(r.msgId, ID_B);
  assert.equal(r.message, 'newest');
  assert.equal(r.requestedMsgId, undefined, 'no requested id is reported when none was given');
  assert.equal(r.mismatch, undefined);
  assert.deepEqual(Object.keys(r), ['ok', 'found', 'message', 'msgId', 'elapsedMs', 'timeoutMs'], 'the no-id result shape is unchanged');
});

test('no msgId, empty topic → found:false, and a timed-out read is found:null (unchanged)', async () => {
  let t = 0; const now = () => t;
  const empty = await pullMessage({ pull: async () => null, descriptor: DESC, timeoutMs: 8000, now });
  assert.equal(empty.ok, true); assert.equal(empty.found, false);
  const slow = await pullMessage({ pull: async () => { t += 8000; return null; }, descriptor: DESC, timeoutMs: 8000, now });
  assert.equal(slow.ok, false); assert.equal(slow.found, null); assert.match(slow.reason, /absence NOT established/);
  const threw = await pullMessage({ pull: async () => { throw new Error('route lost'); }, descriptor: DESC, timeoutMs: 8000, now });
  assert.equal(threw.ok, false); assert.equal(threw.found, null); assert.match(threw.reason, /route lost/);
});

// ── wiring, read as text ───────────────────────────────────────────────
const session = readFileSync(new URL('../src/mcp-session.js', import.meta.url), 'utf8');
const mcp     = readFileSync(new URL('../src/mcp.js', import.meta.url), 'utf8');
const watch   = readFileSync(new URL('../src/mcp-watch.js', import.meta.url), 'utf8');

test('the axona_pull tool schema declares msgId (so the SDK stops stripping it)', () => {
  const at = mcp.indexOf("'axona_pull'");
  const shape = mcp.slice(at, mcp.indexOf('run(pull)', at));
  assert.match(shape, /msgId:\s*z\.string\(\)\.optional\(\)/);
  assert.match(shape, /64-char/);
});

test('session pull() takes msgId and hands it to pullMessage over the live peer', () => {
  const at = session.indexOf('export async function pull(');
  const body = session.slice(at, session.indexOf('\n}\n', at));
  assert.match(body, /msgId/);
  assert.match(body, /pullMessage\(/);
  assert.match(body, /s\.peer\.pull\(id, o\)/);
  assert.match(session, /from '\.\/mcp-pull\.js'/);
});

test('the empty-buffer hint names the FULL 64-hex requirement, not "a known msgId"', () => {
  assert.equal(/axona_pull a known msgId/.test(watch), false);
  assert.match(watch, /FULL 64-hex msgId/);
});
