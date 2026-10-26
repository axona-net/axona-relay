// fence_mcp_publish_confirm.mjs — a publish is not "at the root" until it is read back.
//
// 2026-10-25: five council posts returned publish ok and never reached the root.
// peer.pub() resolves on local acceptance (the routed PUB is not awaited); the
// publishing processes closed their peer right after. This fence pins the two
// defences: publish({confirm:true}) reads the message back from the holder, and
// shutdown() drains every unconfirmed publish BEFORE the peer closes.
//
// Pure checks use src/mcp-confirm.js with a fake pull and a fake clock. The
// wiring checks read src/mcp-session.js and src/mcp.js as text: the session
// records every publish as in flight, shutdown drains before close, and the
// tool exposes confirm. Mutants that must go red: a confirm that returns true
// without pulling; a drain removed from shutdown; a publish that does not record.
//
// Run: node test/fence_mcp_publish_confirm.mjs

import { readFileSync } from 'node:fs';
import { confirmPublished, drainInflight } from '../src/mcp-confirm.js';

let n = 0, fail = 0;
const ok = (label, cond, detail = '') => { n++; if (cond) console.log(`  ✓ ${label}`); else { fail++; console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`); } };
const ID_A = 'a'.repeat(64), ID_B = 'b'.repeat(64);
const clock = () => { let t = 0; return { now: () => t, tick: (ms) => { t += ms; } }; };

// ── confirmPublished ───────────────────────────────────────────────────
{
  const c = clock(); let calls = 0;
  const pull = async (id, { timeoutMs }) => { calls++; c.tick(timeoutMs); return calls >= 2 ? { msgId: id, message: 'x' } : null; };
  const r = await confirmPublished({ pull, msgId: ID_A, descriptor: { d: 1 }, timeoutMs: 8000, stepMs: 1500, now: c.now });
  ok('found on the second pull → confirmed:true with attempts=2', r.confirmed === true && r.attempts === 2, JSON.stringify(r));
}
{
  const c = clock(); let calls = 0;
  const pull = async (id, { timeoutMs }) => { calls++; c.tick(timeoutMs); return null; };
  const r = await confirmPublished({ pull, msgId: ID_A, descriptor: {}, timeoutMs: 8000, stepMs: 1500, now: c.now });
  ok('never found → confirmed:false, budget respected (6 attempts of 1500 ms ≤ 8000 ms)', r.confirmed === false && r.attempts === 6 && r.elapsedMs <= 8000, JSON.stringify(r));
}
{
  const c = clock(); let calls = 0;
  const pull = async (id, { timeoutMs }) => { calls++; c.tick(timeoutMs); return { msgId: ID_B, message: 'other' }; };
  const r = await confirmPublished({ pull, msgId: ID_A, descriptor: {}, timeoutMs: 3000, stepMs: 1500, now: c.now });
  ok('a different message read back does NOT confirm', r.confirmed === false && r.attempts === 2);
}
{
  const c = clock(); let calls = 0;
  const pull = async (id, { timeoutMs }) => { calls++; c.tick(timeoutMs); if (calls === 1) throw new Error('route lost'); return { msgId: id }; };
  const r = await confirmPublished({ pull, msgId: ID_A, descriptor: {}, timeoutMs: 8000, stepMs: 1500, now: c.now });
  ok('a throwing pull is retried within the budget and the later success confirms', r.confirmed === true && r.attempts === 2 && r.error === null, JSON.stringify(r));
}
{
  let threw = false;
  try { await confirmPublished({ pull: async () => null, msgId: 'short', descriptor: {} }); } catch { threw = true; }
  ok('a malformed msgId is refused before any pull', threw);
}

// ── drainInflight ──────────────────────────────────────────────────────
{
  const c = clock();
  const inflight = new Map([[ID_A, { topic: 'council', descriptor: {}, at: 0, timeoutMs: 8000 }], [ID_B, { topic: 'jokes', descriptor: {}, at: 0, timeoutMs: 8000 }]]);
  const pull = async (id, { timeoutMs }) => { c.tick(timeoutMs); return id === ID_A ? { msgId: id } : null; };
  const d = await drainInflight({ inflight, pull, budgetMs: 12000, stepMs: 1500, now: c.now });
  ok('drain confirms A, reports B unconfirmed, and empties the map', d.confirmed.length === 1 && d.confirmed[0].msgId === ID_A && d.unconfirmed.length === 1 && d.unconfirmed[0].msgId === ID_B && inflight.size === 0, JSON.stringify(d));
  ok('drain stays inside its total budget', d.elapsedMs <= 12000, String(d.elapsedMs));
}
{
  const c = clock();
  const inflight = new Map([[ID_A, { topic: 't', descriptor: {}, at: 0, timeoutMs: 8000 }], [ID_B, { topic: 't', descriptor: {}, at: 0, timeoutMs: 8000 }]]);
  const pull = async (id, { timeoutMs }) => { c.tick(timeoutMs); return null; };
  const d = await drainInflight({ inflight, pull, budgetMs: 3000, stepMs: 1500, now: c.now });
  ok('an exhausted budget lists the rest as unconfirmed with the reason, never silently', d.unconfirmed.length === 2 && d.unconfirmed.some(u => u.reason === 'drain budget exhausted'), JSON.stringify(d.unconfirmed));
}

// ── wiring, read as text ───────────────────────────────────────────────
const session = readFileSync(new URL('../src/mcp-session.js', import.meta.url), 'utf8');
const mcp = readFileSync(new URL('../src/mcp.js', import.meta.url), 'utf8');
{
  const pubAt = session.indexOf('const msgId = await s.peer.pub(');
  const recAt = session.indexOf('_inflight.set(msgId', pubAt);
  const retAt = session.indexOf('return result;', pubAt);
  ok('publish() records the msgId as in flight after pub() and before returning', pubAt > 0 && recAt > pubAt && retAt > recAt);
  ok('publish() confirms inline when asked, via confirmPublished', session.indexOf('confirmPublished({', pubAt) > pubAt && session.indexOf('confirmPublished({', pubAt) < retAt);
}
{
  const shAt = session.indexOf('export async function shutdown()');
  const drainAt = session.indexOf('drainInflight({', shAt);
  const closeAt = session.indexOf('_session.close()', shAt);
  ok('shutdown() drains in-flight publishes BEFORE closing the peer', shAt > 0 && drainAt > shAt && closeAt > drainAt, `shutdown@${shAt} drain@${drainAt} close@${closeAt}`);
  ok('shutdown() names each unconfirmed publish on stderr', session.indexOf('publish NOT read back before shutdown', shAt) > shAt);
}
ok('the axona_publish tool exposes confirm and confirmTimeoutMs', /confirm: z\.boolean\(\)/.test(mcp) && /confirmTimeoutMs: z\.number\(\)/.test(mcp));

const EXPECTED = 13;
console.log(`\n${fail === 0 && n === EXPECTED ? '✓' : '✗'} fence_mcp_publish_confirm: ${n - fail}/${n} checks passed (expected ${EXPECTED})`);
process.exit(fail === 0 && n === EXPECTED ? 0 : 1);
