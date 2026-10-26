// Fence: a drained watch buffer can be refilled, and an empty poll explains itself.
// Pure checks over src/mcp-watch.js plus text checks that mcp-session.js and
// mcp.js are wired to it. Exit 1 on any failure.
import { readFileSync } from 'node:fs';
import { makeWatch, deliver, collect, emptyHint, settle, SEEN_CAP } from '../src/mcp-watch.js';

const EXPECTED = 17;
let passed = 0, failed = 0;
const check = (name, ok, detail = '') => { if (ok) { passed++; console.log(`ok   ${name}`); } else { failed++; console.log(`FAIL ${name} ${detail}`); } };

let clock = 1_000_000;
const now = () => clock;
const msg = (id, seq) => ({ message: { v: 1, text: `m${seq}` }, signer: 'abc', msgId: id, seq, ts: clock, receivedAt: clock });

const w = makeWatch({ topic: 'council', region: 'eagle', descriptor: { name: 'council' }, since: 'all', now, bufferCap: 5 });

// 1. three new arrivals
for (const [id, seq] of [['a', 1], ['b', 2], ['c', 3]]) deliver(w, msg(id, seq));
check('three new arrivals: total 3, buffered 3', w.total === 3 && w.buffer.length === 3);

// 2. drain
clock += 60_000;
let r = collect(w, { now });
check('drain returns 3, drainedTotal 3, buffer 0', r.drained === 3 && r.drainedTotal === 3 && r.remaining === 0 && w.buffer.length === 0);
check('non-empty result carries no hint', r.hint === undefined);

// 3. empty poll explains itself
r = collect(w, { now });
check('empty poll: hint names 3 arrived and 3 drained', typeof r.hint === 'string' && /3 message\(s\) arrived/.test(r.hint) && /3 were drained/.test(r.hint), r.hint);
check('empty poll: hint names the replay call', /axona_watch \{topic:"council", since:"all"\}/.test(r.hint), r.hint);
check('empty poll: counters still say total 3', r.total === 3 && r.drained === 0 && r.remaining === 0);

// 4. a requested replay delivers the same three again → refilled, counted as replayed
for (const [id, seq] of [['a', 1], ['b', 2], ['c', 3]]) deliver(w, msg(id, seq));
check('replay refills the buffer with the 3 drained messages', w.buffer.length === 3 && w.replayed === 3 && w.total === 3);

// 5. the same replay arriving again while still buffered is ignored
const dup = deliver(w, msg('b', 2));
check('duplicate of a still-buffered message is not buffered', dup.duplicate === true && dup.buffered === false && w.buffer.length === 3 && w.replayed === 3);

// 6. a genuinely new message still counts as new
const fresh = deliver(w, msg('d', 4));
check('new message after replay: total 4, duplicate false', fresh.duplicate === false && w.total === 4 && w.buffer.length === 4);

// 7. cap evicts the oldest and counts it
deliver(w, msg('e', 5)); deliver(w, msg('f', 6));
check('cap 5 evicts one, dropped 1', w.buffer.length === 5 && w.dropped === 1 && w.buffer[0].msgId === 'b');

// 8. hint for a watch that never received anything
const empty = makeWatch({ topic: 'quiet', region: 'eagle', descriptor: {}, since: 'live', now });
check('never-received hint says nothing has arrived and names since', /nothing has arrived/.test(emptyHint(empty, now)) && /since:"live"/.test(emptyHint(empty, now)));

// 9. settle: returns when arrivals stop growing; reports how many arrived
{
  const s = makeWatch({ topic: 't', region: 'eagle', descriptor: {}, now });
  let ticks = 0;
  const sleep = async () => { clock += 100; ticks++; if (ticks <= 3) deliver(s, msg('x' + ticks, ticks)); };
  const res = await settle(s, { maxMs: 2000, quietMs: 400, sleep, now });
  check('settle waits for the quiet window and counts arrivals', res.arrived === 3 && res.elapsedMs >= 700 && res.elapsedMs < 2000, JSON.stringify(res));
}

// 10. seen set is capped
{
  const s = makeWatch({ topic: 't', region: 'eagle', descriptor: {}, now, bufferCap: 1 });
  for (let i = 0; i < SEEN_CAP + 5; i++) deliver(s, msg('id' + i, i));
  check('seen set never exceeds SEEN_CAP', s.seen.size === SEEN_CAP && s.total === SEEN_CAP + 5);
}

// 11-14. wiring
const session = readFileSync(new URL('../src/mcp-session.js', import.meta.url), 'utf8');
const mcp = readFileSync(new URL('../src/mcp.js', import.meta.url), 'utf8');
check('mcp-session delivers arrivals through deliver()', /deliver\(w, m\)/.test(session));
check('mcp-session re-subscribes with since:"all" when an existing watch is asked for a replay', /alreadyWatching: true, replayed: true/.test(session) && /settle\(w/.test(session));
check('mcp-session polls through collectWatch()', /collectWatch\(w, \{ peek, max \}\)/.test(session));
check('axona_watch and axona_poll descriptions say so', /REPLAYS the topic backlog/.test(mcp) && /`hint`/.test(mcp));

console.log(`\n${passed}/${EXPECTED} checks passed, ${failed} failed`);
if (failed || passed !== EXPECTED) process.exit(1);
