// Trace the owned-topic install-race with the probe's concurrency: N peers, each
// OWNS one owned topic and cross-subscribes (since:'all') to all the others, then
// everyone publishes at once — no settle. Kernel logs captured per level via
// peer.onLog. Answers why the first owned messages strand despite since=0.
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const BRIDGE = process.env.BRIDGE || 'wss://testnet.axona.net';
const N = Number(process.env.N || 6);
const MSGS = Number(process.env.MSGS || 8);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const T0 = Date.now();
const KEEP = /root|undeliver|replay|ingest|upstream|pin|home|beacon|deliver|singleton|evict|claim|store|forward|sub/i;

console.error(`connecting ${N} peers to ${BRIDGE}`);
const peers = [];
for (let i = 0; i < N; i++) {
  const h = await connectPeer({ region: 'eagle', bridge: BRIDGE });
  h.idx = i; h.log = [];
  for (const lvl of ['info', 'warn', 'error']) {
    try { h.peer.onLog(lvl, (...a) => h.log.push({ ms: Date.now() - T0, lvl, a })); } catch (e) { if (i === 0) console.error('onLog', lvl, 'reg err', e?.message); }
  }
  peers.push(h);
}
console.error('connected', peers.map((p) => p.author.authorId.slice(0, 6)).join(','));

// topic owned by peer i
const topicOf = (i) => `harness/trace-owned-${T0 % 100000}-${i}`;
const descOf = (i) => ({ region: 'eagle', name: topicOf(i), owner: peers[i].author.authorId, write: 'owner' });

// Everyone subscribes since:'all' to EVERY owned topic (incl none of their own publish loop below)
const got = {};                       // `${ownerIdx}:${seq}` -> Set(readerIdx)
for (const r of peers) {
  for (let o = 0; o < N; o++) {
    if (o === r.idx) continue;
    await r.peer.sub(descOf(o), (env) => {
      const s = env?.message?.seq;
      if (Number.isInteger(s)) (got[`${o}:${s}`] ??= new Set()).add(r.idx);
    }, { since: 'all' });
  }
}
console.error(`all subscribed at +${Date.now() - T0}ms; publishing immediately (race)`);

// Everyone publishes to their own owned topic, concurrently, no settle
await Promise.all(peers.map(async (p) => {
  for (let seq = 0; seq < MSGS; seq++) {
    try { await p.peer.pub(descOf(p.idx), { v: 1, k: 'load', seq, nonce: `${p.idx}-${seq}` }, { signWith: p.author }); } catch { /* */ }
    await sleep(300);
  }
}));
console.error('published; waiting 35s for renewals (5s floor) + replay...');
await sleep(35000);

// Report: for each owned topic, which (seq) did each OTHER peer miss?
console.log('\n===== OWNED-TOPIC DELIVERY (readers = the other ' + (N - 1) + ' peers) =====');
const strandedByOwner = {};
for (let o = 0; o < N; o++) {
  const missRows = [];
  for (let seq = 0; seq < MSGS; seq++) {
    const seen = got[`${o}:${seq}`] ?? new Set();
    const missed = peers.filter((p) => p.idx !== o && !seen.has(p.idx)).map((p) => p.idx);
    if (missed.length) missRows.push(`seq${seq}:missBy[${missed}]`);
    if (missed.length) strandedByOwner[o] = (strandedByOwner[o] || 0) + missed.length;
  }
  console.log(`owned-${o} (owner ${peers[o].author.authorId.slice(0, 6)}): ${missRows.join(' ') || 'ALL DELIVERED'}`);
}
console.log('stranded count per owner:', JSON.stringify(strandedByOwner));

// Dump the owner logs for topics that stranded — root formation + undeliverable + replay
for (const o of Object.keys(strandedByOwner)) {
  console.log(`\n--- owner ${o} kernel log (root/deliver/replay) ---`);
  const rows = peers[o].log.filter((e) => KEEP.test(JSON.stringify(e.a))).map((e) => `  +${e.ms}ms ${e.lvl} ${JSON.stringify(e.a).slice(0, 180)}`);
  console.log(rows.slice(0, 30).join('\n') || '  (none)');
}
// One stranded reader's log too
const someReader = peers.find((p) => Object.keys(strandedByOwner).some((o) => Number(o) !== p.idx));
if (someReader) {
  console.log(`\n--- a subscriber (peer ${someReader.idx}) kernel log ---`);
  console.log(someReader.log.filter((e) => KEEP.test(JSON.stringify(e.a))).slice(0, 25).map((e) => `  +${e.ms}ms ${e.lvl} ${JSON.stringify(e.a).slice(0, 180)}`).join('\n') || '  (none)');
}

for (const p of peers) { try { await p.close(); } catch { /* */ } }
process.exit(0);
