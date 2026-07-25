// scripts/ts-readback.mjs — DIAGNOSTIC (2026-07-25): read the ENVELOPE (not the
// body) for the latest messages on two topics and print the raw signed `ts`,
// so a display-vs-wire timestamp dispute can be settled with numbers.
//
// peer.pull returns the bare body (no ts) — see #355 — so this subscribes with
// since:'all' and captures the envelopes the delivery callback receives.
//
// Usage: node scripts/ts-readback.mjs axona.dev axona.bot
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';

const topics = process.argv.slice(2);
if (!topics.length) { console.error('usage: node scripts/ts-readback.mjs <topic> [topic...]'); process.exit(2); }

const s = await connectPeer({ region: 'eagle' });
const seen = new Map();               // topic -> [{ts, seq, signer, textHead}]

for (const name of topics) {
  seen.set(name, []);
  await s.peer.sub({ region: s.regionName, name }, (env) => {
    const body = env?.message ?? {};
    seen.get(name).push({
      ts: env?.ts ?? null,
      seq: env?.seq ?? null,
      signer: (env?.signerPubkey || '').slice(0, 8),
      text: String(body.text ?? '').slice(0, 60),
    });
  }, { since: 'all' });
}

await new Promise(r => setTimeout(r, 30_000));   // let replay land

const nowMs = Date.now();
console.log(`\nreader clock: ${new Date(nowMs).toISOString()}  (epoch ${nowMs})`);
console.log('local tz offset from UTC:', new Date().getTimezoneOffset() / -60, 'h\n');

for (const [name, rows] of seen) {
  console.log(`── ${name} — ${rows.length} envelope(s), newest 3 by ts ──`);
  rows.sort((a, b) => (b.ts ?? 0) - (a.ts ?? 0));
  for (const r of rows.slice(0, 3)) {
    const utc = r.ts ? new Date(r.ts).toISOString() : 'null';
    const loc = r.ts ? new Date(r.ts).toLocaleTimeString('en-US', { hour12: false }) : 'null';
    const skewMin = r.ts ? ((r.ts - nowMs) / 60000).toFixed(1) : 'n/a';
    console.log(`  ts=${r.ts}  UTC ${utc}  local ${loc}  vs-now ${skewMin}min  seq=${r.seq} signer=${r.signer}`);
    console.log(`     "${r.text}"`);
  }
  console.log('');
}

try { await s.disconnect?.(); } catch { /* */ }
cleanupWebRTC();
process.exit(0);
