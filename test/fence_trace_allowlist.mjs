#!/usr/bin/env node
// fence_trace_allowlist — relay 0.169.0 (2026-10-22). Kernel 4.120.0 added a
// signal trace (offer-sent, offer-recv, answer-recv, answer-applied,
// first-remote-candidate) for the failed-dial triage. The relay prints a
// transport 'debug' event only if its name matches INTERESTING or it is in the
// LIFECYCLE allowlist; none of the five matched, so the first 4.120.0 roll
// printed none of them (19:57Z). This fence reads every trace event the vendored
// kernel emits under a "Signal trace" comment and fails if the relay would drop
// one. Mutant: delete any LIFECYCLE row → the fence names it.
import { readFileSync } from 'node:fs';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };

const relay = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');
const mesh  = readFileSync(new URL('../vendor/axona-protocol/src/transport/web/mesh.js', import.meta.url), 'utf8');

const interesting = new RegExp(relay.match(/const INTERESTING = \/(.+)\/i;/)[1], 'i');
const lifecycleBlock = relay.match(/const LIFECYCLE = \{([\s\S]*?)\n\};/)[1];
const lifecycle = new Set([...lifecycleBlock.matchAll(/'([a-z-]+)':\s*\[/g)].map((m) => m[1]));

// Trace events: a _log('<name>' within 6 lines after a "Signal trace" comment, or on the same line.
const lines = mesh.split('\n');
const traced = new Set();
lines.forEach((l, i) => {
  if (!/Signal trace|signal trace/.test(l)) return;
  for (let j = i; j < Math.min(lines.length, i + 7); j++) {
    const m = lines[j].match(/this\._log\('([a-z-]+)'/);
    if (m) { traced.add(m[1]); break; }
  }
});

check('the vendored kernel declares the five 4.120.0 trace events', ['offer-sent', 'offer-recv', 'answer-recv', 'answer-applied', 'first-remote-candidate'].every((e) => traced.has(e)), JSON.stringify([...traced]));
for (const ev of traced) {
  check(`${ev} reaches the relay log (LIFECYCLE or INTERESTING)`, lifecycle.has(ev) || interesting.test(ev));
}
for (const ev of traced) {
  if (!lifecycle.has(ev)) continue;
  const fields = lifecycleBlock.match(new RegExp(`'${ev}':\\s*\\[([^\\]]*)\\]`))[1];
  const names = [...fields.matchAll(/'([A-Za-z]+)'/g)].map((m) => m[1].toLowerCase());
  const banned = ['sdp', 'address', 'ip', 'port', 'candidate', 'stats', 'local', 'remote'];
  check(`${ev}'s allowlist carries no sdp, address or candidate body`, !names.some((n) => banned.includes(n)), fields);
}

console.log(`\nfence_trace_allowlist: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
