// smoke_emission_sites.mjs — the doc↔code coherence guard for wire emissions
// (v4.25.0, Phase 6c of the v0.2 refactor program).
//
// The principal-liveness rule (Axona-Architecture §VIII; INVARIANTS I-10) is
// enforced here STATICALLY: REPLICATE plants a durable BACKUP relationship on
// its receiver, so it may be emitted from exactly ONE place — the live-root
// replication sweep. The 4.24.0 backbone collapse was a second emission site
// (the departure-path spray) that read as a cheap durability win; this smoke
// makes that mistake a CI failure instead of a fleet incident. HANDOFF is the
// departure path's ONLY outbound data verb (two sites: the acked rounds and
// the runner-up fallback, both inside pubsubLeaveHandoff); HANDOFFACK is the
// heir's single confirming reply.
//
// If this smoke fails your change added an emission site. That is sometimes
// right — but it requires (per the architecture doc): naming the nature the
// new emission creates on the receiver, naming its eviction path, and
// updating the doc's policy table + this smoke IN THE SAME CHANGE.
//
// Run: node test/smoke_emission_sites.mjs
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

const dir = join(dirname(fileURLToPath(import.meta.url)), '../src/pubsub');
const files = readdirSync(dir).filter(f => f.endsWith('.js'));

// An emission = a line that both routes/sends AND names the verb constant.
function emissions(verb) {
  const hits = [];
  for (const f of files) {
    const lines = readFileSync(join(dir, f), 'utf8').split('\n');
    lines.forEach((line, i) => {
      const t = line.trim();
      if (t.startsWith('//') || t.startsWith('*')) return;
      if (!(t.includes('_route(') || t.includes('_send('))) return;
      if (new RegExp(`\\bT\\.${verb}\\b`).test(t)) hits.push(`${f}:${i + 1}`);
    });
  }
  return hits;
}

console.log('emission-site guard — the wire verbs with structural constraints\n');

// Phase 8: the four repair/durability verbs emit ONLY from the sync engine —
// one emission site per verb, all in syncEngine.js. A second site anywhere
// else is the 4.24.0 mistake class (a spray path outside the policy table).
for (const verb of ['REPLICATE', 'HANDOFF', 'PULLUP', 'REPLAYUP']) {
  const hits = emissions(verb);
  check(`${verb} emitted from exactly ONE site`, hits.length === 1, `[${hits}]`);
  check(`…and that site is the sync engine`,
    hits.every(h => h.startsWith('syncEngine.js')), `[${hits}]`);
}

const ack = emissions('HANDOFFACK');
check('HANDOFFACK emitted from exactly ONE site (the heir\'s confirming reply, in the engine\'s HANDOFF ingest hook)',
  ack.length === 1 && ack[0].startsWith('syncEngine.js'), `[${ack}]`);

// ── _log prefix coherence — observability strings must fail loudly or exist ──
// AxonaManager._log prepends 'pubsub:' to every event before it reaches the
// sink, so a call site must pass the BARE event name. A pre-prefixed arg emits
// a DOUBLED 'pubsub:pubsub:…' string that silently never matches any operator
// alert or log-analysis rule keyed on the documented name (task #362, the
// singleton-root honesty signal — the exact miss this guard now locks).
console.log('');
const logCalls = [];       // { site, event } for every _log(level, event, …) call
for (const f of files) {
  const lines = readFileSync(join(dir, f), 'utf8').split('\n');
  lines.forEach((line, i) => {
    const t = line.trim();
    if (t.startsWith('//') || t.startsWith('*')) return;
    const m = t.match(/\b_log\(\s*'[a-z]+'\s*,\s*'([^']*)'/);
    if (m) logCalls.push({ site: `${f}:${i + 1}`, event: m[1] });
  });
}
const prefixed = logCalls.filter(c => c.event.startsWith('pubsub:'));
check('no _log call site pre-prefixes its event with "pubsub:" (would emit a doubled prefix)',
  prefixed.length === 0, `[${prefixed.map(c => `${c.site} → '${c.event}'`)}]`);

// The #362 honesty signal must reach the sink as EXACTLY this documented string.
const emitted = e => 'pubsub:' + e;   // mirrors AxonaManager._log
const singleton = logCalls.find(c => c.event.endsWith('singleton-root-confirm'));
check('singleton-root-confirm emitted as exactly \'pubsub:singleton-root-confirm\'',
  !!singleton && emitted(singleton.event) === 'pubsub:singleton-root-confirm',
  singleton ? `[${singleton.site} → '${emitted(singleton.event)}']` : '[no emission site found]');

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
