// =============================================================================
// harness/probe-summary.mjs — one compact health line from an analyzer summary.
//   node harness/probe-summary.mjs <summary.json> <ts> <arm> <logfile>
// Appends the line to <logfile> (the rolling probe-log) and prints it.
// =============================================================================
import fs from 'node:fs';
const [, , sumPath, ts, arm, logPath] = process.argv;
const s = JSON.parse(fs.readFileSync(sumPath, 'utf8'));
const f = s.findings || {};
const owed = (s.deliveredLive || 0) + (s.deliveredLate || 0) + (s.eventualReplay || 0) + (s.missing || 0);
const line = {
  ts, arm,
  ops: s.ops, owed,
  stranded: s.missing, strandedSteady: s.missingSteady, strandedStartup: s.missingStartup,
  ownerUnresolved: s.ownerUnresolved,
  strandedPct: owed ? +((100 * s.missing) / owed).toFixed(1) : null,
  fullSetPct: s.ops ? +((100 * s.fullSetComplete) / s.ops).toFixed(1) : null,
  splitRoot: f['split-root'] || 0,
  stalePull: f['stale-pull'] || 0,
  wedged: f['wedged-watch'] || 0,
  late: s.deliveredLate || 0,
  connMinSynaptome: s.connHealth?.minSynaptome ?? null,
  p99: s.latencyMs?.p99 ?? null,
};
fs.appendFileSync(logPath, JSON.stringify(line) + '\n');
console.log(JSON.stringify(line));
