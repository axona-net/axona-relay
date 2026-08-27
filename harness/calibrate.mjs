// =============================================================================
// harness/calibrate.mjs — the calibration run (spec v0.3 §7, a gate-2 artifact).
//
// What has to be true before "p99 ≤ 2500ms" means anything? That per-host
// clock error is small against the threshold, and measured with its own error
// bound stated. This probe samples every remote fleet host from the M4
// (where the analyzer runs): N round-trips of `date +%s%N` over ssh, offset
// estimated at the local midpoint, error bounded by ±RTT/2 per sample. The
// analyzer will additionally join on in-band clock records (ledger 'clock'
// rows); this run is the host-level floor.
//
// ssh transport RTT (hundreds of ms incl. handshake) does NOT contaminate the
// offset estimate — the midpoint method cancels symmetric latency; what
// remains is asymmetry, bounded by the reported minRtt/2.
//
// Run from the M4:  node harness/calibrate.mjs [samples=20]
// Output: harness/results/calibration-<utcstamp>.jsonl + a summary table.
// =============================================================================
import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdirSync } from 'node:fs';

const HOSTS = [
  { alias: 'm1',          cmd: ['ssh', 'm1', 'date +%s%N'] },
  { alias: 'axona-linux', cmd: ['ssh', 'axona-linux', 'date +%s%N'] },
  { alias: 'axona-win',   cmd: ['ssh', 'axona-win', '"C:\\Program Files\\Git\\bin\\bash.exe" -c "date +%s%N"'] },
];

const N = Number(process.argv[2] ?? 20);
mkdirSync(new URL('./results/', import.meta.url).pathname, { recursive: true });
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const out = new URL(`./results/calibration-${stamp}.jsonl`, import.meta.url).pathname;

const summary = [];
for (const h of HOSTS) {
  const rows = [];
  for (let i = 0; i < N; i++) {
    const t0 = process.hrtime.bigint();
    const wall0 = BigInt(Date.now()) * 1_000_000n;
    let remote;
    try {
      remote = BigInt(execFileSync(h.cmd[0], h.cmd.slice(1), { timeout: 15_000 }).toString().trim());
    } catch (e) {
      rows.push({ i, error: String(e.message).slice(0, 80) });
      continue;
    }
    const t1 = process.hrtime.bigint();
    const rttMs = Number(t1 - t0) / 1e6;
    // local wall-clock at the midpoint of the exchange
    const localMidNs = wall0 + (t1 - t0) / 2n;
    const offsetMs = Number(remote - localMidNs) / 1e6;
    const row = { host: h.alias, i, offsetMs: +offsetMs.toFixed(2), rttMs: +rttMs.toFixed(1) };
    rows.push(row);
    appendFileSync(out, JSON.stringify(row) + '\n');
  }
  const ok = rows.filter((r) => r.error === undefined);
  if (!ok.length) { summary.push({ host: h.alias, error: 'ALL SAMPLES FAILED' }); continue; }
  // The best offset estimate is the sample with the smallest RTT (least
  // asymmetry exposure); the error bound is that RTT/2.
  const best = ok.reduce((a, b) => (a.rttMs < b.rttMs ? a : b));
  const offs = ok.map((r) => r.offsetMs).sort((a, b) => a - b);
  summary.push({
    host: h.alias, samples: ok.length,
    offsetMs: best.offsetMs, boundMs: +(best.rttMs / 2).toFixed(1),
    offsetMedianMs: offs[Math.floor(offs.length / 2)],
    minRttMs: best.rttMs,
  });
}

console.log(`calibration ${stamp} — offsets vs M4 wall clock (${N} samples/host)`);
for (const s of summary) {
  if (s.error) { console.log(`  ${s.host}: ${s.error}`); continue; }
  console.log(`  ${s.host}: offset ${s.offsetMs}ms (±${s.boundMs}ms bound, median ${s.offsetMedianMs}ms, minRtt ${s.minRttMs}ms, n=${s.samples})`);
}
console.log(`rows: ${out}`);
const worst = Math.max(...summary.filter((s) => !s.error).map((s) => Math.abs(s.offsetMs) + s.boundMs));
console.log(`worst-case |offset|+bound: ${worst.toFixed(1)}ms — the p99≤2500ms gate needs this ≪ 2500`);
process.exit(summary.some((s) => s.error) ? 1 : 0);
