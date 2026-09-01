// =====================================================================
// clock-probe.mjs — measure each host's wall-clock OFFSET and UNCERTAINTY vs the
// m4 basis, immediately before and after an arm (Aster 2a2778b2). A measured
// offset is a CORRECTION; the uncertainty + observed pre/post drift is what bounds
// the analyzer's boundary band. Writes harness/results/clock-<seed>-<phase>.json.
//
// DECLARED METHOD (NTP-style round trip, N samples per host):
//   t1 = local ms; ssh host -> remote stamps t2 = Date.now(); reply; t4 = local ms.
//   one-way delay assumed symmetric => offset = t2 - (t1 + t4)/2,
//   uncertainty of that sample = RTT/2 = (t4 - t1)/2.
// Report the MEDIAN offset and the MIN-uncertainty sample's offset is NOT used;
// we keep the median offset and the MIN uncertainty achieved (best-timed sample)
// AND the sample spread, so the analyzer can bound residual ambiguity honestly.
// m4 (local) is the basis: offset 0, uncertainty 0.
//
//   node harness/clock-probe.mjs <seed> <pre|post> [hosts...]
// =====================================================================
import { execSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';

const SEED = process.argv[2] || '0';
const PHASE = process.argv[3] || 'pre';
const HOSTS = process.argv.slice(4).length ? process.argv.slice(4) : ['m4', 'm1', 'axona-linux', 'axona-win'];
const SAMPLES = 7;

// pure, testable: given [{t1,t2,t4}] samples -> {offset, uncertainty, spread, n}
export function computeOffset(samples) {
  const rows = samples.filter((s) => Number.isFinite(s.t1) && Number.isFinite(s.t2) && Number.isFinite(s.t4) && s.t4 >= s.t1);
  if (!rows.length) return null;
  const offs = rows.map((s) => s.t2 - (s.t1 + s.t4) / 2).sort((a, b) => a - b);
  const unc = rows.map((s) => (s.t4 - s.t1) / 2);
  const median = offs[Math.floor(offs.length / 2)];
  return {
    offset: Math.round(median),
    uncertainty: Math.round(Math.min(...unc)),      // best-timed sample's half-RTT
    spread: Math.round(offs[offs.length - 1] - offs[0]),
    n: rows.length,
  };
}

function remoteNow(host) {
  const NODE = host === 'm1' ? '/opt/homebrew/Cellar/node/26.6.0/bin/node'
    : host === 'axona-linux' ? '$HOME/bin/node' : 'node';
  if (host === 'axona-win') {
    const script = 'node -e "process.stdout.write(String(Date.now()))"';
    return Number(execSync(`printf '%s\\n' '${script}' | ssh -o ConnectTimeout=10 axona-win '"C:\\\\Program Files\\\\Git\\\\bin\\\\bash.exe" -s'`, { encoding: 'utf8', timeout: 15000 }).trim());
  }
  return Number(execSync(`ssh -o ConnectTimeout=10 ${host} '${NODE} -e "process.stdout.write(String(Date.now()))"'`, { encoding: 'utf8', timeout: 15000 }).trim());
}

const out = {};
for (const host of HOSTS) {
  if (host === 'm4') { out[host] = { offset: 0, uncertainty: 0, spread: 0, n: SAMPLES, basis: true }; continue; }
  const samples = [];
  for (let i = 0; i < SAMPLES; i++) {
    const t1 = Date.now();
    let t2; try { t2 = remoteNow(host); } catch (e) { continue; }
    const t4 = Date.now();
    if (Number.isFinite(t2)) samples.push({ t1, t2, t4 });
  }
  const r = computeOffset(samples);
  out[host] = r || { offset: null, uncertainty: null, spread: null, n: 0, failed: true };
}

// mono is unavailable across process boundaries, so stamp epoch only; the ANALYZER
// pairs <seed>-pre with <seed>-post to get drift = |offsetPost - offsetPre| per host.
const path = `harness/results/clock-${SEED}-${PHASE}.json`;
writeFileSync(path, JSON.stringify({ seed: SEED, phase: PHASE, method: 'ntp-roundtrip-median', hosts: out }, null, 2) + '\n');
console.log(`clock-probe ${PHASE}: ${path}`);
for (const [h, r] of Object.entries(out)) console.log(`  ${h}: offset=${r.offset}ms uncertainty=${r.uncertainty}ms spread=${r.spread}ms n=${r.n}${r.failed ? ' FAILED' : ''}`);
