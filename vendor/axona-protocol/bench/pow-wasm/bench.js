// Bench orchestrator (main thread): rich device info → cycle a SUITE of tests in
// a Worker (fault-tolerant: a test that fails is skipped with a note, the rest
// keep going) → aggregate → render → report. See README.md.

// Bump on every bench change so a stale cached app is obvious in the UI.
const BENCH_VERSION = '0.19.0';
// Kernel-version visibility: show which kernel this tab is actually running and
// tag every published result with it — long-running bench tabs keep an OLD
// kernel in memory across kernel deploys until reloaded, and this is how we
// spot them in the fleet. handshake.js is tiny and side-effect-free.
import { KERNEL_VERSION } from '/src/transport/handshake.js?v=4.18.2';

// Crash-safe WebAssembly memory-ceiling probe. On iOS Safari navigator.deviceMemory
// is null and a per-tab WASM cap (not RAM) is the real floor; this measures it
// without losing the answer to an uncatchable jetsam kill (localStorage watermark).
import { probeWasmMemory, consumePriorCrash } from './wasm-mem-probe.js';

// ── diagnostics: everything needed to answer "why didn't my results report?" ──
// A timestamped ring buffer of status changes, connect/publish failures, kernel
// log events, and page errors — persisted across reloads, exported (with the
// device's full local results) by the Copy data button so a tester can email it.
const DIAG_CAP = 400;
let diagLog = [];
try { diagLog = JSON.parse(localStorage.getItem('powbench-diag') || '[]'); } catch { /* */ }
function diag(msg) {
  diagLog.push(`${new Date().toISOString()} ${String(msg).slice(0, 300)}`);
  if (diagLog.length > DIAG_CAP) diagLog = diagLog.slice(-DIAG_CAP);
  try { localStorage.setItem('powbench-diag', JSON.stringify(diagLog)); } catch { /* */ }
}
window.addEventListener('error', (e) => diag(`window.onerror: ${e.message} @ ${e.filename || '?'}:${e.lineno || '?'}`));
window.addEventListener('unhandledrejection', (e) => diag(`unhandledrejection: ${e.reason?.message || e.reason}`));
const _consoleError = console.error.bind(console);
console.error = (...a) => {
  try { diag('console.error: ' + a.map((x) => (x?.stack ? String(x.stack).split('\n')[0] : String(x))).join(' ')); } catch { /* */ }
  _consoleError(...a);
};
window.addEventListener('online',  () => diag('network: online'));
window.addEventListener('offline', () => diag('network: OFFLINE'));
document.addEventListener('visibilitychange', () => diag(`page: ${document.visibilityState}`));   // background throttling is a prime suspect on phones

// Local journal of every result this device produced + what happened to it
// (published ✓ / queued / failed). THE data the Copy data button exports —
// even a device that never connected still accumulates everything here.
let localResults = [];
try { localResults = JSON.parse(localStorage.getItem('powbench-results') || '[]'); } catch { /* */ }
function recordLocal(result, publishState) {
  localResults.push({ at: new Date().toISOString(), publish: publishState, result });
  if (localResults.length > 300) localResults = localResults.slice(-300);
  try { localStorage.setItem('powbench-results', JSON.stringify(localResults)); }
  catch (e) { diag('localStorage save failed: ' + (e.message || e)); }
}

// Results that could not be published (no connection / publish threw) wait here
// — persisted, retried after every reconnect — instead of being dropped.
let pendingPublishes = [];
try { pendingPublishes = JSON.parse(localStorage.getItem('powbench-pending') || '[]'); } catch { /* */ }
function savePending() {
  try { localStorage.setItem('powbench-pending', JSON.stringify(pendingPublishes.slice(-100))); } catch { /* */ }
}

// Register memory-hard candidates here as they compile (drop the file in
// candidates/ implementing candidates/template.js):
const CANDIDATES = {
  'cuckoo':          './candidates/cuckoo.js',            // asymmetric, memory-bandwidth-hard (run first)
  'equihash':        './candidates/equihash.js',          // asymmetric, generalized-birthday (memory-capacity)
  'sha256-baseline': './candidates/sha256-baseline.js',   // reference (not memory-hard)
  'argon2id':        './candidates/argon2id.js',          // demoted symmetric fallback (not in suite)
};

// Per-candidate suite metadata (suiteDifficulties + label), read once on load by
// importing each module (cheap — heavy deps inside candidates are lazy).
const CANDIDATE_META = {};
async function loadMeta() {
  for (const [k, url] of Object.entries(CANDIDATES)) {
    try {
      const m = await import(url);
      CANDIDATE_META[k] = {
        name: m.name || k,
        version: m.version || '?',
        suiteDifficulties: Array.isArray(m.suiteDifficulties) ? m.suiteDifficulties : [12, 16, 18, 20],
        difficultyLabel: m.difficultyLabel || 'difficulty',
        trials: Number.isInteger(m.trials) && m.trials > 0 ? m.trials : null,   // optional per-candidate override
        estimateMemMB: typeof m.estimateMemMB === 'function' ? m.estimateMemMB : null,   // pre-run memory estimate
      };
    } catch (e) {
      CANDIDATE_META[k] = { name: k, version: 'load-error', suiteDifficulties: [12, 16, 18, 20], difficultyLabel: 'difficulty', loadError: String(e.message || e) };
    }
  }
}

// One line that proves which app + candidate files actually loaded (vs a stale
// cached copy). Each version lives in its own file, so a cached file shows its
// old number here.
function renderBuildInfo() {
  const el = $('buildinfo'); if (!el) return;
  const parts = Object.keys(CANDIDATES).map((k) => `${k} v${CANDIDATE_META[k] ? CANDIDATE_META[k].version : '?'}`);
  el.textContent = `app v${BENCH_VERSION} · kernel v${KERNEL_VERSION} · ${parts.join(' · ')} · mem cap ${MEM_BUDGET_MB}MB${IS_IOS ? ' (iOS)' : ''}`;
}

// Per-device memory budget (on the estimateMemMB scale). ONLY iOS Safari
// page-CRASHES the whole tab on OOM (no catchable worker error), so it alone
// gets a hard cap. Android Chrome gives a catchable worker OOM, or just completes
// + times out — verified: a Galaxy S24 Ultra finished equihash B=20/21 before
// the guard — so Android runs the full suite like desktop.
const IS_IOS = /iPhone|iPad|iPod/i.test(navigator.userAgent)
  || (navigator.platform === 'MacIntel' && (navigator.maxTouchPoints || 0) > 1);   // iPadOS reports as Mac
// Starts as a conservative hardcoded guess; the WASM memory probe (initMemProbe,
// run on load) replaces it with a MEASURED per-device ceiling. Tighten-only: the
// probe can lower this if it finds a real boundary below what the suite would
// run, but never raises it above the safe default.
let MEM_BUDGET_MB = IS_IOS ? 700 : 6000;
function estMemFor(spec) {
  const f = CANDIDATE_META[spec.candidate] && CANDIDATE_META[spec.candidate].estimateMemMB;
  return typeof f === 'function' ? (f(spec.difficulty) || 0) : 0;
}
const overBudget = (spec) => estMemFor(spec) > MEM_BUDGET_MB;

// Target sizes to probe = the memory-hard working sets the suite will actually
// allocate (so we never probe past what the benchmark itself touches), plus a
// light ladder, clamped to the current budget envelope.
function memProbeTargets() {
  const sizes = new Set([64, 128, 256, 384, 512]);
  try { for (const spec of buildSuite()) { const m = estMemFor(spec); if (m > 0) sizes.add(Math.round(m)); } } catch { /* */ }
  const cap = MEM_BUDGET_MB;                          // don't touch more than the suite would
  return [...sizes].filter((m) => m <= cap).sort((a, b) => a - b);
}

// Measure the real WASM memory ceiling and fold it into MEM_BUDGET_MB. The
// destructive (touch) part is crash-recoverable: consumePriorCrash() reports a
// tab the OS killed on a previous attempt, and the probe never re-touches that
// size. Tighten-only — see MEM_BUDGET_MB above.
async function initMemProbe() {
  try {
    const crash = consumePriorCrash();
    if (crash) {
      DEVICE.wasmCrash = crash;
      diag(`wasm-mem: recovered a prior tab kill at ${crash.crashedAtMB}MB (last safe ${crash.lastSafeMB}MB) — budget capped below it`);
      MEM_BUDGET_MB = Math.min(MEM_BUDGET_MB, Math.max(32, crash.lastSafeMB));
    }
    // The destructive (touch) probe is only worth its cost where we can't learn
    // the ceiling otherwise: iOS Safari (page-crashes on OOM, hides deviceMemory)
    // or any device not exposing navigator.deviceMemory. Elsewhere (desktop /
    // Android Chrome: deviceMemory known + catchable OOM) run the cheap,
    // non-destructive grow-ceiling only.
    const wantCommit = IS_IOS || DEVICE.deviceMemoryGB == null;
    const targets = wantCommit ? memProbeTargets() : [];
    diag(`wasm-mem: probing ${wantCommit ? `ceiling (grow + touch ${targets.join('/')}MB)` : 'grow ceiling only (deviceMemory known)'}…`);
    const res = await probeWasmMemory({ targetsMB: targets, growCapMB: IS_IOS ? 1536 : 2048 });
    DEVICE.wasmMem = res;
    if (res.supported) {
      diag(`wasm-mem: grow ceiling ${res.growCeilingMB}MB · commit-safe ${res.commitMaxMB}MB`);
      const maxTarget = targets.length ? Math.max(...targets) : 0;
      // Only tighten when we found a REAL boundary below what the suite runs.
      if (res.commitMaxMB > 0 && res.commitMaxMB < maxTarget) {
        const safe = Math.floor(res.commitMaxMB * (IS_IOS ? 0.9 : 0.95));
        MEM_BUDGET_MB = Math.min(MEM_BUDGET_MB, safe);
        diag(`wasm-mem: MEM_BUDGET_MB → ${MEM_BUDGET_MB} (measured ceiling, was a hardcoded guess)`);
      }
    } else {
      diag('wasm-mem: WebAssembly.Memory unavailable — keeping default budget');
    }
    renderDevice();
    try { renderSuite(); } catch { /* suite may not be built yet */ }
  } catch (e) { diag('wasm-mem: probe error ' + (e && e.message || e)); }
}

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function percentile(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

// ── device identity + rich info ─────────────────────────────────────
function deviceId() {
  try {
    let id = localStorage.getItem('powbench-device-id');
    if (!id) {
      id = (crypto.randomUUID ? crypto.randomUUID()
                              : Date.now().toString(36) + Math.random().toString(36).slice(2, 10));
      localStorage.setItem('powbench-device-id', id);
    }
    return id;
  } catch { return 'no-storage'; }
}
function deviceLabel() { try { return localStorage.getItem('powbench-device-label') || ''; } catch { return ''; } }

function shortUa(ua) {
  ua = ua || '';
  if (/iPhone/.test(ua)) return 'iPhone'; if (/iPad/.test(ua)) return 'iPad';
  if (/Android/.test(ua)) return 'Android'; if (/Macintosh/.test(ua)) return 'Mac';
  if (/Windows/.test(ua)) return 'Windows'; if (/Linux/.test(ua)) return 'Linux';
  return ua.slice(0, 16);
}

// WebGL unmasked renderer — the real GPU string where the browser allows it.
function gpuInfo() {
  try {
    const c = document.createElement('canvas');
    const gl = c.getContext('webgl') || c.getContext('experimental-webgl');
    if (!gl) return null;
    const ext = gl.getExtension('WEBGL_debug_renderer_info');
    return ext ? String(gl.getParameter(ext.UNMASKED_RENDERER_WEBGL)) : null;
  } catch { return null; }
}

let DEVICE = baseDevice();
function baseDevice() {
  return {
    benchVersion: BENCH_VERSION,                       // tag each result with the app build
    kernelVersion: KERNEL_VERSION,                     // …and the kernel this tab is actually running (stale-tab detection)
    deviceId: deviceId(),
    deviceLabel: deviceLabel(),
    ua: navigator.userAgent,
    platform: navigator.platform || null,
    deviceMemoryGB: navigator.deviceMemory ?? null,    // coarse (Chrome): 0.25..8
    cores: navigator.hardwareConcurrency ?? null,
    screen: `${screen.width}x${screen.height}@${window.devicePixelRatio}`,
    crossOriginIsolated: self.crossOriginIsolated === true,
    gpu: gpuInfo(),
    ts: new Date().toISOString(),
  };
}
// Async enrichment where available: Client Hints (real model/arch on Chromium),
// network type. iOS Safari lacks userAgentData → falls back to UA gracefully.
async function enrichDevice() {
  try {
    const uad = navigator.userAgentData;
    if (uad?.getHighEntropyValues) {
      const h = await uad.getHighEntropyValues(['model', 'platformVersion', 'architecture', 'bitness']);
      DEVICE.model = h.model || null;
      DEVICE.uaPlatform = uad.platform || null;
      DEVICE.platformVersion = h.platformVersion || null;
      DEVICE.arch = h.architecture || null;
      DEVICE.bitness = h.bitness || null;
      DEVICE.mobile = uad.mobile ?? null;
    }
  } catch { /* */ }
  try { DEVICE.connection = navigator.connection?.effectiveType || null; } catch { /* */ }
  renderDevice();
}
function deviceInfo() {
  DEVICE.deviceLabel = deviceLabel();
  DEVICE.ts = new Date().toISOString();
  return { ...DEVICE };
}
function renderDevice() {
  const d = DEVICE;
  const name = d.deviceLabel || d.model || shortUa(d.ua);
  const bits = [
    name,
    d.gpu ? `GPU: ${d.gpu}` : null,
    (d.uaPlatform || d.platform) && d.platformVersion ? `${d.uaPlatform || d.platform} ${d.platformVersion}` : (d.platform || null),
    d.arch ? `${d.arch}${d.bitness ? '/' + d.bitness : ''}` : null,
    d.deviceMemoryGB != null ? `~${d.deviceMemoryGB} GB RAM` : null,
    d.cores != null ? `${d.cores} cores` : null,
    d.connection ? `net ${d.connection}` : null,
    `isolated=${d.crossOriginIsolated}`,
  ].filter(Boolean);
  if ($('devsummary')) $('devsummary').textContent = bits.join(' · ');
  if ($('dev')) $('dev').textContent = JSON.stringify(d, null, 2);
}

async function uaMemoryBytes() {
  try {
    if (self.crossOriginIsolated && performance.measureUserAgentSpecificMemory) {
      const m = await performance.measureUserAgentSpecificMemory();
      return m.bytes;
    }
  } catch { /* */ }
  return null;
}

// ── the test suite ──────────────────────────────────────────────────
// Each candidate declares its own meaningful sweep (SHA: zero-bits; argon2id:
// memory-MB; Cuckoo: edge-bits), so the suite is candidate × that candidate's
// difficulties — apples-to-apples within a candidate.
function buildSuite() {
  const specs = [];
  for (const c of Object.keys(CANDIDATES)) for (const d of (CANDIDATE_META[c]?.suiteDifficulties || [])) specs.push({ candidate: c, difficulty: d });
  return specs;
}
const testKey = (s) => `${s.candidate}@${s.difficulty}`;
const suiteState = new Map();   // testKey → { status, note, lastMint, lastMem }
let currentTest = null;         // testKey currently running, for the live highlight
function stateFor(s) {
  let v = suiteState.get(testKey(s));
  if (!v) { v = { status: 'pending', note: '', lastMint: null, lastMem: null }; suiteState.set(testKey(s), v); }
  return v;
}
function renderSuite() {
  const el = $('suite'); if (!el) return;
  el.innerHTML = '<table><tbody>' + buildSuite().map((s) => {
    const v = stateFor(s);
    const label = CANDIDATE_META[s.candidate]?.difficultyLabel || 'd';
    const over = overBudget(s);
    const running = testKey(s) === currentTest;
    const icon = running ? '▶' : v.status === 'ok' ? '✓' : v.status === 'skipped' ? '⏭' : over ? '∅' : '·';
    const mem = v.lastMem != null ? ` · ${v.lastMem < 10 ? v.lastMem.toFixed(1) : Math.round(v.lastMem)} MB` : '';
    const note = running ? '<span style="color:#a60">running…</span>'
               : v.status === 'skipped' ? `<span style="color:#b00">skipped: ${v.note}</span>`
               : over ? `<span style="color:#a60">capped · est ${Math.round(estMemFor(s))}MB > ${MEM_BUDGET_MB}MB device limit</span>`
               : (v.lastMint != null ? `${v.lastMint} ms${mem}` : '');
    return `<tr class="${running ? 'running' : ''}"><td>${icon}</td><td>${s.candidate} · ${label}=${s.difficulty}</td><td class="muted">${note}</td></tr>`;
  }).join('') + '</tbody></table>';
}

let lastResult = null;

// ── one benchmark run (spec optional; default = manual inputs) ───────
// Fault-tolerant: a worker OOM (worker.onerror) or a run exceeding maxMs is
// caught and returned as a result with oom/timedOut set — never throws.
async function runOnce(spec) {
  const candidateKey = spec.candidate;
  const difficulty   = spec.difficulty;
  const trials       = CANDIDATE_META[candidateKey]?.trials ?? 5;
  const maxMs        = 300000;                  // 5-min safety ceiling (only a wedged/OOM test trips it)
  const pubkeyHex    = 'aa'.repeat(32);         // fixed benchmark key — not user-configurable

  $('status').textContent = `loading ${candidateKey} d=${difficulty}…`;
  const trialsData = [];
  let oom = false, error = null, timedOut = false, candidateName = candidateKey;
  const memBefore = await uaMemoryBytes();
  const worker = new Worker('./worker.js', { type: 'module' });

  await new Promise((resolve) => {
    const finish = () => { clearTimeout(timer); resolve(); };
    const timer = setTimeout(() => { timedOut = true; try { worker.terminate(); } catch { /* */ } finish(); }, maxMs);
    worker.onmessage = (e) => {
      const m = e.data;
      if (m.type === 'loaded') {
        candidateName = m.name;
        $('status').textContent = `${candidateKey} d=${difficulty}: ${trials} trials…`;
        worker.postMessage({ type: 'run', pubkeyHex, difficulty, trials });
      } else if (m.type === 'trial') {
        trialsData.push(m);
        $('status').textContent = `${candidateKey} d=${difficulty} · trial ${m.i + 1}/${trials} · ${m.mintMs.toFixed(0)}ms · ${(m.peakMemBytes / 1e6).toFixed(0)}MB`;
      } else if (m.type === 'done') {
        finish();
      } else if (m.type === 'error') {
        error = m.message; finish();
      }
    };
    worker.onerror = (ev) => { oom = true; error = ev.message || 'worker died (likely OOM)'; finish(); };
    worker.postMessage({ type: 'load', candidateUrl: CANDIDATES[candidateKey] });
  });
  try { worker.terminate(); } catch { /* */ }
  const memAfter = await uaMemoryBytes();

  const mint = trialsData.map((t) => t.mintMs);
  const result = {
    device: deviceInfo(),
    candidate: candidateName, candidateKey, difficulty, trials: trialsData.length,
    mint_ms: {
      p50: percentile(mint, 50), p90: percentile(mint, 90), p99: percentile(mint, 99),
      min: mint.length ? Math.min(...mint) : null, max: mint.length ? Math.max(...mint) : null,
    },
    verify_ms_avg: trialsData.length ? trialsData.reduce((a, t) => a + t.verifyMs, 0) / trialsData.length : null,
    peak_wasm_mem_mb: Math.max(0, ...trialsData.map((t) => t.peakMemBytes)) / 1e6,
    ua_mem_delta_mb: (memBefore != null && memAfter != null) ? (memAfter - memBefore) / 1e6 : null,
    witness_len: trialsData.length ? trialsData[trialsData.length - 1].witnessLen : null,
    all_verified: trialsData.length > 0 && trialsData.every((t) => t.ok),
    oom, timedOut, error,
  };
  lastResult = result;
  return result;
}

function specFailed(r)  { return !!r.oom || !!r.timedOut || !!r.error || r.trials === 0 || !r.all_verified; }
function failNote(r)    {
  if (r.skipped) return 'over budget (won\'t fit)';
  if (r.oom) return 'OOM (out of memory)';
  if (r.timedOut) return 'too slow (timeout)';
  if (r.error) return String(r.error).slice(0, 40);
  if (r.trials === 0) return 'no trials';
  if (!r.all_verified) return 'verify failed';
  return 'failed';
}
// A device that won't even attempt a test (estimated memory exceeds its cap) is a
// floor signal too — publish it as a non-completing result so it shows in the chart.
function skipResult(spec) {
  return {
    device: deviceInfo(),
    candidate: spec.candidate, candidateKey: spec.candidate, difficulty: spec.difficulty, trials: 0,
    mint_ms: { p50: null, p90: null, p99: null, min: null, max: null },
    verify_ms_avg: null, peak_wasm_mem_mb: null, ua_mem_delta_mb: null, witness_len: null,
    all_verified: false, oom: false, timedOut: false, error: null,
    skipped: true, skipNote: `est ${Math.round(estMemFor(spec))}MB > ${MEM_BUDGET_MB}MB cap`,
  };
}


// ── comparison report (collector → device) ──────────────────────────
function renderLeaderboard(report) {
  const el = $('compare'); if (!el) return;
  // Roster of ALL distinct devices reporting (on ANY test) — each device cycles
  // the suite independently, so the per-test comparison below only shows same-test
  // peers; this line confirms every participant, e.g. a second browser on your
  // own machine.
  const seenDev = new Map();
  (report.devices || []).forEach((e) => { if (!seenDev.has(e.id)) seenDev.set(e.id, e.label || (e.ua ? shortUa(e.ua) : String(e.id).slice(0, 10))); });
  const roster = seenDev.size ? `<div class="muted" style="margin-top:.5rem">all devices reporting (${seenDev.size}): ${[...seenDev.values()].join(' · ')}</div>` : '';
  if (!lastResult) { el.innerHTML = 'run a benchmark to see where your device stands.' + roster; return; }
  const myId = deviceId(), myUa = navigator.userAgent;
  const candName = lastResult.candidate, diff = lastResult.difficulty;
  const candKey = lastResult.candidateKey || lastResult.candidate;   // match on the STABLE key (failed runs may carry only the short key)
  const myMint = lastResult.mint_ms?.p50 != null ? Math.round(lastResult.mint_ms.p50) : null;
  const myFailed = specFailed(lastResult);
  // Capability test, not a race. Order: FAILED first (devices below the floor —
  // the sharpest signal), then SLOWEST → FASTEST of the completers.
  const all = (report.devices || []).filter((e) => (e.c === candKey || e.c === candName) && e.d === diff);
  if (!all.length) {
    el.innerHTML = (lastResult ? `your result: <b>${myFailed ? failNote(lastResult) : (myMint + ' ms')}</b> — waiting for others on this test…` : '(no comparison data yet)') + roster;
    return;
  }
  const isFail = (e) => e.failed || e.mint == null;
  const failed = all.filter(isFail);
  const done   = all.filter((e) => !isFail(e)).sort((a, b) => b.mint - a.mint);   // slowest → fastest
  const ordered = [...failed, ...done];
  const dm = done.map((e) => e.mint);
  const summary = done.length
    ? `slowest ${dm[0]} ms · median ${dm[Math.floor((dm.length - 1) / 2)]} ms · fastest ${dm[dm.length - 1]} ms`
    : 'none completed yet';
  const meIdx = ordered.findIndex((e) => e.id === myId || e.id === 'ua:' + myUa);
  const youTxt = myFailed
    ? `your result: <b style="color:#b00">✗ ${failNote(lastResult)}</b>`
    : `your mint p50: <b>${myMint ?? '?'} ms</b>`;
  const head =
    `<div><b>${candName}</b> · difficulty ${diff} — the capability floor (failed → slowest → fastest)</div>` +
    `<div>${youTxt}${meIdx >= 0 ? ` · you are #${meIdx + 1} of ${ordered.length}` : ''}</div>` +
    `<div class="muted"><b>${failed.length}</b> failed · ${done.length} completed · ${summary}</div>`;
  const SHOW_N = 100;
  const rowHtml = (e, pos) => {
    const me = (e.id === myId || e.id === 'ua:' + myUa);
    const name = e.label || (e.ua ? shortUa(e.ua) : String(e.id).slice(0, 10));
    const result = isFail(e) ? `<span style="color:#b00">✗ ${e.fail || 'failed'}</span>` : `${e.mint} ms`;
    return `<tr style="${me ? 'font-weight:700;background:#eef' : ''}"><td>${pos}</td><td>${name}${me ? ' (you)' : ''}</td><td>${result}</td><td>${e.mem ? Math.round(e.mem) + 'MB' : '—'}</td></tr>`;
  };
  const thead = '<thead><tr style="color:#888;font-size:.85em"><th>#</th><th>device</th><th>result</th><th>peak mem</th></tr></thead>';
  let list = ordered.slice(0, SHOW_N).map((e, i) => rowHtml(e, i + 1)).join('');
  if (meIdx >= SHOW_N) list += `<tr><td colspan="4" style="text-align:center;color:#bbb">⋯</td></tr>` + rowHtml(ordered[meIdx], meIdx + 1);   // always show YOUR row
  el.innerHTML = head + `<table style="margin-top:.4rem">${thead}<tbody>${list}</tbody></table>` + roster;
}

function maybeSubmit(result) {
  const el = $('collector'); const url = el ? el.value.trim() : '';   // optional LAN collector (UI removed)
  if (!url) return;
  fetch(url.replace(/\/$/, '') + '/submit', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(result),
  }).catch(() => { /* best-effort */ });
}


// ── orchestration ───────────────────────────────────────────────────
let continuous = false, reporter = null, iter = 0;

// Resilient publish: never DROP a result. The old path published only when the
// initial connect had succeeded and silently lost the result if publish threw —
// so a phone that slept, a network blip, or a bridge restart turned a device
// into a ghost ("sometimes apps don't report"). Now every failure queues the
// result (persisted) and triggers a reconnect; the queue flushes after.
let reconnecting = false;
async function reconnectReporter(why) {
  if (reconnecting || reporter || !continuous) return;
  reconnecting = true;
  diag(`reporter (re)connect — ${why}; ${pendingPublishes.length} queued`);
  try {
    const { createReporter } = await import('./axona-report.js?v=0.19.0');
    reporter = await createReporter((m) => { $('status').textContent = m; }, renderLeaderboard, diag);
    diag(`reporter connected (node ${String(reporter.nodeId).slice(0, 10)}…)`);
  } catch (e) {
    diag('reporter connect FAILED: ' + (e.message || e));
    $('status').textContent = 'Axona connect failed — results queue locally; retrying…';
    setTimeout(() => { reconnecting = false; reconnectReporter('retry timer'); }, 30000);
    return;
  }
  reconnecting = false;
  await flushPending();
}
async function flushPending() {
  while (pendingPublishes.length && reporter) {
    const r = pendingPublishes[0];
    try {
      const { msgId } = await reporter.publish(r);
      pendingPublishes.shift(); savePending();
      recordLocal(r, 'published-after-retry ' + String(msgId).slice(0, 12));
      diag(`flushed queued result ${r.candidateKey ?? r.candidate} d=${r.difficulty}`);
    } catch (e) { diag('flush failed (will retry after next reconnect): ' + (e.message || e)); break; }
  }
}
async function publishResult(r) {
  maybeSubmit(r);
  if (!reporter) {
    pendingPublishes.push(r); savePending();
    recordLocal(r, 'queued (not connected)');
    reconnectReporter('result while disconnected');
    return;
  }
  try {
    const { msgId } = await reporter.publish(r);
    recordLocal(r, 'published ' + String(msgId).slice(0, 12));
  } catch (e) {
    pendingPublishes.push(r); savePending();
    recordLocal(r, 'queued (publish failed: ' + (e.message || e) + ')');
    diag(`publish FAILED (${e.message || e}) — queued, reconnecting`);
    $('status').textContent = 'publish failed — result queued; reconnecting…';
    const dead = reporter; reporter = null;
    try { await dead.close(); } catch { /* */ }
    reconnectReporter('publish failure');
  }
}

// Continuous mode: cycle the whole suite, repeat. A test that fails on this
// device is marked skipped (with a note) and not retried; the rest keep going.
// Failed runs are still PUBLISHED (oom/timeout is useful Stage-4 data).
async function startContinuous() {
  if (continuous) return;
  continuous = true; iter = 0;
  $('run').textContent = 'Stop';
  const gap = 3000;                                  // pause between tests
  for (const s of buildSuite()) { const v = stateFor(s); v.status = 'pending'; v.note = ''; }   // reset on start
  renderSuite();

  diag(`run started — app v${BENCH_VERSION} kernel v${KERNEL_VERSION} label "${deviceLabel() || '(unset)'}"`);
  await reconnectReporter('run start');               // failure queues results + keeps retrying (was: local-only forever)

  while (continuous) {
    let ranAny = false;
    for (const spec of buildSuite()) {
      if (!continuous) break;
      const st = stateFor(spec);
      if (st.status === 'skipped') continue;             // a failed test stays skipped
      if (overBudget(spec)) {                            // gate BEFORE allocating → can't crash the tab
        st.status = 'skipped'; st.note = `capped: est ${Math.round(estMemFor(spec))}MB > ${MEM_BUDGET_MB}MB device limit`;
        renderSuite();
        const skipR = skipResult(spec);                  // publish the cap as floor data (e.g. iOS can't do equihash d=20)
        await publishResult(skipR);
        continue;
      }
      ranAny = true;
      iter++;
      currentTest = testKey(spec); renderSuite();        // highlight the row as it runs
      let r;
      try { r = await runOnce(spec); }
      catch (e) { currentTest = null; st.status = 'skipped'; st.note = 'threw: ' + (e.message || e); renderSuite(); continue; }
      currentTest = null;

      if (specFailed(r)) {
        st.status = 'skipped'; st.note = failNote(r);
        $('status').textContent = `${spec.candidate} d=${spec.difficulty} — skipped (${st.note})`;
      } else {
        st.status = 'ok'; st.note = '';
        st.lastMint = r.mint_ms?.p50 != null ? Math.round(r.mint_ms.p50) : null;
        st.lastMem = r.peak_wasm_mem_mb != null ? r.peak_wasm_mem_mb : null;
      }
      renderSuite();

      await publishResult(r);                            // publish success AND failure (failures are data); queues + reconnects on error
      if (!continuous) break;
      await sleep(gap);
    }
    if (!ranAny) { $('status').textContent = 'all tests skipped on this device — stopping.'; break; }
  }
  await stopContinuous();
}

async function stopContinuous() {
  continuous = false;
  currentTest = null; renderSuite();
  $('run').textContent = 'Run benchmark';
  if (reporter) { try { await reporter.close(); } catch { /* */ } reporter = null; }
}

// ── Copy data: export EVERYTHING this device generated, for emailing to us ──
// Works even when the device never connected — local results + the diagnostic
// log (connect failures, publish errors, kernel events, page errors) + a live
// connection-health snapshot are all assembled into one JSON blob.
async function copyAllData() {
  let bridge = null; try { bridge = (await import('./axona-report.js?v=0.19.0')).reportBridge; } catch (e) { diag('copy: reporter module unloadable: ' + (e.message || e)); }
  let health = null;
  if (reporter?.health) { try { health = reporter.health(); } catch (e) { health = { error: String(e.message || e) }; } }
  const payload = {
    exportedAt: new Date().toISOString(),
    app: `pow-bench v${BENCH_VERSION}`, kernel: `v${KERNEL_VERSION}`,
    page: location.href,
    device: deviceInfo(),
    bridge,
    connection: reporter
      ? { connected: true, nodeId: reporter.nodeId, health }
      : { connected: false, note: continuous ? 'run active but no reporter (see diagnosticLog)' : 'no run active' },
    queuedUnpublished: pendingPublishes.length,
    suite: [...suiteState.entries()].map(([test, v]) => ({ test, status: v.status, note: v.note, lastMint: v.lastMint, lastMem: v.lastMem })),
    results: localResults,                 // full journal: every result + its publish outcome
    diagnosticLog: diagLog,                // status/connect/publish/error history (persisted across reloads)
  };
  const text = JSON.stringify(payload, (k, v) => (typeof v === 'bigint' ? v.toString() : v), 2);
  let copied = false;
  try { await navigator.clipboard.writeText(text); copied = true; } catch { /* fall through */ }
  if (!copied) {                            // older browsers / non-secure contexts
    try {
      const ta = document.createElement('textarea');
      ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.focus(); ta.select(); ta.setSelectionRange(0, text.length);
      copied = document.execCommand('copy'); ta.remove();
    } catch { /* */ }
  }
  if (copied) {
    $('status').textContent = `✓ copied ${Math.round(text.length / 1024)} KB of bench data — paste into an email`;
  } else {
    // Last resort (very old WebViews): show the JSON for manual long-press copy.
    let ta = $('copyFallback');
    if (!ta) {
      ta = document.createElement('textarea'); ta.id = 'copyFallback';
      ta.style.cssText = 'width:100%;height:10rem;margin-top:.5rem;font-size:.7em';
      $('status').after(ta);
    }
    ta.value = text;
    $('status').textContent = 'clipboard unavailable — long-press the box below, Select All, Copy';
  }
  diag(`copy data exported (${Math.round(text.length / 1024)} KB, copied=${copied})`);
}

// ── wiring ──────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const lab = $('label');
  lab.value = deviceLabel();
  lab.addEventListener('input', () => {
    try { localStorage.setItem('powbench-device-label', lab.value.trim()); } catch { /* */ }
  });

  diag(`page loaded — app v${BENCH_VERSION} kernel v${KERNEL_VERSION} (${navigator.onLine ? 'online' : 'OFFLINE'})`);
  // Capture every status-line change into the diagnostic log — the status line
  // is where connect/publish outcomes surface, but it's transient on screen.
  let lastStatus = '';
  new MutationObserver(() => {
    const t = $('status')?.textContent || '';
    if (t && t !== lastStatus) { lastStatus = t; diag('status: ' + t); }
  }).observe($('status'), { childList: true, characterData: true, subtree: true });
  if ($('copyData')) $('copyData').addEventListener('click', () => { copyAllData().catch((e) => { $('status').textContent = 'copy failed: ' + (e.message || e); }); });

  enrichDevice();            // async: model / GPU / arch — captured into each result
  if ($('buildinfo')) $('buildinfo').textContent = `app v${BENCH_VERSION} · loading candidates…`;
  loadMeta().then(() => { renderSuite(); renderBuildInfo(); initMemProbe(); });   // suite matrix + loaded-version line + measured memory ceiling

  const shareUrl = location.origin + location.pathname;
  const link = $('shareUrl'); link.textContent = 'open link'; link.href = shareUrl;
  const qr = $('qr');
  qr.src = 'https://api.qrserver.com/v1/create-qr-code/?size=180x180&margin=8&data=' + encodeURIComponent(shareUrl);
  qr.alt = 'QR · ' + shareUrl;

  // single button: start / stop the continuous suite (iteration is all we run)
  $('run').addEventListener('click', () => { if (continuous) stopContinuous(); else startContinuous(); });
});
