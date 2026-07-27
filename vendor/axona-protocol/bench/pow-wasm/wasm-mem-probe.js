// wasm-mem-probe.js — measure the usable WebAssembly linear-memory ceiling of
// the current device WITHOUT crashing the tab (the floor signal the PoW
// benchmark needs, especially on iOS Safari where navigator.deviceMemory is
// null and a per-tab WASM cap — not RAM — is the real constraint).
//
// There are TWO ceilings, with very different danger:
//
//   1. RESERVE / GROW ceiling — WebAssembly.Memory.grow() throws a *catchable*
//      RangeError at WebKit's declared cap, and grown-but-untouched pages are
//      copy-on-write zero pages (no RSS commit). Probing this by incremental
//      grow() in a try/catch CANNOT crash the tab. Fully safe.
//
//   2. COMMIT ceiling — writing to a page forces real physical commit, and iOS
//      jetsam can kill the whole tab with NO catchable exception. We make this
//      safe-in-aggregate by (a) only testing the specific working-set sizes the
//      PoW candidates need — never an open-ended search to the crash — and
//      (b) checkpointing a high-water mark to localStorage *before* each touch,
//      so a jetsam kill is RECOVERED on the next load (resume pattern) instead
//      of lost. Each allocation is released before the next, and we yield to the
//      event loop between steps.
//
// Output (attach to the device record so it flows to the collector):
//   {
//     supported, growCeilingMB,
//     commitMaxMB,                  // largest size we allocated AND touched ok
//     targets: [{ mb, status: 'ok'|'reserve-fail'|'crashed' }],
//     recovered,                    // {crashedAtMB, lastSafeMB} if a prior run died
//     notes[]
//   }

const LS_KEY    = 'axona.wasmprobe.v2';
const WASM_PAGE = 65536;                 // 64 KiB
const MIB       = 1024 * 1024;
const sleep     = (ms) => new Promise((r) => setTimeout(r, ms));

function lsGet()  { try { return JSON.parse(localStorage.getItem(LS_KEY) || 'null'); } catch { return null; } }
function lsSet(o) { try { localStorage.setItem(LS_KEY, JSON.stringify(o)); } catch { /* private mode */ } }
function lsDel()  { try { localStorage.removeItem(LS_KEY); } catch { /* */ } }

/**
 * Call once on page load, BEFORE probing, to detect a tab the OS killed during a
 * prior commit attempt. Returns {crashedAtMB, lastSafeMB} or null, and clears
 * the marker so it's reported exactly once.
 */
export function consumePriorCrash() {
  const cp = lsGet();
  if (cp && cp.phase === 'commit' && cp.attemptingMB != null) {
    lsDel();
    return { crashedAtMB: cp.attemptingMB, lastSafeMB: cp.lastSafeMB ?? 0 };
  }
  return null;
}

/** Phase 1 — reserve/grow ceiling. Non-destructive; never crashes. */
async function growCeilingMB({ stepMB = 16, capMB = 2048 } = {}) {
  if (typeof WebAssembly === 'undefined' || !WebAssembly.Memory) return null;
  let mem;
  try { mem = new WebAssembly.Memory({ initial: 1 }); }   // no maximum → grow allocates lazily
  catch { return null; }
  const stepPages = Math.max(1, (stepMB * MIB) / WASM_PAGE);
  const capPages  = (capMB * MIB) / WASM_PAGE;
  let pages = 1, n = 0;
  while (pages < capPages) {
    const want = Math.min(stepPages, capPages - pages);
    try {
      const prev = mem.grow(want);          // throws RangeError, OR returns -1 on old engines, at the cap
      if (prev === -1) break;
      pages += want;
    } catch { break; }                       // RangeError → declared ceiling reached
    if ((++n & 7) === 0) await sleep(0);     // yield so the UI stays alive
  }
  mem = null;                                // release the reservation
  return Math.round((pages * WASM_PAGE) / MIB);
}

/**
 * Phase 2 — commit feasibility for a fixed list of target sizes (MB), ascending.
 * Allocates + touches each (one byte per page) to force real commit, checkpointing
 * before each so an uncatchable jetsam kill is recovered on the next load.
 */
async function commitTargets(targetsMB, { settleMs = 60, recovered = null } = {}) {
  const out = [];
  let commitMaxMB = 0;
  // If a prior run died at size X, never re-attempt X or anything ≥ X this session.
  const crashFloor = recovered ? recovered.crashedAtMB : Infinity;

  for (const mb of targetsMB) {
    if (mb >= crashFloor) { out.push({ mb, status: 'crashed' }); continue; }

    // Durable high-water BEFORE the risky allocation — survives a jetsam kill.
    lsSet({ phase: 'commit', attemptingMB: mb, lastSafeMB: commitMaxMB, ts: 0 });

    let mem, ok = true;
    try {
      mem = new WebAssembly.Memory({ initial: 1 });
      const pages = Math.ceil((mb * MIB) / WASM_PAGE);
      mem.grow(pages - 1);                                  // reserve
      const u8 = new Uint8Array(mem.buffer);
      for (let off = 0; off < pages * WASM_PAGE; off += WASM_PAGE) u8[off] = 1;  // commit every page
      // touched the whole thing and we're still alive → this size is feasible
    } catch { ok = false; }                                 // reserve failed (graceful) → not feasible
    mem = null;                                             // release before the next, larger step

    if (ok) { commitMaxMB = mb; out.push({ mb, status: 'ok' }); }
    else    { out.push({ mb, status: 'reserve-fail' }); }

    // Clear the "attempting" flag (we survived) and let RSS settle / GC reclaim.
    lsSet({ phase: 'commit', attemptingMB: null, lastSafeMB: commitMaxMB, ts: 0 });
    await sleep(settleMs);
  }
  lsDel();                                                  // clean exit
  return { commitMaxMB, targets: out };
}

/**
 * Full probe. `targetsMB` should be the working-set sizes the candidates need
 * (plus a small ladder); keep the max bounded — there's no reason to probe past
 * what the heaviest candidate will allocate.
 */
export async function probeWasmMemory({
  targetsMB = [32, 64, 128, 256, 512],
  growCapMB = 2048,
  commitSettleMs = 60,
} = {}) {
  const notes = [];
  if (typeof WebAssembly === 'undefined' || !WebAssembly.Memory) {
    return { supported: false, notes: ['no WebAssembly.Memory'] };
  }
  const recovered = consumePriorCrash();
  if (recovered) notes.push(`recovered prior jetsam: died at ${recovered.crashedAtMB}MB, last safe ${recovered.lastSafeMB}MB`);

  const grow = await growCeilingMB({ capMB: growCapMB });
  if (grow != null) notes.push(`grow ceiling ${grow}MB (reserve-only, non-destructive)`);

  // Don't bother touch-testing past the reserve ceiling — it can't fit.
  const targets = [...new Set(targetsMB)].sort((a, b) => a - b)
    .filter((mb) => grow == null || mb <= grow);

  const { commitMaxMB, targets: results } = await commitTargets(targets, { settleMs: commitSettleMs, recovered });

  return {
    supported: true,
    growCeilingMB: grow,
    commitMaxMB,
    targets: results,
    recovered,
    notes,
  };
}
