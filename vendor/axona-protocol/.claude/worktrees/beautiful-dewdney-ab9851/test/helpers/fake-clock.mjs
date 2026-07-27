// =====================================================================
// fake-clock.mjs — a deterministic virtual clock for driving timer-based
// code under test (the mesh lifecycle's ping/stale/retry/negotiation
// timers, all on 0.5–30 s intervals).
//
// Patches the GLOBAL setTimeout/setInterval/clear*/Date.now so code that
// calls them bare (as MeshManager does) runs on virtual time. advance(ms)
// fires every due timer in chronological order, awaiting async callbacks
// and re-scanning so callbacks that (re)schedule or clear timers compose.
//
// Usage:
//   const clock = createFakeClock(); clock.install();
//   ... drive code ...
//   await clock.advance(31_000);   // fire the 30 s watchdog
//   clock.uninstall();
// =====================================================================

export function createFakeClock(startMs = 1_700_000_000_000) {
  let now = startMs;
  let seq = 1;
  /** @type {Map<number, {due:number, interval:number|null, cb:Function, args:any[]}>} */
  const timers = new Map();

  const real = {
    setTimeout:    globalThis.setTimeout,
    setInterval:   globalThis.setInterval,
    clearTimeout:  globalThis.clearTimeout,
    clearInterval: globalThis.clearInterval,
    dateNow:       Date.now,
  };

  const set = (cb, ms, interval, args) => {
    const id = seq++;
    timers.set(id, { due: now + Math.max(0, ms | 0), interval, cb, args });
    // Mimic Node's Timeout object enough for `if (timer)` / clearX(timer).
    return id;
  };

  function install() {
    globalThis.setTimeout    = (cb, ms = 0, ...a) => set(cb, ms, null, a);
    globalThis.setInterval   = (cb, ms = 0, ...a) => set(cb, ms, Math.max(1, ms | 0), a);
    globalThis.clearTimeout  = (id) => { timers.delete(id); };
    globalThis.clearInterval = (id) => { timers.delete(id); };
    Date.now = () => now;
    return clockApi;
  }

  function uninstall() {
    globalThis.setTimeout    = real.setTimeout;
    globalThis.setInterval   = real.setInterval;
    globalThis.clearTimeout  = real.clearTimeout;
    globalThis.clearInterval = real.clearInterval;
    Date.now = real.dateNow;
  }

  // Advance virtual time by `ms`, firing all timers due in (now, now+ms],
  // earliest-first. Callbacks may schedule/clear timers; we re-scan each
  // iteration. Async callbacks are awaited so microtasks settle between fires.
  async function advance(ms) {
    const target = now + ms;
    for (let guard = 0; guard < 1_000_000; guard++) {
      let next = null;
      for (const [id, t] of timers) {
        if (t.due <= target && (next === null || t.due < next.due || (t.due === next.due && id < next.id))) {
          next = { id, ...t };
        }
      }
      if (!next) break;
      now = next.due;
      const t = timers.get(next.id);
      if (!t) continue;                       // cleared meanwhile
      if (t.interval != null) t.due = now + t.interval;   // reschedule interval
      else timers.delete(next.id);             // one-shot
      try {
        const r = t.cb(...t.args);
        if (r && typeof r.then === 'function') await r;
      } catch { /* a timer callback throwing must not stop the clock */ }
    }
    now = target;
    // let any trailing microtasks (awaited promises) settle
    await Promise.resolve();
  }

  const clockApi = { install, uninstall, advance, now: () => now, pending: () => timers.size };
  return clockApi;
}
