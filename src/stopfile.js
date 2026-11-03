// stopfile.js — a graceful-stop request from the Windows service wrapper.
//
// WHY (Closer-Born Relay Inherits the Root v0.6 Part 6a; David 2026-11-03 "go
// ahead with 6a"). Every holder of a root or a backup hands its history off on
// the shutdown signal its platform gives it. On Linux and macOS that signal is
// SIGTERM, and shutdown() runs stopRelay(), whose peer.leave() hands off every
// root and every live backup. On axona-win the 20 relays are services under
// windows/relaysvc.cs, and a service stop TERMINATED THE JOB: no signal reached
// node, nothing was handed off, on every Stop-Service, every roll and every
// Windows reboot (about 11 in 60 days per hosts/axona-win.json).
//
// A service has no console, so no Ctrl event can reach it. The wrapper instead
// sets RELAY_STOP_FILE for its child and, on stop, writes that file and waits
// up to its grace for the relay to leave; only then does it terminate the job.
// This module watches the file and calls onStop once. A stale file at start is
// removed first, so an old request cannot stop a new process.

import { existsSync, unlinkSync } from 'node:fs';

export const STOP_FILE_POLL_MS = 500;

export function watchStopFile(path, onStop, { intervalMs = STOP_FILE_POLL_MS } = {}) {
  if (typeof path !== 'string' || path.length === 0) return null;
  try { if (existsSync(path)) unlinkSync(path); } catch { /* best-effort: a stale request */ }
  const timer = setInterval(() => {
    let present = false;
    try { present = existsSync(path); } catch { present = false; }
    if (!present) return;
    clearInterval(timer);   // once: the interval ends here
    try { unlinkSync(path); } catch { /* the wrapper may have removed it */ }
    try { onStop('stop-file'); } catch { /* the caller's shutdown owns its errors */ }
  }, intervalMs);
  if (typeof timer.unref === 'function') timer.unref();
  return { stop: () => clearInterval(timer) };
}
