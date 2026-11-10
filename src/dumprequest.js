// =============================================================================
// dumprequest.js — ask a relay for its dump where no signal can reach it.
//
// WHY. `health-dump` and `peer-dump` are asked for with SIGUSR1. Windows has no
// SIGUSR1, and axona-win's 20 relays are services with no console, so 20 of the
// fleet's 48 relays could not be asked at all. This is the same answer
// src/stopfile.js gave for a graceful stop: a file on the host.
//
// HOW IT IS ASKED. Write (or rewrite) ONE file, `<dir>/request`. Every relay
// sharing the directory polls its modification time and dumps once each time
// the time changes. Nothing is deleted, so 20 relays do not race to
// unlink one file, and the asker needs no process ids. The time a relay saw at
// start is its baseline: a request older than the process is never served.
//
// WHAT IT IS NOT. Not a network surface: the only way to ask is to write a file
// on the host, as with probe-requests/. Read-only: onRequest dumps and returns.
// =============================================================================
import { statSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

export const DUMP_REQUEST_POLL_MS = 2000;
export const DUMP_REQUEST_FILE = 'request';

const mtimeOf = (path) => { try { return statSync(path).mtimeMs; } catch { return null; } };

export function watchDumpRequest(dir, onRequest, { intervalMs = DUMP_REQUEST_POLL_MS } = {}) {
  if (typeof dir !== 'string' || dir.length === 0 || typeof onRequest !== 'function') return null;
  try { mkdirSync(dir, { recursive: true }); } catch { /* the poll below reports nothing until it exists */ }
  const path = join(dir, DUMP_REQUEST_FILE);
  let seen = mtimeOf(path) ?? 0;
  const timer = setInterval(() => {
    const m = mtimeOf(path);
    // A CHANGED mtime is a request, in either direction: a host clock stepped
    // back must not silence every later request.
    if (m === null || m === seen) return;
    seen = m;
    try { onRequest('request-file'); } catch { /* the dump owns its errors */ }
  }, intervalMs);
  if (typeof timer.unref === 'function') timer.unref();
  return { path, stop: () => clearInterval(timer) };
}
