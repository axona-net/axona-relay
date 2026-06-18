// main.js — Electron entrypoint.
//
// HARD INVARIANT: the relay's polyfill (RTCPeerConnection / WebSocket globals)
// MUST be installed before any kernel code evaluates — and in Phase 0/1 the
// relay runs IN THIS PROCESS, so we install it here, first. Because the relay
// core lives at a path that differs dev-vs-packaged, we resolve a file: URL and
// dynamic-import the polyfill as the very first statement, then hand off to
// bootstrap.js (which imports Electron + the relay controller).
//
// resolve-relay.js imports only `electron` + node builtins (never the kernel),
// so evaluating it to obtain `polyfillURL` is safe before the polyfill runs.

import { polyfillURL } from './resolve-relay.js';

await import(polyfillURL);        // installs globals — must be before the kernel
await import('./bootstrap.js');   // Electron app + tray + window + relay controller
