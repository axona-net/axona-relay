// resolve-relay.js — locate the relay core (../src + ../vendor) as file: URLs,
// in BOTH dev and packaged modes.
//
//   dev      : imported from desktop/src/main → repo root is three dirs up.
//   packaged : electron-builder copies ../src and ../vendor into
//              Contents/Resources/app-src (see build.extraResources), reachable
//              via process.resourcesPath.
//
// We return URL strings (not static specifiers) because the path differs by
// mode, so callers do `await import(<url>)`. This file imports only `electron`
// + node builtins — NEVER the kernel — so it is safe to evaluate before the
// polyfill is installed.

import { app } from 'electron';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { readFileSync } from 'node:fs';

const here = path.dirname(fileURLToPath(import.meta.url));   // …/desktop/src/main
const base = app.isPackaged
  ? path.join(process.resourcesPath, 'app-src')             // packaged: extraResources
  : path.join(here, '..', '..', '..');                      // dev: axona-relay repo root

export const RELAY_BASE = base;

const url = (rel) => pathToFileURL(path.join(base, rel)).href;

export const polyfillURL = url('src/polyfill.js');
export const relayURL    = url('src/relay.js');
export const identityURL = url('src/identity.js');
export const networkURL  = url('src/network.js');
export const s2URL       = url('vendor/axona-protocol/src/utils/s2.js');

export const RELAY_VERSION = (() => {
  try { return JSON.parse(readFileSync(path.join(base, 'package.json'), 'utf8')).version; }
  catch { return null; }
})();
