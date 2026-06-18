// settings.js — tiny JSON persistence in the app's userData dir. Three fields:
//   network     'prod' | 'testnet'   (which mesh to join)
//   openAtLogin boolean              (cache; macOS login-item is the source of truth)
//   region      string | null        (last region token; null → relay's SF default)
// No electron-store dependency — these are three primitives.

import { app } from 'electron';
import { readFileSync, writeFileSync, mkdirSync, renameSync } from 'node:fs';
import path from 'node:path';

const FILE = () => path.join(app.getPath('userData'), 'settings.json');
const DEFAULTS = { network: 'prod', openAtLogin: false, region: null };

export function load() {
  try { return { ...DEFAULTS, ...JSON.parse(readFileSync(FILE(), 'utf8')) }; }
  catch { return { ...DEFAULTS }; }
}

export function save(patch) {
  const next = { ...load(), ...patch };
  const file = FILE();
  mkdirSync(path.dirname(file), { recursive: true });
  const tmp = file + '.tmp';
  writeFileSync(tmp, JSON.stringify(next, null, 2));
  renameSync(tmp, file);          // atomic-ish replace
  return next;
}
