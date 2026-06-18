// windows.js — the dashboard BrowserWindow. Created hidden; shown on first
// launch / tray click. Closing it hides to the tray (the relay keeps running);
// only an explicit Quit tears the relay down.

import { app, BrowserWindow } from 'electron';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // …/desktop/src/main

export function createWindow() {
  const win = new BrowserWindow({
    width: 540,
    height: 720,
    show: false,
    title: 'Axona Relay',
    backgroundColor: '#0f1419',
    titleBarStyle: 'hiddenInset',
    webPreferences: {
      preload: path.join(here, '..', 'preload', 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  win.loadFile(path.join(here, '..', 'renderer', 'index.html'));

  win.on('close', (e) => {
    if (!app.isQuitting) { e.preventDefault(); win.hide(); }
  });

  return win;
}
