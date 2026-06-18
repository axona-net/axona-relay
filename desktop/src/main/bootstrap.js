// bootstrap.js — wires the Electron app together once the polyfill is live.
// Imported by main.js AFTER the polyfill, so importing the relay controller
// (which transitively loads the kernel) is safe here.

import { app } from 'electron';
import { RelayController } from './relay-controller.js';
import { createWindow } from './windows.js';
import { createTray } from './tray.js';
import { registerIpc } from './ipc.js';
import * as settings from './settings.js';

// Single instance — never run two relays from two app launches.
if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  let win = null;
  let tray = null;
  let controller = null;
  let quitting = false;

  const getWindow = () => win;

  async function gracefulQuit() {
    if (quitting) return;
    quitting = true;
    app.isQuitting = true;
    try { await controller?.stop(); } catch { /* */ }
    app.quit();
  }

  app.on('second-instance', () => { if (win) { win.show(); win.focus(); } });

  // before-quit: run async teardown (stop relay + cleanupWebRTC) before exit.
  app.on('before-quit', (e) => {
    if (!quitting) { e.preventDefault(); gracefulQuit(); }
  });

  // Menu-bar app: closing the last window must NOT quit (relay keeps running).
  app.on('window-all-closed', (e) => { e.preventDefault?.(); });

  app.whenReady().then(() => {
    app.setActivationPolicy?.('accessory');   // menu-bar only, no Dock icon

    const cfg = settings.load();
    controller = new RelayController({});

    // Dev-only: echo lifecycle to stdout so `npm start` is observable from a
    // terminal (the UI streams go to the renderer, not stdout).
    if (!app.isPackaged) {
      controller.on('status', (s) => console.log(`[relay] status: ${s.color} ${s.state}`));
      controller.on('log', (l) =>
        console.log(`[relay] ${l.level} ${l.event}${l.ctx ? ' ' + JSON.stringify(l.ctx) : ''}`));
    }

    win = createWindow();
    tray = createTray({ controller, getWindow, settings, onQuit: gracefulQuit });
    registerIpc({ controller, getWindow, settings });

    win.once('ready-to-show', () => win.show());   // show on manual launch

    controller.start(cfg.network).catch((err) =>
      console.error('axona-relay: initial start failed:', err));
  });
}
