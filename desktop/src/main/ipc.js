// ipc.js — the main-side IPC surface. Commands use invoke/handle (request →
// response); controller events are pushed to the renderer over send(). The
// renderer never gets `ipcRenderer` itself (see preload.cjs).

import { app, ipcMain } from 'electron';

export function registerIpc({ controller, getWindow, settings }) {
  // ── commands ──
  ipcMain.handle('relay:start', () => controller.start());
  ipcMain.handle('relay:stop', () => controller.stop());
  ipcMain.handle('relay:switchNetwork', async (_e, network) => {
    if (network !== 'prod' && network !== 'testnet') {
      throw new Error(`invalid network: ${network}`);
    }
    settings.save({ network });
    await controller.start(network);
    return controller.getMeta();
  });
  ipcMain.handle('relay:getHealth', () => controller.getHealth());
  ipcMain.handle('relay:getStatus', () => controller.getStatus());
  ipcMain.handle('relay:getMeta', () => controller.getMeta());

  ipcMain.handle('app:getSettings', () => ({
    ...settings.load(),
    openAtLogin: app.getLoginItemSettings().openAtLogin,
    appVersion: app.getVersion(),
  }));
  ipcMain.handle('app:setOpenAtLogin', (_e, enabled) => {
    const on = !!enabled;
    app.setLoginItemSettings({ openAtLogin: on, openAsHidden: true });
    settings.save({ openAtLogin: on });
    return app.getLoginItemSettings().openAtLogin;
  });

  // ── push controller events → renderer ──
  const send = (channel, payload) => {
    const win = getWindow();
    if (win && !win.isDestroyed()) {
      const wc = win.webContents;
      if (wc && !wc.isDestroyed()) wc.send(channel, payload);
    }
  };
  controller.on('health', (h) => send('relay:health', h));
  controller.on('status', (s) => send('relay:status', s));
  controller.on('peerJoin', (id) => send('relay:peerJoin', id));
  controller.on('peerLeave', (id) => send('relay:peerLeave', id));
  controller.on('log', (l) => send('relay:log', l));
  controller.on('error', (e) => send('relay:error', e));
}
