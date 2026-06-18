// tray.js — menu-bar presence. The icon is a colored dot reflecting relay
// status (green/orange/red); the menu shows status, opens the dashboard, lets
// the user switch network + toggle open-at-login, and quits.
//
// The dot is built as a raw BGRA bitmap (macOS pixel order) rather than an
// image file, so it's guaranteed colored (not a monochrome template) and needs
// no asset to ship.

import { app, Tray, Menu, nativeImage } from 'electron';

const COLORS = {
  green:  [80, 192, 96],
  orange: [232, 140, 32],
  red:    [208, 80, 80],
  none:   [90, 99, 112],
};
const ICON_CACHE = {};

function dotIcon(color) {
  if (ICON_CACHE[color]) return ICON_CACHE[color];
  const [r, g, b] = COLORS[color] || COLORS.none;
  const size = 18;
  const buf = Buffer.alloc(size * size * 4);
  const c = (size - 1) / 2;
  const rad = size / 2 - 1.5;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const i = (y * size + x) * 4;
      const d = Math.hypot(x - c, y - c);
      let a = 0;
      if (d <= rad - 0.5) a = 255;
      else if (d <= rad + 0.5) a = Math.round(255 * (rad + 0.5 - d));
      buf[i] = b; buf[i + 1] = g; buf[i + 2] = r; buf[i + 3] = a;   // BGRA
    }
  }
  const img = nativeImage.createFromBitmap(buf, { width: size, height: size });
  ICON_CACHE[color] = img;
  return img;
}

export function createTray({ controller, getWindow, settings, onQuit }) {
  const tray = new Tray(dotIcon(controller.lastStatus?.color || 'red'));

  function setNetwork(network) {
    settings.save({ network });
    controller.start(network).catch(() => { /* status reflects failure */ });
  }

  function rebuild() {
    const st = controller.lastStatus || { color: 'red', state: 'stopped' };
    const meta = controller.getMeta();
    const peers = controller.lastHealth?.peers?.length ?? 0;
    const openAtLogin = app.getLoginItemSettings().openAtLogin;
    const netLabel = meta.network === 'testnet' ? 'testnet' : 'main';

    tray.setToolTip(`Axona Relay — ${st.state} · ${peers} peers · ${netLabel}`);
    tray.setContextMenu(Menu.buildFromTemplate([
      { label: `${st.state} · ${peers} peers · ${netLabel}`, enabled: false },
      { type: 'separator' },
      { label: 'Open Dashboard', click: () => { const w = getWindow(); if (w) { w.show(); w.focus(); } } },
      {
        label: 'Network',
        submenu: [
          { label: 'Main (production)', type: 'radio', checked: meta.network === 'prod', click: () => setNetwork('prod') },
          { label: 'Testnet', type: 'radio', checked: meta.network === 'testnet', click: () => setNetwork('testnet') },
        ],
      },
      {
        label: 'Open at Login',
        type: 'checkbox',
        checked: openAtLogin,
        click: (mi) => {
          app.setLoginItemSettings({ openAtLogin: mi.checked, openAsHidden: true });
          settings.save({ openAtLogin: mi.checked });
        },
      },
      { type: 'separator' },
      { label: 'Quit Axona Relay', click: () => onQuit() },
    ]));
  }

  tray.on('click', () => {
    const w = getWindow();
    if (!w) return;
    if (w.isVisible()) w.hide();
    else { w.show(); w.focus(); }
  });

  controller.on('status', (s) => { tray.setImage(dotIcon(s.color)); rebuild(); });
  // Peer count lives in health; refresh the menu/tooltip when it changes too.
  let lastPeers = -1;
  controller.on('health', (h) => {
    const p = h?.peers?.length ?? 0;
    if (p !== lastPeers) { lastPeers = p; rebuild(); }
  });

  rebuild();
  return tray;
}
