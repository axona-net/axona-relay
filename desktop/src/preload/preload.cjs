// preload.cjs — the ONLY bridge between the sandboxed renderer and main.
// CommonJS (.cjs) because sandboxed preloads must not be ESM. Exposes a small,
// explicit `window.relay` API; never leaks ipcRenderer itself.

const { contextBridge, ipcRenderer } = require('electron');

function sub(channel, cb) {
  const handler = (_e, payload) => cb(payload);
  ipcRenderer.on(channel, handler);
  return () => ipcRenderer.removeListener(channel, handler);
}

contextBridge.exposeInMainWorld('relay', {
  // commands
  start:          () => ipcRenderer.invoke('relay:start'),
  stop:           () => ipcRenderer.invoke('relay:stop'),
  switchNetwork:  (network) => ipcRenderer.invoke('relay:switchNetwork', network),
  getHealth:      () => ipcRenderer.invoke('relay:getHealth'),
  getStatus:      () => ipcRenderer.invoke('relay:getStatus'),
  getMeta:        () => ipcRenderer.invoke('relay:getMeta'),
  getSettings:    () => ipcRenderer.invoke('app:getSettings'),
  setOpenAtLogin: (enabled) => ipcRenderer.invoke('app:setOpenAtLogin', enabled),

  // subscriptions (each returns an unsubscribe fn)
  onHealth:    (cb) => sub('relay:health', cb),
  onStatus:    (cb) => sub('relay:status', cb),
  onPeerJoin:  (cb) => sub('relay:peerJoin', cb),
  onPeerLeave: (cb) => sub('relay:peerLeave', cb),
  onLog:       (cb) => sub('relay:log', cb),
  onError:     (cb) => sub('relay:error', cb),
});
