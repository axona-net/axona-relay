// relay-controller.js — owns the relay lifecycle and turns peer.health() into a
// 1 Hz stream of {health, status} the UI + tray consume.
//
// Phase 0/1: the relay runs in THIS (main) process. Phase 2 will move it into a
// utilityProcess child; this class's public surface (start/stop/switchNetwork +
// the 'health'/'status'/'peerJoin'/'peerLeave'/'log'/'error' events) stays the
// same, so only the internals change.
//
// The relay core (createRelay/startRelay/stopRelay, identity, network, s2) is
// imported lazily via file: URLs (dev vs packaged) AFTER the polyfill is live —
// main.js guarantees the polyfill import precedes the first import of this file.

import { EventEmitter } from 'node:events';
import {
  polyfillURL, relayURL, identityURL, networkURL, s2URL, RELAY_VERSION,
} from './resolve-relay.js';

const DEFAULT_REGION = { lat: 37.77, lng: -122.42 };   // SF (us-west)
const CONNECT_TIMEOUT_MS = 20000;
const MESH_READY_SIZE = 3;
const HOST_WAIT_MS = 25000;

/** Map a health snapshot to a {color,state} the green light + tray use. */
export function deriveStatus(health, running) {
  if (!running) return { color: 'red', state: 'stopped' };
  if (!health) return { color: 'orange', state: 'starting' };
  const t = health.transport || {};
  const bs = t.bridgeState;
  if (!health.started || bs == null) return { color: 'orange', state: 'starting' };
  if (bs === 'closed' || bs === 'disconnected' || bs === 'failed')
    return { color: 'red', state: bs };
  if (bs !== 'open') return { color: 'orange', state: bs || 'connecting' };
  if (health.meshDegraded) return { color: 'orange', state: 'degraded' };
  if ((t.meshBound ?? 0) < 1) return { color: 'orange', state: 'meshing' };
  return { color: 'green', state: 'connected' };
}

export class RelayController extends EventEmitter {
  constructor({ region = DEFAULT_REGION } = {}) {
    super();
    this.region = region;
    this.network = 'prod';
    this.bridgeUrl = null;
    this.regionLabel = '?';
    this.running = false;
    this.peer = null;
    this.transport = null;
    this.node = null;
    this.identity = null;
    this.lastHealth = null;
    this.lastStatus = { color: 'red', state: 'stopped' };
    this._mods = null;
    this._healthTimer = null;
    this._unhooks = [];
    this._startToken = 0;
    this._hosted = false;
  }

  async _modules() {
    if (this._mods) return this._mods;
    const [relay, idmod, net, s2, pf] = await Promise.all([
      import(relayURL), import(identityURL), import(networkURL),
      import(s2URL), import(polyfillURL),
    ]);
    this._mods = {
      createRelay: relay.createRelay,
      startRelay: relay.startRelay,
      stopRelay: relay.stopRelay,
      KERNEL_VERSION: relay.KERNEL_VERSION,
      regionName: relay.regionName,
      resolveNetwork: net.resolveNetwork,
      bridgeForNetwork: net.bridgeForNetwork,
      createEphemeralIdentity: idmod.createEphemeralIdentity,
      geoCellId: s2.geoCellId,
      cleanupWebRTC: pf.cleanupWebRTC,
    };
    return this._mods;
  }

  getMeta() {
    return {
      network: this.network,
      bridgeUrl: this.bridgeUrl,
      region: this.regionLabel,
      kernelVersion: this._mods?.KERNEL_VERSION ?? null,
      relayVersion: RELAY_VERSION,
    };
  }

  getStatus() { return this.lastStatus; }
  getHealth() { return this.lastHealth; }

  _emitStatus(s) {
    if (this.lastStatus && this.lastStatus.color === s.color && this.lastStatus.state === s.state) {
      this.lastStatus = s;   // keep latest object, skip event when unchanged
      return;
    }
    this.lastStatus = s;
    this.emit('status', s);
  }

  _log(level, event, ctx) { this.emit('log', { level, event, ctx, ts: Date.now() }); }

  /** (Re)start the relay on `network`. Tears down any existing relay first. */
  async start(network = this.network) {
    await this.stop();
    const m = await this._modules();

    const net = m.resolveNetwork(network) || 'prod';
    this.network = net;
    this.bridgeUrl = m.bridgeForNetwork(net);
    const code = m.geoCellId(this.region.lat, this.region.lng, 8);
    this.regionLabel = m.regionName(code) ?? `0x${code.toString(16)}`;

    const token = ++this._startToken;
    this.running = true;
    this._emitStatus({ color: 'orange', state: 'starting' });
    this._log('info', 'starting', { network: net, bridge: this.bridgeUrl, region: this.regionLabel });

    // Resilient connect loop — a down/restarting bridge must not wedge us.
    // Each attempt mints a fresh ephemeral identity and rebuilds the stack
    // (re-start() on a half-dead transport is unsafe), bounded by a timeout.
    for (let attempt = 1; this.running && this._startToken === token; attempt++) {
      try {
        this.identity = await m.createEphemeralIdentity(this.region);
        const stack = m.createRelay({
          bridgeUrl: this.bridgeUrl,
          identity: this.identity,
          region: this.region,
          onLog: (level, event, ctx) => {
            if (level === 'warn' || level === 'error') this._log(level, event, ctx);
          },
        });
        this.peer = stack.peer;
        this.transport = stack.transport;
        this.node = stack.node;
        this._wirePeer();
        await Promise.race([
          m.startRelay({ peer: this.peer, transport: this.transport }),
          new Promise((_, rej) =>
            setTimeout(() => rej(new Error('connect/handshake timeout')), CONNECT_TIMEOUT_MS)),
        ]);
        break;   // connected
      } catch (err) {
        this._log('warn', 'connect-failed', { attempt, message: String(err?.message || err) });
        await this._teardownStack();
        if (!this.running || this._startToken !== token) return;
        this._emitStatus({ color: 'red', state: 'reconnecting' });
        await new Promise((r) => setTimeout(r, Math.min(30000, 2000 * attempt)));
      }
    }
    if (!this.running || this._startToken !== token) return;

    this._log('info', 'connected', { bridge: this.bridgeUrl });
    this._startHealthLoop();
    this._hostKeyspaceWhenReady(token);
  }

  _wirePeer() {
    const p = this.peer;
    const add = (u) => { if (typeof u === 'function') this._unhooks.push(u); };
    add(p.onPeerJoin?.((id) => this.emit('peerJoin', id)));
    add(p.onPeerLeave?.((id) => this.emit('peerLeave', id)));
    add(p.onError?.((err) => this.emit('error', { code: err?.code, message: err?.message })));
    ['warn', 'error'].forEach((lvl) =>
      add(p.onLog?.(lvl, (msg, ctx) => this._log(lvl, msg, ctx))));
  }

  _startHealthLoop() {
    clearInterval(this._healthTimer);
    this._healthTimer = setInterval(() => {
      let h;
      try { h = this.peer?.health?.(); } catch { return; }
      if (!h) return;
      this.lastHealth = h;
      this.emit('health', h);
      this._emitStatus(deriveStatus(h, this.running));
    }, 1000);
  }

  // A relay's whole job: host its keyspace neighbourhood (store+serve topics
  // that land near its id, WITHOUT subscribing). Wait briefly for the mesh to
  // converge so the announce anchors on the right K-closest set.
  async _hostKeyspaceWhenReady(token) {
    const readyBy = Date.now() + HOST_WAIT_MS;
    while (this.running && this._startToken === token &&
           (this.node?.synaptome?.size ?? 0) < MESH_READY_SIZE && Date.now() < readyBy) {
      await new Promise((r) => setTimeout(r, 500));
    }
    if (!this.running || this._startToken !== token || this._hosted) return;
    try {
      await this.peer.host();
      this._hosted = true;
      this._log('info', 'hosting-keyspace', { region: this.regionLabel });
    } catch (err) {
      this._log('error', 'host-failed', { message: String(err?.message || err) });
    }
  }

  async _teardownStack() {
    for (const u of this._unhooks.splice(0)) { try { u(); } catch { /* */ } }
    const m = this._mods;
    if (m && (this.peer || this.transport)) {
      try { await m.stopRelay({ peer: this.peer, transport: this.transport }); } catch { /* */ }
    }
    this.peer = null;
    this.transport = null;
    this.node = null;
  }

  /** Stop the relay and release WebRTC. Safe to call when already stopped. */
  async stop() {
    this._startToken++;          // cancel any in-flight start loop
    this.running = false;
    this._hosted = false;
    clearInterval(this._healthTimer);
    this._healthTimer = null;
    await this._teardownStack();
    // cleanupWebRTC is process-global; safe between stop and the next start
    // (matches the relay CLI's retry behaviour). Releases libdatachannel.
    try { this._mods?.cleanupWebRTC?.(); } catch { /* */ }
    this.identity = null;
    this.lastHealth = null;
    this._emitStatus({ color: 'red', state: 'stopped' });
  }
}
