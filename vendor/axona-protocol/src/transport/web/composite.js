// =====================================================================
// composite_transport.js — fan-out Transport that delegates each
//                          peer to the appropriate sub-transport.
//
// In the browser peer the AxonaPeer has ONE transport reference.
// We need to address both:
//   - Mesh peers (other browsers): WebRTCTransport over RTCDataChannel
//   - The bridge (one peer): BridgeTransport over the bridge WebSocket
//
// CompositeTransport holds a list of sub-transports.  For
// send/notify/openConnection/isConnected/getLatency, it asks each
// sub-transport "do you own this peer?" via the optional `ownsPeer`
// method (or falls back to isConnected) and routes to the first
// hit.  Handler registrations (onRequest/onNotification/onPeerDied)
// fan out — when a handler is registered, we register it on every
// sub-transport so incoming messages from either channel reach it.
//
// The component sub-transports keep their own nodeId↔connId bindings
// internally.  The orchestrator (axona_node.js) calls bindPeer on
// the correct sub-transport directly when each handshake completes.
//
// nodeId convention: 264-bit BigInt throughout the public surface
// here.  The sub-transports likewise speak BigInt internally; hex is
// only on the wire (JSON payloads) and at user-facing display points.
// =====================================================================

import { Transport }       from '../../contracts/Transport.js';
import { TransportError, ErrorCodes } from '../../errors.js';
import { depositDispatchCapability, readDispatchCapability } from '../../registry/index.js';

export class CompositeTransport extends Transport {
  /**
   * @param {Object} opts
   * @param {bigint} opts.localNodeId   264-bit BigInt nodeId
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor({ localNodeId, log }) {
    super();
    if (typeof localNodeId !== 'bigint') {
      throw new TypeError(`CompositeTransport: localNodeId must be bigint, got ${typeof localNodeId}`);
    }
    this._localNodeId = localNodeId;
    this._log         = log ?? (() => {});

    /** @type {Transport[]} */
    this._subs = [];

    // Track registered handlers so newly-added sub-transports
    // inherit them.
    /** @type {Map<string, Function>} */ this._reqHandlers = new Map();
    /** @type {Map<string, Function>} */ this._ntfHandlers = new Map();
    /** @type {Function[]}            */ this._peerDiedHandlers = [];
    // onPeerBound is registered per-handler via a registrar closure so a
    // sub-transport added AFTER onPeerBound() was called (e.g. an uplink added
    // post-start) still propagates its bound peers. Without this, late subs
    // never reach the routing layer and their mesh peers never enter the
    // synaptome.
    /** @type {Array<(t: Transport) => void>} */ this._peerBoundRegistrars = [];

    this._started = false;

    // REF-1.1 E3b.2b (SEAL): the composite is itself a sealed transport — the
    // peer registers frames on it through registerFrame, which reaches these
    // deposited closures (never a public onRequest/onNotification). Each closure
    // records the handler (so a sub added later inherits it) and fans it out to
    // every current sub-transport via the sub's OWN capability channel.
    depositDispatchCapability(this, {
      request: (type, handler) => {
        this._reqHandlers.set(type, handler);
        for (const t of this._subs) this._fanOutRequest(t, type, handler);
      },
      notification: (type, handler) => {
        this._ntfHandlers.set(type, handler);
        for (const t of this._subs) this._fanOutNotification(t, type, handler);
      },
    });
  }

  // REF-1.1 E3b.4 (SEAL): fan one handler onto a single sub-transport through its
  // capability channel ONLY. Every sub-transport is a sealed transport that deposits
  // at construction, so the literal-method fallback (t.onRequest / t.onNotification)
  // is GONE — an undeposited sub cannot receive a fan-out, it throws. This keeps the
  // composite fan-out on the same mandatory-capability path as registerFrame (Aster
  // boundary ruling 39012d73 / option 1): no receiver reaches a raw primitive by name.
  _fanOutRequest(t, type, handler) {
    const cap = readDispatchCapability(t);
    if (typeof cap?.request !== 'function') throw new TypeError('CompositeTransport._fanOutRequest: sub-transport has no deposited request dispatch capability (E3 seal: every sub-transport must deposit at construction)');
    cap.request(type, handler);
  }

  _fanOutNotification(t, type, handler) {
    const cap = readDispatchCapability(t);
    if (typeof cap?.notification !== 'function') throw new TypeError('CompositeTransport._fanOutNotification: sub-transport has no deposited notification dispatch capability (E3 seal: every sub-transport must deposit at construction)');
    cap.notification(type, handler);
  }

  /**
   * Add a sub-transport.  If start() has already been called, the
   * new sub-transport inherits the currently-registered handlers
   * (idempotent — register on the sub-transport).
   */
  addSubtransport(t) {
    // Bridge fill v0.8 (axona-docs 9b1ed08): ONE DIALER. The sub-transport that
    // exposes connectViaRelay is the composite's dialer; a second one is a
    // configuration error and refuses here, before the sub is added, so the
    // kernel never sees two allocators behind one transport (Aster BF-B).
    if (typeof t.connectViaRelay === 'function') {
      if (this._dialer) {
        throw new TypeError('CompositeTransport.addSubtransport: two sub-transports expose connectViaRelay; a composite has one dialer');
      }
      this._setDialer(t);
    }
    this._subs.push(t);
    // Replay handler registrations to the new sub-transport (via its capability).
    for (const [type, h] of this._reqHandlers) this._fanOutRequest(t, type, h);
    for (const [type, h] of this._ntfHandlers) this._fanOutNotification(t, type, h);
    for (const h of this._peerDiedHandlers)    t.onPeerDied(h);
    if (typeof t.onNegotiationFailed === 'function') for (const e of (this._negotiationFailedHandlers ?? [])) e.unsubs.push(t.onNegotiationFailed(e.handler));
    for (const reg of this._peerBoundRegistrars) reg(t);
    for (const reg of this._peerListRegistrars ?? []) reg(t);
  }

  /**
   * Bridge fill v0.8: install the dial surface of the one dialer on THIS
   * composite, as instance properties, so that:
   *
   *   - a composite with NO dialer has no `connectViaRelay` at all, and the
   *     kernel's `openIsTheDial` (AxonaPeer._considerCandidate) reads true,
   *     exactly as before this change — the sim and every legacy composite
   *     are untouched;
   *   - a composite WITH a dialer forwards `connectViaRelay`, `mayDial`,
   *     `canAllocate` and `allocRefusedFor` to that one sub-transport and
   *     returns each answer UNCHANGED (the incarnation string, true, false or
   *     null mean to the kernel exactly what the dialer meant), so the
   *     ledger the kernel reads before a dial is the ledger the dial
   *     allocates against;
   *   - `openConnection` is NOT touched: it stays owner-or-false and
   *     allocates nothing, which is what the kernel takes a bound-only open
   *     for on a transport that has connectViaRelay (Aster BF-A).
   *
   * A surface the dialer lacks is not installed: the kernel treats a missing
   * mayDial as "no ledger, nothing to reserve against", as it does today.
   */
  _setDialer(t) {
    this._dialer = t;
    this.connectViaRelay = (toHex) => t.connectViaRelay(toHex);
    if (typeof t.mayDial         === 'function') this.mayDial         = ()     => t.mayDial();
    if (typeof t.canAllocate     === 'function') this.canAllocate     = (dir)  => t.canAllocate(dir);
    if (typeof t.allocRefusedFor === 'function') this.allocRefusedFor = (peer) => t.allocRefusedFor(peer);
    this._log('dialer-set', { mayDial: typeof t.mayDial === 'function', canAllocate: typeof t.canAllocate === 'function', allocRefusedFor: typeof t.allocRefusedFor === 'function' });
  }

  /** The one sub-transport that dials, or null. */
  dialer() { return this._dialer ?? null; }

  /**
   * Bridge fill v0.8: the DIRECTORY sample, fanned IN from every sub-transport
   * that emits `peer-list` frames (the web transport does; a WebSocket server
   * does not). Registered per handler through a registrar, like onPeerBound,
   * so a sub-transport added after the kernel subscribed (the bridge's uplink
   * is added after start) still feeds the handler. The frame's hex nodeIds
   * pass through unchanged.
   *
   * @param {(peers: string[]) => void} handler
   * @returns {() => void} unsubscribe
   */
  onPeerList(handler) {
    if (typeof handler !== 'function') throw new TypeError('onPeerList: handler must be a function');
    const unsubs = [];
    const register = (t) => {
      if (typeof t.onPeerList === 'function') unsubs.push(t.onPeerList(handler));
    };
    (this._peerListRegistrars ??= []).push(register);
    for (const t of this._subs) register(t);
    return () => {
      const a = this._peerListRegistrars;
      const i = a ? a.indexOf(register) : -1;
      if (i >= 0) a.splice(i, 1);
      for (const u of unsubs) try { u(); } catch { /* swallow */ }
    };
  }

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    if (this._started) return;
    for (const t of this._subs) await t.start(this._localNodeId);
    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    for (const t of this._subs) await t.stop();
    this._started = false;
  }

  getLocalNodeId() { return this._localNodeId; }

  // ── Routing: pick the sub-transport that owns this peer ─────────────
  //
  // Two ways a sub-transport identifies "its" peer:
  //   - explicit `ownsPeer(nodeId)` method (BridgeTransport implements this)
  //   - isConnected(nodeId) === true (WebRTCTransport's natural answer)
  // We prefer the explicit method when present (cheaper for the
  // single-peer BridgeTransport) and fall back to isConnected.

  _routeFor(nodeId) {
    for (const t of this._subs) {
      if (typeof t.ownsPeer === 'function') {
        if (t.ownsPeer(nodeId)) return t;
      } else {
        if (t.isConnected(nodeId)) return t;
      }
    }
    return null;
  }

  /**
   * Aggregate boundPeers() across sub-transports.  Each sub may
   * implement `boundPeers()` (BridgeTransport, WebRTCTransport) to
   * report the BigInt nodeIds it has admitted via its own handshake.
   * AxonaPeer.start() consumes this to auto-admit peers into the
   * synaptome, so consumers don't have to wire the synapse by hand
   * after a webTransport handshake.
   *
   * Sub-transports without `boundPeers()` contribute nothing here;
   * the SimNetwork-only path (dht-sim, tests) keeps its existing
   * synaptome-seeding flow.
   *
   * @returns {bigint[]} deduplicated list of bound nodeIds
   */
  boundPeers() {
    const seen = new Set();
    for (const t of this._subs) {
      if (typeof t.boundPeers !== 'function') continue;
      for (const id of t.boundPeers()) {
        if (typeof id === 'bigint') seen.add(id);
      }
    }
    return [...seen];
  }

  /**
   * Subscribe to bind events across all sub-transports that emit them.
   * The composite's handler fires for every new peer bound on any sub,
   * deduplicated across sub-transports (a peer that gets bound on both
   * the bridge and the mesh fires once).
   *
   * Row 8 (Hold-and-Fill v0.15, R8-2; Aster 20904613): the sub-transport's
   * meshId and channel incarnation travel THROUGH this adapter unchanged, so
   * the kernel's guard can tell the bind of the channel its attempt started
   * from a stale channel's late bind. A handler that returns `false` has
   * REJECTED the event (the kernel does, for a bind on a stale incarnation):
   * the peer is then not recorded as seen, so the current channel's bind that
   * follows still fires. A sub that names no incarnation (the bridge) passes
   * none and the kernel ends by identity, as before.
   *
   * @param {(nodeIdBig: bigint, meshId?: string, inc?: string|null) => (void|boolean)} handler
   * @returns {() => void} unsubscribe
   */
  onPeerBound(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPeerBound: handler must be a function');
    }
    // Dedup the fan-out so `handler` fires once per peer even if more than one
    // sub-transport binds the same nodeId.  The dedup MUST be re-armed when the
    // peer dies — otherwise it is PERMANENT: a peer that drops and later
    // reconnects (churn, or a bridgeless relay reconnect) would never re-fire
    // onPeerBound, so the routing layer never re-admits it to the synaptome and
    // ignores a peer it is actually connected to.  Clearing the nodeId from
    // `seen` on peer-death lets the next bind re-fire.
    const seen = new Set();
    const wrapped = (nodeIdBig, meshId, inc) => {
      if (typeof nodeIdBig !== 'bigint') return;
      if (seen.has(nodeIdBig)) return;
      seen.add(nodeIdBig);
      let r;
      try { r = handler(nodeIdBig, meshId, inc); }
      catch (err) { this._log?.('peer-bound-fanout-threw', { err: err.message }); }
      // R8-2: a rejected event (stale incarnation) did not bind this peer for
      // the handler; un-see it so the current channel's bind is not swallowed.
      if (r === false) seen.delete(nodeIdBig);
    };
    const rearm = (nodeIdBig) => { if (typeof nodeIdBig === 'bigint') seen.delete(nodeIdBig); };
    const unsubs = [];
    // A registrar wires this handler onto one sub-transport. Stored so that
    // subs added later (addSubtransport) inherit it too — same `seen` set, so
    // dedup stays correct across all subs including late ones.
    const register = (t) => {
      if (typeof t.onPeerBound === 'function') unsubs.push(t.onPeerBound(wrapped));
      if (typeof t.onPeerDied  === 'function') unsubs.push(t.onPeerDied(rearm));
    };
    this._peerBoundRegistrars.push(register);
    for (const t of this._subs) register(t);
    return () => {
      const i = this._peerBoundRegistrars.indexOf(register);
      if (i >= 0) this._peerBoundRegistrars.splice(i, 1);
      for (const u of unsubs) try { u(); } catch { /* swallow */ }
    };
  }

  // ── Channel pool ────────────────────────────────────────────────────

  // Bridge fill v0.8 (Aster BF-A): this open is BOUND-ONLY and stays so. It
  // routes to the sub-transport that already owns the peer and returns false
  // for a peer none owns; it never dials and never allocates. On a composite
  // with a dialer the kernel dials through `connectViaRelay` (installed by
  // _setDialer), where the CONSUME and the incarnation live.
  async openConnection(nodeId) {
    const t = this._routeFor(nodeId);
    if (!t) return false;
    return t.openConnection(nodeId);
  }

  async closeConnection(nodeId) {
    const t = this._routeFor(nodeId);
    if (t) await t.closeConnection(nodeId);
  }

  isConnected(nodeId) {
    const t = this._routeFor(nodeId);
    return t != null && t.isConnected(nodeId);
  }

  // ── Messaging ───────────────────────────────────────────────────────

  async send(nodeId, type, body) {
    const t = this._routeFor(nodeId);
    if (!t) {
      throw new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `CompositeTransport.send: no route to ${String(nodeId)}`,
        { context: { nodeId: String(nodeId), type } });
    }
    return t.send(nodeId, type, body);
  }

  async notify(nodeId, type, body) {
    const t = this._routeFor(nodeId);
    if (!t) {
      // Fire-and-forget but log: pubsub diagnostics correlate with
      // this when fan-out targets can't be reached.
      this._log('notify-no-route', { nodeId: String(nodeId), type });
      return;
    }
    return t.notify(nodeId, type, body);
  }

  // REF-1.1 E3b.2b (SEAL): onRequest/onNotification are no longer public
  // instance methods. registerFrame reaches the deposited capability closures
  // (constructor), which record + fan out via _fanOutRequest/_fanOutNotification.

  // ── Liveness & latency ─────────────────────────────────────────────

  onPeerDied(handler) {
    this._peerDiedHandlers.push(handler);
    const unsubs = this._subs.map(t => t.onPeerDied(handler));
    return () => {
      const i = this._peerDiedHandlers.indexOf(handler);
      if (i >= 0) this._peerDiedHandlers.splice(i, 1);
      for (const u of unsubs) try { u(); } catch {}
    };
  }

  /** Row 13: fan out to every sub-transport that has the signal (the WebRTC
   *  one); the bridge WebSocket never negotiates a peer channel. Each
   *  registration is an entry {handler, unsubs}; a sub-transport added later
   *  appends its unsubscribe to the entry (addSubtransport), so the closure
   *  returned here removes the late registration too (Aster 1816f5e6
   *  R10/13-A). Idempotent: a second call of the closure does nothing. */
  onNegotiationFailed(handler) {
    const entry = { handler, unsubs: [] };
    (this._negotiationFailedHandlers ??= []).push(entry);
    for (const t of this._subs) if (typeof t.onNegotiationFailed === 'function') entry.unsubs.push(t.onNegotiationFailed(handler));
    return () => {
      const a = this._negotiationFailedHandlers;
      const i = a ? a.indexOf(entry) : -1;
      if (i >= 0) a.splice(i, 1);
      const us = entry.unsubs; entry.unsubs = [];
      for (const u of us) try { u(); } catch {}
    };
  }

  /**
   * v2.0.2 — Aggregate onPingTraffic across sub-transports.  Only
   * sub-transports that implement it contribute; the rest silently
   * skip.  Returns an unsubscribe that detaches from all of them.
   */
  onPingTraffic(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPingTraffic: handler must be a function');
    }
    const unsubs = [];
    for (const t of this._subs) {
      if (typeof t.onPingTraffic === 'function') {
        unsubs.push(t.onPingTraffic(handler));
      }
    }
    return () => { for (const u of unsubs) try { u(); } catch {} };
  }

  getLatency(nodeId) {
    const t = this._routeFor(nodeId);
    return t ? t.getLatency(nodeId) : -1;
  }

  // ── Convenience: ask every sub-transport for its mapping ──────────

  /** Reverse-lookup BigInt nodeId from a mesh-layer connId / meshId.
   *  Tries every sub-transport that exposes the helper; returns the
   *  first hit, or null.
   *  @param {string} channelId
   *  @returns {bigint|null} */
  nodeIdFor(channelId) {
    for (const t of this._subs) {
      if (typeof t.nodeIdFor !== 'function') continue;
      const id = t.nodeIdFor(channelId);
      if (id != null) return id;
    }
    return null;
  }

  /** Forward-lookup the channel id (meshId or 'bridge') for a BigInt nodeId.
   *  @param {bigint} nodeId */
  channelIdFor(nodeId) {
    for (const t of this._subs) {
      if (typeof t.meshIdFor === 'function') {
        const id = t.meshIdFor(nodeId);
        if (id != null) return id;
      }
      if (typeof t.connIdFor === 'function') {
        const id = t.connIdFor(nodeId);
        if (id != null) return id;
      }
    }
    return null;
  }
}
