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
    // Socket-is-bootstrap v0.5 (axona-docs 7a27d24, § Ownership): ONE
    // subscription per sub-transport for bind and death events, dispatched
    // through the ROUTE-TOKEN RULE below before any kernel handler sees them.
    // Handlers registered through onPeerBound / onPeerDied are entries here;
    // a sub added later is subscribed once in addSubtransport.
    /** @type {Array<{handler: Function, seen: Set<bigint>}>} */ this._peerBoundEntries = [];
    /**
     * Per identity: the one ADMITTED route and the routes SUPERSEDED under it.
     * A route is (sub, token); the token is the sub's channel id for the
     * identity (meshId / connId). The record outlives the admitted route until
     * every superseded route has closed, so a retired token cannot re-admit.
     * @type {Map<bigint, {admitted: {sub: Transport, token: string|null}|null, superseded: Map<Transport, string|null>}>}
     */
    this._routes = new Map();
    this.routeStats = { admitted: 0, switched: 0, bornSuperseded: 0, staleToken: 0, deathSwallowed: 0, deathForwarded: 0 };
    /** Route-change listeners (Aster 88f4c2f7): a switch or a same-sub token
     *  update is announced upward WITHOUT a bind, so a parent composite keeps
     *  its admitted token for this child current. @type {Function[]} */
    this._routeChangedHandlers = [];

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
    // Socket-is-bootstrap v0.5: ONE death and ONE bind subscription per sub,
    // both dispatched through the route-token rule (_onSubDeath / _onSubBound).
    if (typeof t.onPeerDied === 'function') t.onPeerDied((id, reason, token) => this._onSubDeath(t, id, reason, token));
    if (typeof t.onPeerBound === 'function') t.onPeerBound((n, m, inc) => this._onSubBound(t, n, m, inc));
    // Nested composites (Aster 88f4c2f7): a child's route change (its own
    // switch, or a same-sub token update) fires no bind upward; this keeps the
    // parent's admitted token for the child current, so the child's later
    // death, which carries that token, is read as the admitted route's.
    if (typeof t.onRouteChanged === 'function') t.onRouteChanged((n, token) => this._onSubRouteChanged(t, n, token));
    if (typeof t.onNegotiationFailed === 'function') for (const e of (this._negotiationFailedHandlers ?? [])) e.unsubs.push(t.onNegotiationFailed(e.handler));
    for (const reg of this._peerListRegistrars ?? []) reg(t);
  }

  // ── Route-token rule (socket-is-bootstrap v0.5, § Ownership) ───────────
  //
  // Each sub-transport reports its own peer's bind and death. Without a rule
  // the first sub in order owns a disputed identity and EVERY sub's death
  // reaches the kernel, so closing a bootstrap socket after a mesh channel to
  // the same identity has bound evicts the identity (Vega 2b16970f, Aster
  // BS-1). The rule, run here before the attempt guard and before any kernel
  // side effect:
  //
  //   STEP 0  a bind whose token is not the sub's CURRENT token for the
  //           identity, or whose route is superseded, is ignored (R8-2 is not
  //           this check: it rejects only with a live attempt).
  //   (a)     no admitted route → admitted; the kernel handler runs.
  //   (b)     DIRECTIONAL. admitted on a BOOTSTRAP sub + binding on a
  //           non-bootstrap sub → switch: the new route is admitted, the old
  //           is superseded (routing skips it, its death is swallowed, its
  //           pending requests fail `route-superseded`); the kernel sees NO
  //           re-admission. admitted on a non-bootstrap sub + binding on a
  //           bootstrap sub → the bootstrap route is born superseded.
  //   (c)     same sub, current token → the sub's own duplicate rule; the
  //           kernel handler runs as today (R8-2 inside it).
  //   death   from a superseded route: swallowed. From the admitted route:
  //           forwarded; the identity dies. A superseded route is never
  //           re-promoted.
  //
  // A sub declares `isBootstrap === true` (the bridge door's WebSocket
  // transport, the client's BridgeTransport). With no bootstrap sub in the
  // composite, (b) never applies and behaviour is as before this rule.

  _currentToken(t, nodeId) {
    try {
      if (typeof t.channelIdFor === 'function') return t.channelIdFor(nodeId) ?? null;
      if (typeof t.meshIdFor    === 'function') return t.meshIdFor(nodeId)    ?? null;
      if (typeof t.connIdFor    === 'function') return t.connIdFor(nodeId)    ?? null;
    } catch { /* fall through */ }
    return undefined;   // the sub cannot name a token: validation is skipped for it
  }

  _isSuperseded(t, nodeId) {
    const rec = this._routes.get(nodeId);
    return !!rec && rec.superseded.has(t);
  }

  _supersede(rec, nodeId, t, token, why) {
    rec.superseded.set(t, token ?? null);
    this._log('route-superseded', { nodeId: String(nodeId), sub: t.constructor?.name, token: token ?? null, why });
    try { t.supersedePeer?.(nodeId, token ?? null); } catch (err) { this._log('supersede-hook-threw', { err: err?.message }); }
  }

  /**
   * Decide what a bind event does. Returns 'admit' (run the kernel handlers),
   * 'switch' (route changed, no kernel handler), or 'ignore'.
   */
  _routeBind(t, nodeId, token) {
    // STEP 0: token validation, independent of the attempt guard.
    const cur = this._currentToken(t, nodeId);
    if (cur !== undefined && token != null && cur !== token) {
      this.routeStats.staleToken++;
      this._log('bind-stale-token', { nodeId: String(nodeId), sub: t.constructor?.name, token, current: cur });
      return 'ignore';
    }
    let rec = this._routes.get(nodeId);
    if (rec && rec.superseded.has(t)) {
      this.routeStats.staleToken++;
      this._log('bind-on-superseded-route', { nodeId: String(nodeId), sub: t.constructor?.name, token });
      return 'ignore';
    }
    if (!rec) { rec = { admitted: null, superseded: new Map() }; this._routes.set(nodeId, rec); }
    const tok = token ?? cur ?? null;
    if (!rec.admitted) {
      // (a): admitted only once the bind policy (if any) has passed — see
      // _onSubBound, which sets rec.admitted for 'admit-new'.
      this._pendingAdmit = { rec, sub: t, token: tok };
      return 'admit-new';
    }
    if (rec.admitted.sub === t) {                          // (c): the sub's own duplicate rule, kernel handler as today
      // Step 0 proved `tok` is the sub's CURRENT token; the admitted token
      // follows it, so a later death on an older token of this sub is stale
      // and a death on this token is the route's.
      if (tok != null && rec.admitted.token !== tok) { rec.admitted.token = tok; this._emitRouteChanged(nodeId, tok); }
      return 'admit';
    }
    const oldBoot = rec.admitted.sub.isBootstrap === true;
    const newBoot = t.isBootstrap === true;
    if (oldBoot && !newBoot) {                              // (b) socket → mesh: the switch
      this._supersede(rec, nodeId, rec.admitted.sub, rec.admitted.token, 'switch');
      rec.admitted = { sub: t, token: tok };
      this.routeStats.switched++;
      this._log('route-switched', { nodeId: String(nodeId), to: t.constructor?.name, token: tok });
      this._emitRouteChanged(nodeId, tok);
      return 'switch';
    }
    if (!oldBoot && newBoot) {                              // (b) reverse: born superseded
      this._supersede(rec, nodeId, t, tok, 'born-superseded');
      this.routeStats.bornSuperseded++;
      return 'ignore';
    }
    return 'admit';   // two non-bootstrap subs: the dedup below keeps today's one-fire behaviour
  }

  /**
   * Socket-is-bootstrap v0.5 (§ Make room): a BIND POLICY consulted after
   * step 0 and before any kernel handler, for a bind that would ADMIT a new
   * route (verdict (a)). `fn(nodeIdBig, sub, token) → boolean`; false refuses
   * the bind: no route is recorded, no kernel handler runs, and the policy
   * owner is expected to close the channel it refused. Side-effect-free
   * preflights belong here (a bridge's identity cooldown, the kernel's
   * gatePreflight, make-room victim and budget); the commit is the kernel's
   * own handler, which runs only when the policy passes. Null clears it.
   */
  setBindPolicy(fn) {
    if (fn !== null && typeof fn !== 'function') throw new TypeError('setBindPolicy: fn must be a function or null');
    this._bindPolicy = fn;
  }

  /**
   * (a) ADMIT A NEW ROUTE: the ONE place a new admitted route is written, used
   * by live delivery (_onSubBound) and by the existing-peer replay in
   * onPeerBound alike (RT-1, Aster 3d778257: the replay must run the same
   * admission policy as live delivery). Returns true when admitted.
   */
  _admitNew(t, nodeIdBig, pa) {
    if (this._bindPolicy) {
      let ok = true;
      try { ok = this._bindPolicy(nodeIdBig, t, pa.token) !== false; }
      catch (err) { ok = false; this._log('bind-policy-threw', { err: err?.message }); }
      if (!ok) {
        this.routeStats.policyRefused = (this.routeStats.policyRefused ?? 0) + 1;
        this._log('bind-refused-by-policy', { nodeId: String(nodeIdBig), sub: t.constructor?.name, token: pa.token });
        if (!pa.rec.admitted && pa.rec.superseded.size === 0) this._routes.delete(nodeIdBig);
        return false;
      }
    }
    pa.rec.admitted = { sub: t, token: pa.token };
    this.routeStats.admitted++;
    return true;
  }

  /** Subscribe to this composite's route changes: `handler(nodeIdBig, newToken)`. */
  onRouteChanged(handler) {
    if (typeof handler !== 'function') throw new TypeError('onRouteChanged: handler must be a function');
    this._routeChangedHandlers.push(handler);
    return () => { const i = this._routeChangedHandlers.indexOf(handler); if (i >= 0) this._routeChangedHandlers.splice(i, 1); };
  }

  _emitRouteChanged(nodeId, token) {
    for (const h of this._routeChangedHandlers) {
      try { h(nodeId, token); } catch (err) { this._log('route-changed-handler-threw', { err: err?.message }); }
    }
  }

  /**
   * A child composite announced a route change for `nodeId`. If that child is
   * this identity's admitted route here, follow the child's CURRENT token —
   * never the notified value (Aster b4d4516c): under the synchronous listener
   * API a listener registered on the child earlier than this parent can,
   * during the notification for token A, cause a same-sub replacement to B
   * whose nested notification already moved this parent to B; when A's loop
   * resumes, copying A would write it over B. Re-reading the child's route
   * table resolves every ordering to the child's present state. A child whose
   * current token is null (route gone) changes nothing; its death settles it.
   */
  /** The identity's ADMITTED route token from this composite's route table, or null. No fallback to a sub's bound mapping (Aster 6a8d4ab9). */
  admittedTokenOf(nodeId) { return this._routes.get(nodeId)?.admitted?.token ?? null; }

  _onSubRouteChanged(t, nodeId, _notified) {
    const rec = this._routes.get(nodeId);
    if (!rec?.admitted || rec.admitted.sub !== t) return;
    // Authoritative admission, not lookup: a child's channelIdFor falls back
    // to a sub's bound mapping when the child has no admitted route, which is
    // exactly the state (still bound, no longer admitted) this must not follow.
    const cur = (typeof t.admittedTokenOf === 'function') ? t.admittedTokenOf(nodeId) : this._currentToken(t, nodeId);
    if (cur == null) { this.routeStats.routeChangeNull = (this.routeStats.routeChangeNull ?? 0) + 1; return; }
    if (rec.admitted.token !== cur) {
      rec.admitted.token = cur;
      this.routeStats.tokenFollowed = (this.routeStats.tokenFollowed ?? 0) + 1;
      this._emitRouteChanged(nodeId, cur);   // and onward to our own parent, if any
    }
  }

  _onSubBound(t, nodeIdBig, meshId, inc) {
    if (typeof nodeIdBig !== 'bigint') return;
    const token = typeof meshId === 'string' ? meshId : null;
    const verdict = this._routeBind(t, nodeIdBig, token);
    if (verdict === 'admit-new') {
      const pa = this._pendingAdmit; this._pendingAdmit = null;
      if (!this._admitNew(t, nodeIdBig, pa)) return false;
    } else if (verdict !== 'admit') {
      return false;
    }
    for (const e of this._peerBoundEntries) this._fireBound(e, nodeIdBig, meshId, inc);
    return true;
  }

  _fireBound(e, nodeIdBig, meshId, inc) {
    // Dedup per handler: fires once per identity until the admitted route
    // dies (rearmed in _onSubDeath). R8-2: a handler returning false did not
    // bind the peer; un-see it so the current channel's bind still fires.
    if (e.seen.has(nodeIdBig)) return;
    e.seen.add(nodeIdBig);
    let r;
    try { r = e.handler(nodeIdBig, meshId, inc); }
    catch (err) { this._log?.('peer-bound-fanout-threw', { err: err.message }); }
    if (r === false) e.seen.delete(nodeIdBig);
  }

  /**
   * A death from sub `t` for identity `id`, optionally with the route token
   * the sub reports it for (a connId, a meshId). Forwarded to the kernel ONLY
   * when it is the ADMITTED route's death (RT-2, Aster 3d778257): a death from
   * a superseded sub is swallowed however many times it arrives; a death from
   * a sub that is not the identity's admitted route is swallowed; a death the
   * admitted sub reports for an OLDER token than the admitted one is stale
   * and swallowed. An identity this composite never saw bound forwards as
   * before.
   */
  _onSubDeath(t, id, reason, token) {
    let big = null;
    if (typeof id === 'bigint') big = id;
    else if (typeof id === 'string' && /^[0-9a-f]{66}$/i.test(id)) { try { big = BigInt('0x' + id); } catch { big = null; } }
    let forward = true;
    let tokenOut = token;   // what this composite reports upward: its admitted token for the identity
    if (big !== null) {
      const rec = this._routes.get(big);
      if (rec) {
        if (rec.admitted && rec.admitted.sub === t) tokenOut = rec.admitted.token ?? token;
        if (rec.superseded.has(t)) {
          rec.superseded.delete(t);
          forward = false;
          this.routeStats.deathSwallowed++;
          this._log('death-superseded-swallowed', { nodeId: String(big), sub: t.constructor?.name, reason: reason ?? null });
        } else if (rec.admitted && rec.admitted.sub === t) {
          if (token != null && rec.admitted.token != null && token !== rec.admitted.token) {
            forward = false;
            this.routeStats.deathStaleToken = (this.routeStats.deathStaleToken ?? 0) + 1;
            this._log('death-stale-token-swallowed', { nodeId: String(big), sub: t.constructor?.name, token, admitted: rec.admitted.token });
          } else {
            rec.admitted = null;
            this.routeStats.deathForwarded++;
          }
        } else if (rec.admitted && (t.isBootstrap === true || rec.admitted.sub.isBootstrap === true)) {
          // Where a bootstrap route is involved, a sub that is neither the
          // admitted route nor (any longer) a superseded one cannot kill the
          // identity: a REPEATED death from a retired socket (RT-2). A
          // composite with no bootstrap sub keeps its pre-rule behaviour here
          // (every sub's death forwards), byte-identical for the sim and
          // every legacy two-mesh composite.
          forward = false;
          this.routeStats.deathSwallowed++;
          this._log('death-non-owner-swallowed', { nodeId: String(big), sub: t.constructor?.name, reason: reason ?? null });
        }
        if (!rec.admitted && rec.superseded.size === 0) this._routes.delete(big);
      }
    }
    if (!forward) return;
    if (big !== null) for (const e of this._peerBoundEntries) e.seen.delete(big);
    for (const h of this._peerDiedHandlers) {
      // The token travels upward (Aster 88f4c2f7): a parent composite reads it
      // against the admitted token it keeps current for this child.
      try { h(id, reason, tokenOut); } catch (err) { this._log('peer-died-handler-threw', { err: err?.message }); }
    }
  }

  /** The admitted route of an identity: {sub, token} or null. */
  routeOf(nodeId) { return this._routes.get(nodeId)?.admitted ?? null; }

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
      // Socket-is-bootstrap v0.5: a superseded route is never routable again.
      if (this._isSuperseded(t, nodeId)) continue;
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
    // sub-transport binds the same nodeId.  The dedup is re-armed when the
    // ADMITTED route dies (_onSubDeath) — otherwise it is PERMANENT: a peer
    // that drops and later reconnects would never re-fire onPeerBound, so the
    // routing layer never re-admits it. A superseded route's death does NOT
    // re-arm it: the identity is still admitted elsewhere.
    //
    // Socket-is-bootstrap v0.5: the per-sub subscription lives in
    // addSubtransport and dispatches through the route-token rule; this entry
    // only receives what that rule admits. Peers a sub already holds bound at
    // subscribe time are replayed through the same rule.
    const entry = { handler, seen: new Set() };
    this._peerBoundEntries.push(entry);
    for (const t of this._subs) {
      if (typeof t.boundPeers !== 'function') continue;
      let ids = [];
      try { ids = t.boundPeers(); } catch { ids = []; }
      for (const id of ids) {
        if (typeof id !== 'bigint') continue;
        const token = this._currentToken(t, id);
        const verdict = this._routeBind(t, id, token ?? null);
        if (verdict === 'admit-new') {
          const pa = this._pendingAdmit; this._pendingAdmit = null;
          if (this._admitNew(t, id, pa)) this._fireBound(entry, id, token ?? undefined, null);   // RT-1: same policy as live delivery
        } else if (verdict === 'admit') {
          this._fireBound(entry, id, token ?? undefined, null);
        }
      }
    }
    return () => {
      const i = this._peerBoundEntries.indexOf(entry);
      if (i >= 0) this._peerBoundEntries.splice(i, 1);
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
    // Socket-is-bootstrap v0.5: a voluntary close ends the identity's route
    // record here (the sub unbinds before it closes, so no death follows to
    // clear it); superseded routes it still holds close by their own paths.
    const rec = this._routes.get(nodeId);
    if (rec) { rec.admitted = null; if (rec.superseded.size === 0) this._routes.delete(nodeId); }
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
    if (typeof handler !== 'function') throw new TypeError('onPeerDied: handler must be a function');
    // Socket-is-bootstrap v0.5: subs are subscribed once each in
    // addSubtransport; every death reaches _onSubDeath, which forwards only a
    // death from an identity's ADMITTED route (or from a route this composite
    // never saw bound) to the handlers registered here.
    this._peerDiedHandlers.push(handler);
    return () => {
      const i = this._peerDiedHandlers.indexOf(handler);
      if (i >= 0) this._peerDiedHandlers.splice(i, 1);
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
    // Socket-is-bootstrap v0.5 (Aster 3d778257): the ADMITTED route's token
    // first; then the subs in order, skipping a sub superseded for this
    // identity, and recursing into a nested composite's own channelIdFor.
    const adm = this._routes.get(nodeId)?.admitted;
    if (adm) {
      const id = this._tokenFrom(adm.sub, nodeId);
      if (id != null) return id;
    }
    for (const t of this._subs) {
      if (this._isSuperseded(t, nodeId)) continue;
      const id = this._tokenFrom(t, nodeId);
      if (id != null) return id;
    }
    return null;
  }

  _tokenFrom(t, nodeId) {
    try {
      if (typeof t.channelIdFor === 'function') { const id = t.channelIdFor(nodeId); if (id != null) return id; }
      if (typeof t.meshIdFor === 'function')    { const id = t.meshIdFor(nodeId);    if (id != null) return id; }
      if (typeof t.connIdFor === 'function')    { const id = t.connIdFor(nodeId);    if (id != null) return id; }
    } catch { /* a sub that cannot answer names no token */ }
    return null;
  }
}
