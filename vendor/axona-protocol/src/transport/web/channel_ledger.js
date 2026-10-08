// =====================================================================
// channel_ledger.js — the CHANNEL and PEER records of Hold-and-Fill
// (axona-docs 4334504 v0.5, 95c2ff4 v0.7, "Channels, peers and transitions"),
// repair row 3.
//
// WHAT IT IS. One record per RTCPeerConnection, keyed by a token `t` minted
// at allocation and never reused in the process (the mesh's per-PC
// incarnation tag, MeshManager._attachPc), with a state from
// ALLOCATED → NEGOTIATING → OPEN → CLOSING → GONE; and one record per bound
// identity (hex nodeId), pointing at its current channel token or none.
// Counts over those records are the bounds the design names:
//
//   chan(ALLOCATED ∪ NEGOTIATING ∪ OPEN ∪ CLOSING)   ≤ C_phys
//   chan(ALLOCATED ∪ NEGOTIATING, inbound, unbound)  ≤ C_inbound
//   chan(ALLOCATED ∪ NEGOTIATING, outbound)          ≤ P_pending   (the attempt bound)
//
// WHAT IT IS NOT, in this row. It is BOOKKEEPING AND A PREDICATE. It issues
// no dial, closes nothing, and by default REFUSES NOTHING: `mayAllocate()`
// answers, the mesh asks before every PC, and with `enforce: false` (the
// default) a refusal is counted (`wouldRefuse`) and the PC is built as
// today. That is rollout step 2 of the design: measure the open-channel
// counts on the running fleet before any bound is believed. `enforce: true`
// turns the same predicate into a refusal; nothing else changes. The
// accounting release decides the default; this row does not.
//
// The mesh's existing negotiation deadline (NEGOTIATION_DEADLINE_MS from
// state creation, mesh.js _armReaper) already bounds a channel that never
// sends a first frame, so ALLOCATED has no separate timer here; the record
// just says how long it sat there.
//
// GONE is confirmed by the transport (connectionState 'closed') and by
// NOTHING ELSE. A PC that was closed locally and has not reported 'closed'
// after `closeEscalateMs` is ESCALATED: `onEscalate(t, meshId)` is called so
// the owner can force a second pc.close(), `closeEscalated` counts it, and
// the record STAYS CLOSING AND CHARGED until the transport confirms. A
// timeout is not evidence that the physical resource is gone (Aster
// 09f59626 R3-A); an implementation that never confirms shows up as
// `oldestClosingMs` growing in stats(), which is the measurement, not a
// leak to paper over. `closeEscalateMs: 0` turns escalation off.
//
// The peer record is the CURRENT bound set: an identity has a record while
// some live channel binds it and none after. History of lost identities is
// row 1's mark table, not this ledger (R3-C).
// =====================================================================

export const CHAN = Object.freeze({
  ALLOCATED: 'ALLOCATED', NEGOTIATING: 'NEGOTIATING', OPEN: 'OPEN', CLOSING: 'CLOSING', GONE: 'GONE',
});

const LIVE = new Set([CHAN.ALLOCATED, CHAN.NEGOTIATING, CHAN.OPEN, CHAN.CLOSING]);
const PRE_OPEN = new Set([CHAN.ALLOCATED, CHAN.NEGOTIATING]);
// POINTER ELIGIBILITY is not PHYSICAL RETENTION (Aster 38ea5f3e). A CLOSING
// channel is charged until the transport confirms, but a peer record never
// points at one: v0.7, "a peer record points at a channel only in
// ALLOCATED, NEGOTIATING or OPEN; the moment its channel enters CLOSING the
// pointer is cleared in the same step."
const POINTABLE = new Set([CHAN.ALLOCATED, CHAN.NEGOTIATING, CHAN.OPEN]);

export const LEDGER_DEFAULTS = Object.freeze({
  // C_phys_req(cap) = cap + P_pending + C_inbound + 4 with cap 50 (the
  // parameter table of the design): 50 + 8 + 4 + 4.
  cPhys: 66,
  cInbound: 4,
  pPending: 8,
  closeEscalateMs: 10_000,
  enforce: false,
});

export class ChannelLedger {
  /**
   * @param {object} [opts]
   * @param {number}  [opts.cPhys]
   * @param {number}  [opts.cInbound]
   * @param {number}  [opts.pPending]
   * @param {number}  [opts.closeEscalateMs]
   * @param {boolean} [opts.enforce]   false: count would-be refusals; true: refuse
   * @param {(t:string, meshId:string) => void} [opts.onEscalate]  called once per
   *        CLOSING record that reaches closeEscalateMs unconfirmed; the owner
   *        forces a second close. Releases nothing.
   * @param {() => number} [opts.now]
   * @param {(ev:string, data:object) => void} [opts.log]
   * @param {typeof setTimeout} [opts.setTimeout]  injectable for tests
   * @param {typeof clearTimeout} [opts.clearTimeout]
   */
  constructor(opts = {}) {
    const o = { ...LEDGER_DEFAULTS, ...(opts || {}) };
    // Explicit parsing: a finite number is taken as given (0 included where
    // 0 has a meaning); anything else is the default. `Number(x) || dflt`
    // would turn an explicit 0 into the default (R3-C minor).
    const num = (v, dflt, min) => { const n = Number(v); return Number.isFinite(n) ? Math.max(min, n) : dflt; };
    this.cPhys    = num(o.cPhys,    LEDGER_DEFAULTS.cPhys,    1);
    this.cInbound = num(o.cInbound, LEDGER_DEFAULTS.cInbound, 1);
    this.pPending = num(o.pPending, LEDGER_DEFAULTS.pPending, 1);
    this.closeEscalateMs = num(o.closeEscalateMs, LEDGER_DEFAULTS.closeEscalateMs, 0);   // 0 = escalation off
    this.enforce  = o.enforce === true;
    this._onEscalate = typeof o.onEscalate === 'function' ? o.onEscalate : null;
    this._now     = typeof o.now === 'function' ? o.now : Date.now;
    this._log     = typeof o.log === 'function' ? o.log : () => {};
    // BROWSER TRAP (David, 2026-10-08, axona.chat 0.81.0 console): the host's
    // native setTimeout stored on an instance property and then called as a
    // METHOD runs with `this` = this ledger, and Chrome throws
    // "Illegal invocation" — from closing(), inside mesh._retire, BEFORE the
    // peer is deleted from the mesh's map, so every retired channel became a
    // zombie in every browser client. Node tolerates any `this`, which is why
    // no fence saw it. The timers are called as free functions always; an
    // injected pair (tests) is called the same way.
    const st = typeof o.setTimeout === 'function' ? o.setTimeout : setTimeout;
    const ct = typeof o.clearTimeout === 'function' ? o.clearTimeout : clearTimeout;
    this._setTimeout   = (fn, ms) => st(fn, ms);
    this._clearTimeout = (h) => ct(h);

    /** @type {Map<string, {t:string, meshId:string, dir:'in'|'out', state:string, since:number, negotiatingAt:number, openedAt:number, closingAt:number, goneAt:number, reason:string|null, nodeId:string|null, escalateTimer:any}>} */
    this._chan = new Map();
    /** meshId → current token (the live channel for that signalling id). */
    this._tByMeshId = new Map();
    /** nodeId hex → { nodeId, t: string|null, boundAt: number } */
    this._peer = new Map();
    this._seq = 0;
    this._stats = {
      allocated: 0, refusedOut: 0, refusedIn: 0, wouldRefuseOut: 0, wouldRefuseIn: 0,
      closeEscalated: 0, staleEvent: 0, goneTotal: 0,
    };
  }

  // ── counts ─────────────────────────────────────────────────────────

  /** Live channel records (every state but GONE). */
  chanAll() { let n = 0; for (const c of this._chan.values()) if (LIVE.has(c.state)) n++; return n; }
  /** Inbound channels before OPEN with no bound identity. */
  chanInboundUnbound() {
    let n = 0;
    for (const c of this._chan.values()) if (c.dir === 'in' && PRE_OPEN.has(c.state) && c.nodeId == null) n++;
    return n;
  }
  /** Outbound channels before OPEN: the attempt count. */
  chanOutboundPending() {
    let n = 0;
    for (const c of this._chan.values()) if (c.dir === 'out' && PRE_OPEN.has(c.state)) n++;
    return n;
  }

  // ── the predicate ──────────────────────────────────────────────────

  /**
   * May a channel in direction `dir` be allocated now? Evaluated BEFORE the
   * PeerConnection is constructed; the increment happens in allocate(), in
   * the same synchronous step, so two callers in one macrotask see each
   * other's reservation. With `enforce` false the answer is always ok and
   * the refusal is counted.
   * @param {'in'|'out'} dir
   * @returns {{ ok: boolean, why: string|null }}
   */
  /**
   * Row 12: the PURE form of the predicate — the same bounds, no counting, no
   * log. The fill asks it before a dial as the channel-token half of its
   * reservation; a refusal there is counted by the fill as dial-deferred,
   * not here as alloc-refused, because nothing was allocated or refused.
   * @param {'in'|'out'} dir
   * @returns {{ ok: boolean, why: string|null }}
   */
  canAllocate(dir) {
    let why = null;
    if (this.chanAll() >= this.cPhys) why = 'phys';
    else if (dir === 'in' && this.chanInboundUnbound() >= this.cInbound) why = 'inbound';
    else if (dir === 'out' && this.chanOutboundPending() >= this.pPending) why = 'pending';
    if (why == null || !this.enforce) return { ok: true, why };
    return { ok: false, why };
  }

  mayAllocate(dir) {
    let why = null;
    if (this.chanAll() >= this.cPhys) why = 'phys';
    else if (dir === 'in' && this.chanInboundUnbound() >= this.cInbound) why = 'inbound';
    else if (dir === 'out' && this.chanOutboundPending() >= this.pPending) why = 'pending';
    if (why == null) return { ok: true, why: null };
    if (this.enforce) {
      if (dir === 'in') this._stats.refusedIn++; else this._stats.refusedOut++;
      this._log('alloc-refused', { dir, why, all: this.chanAll(), inboundUnbound: this.chanInboundUnbound(), outboundPending: this.chanOutboundPending() });
      return { ok: false, why };
    }
    if (dir === 'in') this._stats.wouldRefuseIn++; else this._stats.wouldRefuseOut++;
    return { ok: true, why };   // counted, not refused
  }

  // ── transitions ────────────────────────────────────────────────────

  /**
   * ALLOCATED. Called with the mesh's per-PC incarnation tag as the token, so
   * the ledger and the mesh log lines join on one value.
   * @param {string} t   unique per PC in this process
   * @param {string} meshId
   * @param {'in'|'out'} dir
   */
  allocate(t, meshId, dir) {
    if (this._chan.has(t)) { this._stats.staleEvent++; return this._chan.get(t); }
    const rec = {
      t, meshId, dir: dir === 'in' ? 'in' : 'out', state: CHAN.ALLOCATED,
      since: this._now(), negotiatingAt: 0, openedAt: 0, closingAt: 0, goneAt: 0,
      reason: null, nodeId: null, escalateTimer: null, seq: ++this._seq,
    };
    this._chan.set(t, rec);
    this._tByMeshId.set(meshId, t);
    this._stats.allocated++;
    return rec;
  }

  /** NEGOTIATING: the first frame (offer sent, or offer received) on `t`. */
  negotiating(t) {
    const c = this._chan.get(t);
    if (!c || c.state !== CHAN.ALLOCATED) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.NEGOTIATING; c.negotiatingAt = this._now();
    return c;
  }

  /** OPEN: the data channel opened on `t`. */
  open(t) {
    const c = this._chan.get(t);
    if (!c || !PRE_OPEN.has(c.state)) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.OPEN; c.openedAt = this._now();
    return c;
  }

  /**
   * The handshake bound `nodeId` on the channel that currently serves
   * `meshId`. The peer record points at that channel; an older channel the
   * same identity pointed at is left to its own close (the duplicate case
   * is the kernel's bindPeer dedup, which closes the loser by meshId).
   * @param {string} meshId
   * @param {string} nodeId hex
   */
  bind(meshId, nodeId) {
    const t = this._tByMeshId.get(meshId);
    const c = t ? this._chan.get(t) : null;
    if (!c || !POINTABLE.has(c.state)) { this._stats.staleEvent++; return this._peer.get(nodeId) ?? null; }
    c.nodeId = nodeId;
    const p = this._peer.get(nodeId) ?? { nodeId, t: null, boundAt: 0 };
    p.t = t;                 // the newest binding is the pointer (the dedup winner, R3-B)
    p.boundAt = this._now();
    this._peer.set(nodeId, p);
    return p;
  }

  /** Another POINTABLE channel (ALLOCATED, NEGOTIATING or OPEN; never
   *  CLOSING), other than `exceptT`, that binds `nodeId`; its token or null.
   *  Prefers an OPEN one. */
  _otherPointableFor(nodeId, exceptT) {
    let pre = null;
    for (const o of this._chan.values()) {
      if (o.t === exceptT || o.nodeId !== nodeId || !POINTABLE.has(o.state)) continue;
      if (o.state === CHAN.OPEN) return o.t;
      if (pre === null) pre = o.t;
    }
    return pre;
  }

  /** Drop or re-point the peer record for `nodeId` after channel `t` stopped
   *  binding it, or stopped being pointable: point at another POINTABLE
   *  binding channel if one exists, else delete the record. The record is the
   *  current bound set (R3-C); a CLOSING channel is never the pointer even
   *  while it is still charged (38ea5f3e). */
  _settlePeer(nodeId, t) {
    if (nodeId == null) return;
    const p = this._peer.get(nodeId);
    if (!p) return;
    const other = this._otherPointableFor(nodeId, t);
    if (other) { if (p.t === t || p.t == null) p.t = other; }
    else this._peer.delete(nodeId);
  }

  /**
   * The binding for `meshId` was dropped (unbindPeer). The channel is found
   * by meshId across LIVE records, not only the current-token map: the mesh
   * marks CLOSING (which retires the token from that map) BEFORE onPeerLost
   * reaches unbindPeer (R3-C), so a CLOSING channel must still be found here.
   */
  unbind(meshId) {
    let c = null;
    const t = this._tByMeshId.get(meshId);
    if (t) c = this._chan.get(t) ?? null;
    if (!c || c.nodeId == null) {
      // the current-token map no longer has it, or it is a bound CLOSING one
      let newest = null;
      for (const o of this._chan.values()) if (o.meshId === meshId && o.nodeId != null && LIVE.has(o.state) && (!newest || o.seq > newest.seq)) newest = o;
      c = newest;
    }
    if (!c) return;
    const nodeId = c.nodeId;
    c.nodeId = null;
    this._settlePeer(nodeId, c.t);
  }

  /** CLOSING: a close was issued on `t` (the mesh's _retire). Capacity is
   *  NOT released here; it waits for gone(t) or the escalation. */
  closing(t, reason = 'close') {
    const c = this._chan.get(t);
    if (!c || c.state === CHAN.CLOSING || c.state === CHAN.GONE) { if (!c || c.state === CHAN.GONE) this._stats.staleEvent++; return c ?? null; }
    c.state = CHAN.CLOSING; c.closingAt = this._now(); c.reason = reason;
    if (this._tByMeshId.get(c.meshId) === t) this._tByMeshId.delete(c.meshId);
    // The pointer leaves this channel in the same step as CLOSING: it moves
    // to another pointable binding channel if one exists, else the identity
    // leaves the bound set. The CLOSING record keeps its nodeId for the
    // unbind that follows, and stays charged.
    if (c.nodeId != null) {
      const p = this._peer.get(c.nodeId);
      if (p && p.t === t) this._settlePeer(c.nodeId, t);
    }
    if (this.closeEscalateMs > 0) {
      c.escalateTimer = this._setTimeout(() => {
        c.escalateTimer = null;
        if (c.state !== CHAN.CLOSING) return;
        // ESCALATE (v0.7 `escalate(t)`): force a second close through the
        // owner; the record stays CLOSING and charged; capacity waits for the
        // transport's confirmation (R3-A). Nothing is released here.
        this._stats.closeEscalated++;
        this._log('close-escalated', { t, meshId: c.meshId, reason: c.reason, closingMs: this._now() - c.closingAt });
        try { this._onEscalate?.(t, c.meshId); }
        catch (err) { this._log('close-escalate-threw', { t, err: err?.message }); }
      }, this.closeEscalateMs);
      try { c.escalateTimer?.unref?.(); } catch { /* browser timers have no unref */ }
    }
    return c;
  }

  /**
   * GONE: the transport confirmed the close (connectionState 'closed'). The
   * ONLY release of a record's capacity. An unprompted close (no closing()
   * before it) is the involuntary-loss row: the record goes straight to GONE
   * and `prompted` is false in the return. An identity this channel still
   * bound is re-pointed to another live binding channel or dropped from the
   * bound set (R3-C).
   */
  gone(t) {
    const c = this._chan.get(t);
    if (!c) { this._stats.staleEvent++; return null; }
    if (c.state === CHAN.GONE) { this._stats.staleEvent++; return c; }
    const prompted = c.state === CHAN.CLOSING;
    if (c.escalateTimer) { try { this._clearTimeout(c.escalateTimer); } catch {} c.escalateTimer = null; }
    c.state = CHAN.GONE; c.goneAt = this._now();
    if (this._tByMeshId.get(c.meshId) === t) this._tByMeshId.delete(c.meshId);
    const nodeId = c.nodeId; c.nodeId = null;
    this._stats.goneTotal++;
    this._chan.delete(t);   // GONE records are not retained; the token is never reused
    this._settlePeer(nodeId, t);
    c.prompted = prompted;
    return c;
  }

  /** Token of the live channel serving a signalling id, or null. */
  tokenFor(meshId) { return this._tByMeshId.get(meshId) ?? null; }

  /** Snapshot for status surfaces and the step-2 measurement. */
  stats() {
    const byState = { ALLOCATED: 0, NEGOTIATING: 0, OPEN: 0, CLOSING: 0 };
    let unboundOpen = 0, oldestPreOpenMs = 0, oldestClosingMs = 0;
    const now = this._now();
    for (const c of this._chan.values()) {
      if (byState[c.state] != null) byState[c.state]++;
      if (c.state === CHAN.OPEN && c.nodeId == null) unboundOpen++;
      if (PRE_OPEN.has(c.state)) oldestPreOpenMs = Math.max(oldestPreOpenMs, now - c.since);
      if (c.state === CHAN.CLOSING) oldestClosingMs = Math.max(oldestClosingMs, now - c.closingAt);
    }
    let peersPointing = 0;
    for (const p of this._peer.values()) if (p.t != null) peersPointing++;
    return {
      bounds: { cPhys: this.cPhys, cInbound: this.cInbound, pPending: this.pPending, enforce: this.enforce },
      all: this.chanAll(), byState,
      inboundUnbound: this.chanInboundUnbound(), outboundPending: this.chanOutboundPending(),
      unboundOpen, boundPeers: this._peer.size, peersPointing,
      oldestPreOpenMs, oldestClosingMs,
      ...this._stats,
    };
  }

  /** For tests: the record for a token, or null. */
  record(t) { return this._chan.get(t) ?? null; }
  /** For tests: the peer record for a nodeId hex, or null. */
  peer(nodeId) { return this._peer.get(nodeId) ?? null; }

  dispose() {
    for (const c of this._chan.values()) if (c.escalateTimer) { try { this._clearTimeout(c.escalateTimer); } catch {} c.escalateTimer = null; }
  }
}
