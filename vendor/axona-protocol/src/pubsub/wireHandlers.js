// wireHandlers.js — the WIRE plane (refactor Phase 2).
//
// Every routed-message handler and the axon-tree mechanics they drive:
// the topic-decision (handle/reroute/forward/reject), seating + delegation
// (widen-before-deepen), root ingress (verify → policy → stamp → fan out),
// stamped replay-up/handoff/replicate ingest, delivery + re-fan, replay,
// kills, pulls, and demand-driven metrics. Methods are mixed into
// AxonaManager.prototype; root-claim decisions delegate to rootClaim.js.

import {
  T, RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS, ROOT_CLAIM_MS,
  ROOT_REPLICAS, BACKUP_EVICT_MS, CACHE_MAX, CACHE_BYTES, MAX_DIRECT,
  DELEGATE_BATCH, MAX_VIA, VIA_HOP_BUDGET, TTL_MS, APP_DEDUP_MAX,
  PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES, COLD_BURST_TRIES,
  COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES, COLD_BURST_SLOW_INTERVAL_MS,
  COLD_PEER_THRESHOLD, FIRST_PUBLISH_RESEND_MS, REPLAY_CHUNK_BYTES,
  FUTURE_TOLERANCE_MS, BEACON_MS, BEACON_TTL_MS, BEACON_SEEN_MS,
  ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH, METRICS_LEASE_MS,
  METRICS_PUB_MS, METRICS_COALESCE_MS,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { verifyEnvelope, checkFreshness } from './envelope.js';
import { verifyKill } from './kill.js';
import { deriveTopicIdBig } from './post.js';
import { signAckProof, verifyAckProof, PURPOSE, OP } from './ackProof.js';
import { shadowEnabled, registerFrame } from '../registry/index.js';

export const wireHandlersMethods = {
  _registerHandlers() {
    // REF-1.1 E2.1: the 19 routed pub/sub handlers register through the ONE
    // canonical door — registerFrame — not the raw this.dht.onRoutedMessage
    // primitive. `_frameDoor` is the always-built Boundary-1 registry (wiring +
    // shadow wrap + registry-owned mintLive certifier, built in the AxonaManager
    // ctor). Its wrap runs each handler verbatim unless the runtime shadow flag is
    // on (DEFAULT-OFF), so this path is byte-identical to the pre-E2.1 raw
    // registration; the door refuses AT REGISTRATION any wire no Boundary-1 row
    // declares (exit criterion 4). Each handler is bound to the manager `this` so it
    // always receives the manager and the same (p, m). The wire literal is passed
    // DIRECTLY at each site — the [V2] wire-literal gate forbids a variable wire, so
    // there is no on(type, fn) indirection. The registry is written as the EXPLICIT
    // canonical reference `this._frameDoor` at every site (not a `const reg` alias),
    // so the shared migration-aware ownership grammar recognizes each as a B1 door
    // (E2.1 council 389be28f / eb71240a). transportKind is OMITTED: every Boundary-1
    // row is a single-primitive bare-keyed routed wire.
    registerFrame(this.dht, T.SUB,          this._onSub.bind(this),          { registry: this._frameDoor });
    registerFrame(this.dht, T.UNSUB,        this._onUnsub.bind(this),        { registry: this._frameDoor });
    registerFrame(this.dht, T.PUB,          this._onPub.bind(this),          { registry: this._frameDoor });
    registerFrame(this.dht, T.DELIVER,      this._onDeliver.bind(this),      { registry: this._frameDoor });
    registerFrame(this.dht, T.ADOPT,        this._onAdopt.bind(this),        { registry: this._frameDoor });
    registerFrame(this.dht, T.PULLUP,       this._onPullUp.bind(this),       { registry: this._frameDoor });
    registerFrame(this.dht, T.HANDOFFACK,   this._onHandoffAck.bind(this),   { registry: this._frameDoor });
    registerFrame(this.dht, T.REPLAYUP,     this._onReplayUp.bind(this),     { registry: this._frameDoor });
    registerFrame(this.dht, T.HANDOFF,      this._onHandoff.bind(this),      { registry: this._frameDoor });
    registerFrame(this.dht, T.REPLICATE,    this._onReplicate.bind(this),    { registry: this._frameDoor });
    registerFrame(this.dht, T.KILL,         this._onKill.bind(this),         { registry: this._frameDoor });
    registerFrame(this.dht, T.INGESTACK,    this._onIngestAck.bind(this),    { registry: this._frameDoor });
    registerFrame(this.dht, T.RECEIPTPROBE, this._onReceiptProbe.bind(this), { registry: this._frameDoor });
    registerFrame(this.dht, T.RECEIPTNACK,  this._onReceiptNack.bind(this),  { registry: this._frameDoor });
    registerFrame(this.dht, T.TOUCH,        this._onTouch.bind(this),        { registry: this._frameDoor });  // no-op (peer.touch deprecated v4.3.0); kept for wire compat
    registerFrame(this.dht, T.PULL,         this._onPull.bind(this),         { registry: this._frameDoor });
    registerFrame(this.dht, T.PULLRESP,     this._onPullResp.bind(this),     { registry: this._frameDoor });
    registerFrame(this.dht, T.ROOTBEACON,   this._onRootBeacon.bind(this),   { registry: this._frameDoor });
    registerFrame(this.dht, T.METRICSON,    this._onMetricsOn.bind(this),    { registry: this._frameDoor });
  },

  // REF-1.1 S2 inspector: the Boundary-1 registry's shadow state. `built` is
  // whether the construction flag armed the registry; `rows` the table size;
  // `traces` a copy of the bounded trace ring (empty flag-off). Read by
  // smoke_boundary1_registry.mjs; no effect on dispatch.
  frameRegistryShadow() {
    return {
      built: !!this._frameRegistry,
      rows: this._frameRegistry ? this._frameRegistry.size() : 0,
      traces: this._frameTraces ? this._frameTraces.slice() : [],
    };
  },

  // REF-1.1 M1a: the single ingestion path for Boundary-1 shadow traces (the
  // registry sink calls this). Updates the MONOTONIC lifetime counters FIRST —
  // they survive ring eviction — then pushes into the bounded 1024-entry ring,
  // counting any eviction as `dropped`. Keeping ingestion in one named method
  // (vs. an inline sink closure) lets the canary regression drive it directly.
  // No effect on dispatch.
  _ingestFrameTrace(rec) {
    const L = this._frameLifetime;
    if (L) {
      L.total++;
      const v = rec && typeof rec.verdict === 'string' ? rec.verdict : 'unknown';
      L.verdicts[v] = (L.verdicts[v] || 0) + 1;
      const ty = rec && typeof rec.type === 'string' ? rec.type : 'unknown';
      L.byType[ty] = (L.byType[ty] || 0) + 1;
      const f = rec && Array.isArray(rec.faults) ? rec.faults : [];
      if (f.length) {
        for (const k of f) L.faultKinds[k] = (L.faultKinds[k] || 0) + 1;
        // M1c coverage split (council Decision 1, Aster/Vega): 'unbranded-source' is
        // a COVERAGE miss (the frame took the shadow no-op — no contract observed),
        // NOT a contract fault. A trace whose ONLY fault code is 'unbranded-source'
        // increments `unobserved`; anything else (schema/projection/variant/
        // unregistered) increments `faults`. They are mutually exclusive by
        // construction — _emitUnbranded emits ['unbranded-source'] alone, a certified
        // observation never carries it. Splitting is SAFE only because the canary
        // predicate's coverage gate (unobserved===0) keeps an all-unbranded window
        // failing; see frameRegistryCanaryVerdict.
        if (f.every((k) => k === 'unbranded-source')) L.unobserved++;
        else L.faults++;
      }
    }
    const b = this._frameTraces;
    if (b) { if (b.length >= 1024) { b.shift(); if (L) L.dropped++; } b.push(rec); }
  },

  // REF-1.1 M1 canary telemetry: the shadow INVARIANT counters the telemetry-only
  // canary reports over live traffic. These are the MONOTONIC lifetime tallies
  // maintained at ingestion (_ingestFrameTrace) — they DO NOT reset on ring
  // rollover, so a fault that scrolled out of the 1024-entry ring is still counted
  // (council F1: the ring suffix alone can report a clean window over a dirty one).
  // The invariant holds iff `faults === 0` and no verdict is 'threw' or
  // 'trace-fault'. `dropped` = ring evictions; `ringSize` = recent-sample window.
  // `built:false` means NOT ARMED — the canary must read it as not-ready, NEVER a
  // clean pass. Nothing here reads or changes dispatch.
  frameRegistrySummary() {
    const L = this._frameLifetime;
    if (!this._frameRegistry || !L) {
      return { built: false, observing: false, rows: 0, total: 0, faults: 0,
        unobserved: 0, covered: 0, dropped: 0,
        faultKinds: {}, verdicts: {}, byType: {}, ringSize: 0 };
    }
    return {
      built: true,
      // observing = the runtime shadow gate is ON, so the wrap actually emits
      // traces. built without observing is a BLIND window (council F1): the canary
      // verdict requires observing===true, not merely built. See
      // frameRegistryCanaryVerdict.
      observing: shadowEnabled(),
      rows: this._frameRegistry.size(),
      total: L.total,       // lifetime — survives rollover
      faults: L.faults,     // lifetime CONTRACT violations (M1c: excludes unbranded) — MUST be 0
      unobserved: L.unobserved,          // M1c: frames that took the unbranded no-op (coverage miss) — MUST be 0
      covered: L.total - L.unobserved,   // M1c: frames that reached a certified snapshot; covered===total to pass
      dropped: L.dropped,   // ring evictions since arm (observability)
      faultKinds: { ...L.faultKinds },
      verdicts: { ...L.verdicts },
      byType: { ...L.byType },
      ringSize: this._frameTraces ? this._frameTraces.length : 0,
    };
  },

  // Decide what a topic-targeted message (SUB/PUB) should do at this node.
  //   'handle'  — this is the node that should act on it (the via waypoint, or,
  //               for a bare-topic message, the routing terminus = the root)
  //   'reroute' — a via waypoint is gone / consumed → pop it and route on
  //   'forward' — keep routing (return falsy so the kernel forwards)
  //
  // Root-ness is decided by ROUTING, not by "do I host it": the node that hosts
  // a topic but is no longer the closest must NOT intercept bare-topic traffic.
  _topicDecision(payload, meta) {
    const via = Array.isArray(payload.via) ? payload.via : [];
    if (via.length) {
      if (idBig(via[0]) === this.nodeId) return this.axonRoles.has(idBig(payload.topicId)) ? 'handle' : 'reroute';
      return meta.isTerminal ? 'reroute' : 'forward';      // waypoint dead; I'm just closest to it
    }
    // Bare topic id → only the terminus handles. REGION RULE: a topic may only be
    // rooted by a node IN ITS REGION. If I'm the routing terminus but out-of-region,
    // the topic's region has no node — REFUSE rather than root it here (which would
    // pull a foreign region's traffic into mine and hotspot my region). The handlers
    // treat 'reject' as "drop, don't seat/store/root."
    if (!meta.isTerminal) return 'forward';
    return this._regionOk(idBig(payload.topicId)) ? 'handle' : 'reject';
  },

  // ── SUBSCRIBE ────────────────────────────────────────────────────────
  _onSub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.SUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction (SUB). A stranded subscribe must not
    // (re)root a near-miss node while a strictly-closer live NEIGHBOUR root is
    // beaconing — defer the seat to that root. Without this only PUB carried the
    // correction, and every stranded/renewing SUB re-rooted the just-demoted
    // relay → the ~20s root flap between same-region relays (split trees, 0%
    // fresh-subscriber delivery on prod). A node that already relays the topic
    // (non-root role with a live upstream) still seats subscribers below.
    if (!this.axonRoles.has(topicBig)) {
      const closer = this._liveCloserRoot(topicBig);
      if (closer) { this._deferToRoot(topicBig, T.SUB, payload, closer); return 'consumed'; }
      // Alone-in-the-dark guard (v4.19.2). A freshly-joined node subscribes
      // before its mesh has formed: with zero non-bridge neighbours its SUB
      // never leaves the node, terminates at self, and (no beacons heard yet)
      // it minted itself "root" — observed live as EVERY joining subscriber
      // creating a transient root for the topic, splitting the tree until
      // reconciliation caught up (which under churn load it often didn't
      // inside the delivery window). A node that can't reach anyone has no
      // business electing itself: hold the seat; mySubscriptions is already
      // set, so the renewFastMs renewal re-runs the election once meshed.
      // Publish-side is deliberately NOT gated — a genuinely solo node still
      // roots on its own publish and serves its local subscriber.
      if (idBig(lc(payload.subscriberId)) === this.nodeId && this._rootClaim.meshBare()) return 'consumed';
    }
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'sub-terminal');
    if (!role) {
      // Admission refused (bridge fence) — do not seat here. This used to return
      // silently, which is the SAME missing concept as the PUB loop wearing the
      // opposite face: the subscriber never attaches and nobody is told. Forward
      // it to a node that can seat it, or record that there is no such node.
      if (!this._rerouteDeclined(T.SUB, payload)) this._undeliverable(T.SUB, topicBig, 'refused-no-forward');
      return 'consumed';
    }
    this._maybePromoteRoot(role, payload, meta);

    const subHex = lc(payload.subscriberId);
    if (!isHexId(subHex)) return 'consumed';
    const since = Number.isFinite(payload.since) ? payload.since : 0;

    // The root's own renewal self-loops here. Don't seat self as a subscriber
    // (no self-fan); just replay locally if the app subscribes.
    if (idBig(subHex) === this.nodeId) {
      if (this.mySubscriptions.has(topicBig)) this._replayLocal(role, since, !!payload.latest);
      return 'consumed';
    }
    // Durability (§6 stamped-replay-up): if this subscriber holds stamped
    // history I lack, ask it to replay its cache UP to me. Two detectable shapes:
    //  · NEWER (hw above mine) — I am a fresh root after the old one died, or a
    //    displaced root reattaching → pull the delta above my hw.
    //  · OLDER (lw below mine) — the post-transition split: a demoted ex-root /
    //    heir re-homing under me holds the PRE-transition half, which sits
    //    entirely below my hw and is invisible to the hw rule (the cold-attach
    //    "exactly half the timeline" replay class) → pull its FULL range
    //    (sinceHw:0; ingest dedups by msgId, tombstones suppress killed bodies).
    //    ONE-SHOT per (child, lw) — when the union succeeds my lw drops to the
    //    child's and the condition quenches naturally, but when the pull is
    //    REFUSED (the child's oldest is tombstoned here, or beyond my retention
    //    window) the condition would hold forever and re-fire a full-cache
    //    replay-up on EVERY renewal (the 4.22.0 testnet relay storm: flapping
    //    roots, zero-delivery runs, leave() drains pinned at timeout). Remember
    //    the lw already pulled per child; re-arm only if its lw DECREASES
    //    (a deeper split — genuinely new history).
    const myHw = this._highWater(role);
    if (Number.isFinite(payload.hw) && payload.hw > myHw) {
      this._syncPull(idBig(subHex), topicBig, 'REPLAY_UP', { sinceHw: myHw });
    } else if (Number.isFinite(payload.lw) && payload.lw > 0 && role.cache.length && payload.lw < this._lowWater(role)) {
      this._syncPull(idBig(subHex), topicBig, 'SPLIT_UNION', { sinceHw: 0, lw: payload.lw, role });
    }
    this._accept(role, subHex, since, !!payload.latest);
    return 'consumed';
  },

  _onUnsub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.UNSUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) { const s = lc(payload.subscriberId); role.subscribers.delete(s); role.children.delete(s); role.sync.pulledLw.delete(s); }
    return 'consumed';
  },

  // Seat a subscriber on a relay, delegating to a child when over capacity.
  // `latest` (since:'latest') folds the newest cache entry into the replay
  // DELIVER regardless of age — served from here (we hold the cache) even when
  // the subscriber is then delegated downward for ongoing fan-out.
  _accept(role, subHex, since, latest = false) {
    const existing = role.subscribers.get(subHex);
    if (existing) {                                   // renewal of a current subscriber
      existing.lastRenewed = this._now();
      this._replayTo(role, subHex, since, false, latest);
      return;
    }
    if (role.subscribers.size >= this.maxDirect) {    // overloaded → delegate
      // WIDEN before DEEPEN: promote a new sibling child (offloading a batch)
      // so the tree grows bushy (depth ~log_MAX_DIRECT(S)), not into a chain.
      // Only when every direct is already a child do we deepen — forward the
      // newcomer down to the child XOR-closest to it.
      if (!this._promoteChild(role)) {
        const c = this._pickChild(role, subHex);
        if (c) {
          if (latest) this._replayTo(role, subHex, since, false, true);  // current value from here, then hand off
          this._delegateTo(c, role, [{ subscriberId: subHex, since }]);
          return;
        }
        // neither possible (no leaf to promote, no child) → seat over capacity
      }
    }
    role.subscribers.set(subHex, { since: Number.isFinite(since) ? since : 0, lastRenewed: this._now() });
    this._replayTo(role, subHex, since, true, latest);  // delta (+ newest if latest) + a via-repin ping
  },

  // Choose the child relay XOR-closest to a subscriber (keyspace locality).
  _pickChild(role, subHex) {
    const target = idBig(subHex);
    let best = null, bestD = null;
    for (const c of role.children) {
      if (!role.subscribers.has(c)) { role.children.delete(c); continue; }  // stale
      const dd = idBig(c) ^ target;
      if (bestD === null || dd < bestD) { bestD = dd; best = c; }
    }
    return best;
  },

  // Promote one leaf subscriber to a child relay and hand it a batch of OTHER
  // leaves. Only succeeds if it can actually free a slot — promoting the sole
  // remaining leaf would just re-label it a child and free nothing, so we need
  // ≥2 leaves. Returning false tells _accept to deepen (delegate to a child)
  // instead of seating the newcomer over capacity.
  _promoteChild(role) {
    // REGION RULE: the tree's relay infrastructure must be IN-REGION. Only promote
    // an in-region leaf to a child relay; foreign leaves stay as direct leaves of
    // the root (they still receive, they just never relay for a region not theirs).
    const leaves = [];
    for (const s of role.subscribers.keys()) {
      if (role.children.has(s)) continue;
      if (!this._regionOk(idBig(s))) continue;   // out-of-region subscriber: never a relay child (when region lock on)
      leaves.push(s);
    }
    if (leaves.length < 2) return false;
    const leaf = leaves[0];
    role.children.add(leaf);
    const batch = [];
    for (let i = 1; i < leaves.length && batch.length < DELEGATE_BATCH; i++) {
      batch.push({ subscriberId: leaves[i], since: role.subscribers.get(leaves[i]).since });
    }
    for (const b of batch) role.subscribers.delete(b.subscriberId);
    this._delegateTo(leaf, role, batch);
    this._log('info', 'delegated', { child: leaf.slice(0, 12), moved: batch.length });
    return true;
  },

  _delegateTo(childHex, role, subs) {
    this._route(idBig(childHex), T.ADOPT, {
      topicId: idHex(role.topicId), parent: idHex(this.nodeId), subs,
    });
  },

  // A node is told to become a child relay and adopt a set of subscribers.
  _onAdopt(payload, meta) {
    if (meta.targetId !== this.nodeId) return;        // routed to me specifically
    const topicBig = idBig(payload.topicId);
    // REGION RULE: never become a child relay for a topic outside my region (the
    // tree infrastructure is region-homogeneous). A correct parent won't delegate
    // here, but refuse defensively so a stale/foreign ADOPT can't spill the tree.
    if (!this._regionOk(topicBig)) return 'consumed';
    const role = this._rootClaim.adoptChild(topicBig, lc(payload.parent));
    for (const s of (Array.isArray(payload.subs) ? payload.subs : [])) {
      const sh = lc(s.subscriberId);
      if (isHexId(sh) && idBig(sh) !== this.nodeId) this._accept(role, sh, s.since);
    }
    // Attach UP toward the parent so we receive the live feed + cache replay.
    this._sendSubscribe(topicBig);
    return 'consumed';
  },

  // ── PUBLISH ──────────────────────────────────────────────────────────
  async _onPub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.PUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction. At this point I'm the acting target for
    // the publish (bare-topic terminus, or via-pinned to me). If a fresh beacon
    // names a different root genuinely CLOSER to the topic than me, forward to it
    // and demote any spurious root I'd wrongly claimed at this near-miss node so
    // I stop intercepting. The strictly-closer test is a second verify-don't-trust
    // gate (never defer to a farther node). Fires regardless of the incoming via:
    // a node that wrongly became root also emits poisoning "root=me" beacons, so a
    // peer can arrive here via-pinned to me — the correction must still re-home it.
    {
      const closer = this._liveCloserRoot(topicBig, { requireReachable: false });
      // _forwardToRoot, NOT _deferToRoot (v4.59.0): the loose gate means
      // `closer` may be a guess, and the guess is tested by the forward itself
      // — the verdict demotes us only on confirmed consumption at that root,
      // and a failed forward invalidates the pointer instead of our state.
      if (closer) { this._forwardToRoot(topicBig, T.PUB, payload, closer); return 'consumed'; }
    }
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'pub-terminal');
    // Admission refused (bridge fence): a declined PUB must be FORWARDED, never
    // swallowed. Dropping it here is silent message loss.
    if (!role) {
      // Declined (bridge fence). Forward if there is a real next hop; if there
      // is not, STOP. The decline path used to call _reroute, which fell through
      // and wedged the process — see its doc comment.
      if (!this._rerouteDeclined(T.PUB, payload)) this._undeliverable(T.PUB, topicBig, 'refused-no-forward');
      return 'consumed';
    }
    this._maybePromoteRoot(role, payload, meta);
    // Only the root (the topic terminus) stamps. A non-root relay can only reach
    // here for a via-routed publish (a security waypoint) — pop the via and
    // continue toward the topic id. Bare-topic publishes always promote above.
    if (!role.isRoot) { this._reroute(T.PUB, payload); return 'consumed'; }

    const ingest = await this._ingestPublish(role, payload.json);
    if (ingest?.ok) this._sendIngestAck(meta?.fromId, role, ingest.msgId, 'pub', payload);
    return 'consumed';
  },

  // Root ingress: authenticate, enforce write policy, stamp, cache, fan out.
  async _ingestPublish(role, json) {
    let env;
    try { env = JSON.parse(json); } catch { this._log('warn', 'drop-unparseable'); return { ok: false, reason: 'unparseable' }; }

    const v = await verifyEnvelope(env);                                 // B-4 sig + msgId
    if (!v.ok) { this._log('warn', 'drop-bad-envelope', { reason: v.reason }); return { ok: false, reason: 'bad-envelope' }; }
    const fr = checkFreshness(env, { now: this._now() });                 // C-2 freshness (live ingress)
    if (!fr.ok) { this._log('warn', 'drop-stale', { reason: fr.reason }); return { ok: false, reason: 'stale' }; }

    const desc = env.topic;
    let tid;
    try { tid = await deriveTopicIdBig({ region: desc.region, owner: desc.owner, name: desc.name, write: desc.write }); }
    catch { this._log('warn', 'drop-bad-descriptor'); return { ok: false, reason: 'bad-descriptor' }; }
    if (tid !== role.topicId) { this._log('warn', 'drop-topic-mismatch'); return { ok: false, reason: 'topic-mismatch' }; }
    if (desc.write === 'owner' && (!env.signerPubkey || lc(env.signerPubkey) !== lc(desc.owner))) {
      this._log('warn', 'drop-write-policy', { topic: desc.name }); return { ok: false, reason: 'write-policy' };
    }

    // Both of these are TERMINAL for the write and ack as SUCCESS (v0.3 §1):
    // already-held is the idempotent-retry case, and a tombstoned republish is
    // settled state — an ack is what stops the writer's flight from probing a
    // root that is doing its job.
    if (role.cacheIds.has(env.msgId)) return { ok: true, msgId: env.msgId, dup: true };
    if (this._tombstoned(role, env.msgId, json)) return { ok: true, msgId: env.msgId, suppressed: true };

    // STAMP — single serialization point; strictly monotonic, floored at now.
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;
    const seq = ++role.seq;                                              // dense per-topic counter (gap detection)
    const msg = { json, publishTs: ts, msgId: env.msgId, seq };
    this._cachePush(role, { msgId: env.msgId, publishTs: ts, json, seq });
    // Phase 3 shadow: observe the VERIFIED body (env passed verifyEnvelope above)
    // ONLY if it survived the cache write (an immediate byte-cap eviction must not
    // leave a stale shadow body). The observer independently re-binds the topic.
    // No-op flag-off.
    if (this._tombAuthority && role.cacheIds.has(env.msgId)) await this._taObserveBody(role.topicId, env, ts);
    // The DURABILITY obligation opens at the stamp and can only be discharged
    // by a cohort verdict below. The DELIVERY leg is _pendingPub and moves
    // independently — that separation is the whole point (Aster, seq 123).
    this._durability.open(env.msgId, role.topicId);
    // rootReplicas = 0 means cohort replication is NOT configured, so the gate
    // below never runs and nothing could ever discharge this entry. Choose the
    // terminal state explicitly rather than leaving it pending forever.
    if (!this._rootReplicas) this._durability.noCohortConfigured(env.msgId);
    this._fanout(role, msg, null);                                       // to subscribers
    // local app (if subscribed)
    //
    // KNOWN DEFECT, NOT YET FIXED — found by test/fence_q2_end_to_end.mjs.
    // _deliverToApp CONFIRMS the pending entry (seeing your own message is the
    // implicit ack, I-9), and it runs HERE, before the durability gate below. So
    // a publisher subscribed to its own topic — the only way to verify a publish,
    // since there is deliberately no publish-ack — confirms regardless of whether
    // one byte reached the cohort. The v4.58.0 fail-closed gate is present,
    // correct, counted, and BYPASSED on the most common path.
    //
    // The obvious fix (pass {confirm:false} here and let the single post-gate
    // confirm below do it) was implemented and REVERTED: it turns every publish
    // on a NON-REPORTING adapter into a permanent pending entry, and the tick's
    // retry pump then re-sends the body forever — smoke_pubsub_kill caught it
    // re-delivering a KILLED message to a late subscriber. Withholding a confirm
    // is not free; it changes what the retry pump does. The real fix has to
    // reconcile the durability gate with the retry pump and is design work, not
    // a one-line change.
    this._deliverToApp(role.topicId, json, env.msgId, ts, seq);
    // EAGER cohort distribution: push the freshly-stamped message to the K-closest the
    // instant it's stamped — a kill is just a publish with a side effect, so a publish
    // must reach the whole cohort exactly as a kill must, else a subscriber landing on a
    // co-hosting root misses it (the post-churn loss). The periodic tick reconciles any miss.
    //
    // PUB_DURABLE gate (Phase 8, #353 — FLAGGED behavior change): a SELF-ROOTED
    // publisher used to confirm its own pending the instant it stamped locally,
    // so leave()'s evidence drain saw nothing pending and an ephemeral publisher
    // could exit before the eager replicate ever dispatched — its root claim
    // (and the message) died with the process (field-proven on prod: the same
    // post stranded twice from a die-fast publisher, landed instantly from a
    // held-alive one). The confirm now waits for the eager cohort replicate's
    // dispatch, so pub→leave() holds until the history has left the node.
    // Cohort-less nodes (rootReplicas 0 / solo network) confirm immediately.
    if (role.isRoot && this._rootReplicas) {
      const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
      // Q2/C4: the outcome is READ, not discarded. `.catch(() => {})` here used to
      // swallow the only evidence available and confirm regardless, so a publish
      // whose every replication push exhausted still reported durable.
      const rep = await this._replicateRole(role.topicId, role, bridge, this._now())
        // A REJECTION FABRICATES NOTHING (Aster, on v4.58.2). I had this catch
        // asserting dispatched:true, snapshot:true — inventing evidence flags for a
        // call that threw, in the middle of a change whose entire purpose is that
        // evidence must be demonstrated. An unexpected _replicateRole rejection shows
        // no dispatch and no snapshot, so it must not burn a ledger attempt. The warn
        // and the withheld confirm below still fire: the publish does NOT confirm, and
        // the periodic path retries. Nothing is silently swallowed; it is simply not
        // counted as an attempt that happened.
        .catch((e) => ({ attempted: 1, verified: 0, failed: 1, unsupported: 0, violation: 0,
                         dispatched: false, snapshot: false, noCohort: false,
                         reason: String(e?.message || e) }));
      // v4.58.0 FAIL-CLOSED. Confirm requires POSITIVE evidence: attempted > 0
      // demands verified > 0. The previous gate also required unreported === 0,
      // which meant a publish with no dispatch evidence at all still confirmed —
      // it fired only when every push had EXPLICITLY failed. Absence of a failure
      // report is not a success report. attempted === 0 is the singleton/no-cohort
      // case and still confirms, deliberately and unchanged.
      // THE ONLY PATH TO 'verified'. The ledger decides; this site just reports
      // what the cohort said. verified === 0 is not final — the tick replicates
      // again until the attempt budget runs out and the entry turns 'expired',
      // which is an honest terminal answer rather than a silent confirm.
      // ONE RULE, IN THE LEDGER (Aster, on v4.58.2). My previous version hand-rolled
      // the guard here as `rep.dispatched !== false && rep.snapshot !== false`, which
      // is not the contract: a MISSING flag passed it. That is inference by default —
      // written by me one commit after "capability is DECLARED, never inferred", which
      // is how deeply the habit runs. recordOne shares _classify with the periodic
      // path, so the two callers cannot drift and neither can restate the rule wrong.
      this._durability.recordOne(env.msgId, rep);
      if (rep.attempted > 0 && rep.verified === 0) {
        this._log('warn', 'pubsub:replicate-all-failed', {
          topic: idHex(role.topicId).slice(0, 12), attempted: rep.attempted, failed: rep.failed,
        });
        // INGEST happened (cached, stamped, fanned) — the ack below still
        // fires; durability is the cohort's job and the tick keeps retrying
        // the replication independently (v0.3: the ack asserts ingestion).
        return { ok: true, msgId: env.msgId, pendingDurability: true };
      }
      // Honesty signal (#362): the eager replicate could recruit NOBODY — this
      // node is a SINGLETON root and the confirm below asserts only "I, one
      // process, hold it" (field case: an in-region burst publisher self-rooted
      // ~50 topics whose cohort never existed because it had discovered no
      // other in-region node; the confirms read as durable, then the histories
      // died with its departure). Confirming anyway is deliberate — a solo/
      // sparse network must still publish — but the condition is surfaced: a
      // once-per-topic warn plus the inspectHosting().singletonRoots count, so
      // apps (and leave()) can see how much sole-copy history they carry.
      if ((role.replicas?.size ?? 0) === 0 && !role._warnedSingleton) {
        role._warnedSingleton = true;
        this._log('warn', 'pubsub:singleton-root-confirm',
          { topic: idHex(role.topicId).slice(0, 12), cache: role.cache.length });
      }
    }
    this._confirmPending(role.topicId, env.msgId);                       // our own publish landed (we're its root) → stop retrying
    return { ok: true, msgId: env.msgId };
  },

  // Emit the correlated INGEST-ack one hop back to the node that handed us the
  // write (v0.3 §1). Never to the origin publisher — there is deliberately no
  // publish-ack (location privacy); the E3 flight lives at the deferral sites.
  _sendIngestAck(fromId, role, msgId, op, hints) {
    // D1 (Write-Flight Ack Routing): if the forwarder stamped an `ackTo` (the
    // flight owner's transport id) AND this node holds a transport identity,
    // sign an INGEST-ACK PROOF and route it STRAIGHT to the owner across however
    // many hops — the deaf-flight fix. Otherwise fall back to the 4.62.1 unsigned
    // ack one hop back to the forwarder: correct on the 1-hop routes where it
    // ever worked, and the path a pre-4.62.2 forwarder (no ackTo) or an
    // identity-less node (sim/test double) still takes. Additive-optional; no
    // wire-version bump.
    if (hints && typeof hints.ackTo === 'string' && this._nodeIdentity) {
      this._sendSignedIngestAck(role, msgId, op, hints);   // async, self-contained, never throws out
      return;
    }
    this._sendUnsignedIngestAck(fromId, role, msgId, op);
  },

  _sendUnsignedIngestAck(fromId, role, msgId, op) {
    if (fromId == null) return;
    let fromBig; try { fromBig = idBig(fromId); } catch { return; }
    if (fromBig === this.nodeId) return;
    this._route(fromBig, T.INGESTACK, {
      topicId: idHex(role.topicId), msgId, epoch: role.epoch ?? 0, op,
    });
    this._log('debug', 'ingest-ack-sent', { topic: idHex(role.topicId).slice(0, 12), op });
  },

  async _sendSignedIngestAck(role, msgId, op, hints) {
    try {
      let ackToBig; try { ackToBig = idBig(hints.ackTo); } catch { return; }
      const frame = await signAckProof(
        (bytes) => this._nodeIdentity.sign(bytes),
        {
          purpose:     PURPOSE.INGEST_ACK,
          op:          op === 'kill' ? OP.kill : OP.pub,
          topicId:     idHex(role.topicId),
          msgId,
          epoch:       role.epoch ?? 0,
          attemptId:   hints.attemptId,
          ackTo:       hints.ackTo,
          flightNonce: hints.flightNonce,
          rootPub:     this._nodeIdentity.pubkey,
        },
      );
      this._route(ackToBig, T.INGESTACK, frame);
      this._log('debug', 'ingest-ack-signed', { topic: idHex(role.topicId).slice(0, 12), op, to: String(hints.ackTo).slice(0, 12) });
    } catch (e) {
      // Malformed hint (bad attemptId/nonce width) or a signer failure: log and
      // let the owner's flight recover via its receipt probe. Never a false
      // completion; never an unhandled rejection.
      this._log('warn', 'ingest-ack-sign-failed', { detail: String((e && (e.code || e.message)) || e) });
    }
  },

  // Receive an INGEST-ack: record the correlation for the write flight (E3).
  // Bounded store; entries are consumed by the flight or age out by insertion
  // order. Everything is validated — a malformed ack records nothing.
  async _onIngestAck(payload, meta) {
    // D1 (Write-Flight Ack Routing): a SIGNED ACK PROOF (carries `sig` + a
    // 64-hex `rootPub`) is the multi-hop-safe completion — verify the signature
    // and bind it to the open flight by attemptId + ackTo + flightNonce + the
    // hash-bound root authority (never the last hop). An UNSIGNED ack is the
    // 4.62.1 one-hop path, unchanged below.
    if (payload && typeof payload.sig === 'string') {
      const res = await verifyAckProof(payload);
      if (!res.ok) { this._log('info', 'ingest-ack-proof-reject', { reason: res.reason }); return 'consumed'; }
      await this._flightCompleteSigned(res.fields);
      return 'consumed';
    }
    if (!payload || typeof payload.topicId !== 'string' || typeof payload.msgId !== 'string') return 'consumed';
    const op = payload.op === 'kill' ? 'kill' : payload.op === 'pub' ? 'pub' : null;
    if (!op) return 'consumed';
    const epoch = (Number.isInteger(payload.epoch) && payload.epoch >= 0) ? payload.epoch : 0;
    if (!this._ingestAcks) this._ingestAcks = new Map();
    const key = `${lc(payload.topicId)}|${payload.msgId}|${op}`;
    this._ingestAcks.set(key, { epoch, at: this._now(), from: meta?.fromId != null ? String(meta.fromId) : null });
    while (this._ingestAcks.size > 512) {
      const oldest = this._ingestAcks.keys().next().value;
      this._ingestAcks.delete(oldest);
    }
    this._log('debug', 'ingest-ack', { key: key.slice(0, 24), epoch });
    // The ONLY terminal write success (E3) — and it must bind: sender +
    // epoch are enforced inside (Aster seq 439), so a stray holder's ack or
    // a stale incarnation never settles a flight against the suspect.
    this._flightComplete(payload.topicId, payload.msgId, op, meta?.fromId, epoch);
    return 'consumed';
  },

  // ── stamped-replay-up durability (§6) ────────────────────────────────
  // A behind parent asked us to replay our stamped history up to it — the
  // engine sends the cache delta (and active tombstones: a behind parent
  // adopting history must not resurrect a killed message).
  _onPullUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    this._syncAnswerPull(payload);
    return 'consumed';
  },

  // Stamped history arriving from below — adopt it WITHOUT re-stamping (the
  // timestamp rule, §5: a timestamp already present is kept), advance lastTs so
  // new publishes continue monotonically above it, and propagate it down.
  // Queued (task #332, I-11): the routing verdict ('consumed') is returned
  // immediately; the verify-heavy body drains through the time-sliced ingest
  // pump so a storm of pushes can't starve mesh liveness.
  async _onReplayUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    await this._ingestEnqueue(() => this._syncIngest(payload, meta, 'REPLAY_UP'));   // inline path completes here; queued path returns at once
    return 'consumed';
  },

  // Bulk-ingest with a macrotask yield (v4.24.1, #333/#332): verifying hundreds
  // of signatures in a microtask-chained await loop starves the macrotask queue —
  // heartbeats/pings go unanswered and the node's mesh peers evict it mid-ingest
  // (the join-storm collapse trigger: bulk role adoption → mass eviction →
  // state=stale). Yielding every 16 messages lets liveness traffic interleave
  // with history adoption; correctness is untouched (ingest is idempotent and
  // order-independent under msgId dedup).
  //
  // Returns { sent, held, rejected } (#402). `held` is what the receiver can
  // honestly claim to account for; `rejected` is what it dropped. Callers that
  // confirm receipt (HANDOFF → HANDOFFACK) MUST report these rather than the
  // bare fact that the loop finished.
  async _ingestStampedBatch(role, msgs) {
    let n = 0, held = 0, rejected = 0;
    const list = Array.isArray(msgs) ? msgs : [];
    for (const m of list) {
      if (m && typeof m.json === 'string' && Number.isFinite(m.publishTs)) {
        if (await this._ingestStamped(role, m) === 'held') held++; else rejected++;
      } else {
        // Shape guard. Previously skipped with NO log at all — the most silent
        // of the drop paths, and indistinguishable from success to the leaver.
        rejected++;
        this._log('warn', 'drop-unshaped-stamped', { have: m && typeof m === 'object' ? Object.keys(m).join(',') : typeof m });
      }
      if ((++n & 15) === 0) {
        await new Promise(r => (typeof setImmediate === 'function' ? setImmediate(r) : setTimeout(r, 0)));
      }
    }
    return { sent: list.length, held, rejected };
  },

  // Returns an OUTCOME, consumed by _ingestStampedBatch's tally:
  //   'held'     — the message is now accounted for in my state (newly cached,
  //                already cached, or deliberately suppressed by a tombstone).
  //   'rejected' — I do NOT hold it and never will: malformed, failed B-4
  //                verification, or a bad-clock stamp.
  // The distinction is load-bearing: a HANDOFFACK must only claim what is held
  // (#402). Silent returns here were invisible to the leaver, so a heir that
  // rejected half a batch acked identically to one that took all of it, and the
  // leaver then exempted the topic from retry AND cohort spray — dropping the
  // last copy of exactly the rejected messages.
  async _ingestStamped(role, m) {
    let env;
    try { env = JSON.parse(m.json); } catch { this._log('warn', 'drop-malformed-stamped', { msgId: String(m?.msgId).slice(0, 12) }); return 'rejected'; }
    const v = await verifyEnvelope(env);                                 // B-4 still applies
    if (!v.ok || env.msgId !== m.msgId) { this._log('warn', 'drop-bad-replayup', { reason: v.reason }); return 'rejected'; }
    // TOPIC BINDING (Aster Phase-3 blocker a, seq 890): the stamped body's SIGNED
    // descriptor must derive to THIS role's topic. _ingestPublish binds; the stamped
    // paths (replay-up / handoff / replicate) did not, so a body signed for topic A
    // could be re-stamped under role B — the V1 msgId is topic-agnostic, so B's
    // history and any co-location basis get corrupted. In honest operation a role's
    // topicId IS deriveTopicId(descriptor), so this rejects only the cross-topic case
    // (claim.topicId == role.topicId already: the role is looked up by the claim).
    // Fail closed on a malformed or mismatched descriptor.
    let stid;
    try { stid = await deriveTopicIdBig({ region: env.topic?.region, owner: env.topic?.owner, name: env.topic?.name, write: env.topic?.write }); }
    catch { this._log('warn', 'drop-stamped-bad-topic', { msgId: String(m?.msgId).slice(0, 12) }); return 'rejected'; }
    if (stid !== role.topicId) { this._log('warn', 'drop-stamped-cross-topic', { msgId: String(m?.msgId).slice(0, 12) }); return 'rejected'; }
    if (m.publishTs > this._now() + FUTURE_TOLERANCE_MS) { this._log('warn', 'drop-future-replayup'); return 'rejected'; } // §5 bad-clock
    if (role.cacheIds.has(m.msgId)) return 'held';                       // already have it
    if (this._tombstoned(role, m.msgId, m.json)) return 'held';          // killed → don't resurrect via replay-up
    this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json, seq: m.seq });
    // Phase 3 shadow: observe the VERIFIED body (env passed verifyEnvelope + the
    // topic-binding check above) if it survived the cache write. The observer
    // independently re-binds the topic too (defense in depth). No-op flag-off.
    if (this._tombAuthority && role.cacheIds.has(m.msgId)) await this._taObserveBody(role.topicId, env, m.publishTs);
    // Seeing our own msgId arrive via ANY stamped path (cohort replicate,
    // replay-up, handoff) is proof it landed on a durable holder — confirm the
    // pending so the retry machinery (and leave()'s evidence-based drain)
    // doesn't keep waiting on a publish that already made it. A true ack is
    // impossible by design (a PUB carries no return address — publisher
    // location privacy), so echoes are the only confirmation signal.
    this._confirmPending(role.topicId, m.msgId);
    if (m.publishTs > role.lastTs) role.lastTs = m.publishTs;            // continue stamping above recovered history
    if (Number.isFinite(m.seq) && m.seq > role.seq) role.seq = m.seq;   // recover dense counter → a new root continues it
    this._fanout(role, { json: m.json, publishTs: m.publishTs, msgId: m.msgId, seq: m.seq }, null);
    this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs, m.seq);
    return 'held';
  },

  // ── graceful-leave cache handoff ─────────────────────────────────────
  // A departing root pushes its cache to its heir (the next-closest live node)
  // BEFORE it leaves, so a topic's history survives the root's departure even
  // when no relay/host held a copy. The heir adopts the cache and becomes the
  // root, so subscribers that re-home to it (or join after) still replay the
  // pre-departure history via since:'all'. This is the common-case durability fix
  // for single-root topics — nodes that leave gracefully (peer.leave()) hand off;
  // abrupt death still needs a replica/in-region host (separate work).
  async _onHandoff(payload, meta) {
    // Diagnostic (#362): every HANDOFF arrival, including misrouted drops —
    // mine:false here means a handoff terminated at the wrong node (the
    // leave-order boomerang signature; see AxonaPeer.leave()).
    this._log('debug', 'handoff-recv', {
      topic: typeof payload?.topicId === 'string' ? payload.topicId.slice(0, 12) : '?',
      from: typeof payload?.from === 'string' ? payload.from.slice(0, 10) : '?',
      mine: meta.targetId === this.nodeId,
    });
    if (meta.targetId !== this.nodeId) return;
    // NOT queued: the HANDOFFACK the engine sends must mean "state actually
    // held" (#331), and departures are rare. Adoption rules (become root,
    // purge the leaver's ghost beacon, never defer back to the leaver) are
    // the HANDOFF policy hooks in _syncIngest.
    await this._syncIngest(payload, meta, 'HANDOFF');
    return 'consumed';
  },

  // Leaver side of the confirmed handoff: mark the topic acked so the
  // pubsubLeaveHandoff retry loop stops and skips the cohort-spray fallback.
  // A SHORT ack is not an ack (#402). The heir reports { held, sent }; if it
  // held fewer than we sent, the topic stays unacked so the retry rounds and the
  // Phase C cohort spray still run — those are the only things standing between
  // a rejected message and permanent loss of its last copy.
  // Compatibility: a pre-4.45.0 heir sends { topicId } only. Absent counters mean
  // "cannot tell", and we accept as before rather than breaking a mixed fleet.
  _onHandoffAck(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    if (typeof payload.topicId !== 'string') return 'consumed';
    const { held, sent } = payload;
    if (Number.isFinite(held) && Number.isFinite(sent) && held < sent) {
      this._log('warn', 'handoff-ack-short',
        { topic: payload.topicId.slice(0, 12), held, sent, missing: sent - held });
      return 'consumed';                    // deliberately NOT added to _handoffAcked
    }
    this._handoffAcked?.add(lc(payload.topicId));
    return 'consumed';
  },

  // Receive a replica push: become (or refresh) a BACKUP for this topic — hold the
  // full cache + tombstones (don't-trust: each message re-verified via stamped-ingest)
  // AND join _backupTopics so refreshTick subscribes us toward the topic like an
  // ordinary child relay. While the root lives our SUB routes to it and we sit as a
  // warm child; when it churns the normal probe-protected subscribe machinery elects
  // ONE new root (the closest self-roots, we re-home under it) — no bespoke local-only
  // promotion to split. The prefetched cache makes whichever backup wins take over
  // gap-free (it already holds the history a since:'all' joiner will ask for).
  // Queued (task #332, I-11): a joining relay receives its whole region's role
  // mass as a burst of REPLICATEs; processed inline they starve the event loop
  // (missed keepalives → mesh eviction → the join-storm collapse). The handler
  // returns the routing verdict immediately and the body drains through the
  // time-sliced pump. Role state is read at PROCESSING time, not arrival time.
  async _onReplicate(payload, meta) {
    if (meta.targetId !== this.nodeId) return;          // routed to me as the backup
    // Root-side: UNION_AT_ROOT (keep the claim, converge to the union). Else:
    // enter the BACKUP nature. Both are the REPLICATE policy hooks in
    // _syncIngest; the body drains through the time-sliced pump (I-11).
    await this._ingestEnqueue(() => this._syncIngest(payload, meta, 'REPLICATE'));   // inline path completes here; queued path returns at once
    return 'consumed';
  },

  // ── DELIVER (parent → subscriber; a relay re-fans down the tree) ──────
  async _onDeliver(payload, meta) {
    if (meta.targetId !== this.nodeId) return;        // forward (intermediate hop)
    const topicBig = idBig(payload.topicId);
    if (payload.from) {
      const fromHex = lc(payload.from);
      const prev = this._upstream.get(topicBig);
      // Re-pin to our relay. If the relay CHANGED (re-home after a churn), snap the
      // adaptive renewal interval back to fast — so we monitor the new attachment
      // closely and re-home quickly again if it too churns (the mobile common case).
      if ((!prev || prev[0] !== fromHex)) {
        const s = this.mySubscriptions.get(topicBig);
        if (s) s.interval = this.renewFastMs;
      }
      this._upstream.set(topicBig, [fromHex]);
    }

    const role = this.axonRoles.get(topicBig);        // set iff I'm a relay → re-fan
    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      if (!m) continue;
      if (m.del) {
        // A propagated proof is verified + BOUND (topicId+msgId+signer) BEFORE it can
        // be retained or re-fanned (Aster B1). A proof that fails is stripped, so
        // _applyKill installs the tombstone WITHOUT an unverified/cross-carrier proof.
        let mm = m;
        if (this._tombAuthority && m.kill && !(await this._taVerifyBoundKill(topicBig, m))) { const { kill, ...rest } = m; mm = rest; }
        this._applyKill(role, topicBig, mm);
        continue;
      }
      if (this._tombstoned(role, m.msgId, m.json)) continue;          // killed → suppress (don't cache/deliver/re-fan)
      if (role && !role.cacheIds.has(m.msgId)) {       // relay: cache once + re-fan once
        this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json, seq: m.seq });
        if (Number.isFinite(m.seq) && m.seq > role.seq) role.seq = m.seq;   // keep counter ready if we're promoted to root
        this._fanout(role, m, lc(payload.from));       // exclude the sender (m carries seq)
      }
      this._deliverToApp(topicBig, m.json, m.msgId, m.publishTs, m.seq);
    }
    return 'consumed';
  },

  // Fan a stamped message to every subscriber (optionally excluding the sender).
  _fanout(role, msg, excludeHex) {
    const base = { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [msg] };
    for (const subHex of role.subscribers.keys()) {
      if (excludeHex && subHex === excludeHex) continue;
      this._route(idBig(subHex), T.DELIVER, { ...base });
    }
  },

  // Replay the cache delta (publishTs > since) to one subscriber, chunked by
  // bytes. `ping` forces a (possibly empty) deliver so a freshly-seated
  // subscriber repins to us even when the cache has nothing newer.
  // `latest` (since:'latest') also includes the single NEWEST retained entry
  // regardless of age — folded into this same DELIVER, no extra message. The
  // ts-floor delta can't express "newest regardless of age": a current value
  // published before the subscriber's now-anchored floor sits below it and is
  // filtered out (the "subscribe latest → no callback" bug). Receiver dedups.
  _replayTo(role, subHex, sinceTs, ping, latest = false) {
    const subBig = idBig(subHex);
    const isSelf = subBig === this.nodeId;
    let batch = [], bytes = 0, sent = false;
    const inBatch = new Set();
    const flush = () => {
      if (!batch.length) return;
      sent = true;
      if (isSelf) for (const m of batch) this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs, m.seq);
      else this._route(subBig, T.DELIVER,
        { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: batch });
      batch = []; bytes = 0;
    };
    for (const c of role.cache) {
      if (c.publishTs <= sinceTs) continue;
      if (bytes + c.bytes > REPLAY_CHUNK_BYTES) flush();
      batch.push({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq });
      inBatch.add(c.msgId);
      bytes += c.bytes;
    }
    if (latest && role.cache.length) {               // ensure the current value rides along
      const newest = role.cache[role.cache.length - 1];
      if (!inBatch.has(newest.msgId)) {
        if (bytes + newest.bytes > REPLAY_CHUNK_BYTES) flush();
        batch.push({ json: newest.json, publishTs: newest.publishTs, msgId: newest.msgId, seq: newest.seq });
      }
    }
    flush();
    // Self-heal kills: re-send EVERY active (non-expired) tombstone on each
    // renewal — NOT gated on `since`. A since-delta can't express "you're missing
    // an OLD deletion": a node that missed the kill but kept receiving newer
    // messages has a `since` already PAST killTs, so a killTs>since gate would
    // never backfill it → it keeps the killed body forever and serves it to late
    // subscribers (the v4.8.7 permanent-leak edge found by the soak). Replaying all
    // live tombstones guarantees convergence; the receiver dedups idempotently
    // (tombstone-set gate on re-fan + exactly-once kill key on app delivery), so a
    // sub that already has them does no extra work. Bounded by the TTL'd tombstone
    // set. Carries killTs+signer so the receiver's tombstone matches.
    const dels = [];
    for (const [tgt, tomb] of role.tombstones) {
      if ((tomb?.exp ?? 0) > this._now()) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, publishTs: tomb.killTs, seq: tomb.seq, ...(this._tombAuthority && tomb.kill ? { kill: tomb.kill } : {}) });
    }
    if (dels.length) {
      sent = true;
      if (isSelf) for (const dm of dels) this._deliverKillToApp(role.topicId, dm.msgId, dm.killTs, dm.seq);
      else this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: dels });
    }
    if (ping && !sent && !isSelf) {                    // repin even with no history
      this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [] });
    }
  },

  _replayLocal(role, sinceTs, latest = false) {
    const seen = new Set();
    for (const c of role.cache) if (c.publishTs > sinceTs) { this._deliverToApp(role.topicId, c.json, c.msgId, c.publishTs, c.seq); seen.add(c.msgId); }
    if (latest && role.cache.length) {               // since:'latest' — newest regardless of age
      const newest = role.cache[role.cache.length - 1];
      if (!seen.has(newest.msgId)) this._deliverToApp(role.topicId, newest.json, newest.msgId, newest.publishTs, newest.seq);
    }
    // kills replay alongside the cache (exactly-once on the kill-specific key).
    for (const [tgt, tomb] of role.tombstones) if ((tomb?.exp ?? 0) > this._now()) this._deliverKillToApp(role.topicId, tgt, tomb.killTs, tomb.seq);
  },

  // Apply a (verified) kill: a kill is a publish-with-a-delete-side-effect. We
  // record a TOMBSTONE keyed by the target msgId — carrying the kill's stamp
  // (killTs) and its authorizing signer — drop the target from cache, and fan the
  // delete down ONCE. The tombstone is now first-class replayable state (see the
  // tombstone emit in _replayTo/_replayLocal): a subscriber that missed the live
  // delete re-acquires it on its next renewal, exactly like a missed publish
  // re-acquires from cache. Idempotent: the fan-out + cache-drop happen only the
  // first time we see the kill (tombstone-gated).
  _applyKill(role, topicBig, m) {
    const target = m.msgId;
    // A retracted message has no durability obligation left. Cancel it BEFORE
    // the tombstone/fan-out work below, so no retry can outlive the retraction
    // (Aster: the kill must cancel atomically and preserve the tombstone).
    this._durability?.cancel(target);
    const killTs = m.killTs ?? this._now();
    const seq = m.seq;                                 // root-assigned dense counter for this kill
    if (role && Number.isFinite(seq) && seq > role.seq) role.seq = seq;   // recover counter (kill occupied a slot)
    // SIGNED-KILL PROOF (Aster Phase-3 blocker b): retain + transport a proof ONLY when
    // it is BOUND to this carrier — the kill's target IS this tombstone's target and it
    // binds to this topic. The SIGNATURE was already verified by the caller (_onKill's
    // verifyKill for a direct kill; _taVerifyBoundKill on every propagation funnel, which
    // also checks the signer and STRIPS an unverified proof before it reaches here). This
    // is the synchronous binding gate; a mismatched proof is never retained or re-fanned.
    let proof = null;
    if (this._tombAuthority && m.kill && typeof m.kill.msgId === 'string' && m.kill.msgId === target) {
      try { if (m.kill.topicId != null && idBig(m.kill.topicId) === topicBig) proof = m.kill; } catch { proof = null; }
    }
    // When a proof is retained, the proof's VERIFIED signer is authoritative and stamps
    // the tombstone + every proof-bearing marker we emit, so a receiver's _taVerifyBoundKill
    // (which now requires a present matching carrier signer) always accepts it — never an
    // internally inconsistent carrier (Aster S1/S2). Flag-off / proof-less keeps m.signer.
    const proofSigner = proof ? lc(proof.signerPubkey) : null;
    const existing = role ? role.tombstones.get(target) : null;
    if (role && existing && proof && !existing.kill) {
      // B2 UPGRADE — a proof-less tombstone gains a verified, bound proof. Attach it and
      // converge it downstream (re-fan now + re-replicate; the replay / replicate /
      // handoff / pull emitters read tomb.kill). The destructive/idempotent side effects
      // (cache drop, app kill delivery, pending confirm) already ran on first sighting,
      // so this is a PURE proof-attach and must not repeat them.
      existing.kill = proof;
      existing.signer = proofSigner;   // the proof's verified signer becomes authoritative (Aster S2) — no stale/conflicting carrier
      this._fanout(role, { del: true, msgId: target, killTs: existing.killTs, signer: existing.signer, publishTs: existing.killTs, seq: existing.seq, kill: proof }, null);
      if (role.isRoot && this._rootReplicas) {
        const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
        this._replicateRole(topicBig, role, bridge, this._now()).catch(() => {});
      }
      return;
    }
    if (role && !existing) {
      const tomb = { exp: this._now() + TTL_MS, killTs, signer: proof ? proofSigner : (m.signer ?? null), seq };
      if (proof) tomb.kill = proof;                    // retain only the bound, caller-verified proof
      role.tombstones.set(target, tomb);
      const i = role.cache.findIndex(c => c.msgId === target);
      if (i >= 0) { role.cacheBytes -= role.cache[i].bytes; role.cache.splice(i, 1); }
      role.cacheIds.delete(target);
      // fan the delete down — carries killTs + signer + seq so each receiver records
      // an identical tombstone (consistent replay + ordering + provisional authorship);
      // flag-on it also carries the BOUND signed kill + its verified signer so receivers verify it.
      this._fanout(role, { del: true, msgId: target, killTs, signer: tomb.signer, publishTs: killTs, seq, ...(proof ? { kill: proof } : {}) }, null);
      // Replicas/cohort aren't subscribers/children — they don't see the fan-out. Push the
      // new tombstone to the cohort EAGERLY (not on the next tick) so a co-hosting root or a
      // backup that promotes mid-window can't serve the killed body it already holds (the
      // kill-leak race). Same eager path a publish takes — a kill is just a publish + side effect.
      // PUB_DURABLE applies to kills too (#353): a self-rooted kill confirms only after the
      // eager replicate dispatch, so a kill→leave() publisher holds until the tombstone left.
      if (role.isRoot && this._rootReplicas) {
        const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
        // Q2/C4: same gate as the publish path — a kill is a publish plus a side
        // effect, so a tombstone whose every replication push failed must not
        // report durable either.
        this._replicateRole(topicBig, role, bridge, this._now())
          .catch((e) => ({ attempted: 1, verified: 0, failed: 1, unsupported: 0, violation: 0, reason: String(e?.message || e) }))
          .then((rep) => {
            if (rep.attempted > 0 && rep.verified === 0) {
              this._log('warn', 'pubsub:kill-replicate-all-failed', {
                topic: idHex(topicBig).slice(0, 12), attempted: rep.attempted, failed: rep.failed,
              });
              return;                 // leave pending → the killer keeps retrying
            }
            this._confirmPending(topicBig, target);
          });
        this._deliverKillToApp(topicBig, target, killTs, seq);
        return;
      }
    }
    this._confirmPending(topicBig, target);            // our own kill landed → stop retrying it
    this._deliverKillToApp(topicBig, target, killTs, seq);
  },

  // ── side-function handlers (thin; TODO Phase 4) ──────────────────────
  async _onKill(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.KILL, payload); return 'consumed'; }
    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction (KILL) — same one-shot semantics as PUB:
    // a kill landing on a near-miss node must reach the true root, not mint a
    // competing root that the rest of the tree never consults.
    if (!this.axonRoles.has(topicBig)) {
      const closer = this._liveCloserRoot(topicBig, { requireReachable: false });
      // _forwardToRoot, NOT _deferToRoot (v4.59.0) — same verdict-driven
      // transition as PUB. A tombstone fed to a corpse is the kill-leak class
      // all over again, with the extra sting that nothing ever retries a kill
      // the app believes it already sent.
      if (closer) { this._forwardToRoot(topicBig, T.KILL, payload, closer); return 'consumed'; }
    }
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'kill-terminal');
    if (!role) {
      // A tombstone is the one thing we least want to lose, but forwarding it
      // to ourselves loses it AND wedges the node. Forward or stop, loudly.
      if (!this._rerouteDeclined(T.KILL, payload)) this._undeliverable(T.KILL, topicBig, 'refused-no-forward');
      return 'consumed';
    }
    const kill = payload.kill;
    const target = kill?.msgId;
    if (!target) return 'consumed';
    // AUTHZ: a kill must be signed, and only the AUTHOR of the target may kill it.
    // (1) signature self-valid (B-4 analog for kills).
    const v = await verifyKill(kill);
    if (!v.ok) { this._log('warn', 'drop-bad-kill', { reason: v.reason }); return 'consumed'; }
    // Phase 3 shadow: the ONLY kill-observe site — the signed kill is locally
    // verifyKill()-verified here, and _taObserveKill re-binds its topicId to this
    // topic before the shadow authority may act on it. No-op flag-off.
    if (this._tombAuthority) this._taObserveKill(topicBig, kill, v.signerPubkey);
    // (2) authorship: if we hold the target, enforce signer===author NOW; if we
    // don't (kill races ahead of the publish, or we're a fresh root), accept
    // PROVISIONALLY — record the kill's signer and enforce when the target arrives
    // (ingest checks the tombstone's signer; a forged kill is revoked on mismatch).
    const cached = role.cache.find(c => c.msgId === target);
    if (cached) {
      let author = null; try { author = JSON.parse(cached.json)?.signerPubkey ?? null; } catch { /* */ }
      if (!author || lc(author) !== lc(kill.signerPubkey)) {
        this._log('warn', 'drop-unauthorized-kill', { target: String(target).slice(0, 12) });
        return 'consumed';
      }
    }
    // STAMP the kill like a publish (monotonic + dense counter) so it orders +
    // replays via `since` AND occupies a seq slot (a missed kill shows as a gap).
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;
    const seq = ++role.seq;
    // Pass the COMPLETE signed kill so _applyKill can retain + transport the proof
    // (blocker b). The root re-stamps ts/seq for replay ordering; the signed kill
    // keeps the author's own ts/seq for verifyKill() at every downstream node.
    this._applyKill(role, topicBig, { msgId: target, killTs: ts, signer: lc(kill.signerPubkey), seq, kill });
    // KILL completes on tombstone ingest, separately from PUB (v0.3 §1).
    // _applyKill is tombstone-gated internally; re-acking a duplicate kill is
    // the idempotent-success case, same as a duplicate publish.
    this._sendIngestAck(meta?.fromId, role, target, 'kill', payload);
    return 'consumed';
  },

  // _onUnpub — REMOVED v4.3.0 (keep kill, drop unpub)
  _onTouch(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.TOUCH, payload); return 'consumed'; }
    return 'consumed';
  },

  _onPull(payload, meta) {
    const role = this.axonRoles.get(idBig(payload.topicId));
    // Cache-hit early-answer (v4.11.1 by-msgId; v4.11.2 pull-latest too): a pull is a
    // read of replicated state — answer from the FIRST replica the routed PULL reaches
    // (a cohort member / child / host) instead of always driving it to the single root.
    //   • by-msgId    → exact: msgId = H(publisher‖message), so a nearer copy IS the copy.
    //   • pull-latest → served with whatever NEWEST this replica holds. Deliberately
    //     "recent, not necessarily THE newest": a hot read path (many pull-latest, e.g.
    //     polling current state) is spread across the cohort/children rather than
    //     hammering the root, which would otherwise be a read throughput bottleneck +
    //     SPOF. The small staleness window closes as the cohort converges (anti-entropy);
    //     a caller that needs the linearizable newest should pull a specific msgId.
    // A cached message is by definition NOT tombstoned here (a kill drops it), so neither
    // path can resurrect a killed message. A replica with an EMPTY cache does not
    // early-answer (nothing on hand) — it forwards so a populated node / the root answers.
    if (role && role.cache.length) {
      const hit = payload.postHash ? role.cache.find(c => c.msgId === payload.postHash)
                                   : role.cache[role.cache.length - 1];
      if (hit) { this._answerPull(payload, hit); return 'consumed'; }
    }
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.PULL, payload); return 'consumed'; }
    // Terminus (root/closest) fall-through: answer if we hold it, else a genuine null.
    const hit = role ? (payload.postHash ? role.cache.find(c => c.msgId === payload.postHash) : role.cache[role.cache.length - 1]) : null;
    this._answerPull(payload, hit || null);
    return 'consumed';
  },

  _answerPull(payload, hit) {
    const resp = { corrId: payload.corrId, json: hit ? hit.json : null, publishTs: hit ? hit.publishTs : null, requesterId: payload.requesterId };
    if (idBig(payload.requesterId) === this.nodeId) this._onPullResp(resp, { targetId: this.nodeId });
    else this._route(idBig(payload.requesterId), T.PULLRESP, resp);
  },

  _onPullResp(payload, meta) {
    if (meta.targetId !== this.nodeId && idBig(payload.requesterId) !== this.nodeId) return;
    const p = this._pending.get(payload.corrId);
    if (!p) return 'consumed';
    // REQUESTER GATE (Aster, council d17ece0b). A read is matched by the WHOLE
    // pair (corrId, requesterId), not corrId alone. The destination OR-guard
    // above admits any response routed to THIS node's targetId — so a certified
    // PULLRESP routed here carrying our corrId but a FOREIGN requesterId would
    // otherwise resolve, and delete, another party's pending read. The requester
    // we recorded when issuing the PULL is this node; a response whose
    // requesterId does not fold to it is not ours — ignore it (leave the pending
    // intact for the real answer / its timeout). A missing/malformed requesterId
    // (idBig throws) is likewise not a match; no conforming responder omits it.
    let respReq;
    try { respReq = idBig(payload.requesterId); } catch { return 'consumed'; }
    if (respReq !== p.requesterId) return 'consumed';
    clearTimeout(p.timer);
    this._pending.delete(payload.corrId);
    // Q1 TAGGED OUTCOME. These three cases were all collapsed to null:
    //   a reply carrying an envelope   -> { kind:'response', envelope }
    //   a reply carrying nothing       -> { kind:'response', envelope:null }
    //   a reply that will not parse    -> { kind:'invalid-response', reason }
    // The middle one is a RESPONDER's negative — this node says it holds nothing.
    // That is not proof the network holds nothing, it is not a timeout, and a caller
    // must be able to tell all three apart. (Aster, council 2026-07-31.)
    //
    // WHAT COUNTS AS A NO-HIT (Aster's Q1 review, and he was right — my first cut
    // was wrong). The ONE responder is `_onPull` at :779, and it ALWAYS sets the
    // field: `json: hit ? hit.json : null`. So a genuine responder negative is
    // `json === null`, exactly. An OMITTED json is not a polite way of saying
    // "nothing" — no conforming responder emits it — and neither is the STRING
    // 'null', which would parse to null and impersonate a no-hit. Accepting either
    // as an empty response would re-introduce the exact confusion Q1 exists to
    // remove, one layer further in: a malformed or foreign message read as an
    // authoritative "I do not have it".
    let outcome;
    if (payload.json === null) {
      outcome = { kind: 'response', envelope: null };            // the real no-hit
    } else if (typeof payload.json !== 'string') {
      outcome = { kind: 'invalid-response', reason: payload.json === undefined
        ? 'PULLRESP omitted json (a conforming responder always sets it)'
        : `PULLRESP json was ${typeof payload.json}, expected string or null` };
    } else {
      let parsed, bad = null;
      try { parsed = JSON.parse(payload.json); }
      catch (e) { bad = String((e && e.message) || e); }
      if (bad !== null) outcome = { kind: 'invalid-response', reason: bad };
      else if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
        // Parsed fine and is still not an envelope. 'null' lands here rather than
        // masquerading as a no-hit; so do bare scalars and arrays.
        outcome = { kind: 'invalid-response', reason: 'PULLRESP json did not parse to an envelope object' };
      } else outcome = { kind: 'response', envelope: parsed };
    }
    // Resolve the FULL envelope (msgId/ts/signer/message …) — the same shape a
    // sub() callback delivers, and what peer.pull has always documented. The
    // previous `parsed.message ?? parsed` unwrap discarded the identity at the
    // last step (task #355): publish-confirm loops comparing env.msgId could
    // never succeed, and pull-then-act (kill/reply/verify by msgId) was
    // impossible even though the wire carried everything.
    p.resolve(outcome);
    return 'consumed';
  },

  // METRICSON routes toward the topic like a SUB. At the root (terminal / via
  // waypoint holding the role) it arms a renewable publish lease. On the path it
  // caches the flag so an inheriting root picks it up on promotion and a second
  // requester's METRICSON short-circuits (fan-in dedup), while still re-forwarding
  // periodically so the root's lease stays alive.
  _onMetricsOn(payload, meta) {
    const topicBig = idBig(payload.topicId);
    const now = this._now();
    const d = this._topicDecision(payload, meta);
    if (d === 'reroute') { this._reroute(T.METRICSON, payload); return 'consumed'; }
    if (d === 'reject') return 'consumed';   // out-of-region terminus: no in-region root to arm a metrics lease on
    if (d === 'handle') {
      // Root-beacon last-mile correction (METRICSON): a metrics lease must arm
      // the TRUE root, not mint a competing one at a near-miss node.
      if (!this.axonRoles.has(topicBig)) {
        const closer = this._liveCloserRoot(topicBig);
        if (closer) { this._deferToRoot(topicBig, T.METRICSON, payload, closer); return 'consumed'; }
      }
      const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'metricson-terminal');
      if (!role) return 'consumed';   // refused: metrics are cosmetic, drop quietly
      this._maybePromoteRoot(role, payload, meta);
      role.metricsOn = now + METRICS_LEASE_MS;                 // arm/renew the publish lease
      // Answer the demand NOW (v4.16.1): the first snapshot rides back at routing
      // latency instead of waiting for the next 5 s refresh tick, so a subscriber
      // that just turned metrics on hears a count roughly when its data-topic
      // replay lands — the difference between "0 alerts" and "cache is near its
      // rollover cap" must not wait on a poll cycle. The METRICS_PUB_MS throttle
      // inside the helper keeps renewals from turning this into a per-METRICSON
      // publish storm; the net cadence stays ~20 s.
      this._publishMetricSnapshot(topicBig, role, now);
      return 'consumed';
    }
    // forward: remember the flag (short-circuit + inheritance) and coalesce upstream.
    const hadFresh = (this._metricsWanted.get(topicBig) || 0) > now;
    this._metricsWanted.set(topicBig, now + METRICS_LEASE_MS);
    const lastFwd = this._metricsFwdAt.get(topicBig) || 0;
    if (hadFresh && now - lastFwd < METRICS_COALESCE_MS) return 'consumed';  // root already informed recently → drop
    this._metricsFwdAt.set(topicBig, now);
    return;   // falsy → kernel forwards toward the root
  },

};

export default wireHandlersMethods;
