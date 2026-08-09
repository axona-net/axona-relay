// writeFlight.js — the write recovery flight (Dead-Root Eviction v0.3, E3).
//
// A forwarded write (PUB/KILL) COMPLETES only on its correlated INGEST-ack
// (E2). Routing verdicts are hop-local evidence: consumed-elsewhere and
// consumed-at-the-named-root are both NON-terminal for a write — the captured
// prod flapper (GH #28) satisfied consumed-and-attributed for two hours while
// ingesting nothing. No ack by the deadline → the named root incarnation is
// suspect → ONE serialized flight per (topic, root, epoch):
//
//   probe    demand the correlated receipt for {topicId, msgId, op} — an
//            INGESTACK (holds it) or a RECEIPTNACK (explicit inability).
//            Reachability is NOT exculpatory (Aster seq 427): a live answer
//            that binds nothing proves nothing.
//   nack     the honest I-never-got-it answer earns ONE direct retry of the
//            write. A root that nacks a retry it was just handed is no longer
//            innocent.
//   silence / second miss → EVICT: tombstone the incarnation (epoch-stamped,
//            consulted by liveCloserRoot and _knownEpoch), then RETRY-PROMOTE:
//            send the write via-pinned to the closest LIVE candidate — arrival
//            at a terminal runs the existing become('pub-terminal') + ingest +
//            ack machinery, which IS the promotion (no new election protocol;
//            the same closest-live rule as everywhere).
//   exhausted  zero live candidates → typed terminal (undeliverable +
//            write-flight-exhausted). Never another false ok.
//
// Flights are swept from refreshTick (the kernel's one scheduler) — no
// per-flight timers, nothing to leak; every exit path deletes the flight.
import { T, BEACON_TTL_MS } from './constants.js';
import { idHex, idBig } from './ids.js';
import { rootPubMatchesNodeHash } from './ackProof.js';

const lc = (s) => String(s).toLowerCase();

// 16 random bytes as lowercase hex — a flightNonce (per flight generation) or an
// attemptId (per entry). crypto.getRandomValues is available in every runtime
// the kernel targets (browser, node, sim).
function rand16hex() {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  let s = '';
  for (let i = 0; i < 16; i++) s += b[i].toString(16).padStart(2, '0');
  return s;
}

// Deadlines. One monotonic budget per stage, spent from flight-open (the
// mesh-gate discipline): ack must land inside INGEST_ACK_MS of the forward;
// each probe round gets FLIGHT_PROBE_MS. Tombstones outlive the beacon TTL so
// a corpse's stale pointer cannot outlive its own conviction.
export const INGEST_ACK_MS = 4_000;
export const FLIGHT_PROBE_MS = 3_000;
export const FLIGHT_MAX_ROUNDS = 2;
export const ROOT_TOMBSTONE_TTL_MS = Math.max(600_000, BEACON_TTL_MS * 12);

const corrKey = (topicHex, msgId, op) => `${lc(topicHex)}|${msgId}|${op}`;
const flightKey = (topicBig, rootHex, epoch) => `${topicBig.toString(16)}|${lc(rootHex)}|${epoch}`;

function writeMsgId(type, payload) {
  if (type === T.KILL) return payload?.kill?.msgId ?? null;
  try { return JSON.parse(payload.json)?.msgId ?? null; } catch { return null; }
}

export const writeFlightMethods = {

  // ── open / complete ──────────────────────────────────────────────────
  // Called by _forwardToRoot for every WRITE it dispatches. Serialization
  // (Aster item 3): one flight per (topic, suspect incarnation); concurrent
  // stranded writers JOIN the standing flight — they never launch parallel
  // probes.
  // Returns the ack-routing tuple {ackTo, flightNonce, attemptId} the caller
  // stamps on the forwarded write (D1) — or null on a malformed write. `ackTo`
  // (this node's transport id) + `flightNonce` (one per flight generation) are
  // per-flight; `attemptId` is per-entry (Aster R10), inheritable across a
  // promotion so the chain stays one attempt (the `attemptId` arg — D2 threads
  // the API-boundary value here; passing null mints one).
  _flightOpen(topicBig, rootHex, type, payload, attemptId = null) {
    const msgId = writeMsgId(type, payload);
    if (!msgId) return null;                              // malformed write — verification refuses it downstream
    if (!this._writeFlights) this._writeFlights = new Map();
    const rec = this._rootBeacons.get(topicBig);
    const epoch = (rec && Number.isInteger(rec.epoch)) ? rec.epoch : 0;
    const key = flightKey(topicBig, rootHex, epoch);
    let f = this._writeFlights.get(key);
    if (!f) {
      f = { topicBig, rootHex: lc(rootHex), epoch, entries: new Map(),
            stage: 'await-ack', rounds: 0, deadline: this._now() + INGEST_ACK_MS,
            ackTo: lc(idHex(this.nodeId)), flightNonce: rand16hex() };
      this._writeFlights.set(key, f);
    }
    const op = type === T.KILL ? 'kill' : 'pub';
    const ck = corrKey(idHex(topicBig), msgId, op);
    let entry = f.entries.get(ck);
    if (!entry) {
      entry = { type, payload, msgId, op, attemptId: (attemptId ? lc(attemptId) : rand16hex()) };
      f.entries.set(ck, entry);
    }
    return { ackTo: f.ackTo, flightNonce: f.flightNonce, attemptId: entry.attemptId };
  },

  // Called from _onIngestAck: a correlated receipt terminates the entry, and
  // an empty flight closes. The ack is the ONLY terminal success (v0.3 §1) —
  // and it must BIND (Aster seq 439): the sender must be the flight's suspect
  // root and the epoch must match the intended incarnation. A valid ack from
  // a DIFFERENT holder, or the wrong epoch, proves nothing about THIS seat
  // and leaves the flight open to run its receipt-bound recovery. An
  // UNVERSIONED flight (epoch 0 — opened without a beacon record, the
  // post-eviction promotion case) carries no incarnation expectation and
  // accepts the right node's first ack at any epoch. An honestly re-minted
  // root whose current epoch outruns a strict flight self-corrects in one
  // extra round: its old incarnation is tombstoned epoch-bounded, its live
  // one is untouched, and the promotion re-flight binds to it cleanly.
  _flightComplete(topicHex, msgId, op, fromId, ackEpoch) {
    if (!this._writeFlights) return;
    const ck = corrKey(topicHex, msgId, op);
    let fromHex = null; try { fromHex = lc(idHex(idBig(fromId))); } catch { /* unattributable ack binds nothing */ }
    for (const [key, f] of this._writeFlights) {
      if (!f.entries.has(ck)) continue;
      if (fromHex !== f.rootHex) {
        this._log('info', 'write-flight-ack-unbound', { key: key.slice(0, 20), from: String(fromHex).slice(0, 12), want: f.rootHex.slice(0, 12) });
        continue;
      }
      if (f.epoch !== 0 && ackEpoch !== f.epoch) {
        this._log('info', 'write-flight-ack-epoch-mismatch', { key: key.slice(0, 20), got: ackEpoch, want: f.epoch });
        continue;
      }
      f.entries.delete(ck);
      if (f.entries.size === 0) this._writeFlights.delete(key);
      this._log('debug', 'write-flight-complete', { key: key.slice(0, 20), via: 'ingest-ack' });
    }
  },

  // D1 (Write-Flight Ack Routing): complete an entry on a VERIFIED signed ACK
  // PROOF that reached the flight owner across any number of hops — the deaf-
  // flight fix. `pf` is verifyAckProof(frame).fields (signature already checked
  // against pf.rootPub by the caller). Binding: the entry's attemptId, the
  // flight's ackTo + flightNonce, the incarnation epoch (unversioned epoch-0
  // flights accept any), and the AUTHORITY — pf.rootPub must hash-bind to the
  // flight's suspect rootHex (SHA-256(pub) hash slot; the region prefix is not
  // key-derivable, so only the hash portion binds — same property as D1 R1).
  // A proof failing any check leaves the flight open to run its receipt-bound
  // recovery; it never falsely completes.
  async _flightCompleteSigned(pf) {
    if (!this._writeFlights || !pf) return;
    const ck = corrKey(pf.topicId, pf.msgId, pf.op);
    for (const [key, f] of this._writeFlights) {
      const entry = f.entries.get(ck);
      if (!entry) continue;
      if (entry.attemptId !== lc(pf.attemptId)) { this._log('info', 'write-flight-ack-unbound-attempt', { key: key.slice(0, 20) }); continue; }
      if (f.ackTo !== lc(pf.ackTo))             { this._log('info', 'write-flight-ack-unbound-ackto',   { key: key.slice(0, 20) }); continue; }
      if (f.flightNonce !== lc(pf.flightNonce)) { this._log('info', 'write-flight-ack-unbound-nonce',   { key: key.slice(0, 20) }); continue; }
      if (f.epoch !== 0 && pf.epoch !== f.epoch) { this._log('info', 'write-flight-ack-epoch-mismatch', { key: key.slice(0, 20), got: pf.epoch, want: f.epoch }); continue; }
      let rootBig; try { rootBig = idBig(f.rootHex); } catch { continue; }
      let authOk = false;
      try { authOk = await rootPubMatchesNodeHash(pf.rootPub, rootBig); } catch { authOk = false; }
      if (!authOk) { this._log('info', 'write-flight-ack-unbound-authority', { key: key.slice(0, 20), want: f.rootHex.slice(0, 12) }); continue; }
      f.entries.delete(ck);
      if (f.entries.size === 0) this._writeFlights.delete(key);
      this._log('debug', 'write-flight-complete', { key: key.slice(0, 20), via: 'signed-proof' });
    }
  },

  // ── the sweep (driven by refreshTick; tests drive it directly) ───────
  _flightSweep() {
    if (!this._writeFlights || this._writeFlights.size === 0) return;
    const now = this._now();
    for (const [key, f] of [...this._writeFlights]) {
      if (now < f.deadline) continue;
      if (f.stage === 'await-ack' || f.stage === 'retrying') {
        if (f.rounds >= FLIGHT_MAX_ROUNDS) { this._flightEvict(key, f); continue; }
        f.stage = 'probing';
        f.rounds += 1;
        f.deadline = now + FLIGHT_PROBE_MS;
        const first = f.entries.values().next().value;
        if (!first) { this._writeFlights.delete(key); continue; }
        let rootBig; try { rootBig = idBig(f.rootHex); } catch { this._writeFlights.delete(key); continue; }
        this._route(rootBig, T.RECEIPTPROBE, {
          topicId: idHex(f.topicBig), msgId: first.msgId, op: first.op,
        });
        this._log('info', 'receipt-probe', {
          topic: idHex(f.topicBig).slice(0, 12), root: f.rootHex.slice(0, 12), round: f.rounds,
        });
      } else if (f.stage === 'probing') {
        this._flightEvict(key, f);                       // probe window ran out — silence convicts
      }
    }
  },

  // ── probe answers ────────────────────────────────────────────────────
  // ROOT side: prove the correlated receipt or state the inability. An
  // already-tombstoned target is held state too — the kill settled it.
  _onReceiptProbe(payload, meta) {
    if (!payload || typeof payload.topicId !== 'string' || typeof payload.msgId !== 'string') return 'consumed';
    let tBig; try { tBig = idBig(payload.topicId); } catch { return 'consumed'; }
    const role = this.axonRoles.get(tBig);
    const op = payload.op === 'kill' ? 'kill' : 'pub';
    const holds = !!role && (role.cacheIds.has(payload.msgId) || role.tombstones?.has(payload.msgId));
    let fromBig; try { fromBig = idBig(meta?.fromId); } catch { return 'consumed'; }
    if (holds) {
      this._route(fromBig, T.INGESTACK, {
        topicId: payload.topicId, msgId: payload.msgId, epoch: role.epoch ?? 0, op,
      });
    } else {
      this._route(fromBig, T.RECEIPTNACK, {
        topicId: payload.topicId, msgId: payload.msgId, op, reason: 'not-held',
      });
    }
    return 'consumed';
  },

  // FLIGHT side: an explicit not-held earns ONE direct retry — the write may
  // simply never have arrived. The retry re-arms the ack deadline; a root
  // that stays receipt-less after being handed the write directly is evicted
  // on the next sweep (rounds cap).
  _onReceiptNack(payload) {
    if (!this._writeFlights || !payload || typeof payload.topicId !== 'string') return 'consumed';
    let tBig; try { tBig = idBig(payload.topicId); } catch { return 'consumed'; }
    for (const [, f] of this._writeFlights) {
      if (f.topicBig !== tBig || f.stage !== 'probing') continue;
      f.stage = 'retrying';
      f.deadline = this._now() + INGEST_ACK_MS;
      // Re-stamp the ack routing (D1): the retry must carry the same ackTo +
      // flightNonce + per-entry attemptId so the root's signed proof still binds
      // to THIS open flight.
      for (const e of f.entries.values()) this._send(e.type, { ...e.payload, via: [f.rootHex], ackTo: f.ackTo, flightNonce: f.flightNonce, attemptId: e.attemptId });
      this._log('info', 'receipt-nack-retry', {
        topic: idHex(f.topicBig).slice(0, 12), root: f.rootHex.slice(0, 12),
      });
    }
    return 'consumed';
  },

  // ── eviction + retry-promotion ───────────────────────────────────────
  _flightEvict(key, f) {
    this._writeFlights.delete(key);
    if (!this._rootTombstones) this._rootTombstones = new Map();
    const prev = this._rootTombstones.get(f.topicBig);
    if (!prev || f.epoch >= prev.epoch) {
      this._rootTombstones.set(f.topicBig, { root: f.rootHex, epoch: f.epoch, exp: this._now() + ROOT_TOMBSTONE_TTL_MS });
    }
    const rec = this._rootBeacons.get(f.topicBig);
    if (rec && lc(rec.root) === f.rootHex && (!Number.isInteger(rec.epoch) || rec.epoch <= f.epoch)) {
      this._rootBeacons.delete(f.topicBig);
    }
    this._log('warn', 'root-evicted', {
      topic: idHex(f.topicBig).slice(0, 12), root: f.rootHex.slice(0, 12), epoch: f.epoch,
      detail: 'no correlated receipt within the flight budget — incarnation tombstoned',
    });
    // Retry-promote: the closest LIVE candidate gets the write via-pinned;
    // its own terminal machinery roots, ingests, and acks. Same closest-live
    // rule as everywhere; the tombstone keeps the corpse out of the pool.
    const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
    let best = null, bestD = null;
    const tBig = f.topicBig;
    if (typeof this.dht.neighbors === 'function') {
      for (const n of (this.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        if (nb === this.nodeId) continue;
        if (bridge != null && nb === bridge) continue;
        if (lc(idHex(nb)) === f.rootHex) continue;        // the tombstoned incarnation's node
        const d = nb ^ tBig;
        if (bestD === null || d < bestD) { bestD = d; best = nb; }
      }
    }
    // Self is a candidate too: if this node is closest live, ingest locally.
    const selfD = this.nodeId ^ tBig;
    if (bestD === null || selfD < bestD) { bestD = selfD; best = this.nodeId; }
    if (best == null) {
      for (const e of f.entries.values()) this._undeliverable(e.type, f.topicBig, 'no-live-holder');
      this._log('error', 'write-flight-exhausted', {
        topic: idHex(f.topicBig).slice(0, 12), writes: f.entries.size,
      });
      return;
    }
    if (best === this.nodeId) {
      // Closest live holder is US — ingest through the normal terminal path.
      for (const e of f.entries.values()) {
        const handler = e.type === T.KILL ? this._onKill : this._onPub;
        Promise.resolve(handler.call(this, e.payload, { fromId: idHex(this.nodeId), isTerminal: true })).catch(() => {});
      }
      this._log('info', 'write-flight-promoted', { topic: idHex(f.topicBig).slice(0, 12), to: 'self' });
      return;
    }
    const bestHex = lc(idHex(best));
    // The promoted write is a fresh forward — open a flight against the NEW
    // candidate FIRST so we can stamp its ack routing (D1) on the re-send, then
    // forward each entry via-pinned. The attemptId is INHERITED from the old
    // entry so the chain stays one attempt across the promotion (D2's budget
    // reads it); the new flight generation gets its own flightNonce. A second
    // flapper thus gets the same receipt-bound treatment, bounded by its own
    // rounds cap.
    for (const e of f.entries.values()) {
      const m = this._flightOpen(f.topicBig, bestHex, e.type, e.payload, e.attemptId);
      if (!m) { this._send(e.type, { ...e.payload, via: [bestHex] }); continue; }  // malformed — forward unstamped, unsigned recovery
      this._send(e.type, { ...e.payload, via: [bestHex], ackTo: m.ackTo, flightNonce: m.flightNonce, attemptId: m.attemptId });
    }
    this._log('info', 'write-flight-promoted', {
      topic: idHex(f.topicBig).slice(0, 12), to: bestHex.slice(0, 12),
    });
  },

  // Tombstone check for the closer-root gate: a beacon naming a tombstoned
  // incarnation (epoch ≤ the conviction) is the corpse's ghost — skip it.
  // A HIGHER epoch from the same node is a legitimate rebirth (E4 adopt).
  _rootTombstoned(topicBig, rootHex, epoch) {
    if (!this._rootTombstones) return false;
    const t = this._rootTombstones.get(topicBig);
    if (!t) return false;
    if (this._now() >= t.exp) { this._rootTombstones.delete(topicBig); return false; }
    return lc(rootHex) === t.root && (Number.isInteger(epoch) ? epoch : 0) <= t.epoch;
  },
};
