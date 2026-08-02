// durability.js — the DURABILITY state machine, deliberately separate from the
// DELIVERY one.
//
// TWO MACHINES, NOT TWO BOOLEANS. Aster's specification, council 2026-08-01:
//
//   Local delivery : pending → delivered | cancelled
//     I-9 self-delivery satisfies ONLY this. Once delivered it stops payload
//     redelivery, and a KILL must CANCEL it before a retry can carry the body to
//     a late subscriber. That machine is `_pendingPub` in AxonaManager; the
//     retry pump reads it, and the kill already cancels it.
//
//   Durability     : pending → verified | expired | cancelled
//     Only a cohort CONSUMED verdict reaches `verified`. leave() drains on THIS.
//
// WHY A SEPARATE FILE. The defect this replaces was one flag carrying two facts:
// _deliverToApp called _confirmPending, so a publisher subscribed to its own
// topic — the only way to verify a publish, since there is deliberately no
// publish-ack — discharged DURABILITY by observing DELIVERY. The v4.58.0
// fail-closed gate was present, correct, counted, and bypassed. Two fields on
// one object would have re-merged within a month; a module with its own
// vocabulary is harder to conflate by accident. Nothing here knows what an app
// delivery is, and there is deliberately no function that a delivery path could
// call to reach `verified`.
//
// FAIL-CLOSED, AND BOUNDED. Silence never advances a message. Only an explicit
// cohort verdict does. `expired` is a real terminal state — a message that ran
// out of attempts is UNDURABLE and says so, rather than silently confirming
// (v4.57.0's mistake) or retrying forever (the mistake my first fix made, which
// re-delivered a killed body). The ledger is capped and prunes terminal entries
// oldest-first, because a diagnostic that outgrows what it describes stops being
// a diagnostic — the same lesson as role.attempted.

export const DURABILITY_MAX      = 4096;   // ledger cap; terminal entries evict first
export const DURABILITY_ATTEMPTS = 6;      // replication attempts before `expired`

export class DurabilityLedger {
  constructor({ now = () => Date.now(), max = DURABILITY_MAX,
                maxAttempts = DURABILITY_ATTEMPTS } = {}) {
    this._m = new Map();                 // msgId -> {topicBig, state, attempts, at}
    this._now = now;
    this._max = max;
    this._maxAttempts = maxAttempts;
  }

  // A root stamped a message. The durability obligation opens here and can only
  // be discharged by evidence — never by the message being seen locally.
  open(msgId, topicBig) {
    if (!msgId || this._m.has(msgId)) return;
    this._m.set(msgId, { topicBig, state: 'pending', attempts: 0, at: this._now() });
    this._prune();
  }

  // The ONLY path to `verified`. Called with the cohort's dispatch verdict count.
  // verified === 0 is not a failure of the message, only of this attempt: the
  // tick will replicate again until the attempt budget runs out.
  record(msgId, { verified = 0, attempted = 0 } = {}) {
    const e = this._m.get(msgId);
    if (!e || e.state !== 'pending') return e?.state ?? null;   // terminal states are final
    if (verified > 0) { e.state = 'verified'; e.at = this._now(); return e.state; }
    e.attempts++;
    // attempted === 0 means there was no cohort to try — a singleton root. That
    // is terminal and it is NOT durable: the node holds the only copy. Saying
    // `expired` here is the honest answer and lets leave() stop waiting for an
    // acknowledgement that can never arrive.
    if (attempted === 0 || e.attempts >= this._maxAttempts) {
      e.state = 'expired'; e.at = this._now();
    }
    return e.state;
  }

  // TOPIC-LEVEL transition, driven by the PERIODIC replication. This is the leg
  // that was missing: record() was called once at ingress and nowhere else, so
  // an entry that started verified:0 stayed pending forever, `expired` was
  // unreachable, and the attempt budget below was decoration. Aster caught that
  // the module documented a lifecycle it did not run (council 2026-08-01).
  //
  // TOPIC-LEVEL IS THE RIGHT GRANULARITY *ONLY FOR A FULL SNAPSHOT*. My v4.58.1
  // justification said "_syncPush sends the role's whole snapshot, so one
  // verified cohort push covers every message this root holds" — true of a full
  // push, and I wrote it unconditionally. It is false for the keepalive, and
  // Aster found both ends of that:
  //
  //   · dispatched:false — a deferral (`deferred-no-budget`) sends NOTHING and
  //     reports attempted:0, which record() reads as "no cohort exists" and
  //     retires to EXPIRED. A message whose snapshot never reached the wire was
  //     being declared permanently undurable because the tick was busy.
  //   · snapshot:false — an empty keepalive still resolves consumed and still
  //     counts verified>0, so it marked every pending message on the topic
  //     durable while carrying none of them. Reachable in ordinary operation:
  //     a new message whose eager full push fails, on a topic with a previously
  //     credited replica, meets a keepalive on the very next tick.
  //
  // So this is FAIL-CLOSED ON THE EVIDENCE FLAGS, and deliberately at the ledger
  // rather than the caller: _replicateRoots is not the only thing that could
  // ever call this, and a rule enforced at one call site is a rule that lasts
  // until the second call site. Anything that is not a dispatched full snapshot
  // is not evidence about a message, in EITHER direction — it must not verify,
  // and it must not burn an attempt. Silence still never advances anything.
  // THE ONE CLASSIFIER. Every replication outcome is exactly one of four kinds,
  // and both call sites — the periodic sweep and the eager ingress — go through
  // it. Aster's v4.58.2 finding was that my first attempt had THREE kinds and
  // needed four: I folded "no cohort exists" into the same no-op as "the tick
  // was busy", so a singleton root's message stayed pending forever instead of
  // reaching the honest undurable terminal it had before.
  //
  //   'no-cohort'   nobody to send to. TERMINAL and undurable — no future tick
  //                 can produce evidence, and this node holds the only copy.
  //   'no-dispatch' nothing was sent THIS TIME (budget deferral, or a caller
  //                 that could not run). Retryable. No state change, no attempt.
  //   'no-snapshot' an empty keepalive went out. Proves a peer is reachable,
  //                 proves nothing about a body it did not carry. Same no-op.
  //   'evidence'    a dispatched FULL snapshot. The only kind that may move a
  //                 message in either direction.
  //
  // STRICT === true, deliberately (Aster again): a missing flag is not a true
  // one. Defaulting the other way is inference, which is the exact habit this
  // module exists to break — and I had written `!== false` at the eager site one
  // commit after writing "capability is DECLARED, never inferred".
  _classify(rep = {}) {
    if (rep.noCohort === true) return 'no-cohort';
    if (rep.dispatched !== true) return 'no-dispatch';
    if (rep.snapshot !== true) return 'no-snapshot';
    return 'evidence';
  }

  // PER-MESSAGE, for the eager ingress replicate. Shares _classify with the
  // topic-level path so the two callers cannot drift — the whole point of
  // putting the rule here rather than at either call site.
  recordOne(msgId, rep = {}) {
    const kind = this._classify(rep);
    if (kind === 'no-cohort')  { this.noCohortAvailable(msgId); return this.state(msgId); }
    if (kind !== 'evidence')   return this.state(msgId);            // retryable, no change
    return this.record(msgId, { verified: rep.verified ?? 0, attempted: rep.attempted ?? 0 });
  }

  recordTopic(topicBig, rep = {}) {
    const kind = this._classify(rep);
    const out = { verified: 0, expired: 0, pending: 0, kind };
    if (kind === 'no-dispatch' || kind === 'no-snapshot') {
      // Counting the pending entries is still useful to the caller; changing
      // them is not.
      for (const e of this._m.values()) {
        if (e.state === 'pending' && e.topicBig === topicBig) out.pending++;
      }
      return out;
    }
    for (const [msgId, e] of this._m) {
      if (e.state !== 'pending' || e.topicBig !== topicBig) continue;
      if (kind === 'no-cohort') { this.noCohortAvailable(msgId); out.expired++; continue; }
      const st = this.record(msgId, { verified: rep.verified ?? 0, attempted: rep.attempted ?? 0 });
      if (st === 'verified') out.verified++;
      else if (st === 'expired') out.expired++;
      else out.pending++;
    }
    return out;
  }

  // NO COHORT IS CONFIGURED (rootReplicas = 0). An explicitly CHOSEN terminal
  // state, not one that falls out of the code: this node will never attempt
  // cohort replication, so the message can never become durable-by-cohort and
  // there is nothing to wait for. 'expired' is the honest label — finished, and
  // the answer is no — and it puts the message in durabilityUndurable(), which
  // is exactly the count an operator needs: history this node alone carries.
  // Leaving it 'pending' (the behaviour Aster found) makes leave() wait out its
  // stall clock for a verdict that cannot arrive, then clear the entry, which
  // reads as success and is not.
  noCohortConfigured(msgId) { this._expire(msgId, 'no-replication-configured'); }

  // NO COHORT WAS FOUND (rootReplicas > 0, but the search returned nobody). A
  // different fact from the one above — replication is configured, there is
  // simply no one in range — and the same honest terminal: this node holds the
  // only copy. Distinguished in the reason so an operator can tell "I never
  // asked for replicas" from "I asked and the network had none", which are very
  // different things to see in a report.
  noCohortAvailable(msgId) { this._expire(msgId, 'no-cohort-available'); }

  _expire(msgId, reason) {
    const e = this._m.get(msgId);
    if (!e || e.state !== 'pending') return;
    e.state = 'expired'; e.reason = reason; e.at = this._now();
  }

  // A kill retracts the message. Cancel the outstanding durability obligation —
  // chasing durability for a body that has been retracted is work that can only
  // do harm. The TOMBSTONE is not this module's business and is untouched.
  cancel(msgId) {
    const e = this._m.get(msgId);
    if (!e || e.state !== 'pending') return;
    e.state = 'cancelled'; e.at = this._now();
  }

  state(msgId)   { return this._m.get(msgId)?.state ?? null; }
  get(msgId)     { return this._m.get(msgId) ?? null; }
  get size()     { return this._m.size; }

  // What leave() must drain: obligations still outstanding. Terminal states —
  // verified, expired, cancelled — are all DONE, including the unhappy ones.
  // Waiting on an expired entry would be waiting for a verdict that will never
  // come, which is the stall the leave() drain already exists to avoid.
  pending() {
    let n = 0;
    for (const e of this._m.values()) if (e.state === 'pending') n++;
    return n;
  }

  // Everything stamped here that never became durable. Distinct from pending:
  // these are FINISHED and the answer was no. This is what an operator needs to
  // see, and what `singletonRoots` only ever approximated.
  undurable() {
    let n = 0;
    for (const e of this._m.values()) if (e.state === 'expired') n++;
    return n;
  }

  clear() { this._m.clear(); }

  // Cap the ledger, evicting TERMINAL entries oldest-first. A pending entry is
  // live obligation and is never evicted to make room — if the ledger is full of
  // pending work, the answer is that the node is in trouble, not that the record
  // should be thinned until it looks calm.
  _prune() {
    if (this._m.size <= this._max) return;
    const terminal = [];
    for (const [k, e] of this._m) if (e.state !== 'pending') terminal.push([k, e.at]);
    terminal.sort((a, b) => a[1] - b[1]);
    for (const [k] of terminal) {
      if (this._m.size <= this._max) break;
      this._m.delete(k);
    }
  }
}

export default DurabilityLedger;
