// =============================================================================
// harness/lib/ledger.mjs — the three-truths ledger (append-only JSONL).
//
// Every operation preserves three separately-recorded truths, and no single
// one serves as the oracle (v0.3 §3, Aster block 1):
//   intent      — what the publisher MEANT to do, recorded before the API call
//   api         — what the kernel SAID happened
//   observation — what a subscriber ACTUALLY saw, one record per observer
//
// Records carry both wall-clock (for humans) and a monotonic elapsed time
// (for latency math — wall-clock cannot be trusted across four hosts; the
// calibration run measures per-host offset and the analyzer joins on it).
// Transport nodeIds are TRUNCATED to 12 hex chars per the standing rule;
// author ids may persist in full.
// =============================================================================
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';

export const sha256 = (s) => createHash('sha256').update(s).digest('hex');
export const truncNode = (id) => String(id ?? '').slice(0, 12);

export class Ledger {
  /** @param {string} path JSONL file @param {object} id {host, os, peerIdx, author} */
  constructor(path, id) {
    this.path = path;
    this.id = { host: id.host, os: id.os, peerIdx: id.peerIdx, author: id.author };
    mkdirSync(dirname(path), { recursive: true });
    this._t0 = process.hrtime.bigint();
  }

  _mono() { return Number((process.hrtime.bigint() - this._t0) / 1000n) / 1000; } // ms, µs precision

  _emit(rec) {
    appendFileSync(this.path, JSON.stringify({
      ...rec, host: this.id.host, os: this.id.os, peerIdx: this.id.peerIdx,
      wall: new Date().toISOString(), mono: this._mono(),
    }) + '\n');
  }

  /** Truth 1 — written BEFORE the publish API call. `descriptor` records the
   *  exact { region, name, owner, write } passed to pub, so a silent
   *  mis-addressing (wrong owner => different topicId, confirmed api — the #393
   *  class) is caught before it reads as a strand. */
  intent({ topic, topicSeq, nonce, payloadHash, descriptor }) {
    this._emit({ t: 'intent', topic, topicSeq, nonce, payloadHash,
      descriptor: descriptor ?? null, author: this.id.author });
  }

  /** Truth 2 — the kernel's answer, written on API completion (or throw). */
  api({ topic, topicSeq, nonce, confirmed, msgId, error }) {
    this._emit({ t: 'api', topic, topicSeq, nonce,
      confirmed: confirmed ?? null, msgId: msgId ?? null, error: error ?? null });
  }

  /** Truth 3 — one per observer per message, however late it arrives.
   *  via: 'watch' | 'pull' | 'replay'. LATE observations are still emitted —
   *  the hours-long-propagation detector reclassifies, it never drops. */
  observe({ topic, topicSeq, nonce, msgId, via, payloadHash }) {
    this._emit({ t: 'observe', topic, topicSeq, nonce: nonce ?? null,
      msgId: msgId ?? null, via, payloadHash: payloadHash ?? null });
  }

  /** Pull-head sample (stale-read detector input): the newest seq this
   *  reader can see for the topic right now. */
  pullHead({ topic, headSeq, headMsgId }) {
    this._emit({ t: 'pullHead', topic, headSeq: headSeq ?? null, headMsgId: headMsgId ?? null });
  }

  /** Watch liveness sample (wedged-watch detector input). `buffered` is the
   *  kernel watch-buffer depth if the peer exposes it, else null — NEVER a
   *  fake 0. `silentMs` is elapsed since the last watch arrival: the real
   *  wedged-watch signal on a peer whose sub is a direct callback. */
  watchState({ topic, buffered, total, lastArrivalMono, silentMs }) {
    this._emit({ t: 'watchState', topic, buffered: buffered ?? null, total,
      lastArrivalMono: lastArrivalMono ?? null, silentMs: silentMs ?? null });
  }

  /** Participant connection set + per-topic role snapshot (the null this run
   *  left, resolved). mesh = { synaptomeSize, peers, state }; roles is the raw
   *  axonRoles array [{ topic, isRoot }] keyed on topicId — count is usable now,
   *  a name join waits on a kernel topicId export. */
  connSnapshot({ mesh, roles }) {
    this._emit({ t: 'conn', mesh: mesh ?? null,
      roles: Array.isArray(roles) ? roles.length : null,
      rootCount: Array.isArray(roles) ? roles.filter((r) => r?.isRoot).length : null });
  }

  /** Cross-reader head sample (splitHead detector input): the newest seq +
   *  msgId this reader sees for the topic, taken in a synchronized full sweep
   *  so two readers' heads for the same topic are comparable. */
  head({ topic, descriptor, headSeq, headMsgId }) {
    this._emit({ t: 'head', topic, descriptor: descriptor ?? null,
      headSeq: headSeq ?? null, headMsgId: headMsgId ?? null });
  }

  /** Clock-offset sample against a reference (calibration + analyzer join). */
  clockSample({ refHost, offsetMs, rttMs }) {
    this._emit({ t: 'clock', refHost, offsetMs, rttMs });
  }

  /** Resource sample (leak-slope gate input). */
  resources({ rssMb, channels, timers }) {
    this._emit({ t: 'resources', rssMb, channels: channels ?? null, timers: timers ?? null });
  }

  /** Scripted-churn or scenario marker — joins detector windows to stimuli. */
  event({ kind, detail }) {
    this._emit({ t: 'event', kind, detail: detail ?? null });
  }
}
