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

  /** Truth 1 — written BEFORE the publish API call. */
  intent({ topic, topicSeq, nonce, payloadHash }) {
    this._emit({ t: 'intent', topic, topicSeq, nonce, payloadHash, author: this.id.author });
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

  /** Watch liveness sample (wedged-watch detector input). */
  watchState({ topic, buffered, total, lastArrivalMono }) {
    this._emit({ t: 'watchState', topic, buffered, total, lastArrivalMono: lastArrivalMono ?? null });
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
