// ids.js — shared id helpers for the pub/sub modules (refactor Phase 2).
//
// Address invariant: bigint internally, hex only on the JSON wire. idHex is the
// egress (bigint → wire hex); idBig is the ingress gate, delegating to the
// kernel's canonical asId() so every wire→internal id conversion is validated
// the same way.
import { asId } from '../utils/hexid.js';

export const idHex = (big) => big.toString(16).padStart(66, '0');
export const idBig = asId;
export const lc    = (s) => String(s ?? '').toLowerCase();
export const isHexId = (s) => /^[0-9a-f]{1,66}$/.test(s);

// 4.156.0: key of a publisher's pending publish/kill retry entry. A msgId is
// sha256(publisher, message) and carries NO topic (envelope.js), so one body
// sent to several topics (an alert to every containing cell) has ONE msgId on
// every topic. Keyed by msgId alone, the later publish overwrote the earlier
// entries and each topic's early re-send carried the last topic's envelope.
// The entry is per (topic, msgId).
export const pendingKey = (topic, msgId) =>
  `${typeof topic === 'bigint' ? idHex(topic) : lc(topic)}:${msgId}`;
