// handoffBinding.js — Closer-Born Relay Inherits the Root v0.6 Part 7,
// "binding handoff to the loser's identity" (David 2026-11-03 "go ahead
// with the handoff identity binding"). Manager option handoffBinding / env
// HANDOFF_BINDING, DEFAULT OFF.
//
// The capability gate (0db60c0) put `handoff: 1` on the MERGE_OFFER, an
// unsigned routed frame: a hop that adds it to an OLD loser's offer makes a
// new candidate stage from a loser that cannot run the staged handover. Here
// the loser signs the claim with its node identity, and an armed candidate
// stages only when the signature verifies AND the signing key hashes into
// the loser's node id (rootPubMatchesNodeHash, the same binding the D1 ack
// proofs use: the region byte is placement, the hash bits are the key).
//
// Transcript (fixed layout, big-endian):
//   DOMAIN "AXONA_HANDOFF_CAP_V1" | u8 corrLen | corr (ASCII) |
//   topicId (33 bytes) | loser (33) | winner (33) | u32 loserEpoch
// The corr is fresh per round and the winner and topic are bound, so a
// signature cannot be moved to another round, candidate or topic.
// Offer fields: handoffSig (128 hex), handoffPub (64 hex). A loser with no
// node identity sends neither; an armed candidate then refuses not-root.

import { verify } from './ed25519.js';
import { hexToBytes, bytesToHex, rootPubMatchesNodeHash } from './ackProof.js';
import { idBig, idHex, lc, isHexId } from './ids.js';

const enc = new TextEncoder();
const DOMAIN = enc.encode('AXONA_HANDOFF_CAP_V1');

export function handoffTranscript({ topicId, corr, loser, winner, loserEpoch }) {
  const c = enc.encode(String(corr));
  if (c.length < 1 || c.length > 255) throw new Error('handoff: corr length');
  const ids = [hexToBytes(lc(String(topicId)), 33), hexToBytes(lc(String(loser)), 33), hexToBytes(lc(String(winner)), 33)];
  if (ids.some((b) => b === null)) throw new Error('handoff: id width');
  const parts = [DOMAIN, Uint8Array.of(c.length), c, ...ids];
  const ep = new Uint8Array(4); new DataView(ep.buffer).setUint32(0, (loserEpoch >>> 0), false);
  parts.push(ep);
  let n = 0; for (const p of parts) n += p.length;
  const out = new Uint8Array(n); let o = 0; for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}

export const handoffBindingMethods = {
  // Loser: the signature fields for an offer, or null with no identity.
  async _signHandoff(offer, winnerHex) {
    const id = this._nodeIdentity;
    if (!id) return null;
    try {
      const sig = await id.sign(handoffTranscript({ topicId: offer.topicId, corr: offer.corr, loser: offer.loser, winner: winnerHex, loserEpoch: offer.loserEpoch | 0 }));
      if (!(sig instanceof Uint8Array) || sig.length !== 64) return null;
      return { handoffSig: bytesToHex(sig), handoffPub: bytesToHex(id.pubkey) };
    } catch { return null; }
  },

  // Candidate: does the loser itself vouch for `handoff: 1` on this offer?
  async _verifyHandoff(payload, loserHex) {
    try {
      if (payload.handoff !== 1 || typeof payload.handoffSig !== 'string' || typeof payload.handoffPub !== 'string') return false;
      if (!/^[0-9a-f]{128}$/.test(payload.handoffSig) || !/^[0-9a-f]{64}$/.test(payload.handoffPub) || !isHexId(loserHex)) return false;
      const pub = hexToBytes(payload.handoffPub, 32);
      if (!(await rootPubMatchesNodeHash(pub, idBig(loserHex)))) return false;
      const t = handoffTranscript({ topicId: payload.topicId, corr: payload.corr, loser: loserHex, winner: lc(idHex(this.nodeId)), loserEpoch: Number.isInteger(payload.loserEpoch) ? payload.loserEpoch : 0 });
      return !!(await verify(pub, t, hexToBytes(payload.handoffSig, 64)));
    } catch { return false; }
  },
};
