// =====================================================================
// presence.js — the dht:presence reset record (Connection-Quality v0.7,
// axona-docs 66f50bc, "The reset record"; implementation slice 2).
//
// A presence record is a node's SELF-SIGNED assertion that it is back:
//   { proto: 'axona/presence/1', nodeId, pubkey, gen, nonce, sig }
// Only the origin's key can mint one — that single fact is the storm-restart
// defense. The transcript-and-signature scheme is the axona/4 handshake's,
// reused unchanged: same canonical encoding, same Ed25519 keys, same
// pubkey-hashes-to-nodeId-suffix binding. A node that can verify a handshake
// can verify a presence record with no new trust material.
//
// `gen` is a per-identity counter, monotonic over the identity's lifetime,
// never persisted. `nonce` makes each record byte-unique for dedup; gen is
// the freshness proof. The receiver-side watermark, refill pacing, and
// attempt-guard reset live on the RECEIVER (AxonaPeer), keyed by the 256-bit
// identity suffix — verifyPresenceRecord returns that key.
//
// A presence record is NOT a nomination: nothing here touches a table.
// =====================================================================

import { canonical }                     from '../pubsub/post.js';
import { sign, verify, importPublicKey } from '../pubsub/ed25519.js';
import { pubkeyMatchesNodeId, makeNonce } from '../transport/handshake-auth.js';

export const PRESENCE_PROTO = 'axona/presence/1';

// Local hex helpers (module-local in handshake-auth too; six lines beats a coupling).
const _enc = new TextEncoder();
function bytesToHex(bytes) { return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join(''); }
function hexToBytes(hex) {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}
function isHex(s) { return typeof s === 'string' && /^[0-9a-f]+$/.test(s); }

// The signed transcript. `hop` (the relay-depth marker) is a SIBLING field on
// the wire payload, never part of the transcript — relays forward the signed
// record unchanged, exactly as the hello keeps its pow field outside the core.
function transcriptBytes({ proto, nodeId, pubkey, gen, nonce }) {
  return _enc.encode(canonical({ proto, nodeId, pubkey, gen, nonce }));
}

/**
 * Build a presence record for this identity at generation `gen`.
 *
 * @param {object} opts.identity  Identity with `.id` (66-hex nodeId),
 *                                `.pubkeyHex` (64-hex), `.sign`.
 * @param {number} opts.gen       Per-identity generation (caller counts).
 * @param {string} [opts.nonce]   Override (tests); default fresh random hex.
 * @returns {Promise<{proto:string,nodeId:string,pubkey:string,gen:number,nonce:string,sig:string}>}
 */
export async function buildPresenceRecord({ identity, gen, nonce = makeNonce(16) }) {
  if (!identity || typeof identity.id !== 'string' || typeof identity.pubkeyHex !== 'string'
      || typeof identity.sign !== 'function') {
    throw new TypeError('buildPresenceRecord: identity with {id, pubkeyHex, sign} required');
  }
  if (!Number.isSafeInteger(gen) || gen < 0) {
    throw new TypeError('buildPresenceRecord: gen must be a non-negative safe integer');
  }
  const core = { proto: PRESENCE_PROTO, nodeId: identity.id, pubkey: identity.pubkeyHex, gen, nonce };
  const sigBytes = await identity.sign(transcriptBytes(core));
  return { ...core, sig: 'ed25519:' + bytesToHex(sigBytes) };
}

/**
 * Verify a presence record. Returns a result object (never throws on a bad
 * record). On success, `identityHex` is the 256-bit suffix of the nodeId —
 * THE key for watermark/pacing/guard state (Connection-Quality v0.6 "What
 * the key is": the geo-prefix byte is the only churn possible under one key,
 * so state never keys on the full 264-bit string).
 *
 * Checks, in order, all local:
 *   1. shape (proto, 66-hex nodeId, 64-hex pubkey, safe non-negative gen,
 *      nonce, 'ed25519:'-schemed 128-hex sig)
 *   2. BIND — pubkey hashes to the 256-bit suffix of nodeId (handshake check)
 *   3. sig verifies over the canonical transcript under pubkey
 *
 * The monotonic-watermark and pacing decisions are the RECEIVER's, not this
 * function's — verification proves who spoke, not whether it is fresh.
 *
 * @returns {Promise<{ok:boolean, identityHex?:string, nodeId?:string, gen?:number, reason?:string}>}
 */
export async function verifyPresenceRecord(record) {
  const fail = (reason) => ({ ok: false, reason });
  if (!record || typeof record !== 'object')                return fail('not_an_object');
  if (record.proto !== PRESENCE_PROTO)                      return fail('proto_mismatch');
  if (typeof record.nodeId !== 'string' || record.nodeId.length !== 66 || !isHex(record.nodeId))
    return fail('bad_nodeId');
  if (typeof record.pubkey !== 'string' || record.pubkey.length !== 64 || !isHex(record.pubkey))
    return fail('bad_pubkey');
  if (!Number.isSafeInteger(record.gen) || record.gen < 0)  return fail('bad_gen');
  // v0.7: nonce is EXACTLY 16 random bytes — 32 hex characters, no more, no
  // less (Aster ASTER-PRESENCE-20260824-01: any-nonempty-hex was looser than
  // the definition and admits degenerate one-byte nonces).
  if (typeof record.nonce !== 'string' || record.nonce.length !== 32 || !isHex(record.nonce))
    return fail('bad_nonce');
  if (typeof record.sig !== 'string' || !record.sig.startsWith('ed25519:'))
    return fail('bad_sig_scheme');
  const sigHex = record.sig.slice('ed25519:'.length);
  if (sigHex.length !== 128 || !isHex(sigHex))              return fail('bad_sig_length');

  const pubkeyBytes = hexToBytes(record.pubkey);
  const ok256 = await pubkeyMatchesNodeId(pubkeyBytes, record.nodeId);
  if (!ok256)                                               return fail('pubkey_nodeid_mismatch');

  let sigOk = false;
  try {
    const pubKey = await importPublicKey(pubkeyBytes);
    sigOk = await verify(
      pubKey,
      transcriptBytes({ proto: PRESENCE_PROTO, nodeId: record.nodeId, pubkey: record.pubkey, gen: record.gen, nonce: record.nonce }),
      hexToBytes(sigHex),
    );
  } catch {
    return fail('verify_threw');
  }
  if (!sigOk)                                               return fail('bad_signature');

  return { ok: true, identityHex: record.nodeId.slice(2), nodeId: record.nodeId, gen: record.gen };
}

export { sign as _presenceSignForTests };
