// ackProof.js — the signed INGEST-ACK proof (Write-Flight Ack Routing, D1).
//
// The deaf-flight defect (#51): a forwarded WRITE opens its flight at the route
// ORIGIN, but the root acks `meta.fromId` — the LAST HOP. On any multi-hop route
// the ack lands on an intermediate relay that holds no flight, the origin never
// hears it, an honest root is convicted, and the eviction storm follows.
//
// The fix: the flight OWNER stamps `ackTo` (its own transport id) + a per-flight
// `flightNonce` + the attempt's `attemptId` on the forwarded PUB/KILL. The root,
// on ingest, signs an ACK PROOF over a byte-exact transcript and ROUTES it to
// `ackTo` — straight back to the owner, however many hops away. The owner
// verifies the signature against the root's transport pubkey and binds the proof
// to the exact open flight. A routed ack now proves ingestion by the addressed
// root, not "some node one hop from the root nodded."
//
// This module is PURE and transport-agnostic: it builds/validates the transcript
// and the wire frame. It never sends anything and never touches flight state —
// the caller (writeFlight.js) owns the binding to an open flight. Keeping it pure
// is what lets the golden/rejection vectors (ackProof.vectors.js) pin the exact
// bytes an independent implementation must reproduce (Aster R7).
//
// Transcript (all integers big-endian; hex id fields signed as DECODED BYTES at
// their canonical width; no length prefixes — every field is fixed-width, so the
// transcript is a fixed-length byte string with no parse ambiguity to canon):
//
//   DOMAIN        ASCII "AXONA_INGEST_ACK_PROOF_V1"   (25 bytes, no NUL)
//   u8(purpose)   0x01 INGEST_ACK | 0x02 RECEIPT_NACK | 0x03 RECEIPT_PROBE_ACK
//   u8(op)        0x01 PUB | 0x02 KILL                (closed set; else reject)
//   topicId       decoded bytes, TOPIC_ID width
//   msgId         decoded bytes, 32 (SHA-256 content hash)
//   u64(epoch)
//   attemptId     16 bytes
//   ackTo         decoded bytes, NODE_ID width
//   flightNonce   16 bytes
//   rootPub       32 bytes (Ed25519 public key)
//
//   sig = Ed25519(rootTransportKey, transcript)
//
// The `purpose` byte is load-bearing (Aster R7): a root-signed RECEIPT_NACK can
// never be replayed as an INGEST_ACK, because the signed bytes differ, so the
// verifier recomputes a different transcript and the signature fails. The three
// frame classes are three separate signed objects, not one transcript with an
// unsigned type field.
import { verify } from './ed25519.js';
import { HASH_MASK } from '../utils/hexid.js';

const _enc = new TextEncoder();

export const DOMAIN = _enc.encode('AXONA_INGEST_ACK_PROOF_V1');   // 25 bytes
export const MSG_ID_BYTES = 32;          // SHA-256 content hash — profile-independent
export const ATTEMPT_ID_BYTES = 16;
export const FLIGHT_NONCE_BYTES = 16;
export const ROOT_PUB_BYTES = 32;
// Canonical id width for topicId/ackTo: the D1 wire serializer idHex()
// (ids.js) pads EVERY id to 66 hex — 33 bytes — unconditionally, regardless of
// the keyspace profile (a shrunk sim id is the same 33 bytes, zero-extended).
// So proof ids are a FIXED 33 bytes, not a profile-derived width; enforcing
// HEX_CHARS/2 (Aster fixed-slice review) wrongly rejected the real signed path
// under a shrunk profile, where HEX_CHARS shrinks but idHex still emits 33.
export const ID_BYTES = 33;

export const PURPOSE = Object.freeze({ INGEST_ACK: 0x01, RECEIPT_NACK: 0x02, RECEIPT_PROBE_ACK: 0x03 });
export const OP      = Object.freeze({ pub: 0x01, kill: 0x02 });
const OP_NAME = Object.freeze({ 0x01: 'pub', 0x02: 'kill' });

const _isPurpose = (p) => p === 0x01 || p === 0x02 || p === 0x03;
const _isOp      = (o) => o === 0x01 || o === 0x02;

// ── byte helpers ──────────────────────────────────────────────────────
// Hex decode that REJECTS anything that is not an even-length lowercase-or-upper
// hex string of the expected byte width. A frame whose field is the wrong width
// (a sim id where a prod id is expected, a truncated nonce) is refused here,
// before any signature check — the "reject on width" rule (Aster R7).
export function hexToBytes(hex, expectBytes = null) {
  if (typeof hex !== 'string' || hex.length === 0 || (hex.length & 1)) return null;
  if (!/^[0-9a-fA-F]+$/.test(hex)) return null;
  const n = hex.length >> 1;
  if (expectBytes != null && n !== expectBytes) return null;
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

export function bytesToHex(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += bytes[i].toString(16).padStart(2, '0');
  return s;
}

function u64be(n) {
  // epoch is a non-negative integer; encode as unsigned 64-bit big-endian.
  const b = new Uint8Array(8);
  let v = BigInt(n);
  if (v < 0n) v = 0n;
  for (let i = 7; i >= 0; i--) { b[i] = Number(v & 0xffn); v >>= 8n; }
  return b;
}

function concat(chunks) {
  let len = 0;
  for (const c of chunks) len += c.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.length; }
  return out;
}

// ── transcript ────────────────────────────────────────────────────────
// The ONE builder shared verbatim by signer and verifier. Returns the exact
// bytes to sign/verify, or throws AckProofError with a machine code so the
// caller can log the precise refusal. `topicId`/`ackTo` are node/topic ids and
// MUST decode to EXACTLY ID_BYTES (33) — the canonical width the D1 serializer
// idHex() always emits, in every profile. A wrong-width id — a truncated field,
// or a raw shrunk-profile id that never went through idHex — is refused before
// signing (Aster conformance blocker, council seq 538 item 2). All widths in the
// transcript are fixed constants, so the transcript is a fixed-length string.
export class AckProofError extends Error {
  constructor(code, msg) { super(msg || code); this.name = 'AckProofError'; this.code = code; }
}

export function buildTranscript(fields) {
  const { purpose, op, topicId, msgId, epoch, attemptId, ackTo, flightNonce, rootPub } = fields || {};
  if (!_isPurpose(purpose)) throw new AckProofError('bad_purpose', `purpose ${purpose} outside closed set`);
  if (!_isOp(op))           throw new AckProofError('bad_op', `op ${op} outside closed set`);

  const topicB = (topicId instanceof Uint8Array) ? topicId : hexToBytes(String(topicId), ID_BYTES);
  if (!topicB || topicB.length !== ID_BYTES) throw new AckProofError('bad_topic', `topicId must be ${ID_BYTES} bytes`);

  const msgB = (msgId instanceof Uint8Array) ? msgId : hexToBytes(String(msgId), MSG_ID_BYTES);
  if (!msgB || msgB.length !== MSG_ID_BYTES) throw new AckProofError('bad_msgid', `msgId must be ${MSG_ID_BYTES} bytes`);

  // Epoch encodes as unsigned 64-bit big-endian (u64be). A value above the
  // safe-integer ceiling can't be an exact JS number, and u64be would truncate
  // two distinct epochs to the same 8 bytes — reject it so the wire domain is
  // exact (Aster conformance blocker). Real epochs are incarnation counters far
  // below 2^53.
  if (!Number.isSafeInteger(epoch) || epoch < 0) throw new AckProofError('bad_epoch', 'epoch must be a non-negative safe integer within the u64 domain');

  const attB = (attemptId instanceof Uint8Array) ? attemptId : hexToBytes(String(attemptId), ATTEMPT_ID_BYTES);
  if (!attB || attB.length !== ATTEMPT_ID_BYTES) throw new AckProofError('bad_attempt', `attemptId must be ${ATTEMPT_ID_BYTES} bytes`);

  const ackB = (ackTo instanceof Uint8Array) ? ackTo : hexToBytes(String(ackTo), ID_BYTES);
  if (!ackB || ackB.length !== ID_BYTES) throw new AckProofError('bad_ackto', `ackTo must be ${ID_BYTES} bytes`);

  const nonceB = (flightNonce instanceof Uint8Array) ? flightNonce : hexToBytes(String(flightNonce), FLIGHT_NONCE_BYTES);
  if (!nonceB || nonceB.length !== FLIGHT_NONCE_BYTES) throw new AckProofError('bad_nonce', `flightNonce must be ${FLIGHT_NONCE_BYTES} bytes`);

  const pubB = (rootPub instanceof Uint8Array) ? rootPub : hexToBytes(String(rootPub), ROOT_PUB_BYTES);
  if (!pubB || pubB.length !== ROOT_PUB_BYTES) throw new AckProofError('bad_rootpub', `rootPub must be ${ROOT_PUB_BYTES} bytes`);

  return concat([
    DOMAIN,
    Uint8Array.of(purpose),
    Uint8Array.of(op),
    topicB,
    msgB,
    u64be(epoch),
    attB,
    ackB,
    nonceB,
    pubB,
  ]);
}

// ── sign ──────────────────────────────────────────────────────────────
// `signFn` is the root's transport signer — identity.sign(bytes)->Promise<sig>.
// Passing it in (rather than a private key) keeps this module free of key
// material and matches how the auth hello signs (buildAuthHello). Returns the
// wire frame: every field hex, plus the 64-byte signature as hex.
export async function signAckProof(signFn, fields) {
  if (typeof signFn !== 'function') throw new AckProofError('no_signer', 'signAckProof needs a sign(bytes) function');
  const transcript = buildTranscript(fields);            // validates fields, throws on bad
  const sig = await signFn(transcript);
  if (!(sig instanceof Uint8Array) || sig.length !== 64) throw new AckProofError('bad_sig_len', 'signer returned a non-64-byte signature');
  return {
    purpose:     fields.purpose,
    op:          OP_NAME[fields.op],
    topicId:     (fields.topicId instanceof Uint8Array) ? bytesToHex(fields.topicId) : String(fields.topicId).toLowerCase(),
    msgId:       (fields.msgId   instanceof Uint8Array) ? bytesToHex(fields.msgId)   : String(fields.msgId),
    epoch:       fields.epoch,
    attemptId:   (fields.attemptId instanceof Uint8Array) ? bytesToHex(fields.attemptId) : String(fields.attemptId).toLowerCase(),
    ackTo:       (fields.ackTo   instanceof Uint8Array) ? bytesToHex(fields.ackTo)   : String(fields.ackTo).toLowerCase(),
    flightNonce: (fields.flightNonce instanceof Uint8Array) ? bytesToHex(fields.flightNonce) : String(fields.flightNonce).toLowerCase(),
    rootPub:     (fields.rootPub instanceof Uint8Array) ? bytesToHex(fields.rootPub) : String(fields.rootPub).toLowerCase(),
    sig:         bytesToHex(sig),
  };
}

// ── verify (structure + signature) ────────────────────────────────────
// Returns { ok, reason, fields } — fields carries the decoded, validated values
// the caller binds to an open flight. This proves ONLY that the holder of the
// private key matching rootPub signed this exact (purpose, op, topic, msg, epoch,
// attempt, ackTo, nonce). It does NOT decide whether rootPub is the flight's
// expected authority — that is rootPubMatchesNodeHash + the flight-key match,
// done by the caller (writeFlight._flightComplete), because only the caller
// knows the open flight's rootHex/attemptId/nonce.
export async function verifyAckProof(frame) {
  if (!frame || typeof frame !== 'object') return { ok: false, reason: 'no_frame' };
  const op = OP[frame.op];
  if (op === undefined) return { ok: false, reason: 'bad_op' };
  const purpose = frame.purpose;
  if (!_isPurpose(purpose)) return { ok: false, reason: 'bad_purpose' };
  const sig = hexToBytes(String(frame.sig ?? ''), 64);
  if (!sig) return { ok: false, reason: 'bad_sig_len' };
  const rootPub = hexToBytes(String(frame.rootPub ?? ''), ROOT_PUB_BYTES);
  if (!rootPub) return { ok: false, reason: 'bad_rootpub' };

  let transcript;
  try {
    transcript = buildTranscript({
      purpose, op,
      topicId: frame.topicId, msgId: frame.msgId, epoch: frame.epoch,
      attemptId: frame.attemptId, ackTo: frame.ackTo, flightNonce: frame.flightNonce,
      rootPub,
    });
  } catch (e) {
    return { ok: false, reason: (e && e.code) || 'bad_transcript' };
  }

  let sigOk = false;
  try { sigOk = await verify(rootPub, transcript, sig); }
  catch { return { ok: false, reason: 'verify_threw' }; }
  if (!sigOk) return { ok: false, reason: 'bad_signature' };

  return {
    ok: true,
    fields: {
      purpose, op: frame.op,
      topicId: String(frame.topicId).toLowerCase(),
      msgId: String(frame.msgId),
      epoch: frame.epoch,
      attemptId: String(frame.attemptId).toLowerCase(),
      ackTo: String(frame.ackTo).toLowerCase(),
      flightNonce: String(frame.flightNonce).toLowerCase(),
      rootPub,
    },
  };
}

// ── authority binding (Aster R1, refined) ─────────────────────────────
// The design says "derive the node id from rootPub and require it to equal the
// flight's expected authority." Taken literally that is not computable: a nodeId
// is [8-bit S2 region prefix from lat/lng] || [SHA-256(pubkey) & HASH_MASK], and
// the verifier does not know the root's lat/lng, so it cannot reproduce the
// region prefix. The implementable, cryptographically-meaningful binding is the
// HASH PORTION: SHA-256(rootPub) & HASH_MASK must equal the low (hash) bits of
// the flight's expected rootHex. The region prefix is placement metadata, not
// key-bound identity; forging a proof still needs a pubkey that preimages the
// target's hash slot. Same security property, exact check.
export async function rootPubMatchesNodeHash(rootPubBytes, expectedRootBig) {
  if (!(rootPubBytes instanceof Uint8Array) || rootPubBytes.length !== ROOT_PUB_BYTES) return false;
  let expected;
  try { expected = BigInt(expectedRootBig) & HASH_MASK; } catch { return false; }
  const buf = await crypto.subtle.digest('SHA-256', rootPubBytes);
  let hex = '';
  const arr = new Uint8Array(buf);
  for (let i = 0; i < arr.length; i++) hex += arr[i].toString(16).padStart(2, '0');
  const derived = (BigInt('0x' + hex)) & HASH_MASK;
  return derived === expected;
}
