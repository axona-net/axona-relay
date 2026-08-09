// capAttest.js — the authenticated capability oracle (Write-Flight Ack Routing,
// R13/R15/R17). The D0 delegate-selection step must know whether an adjacent
// peer implements 4.62.2 before it hands that peer a flight. No such fact exists
// in the base auth transcript (it signs only {proto,nodeId,pubkey,cbv} and
// carries no kernel version), and an unauthenticated self-report would let any
// peer claim capability. So capability is its own signed contract:
// `write-flight-ack-v1`, attested by a post-authentication CAP_ATTEST frame.
//
// Two properties are load-bearing, and both put the trusted material on the
// VERIFIER's side, never the frame:
//
//   R15 (key binding). The frame carries NO public key. The signature is
//        verified with the public key the base handshake already proved for this
//        peer (`peerKey`, passed in by the channel), and the transcript's nodeId
//        must equal that peer's authenticated id. A claim validly signed by some
//        OTHER identity fails: it is checked against this channel's stored key.
//
//   R17 (channel freshness). The transcript covers cbvDigest = SHA-256(UTF-8(the
//        transport's current channel-binding string)), and BOTH sides derive
//        that digest from their own live channel — it is never read from the
//        frame. A prior-channel CAP_ATTEST replayed after a reconnect fails: the
//        verifier derives a different current digest. Replay dies with no nonce
//        table.
//
// Transcript (fixed-width, big-endian; nodeId signed as decoded bytes):
//
//   DOMAIN     ASCII "AXONA_CAP_ATTEST_V1"        (19 bytes, no NUL)
//   u8(capIdLen = 19)
//   capId      ASCII "write-flight-ack-v1"        (19 bytes)
//   nodeId     decoded bytes, NODE_ID width       (the ATTESTER's id)
//   cbvDigest  32 bytes = SHA-256(UTF-8(currentTransportCbvString))
//
//   sig = Ed25519(attesterIdentityKey, transcript)
//
// Wire frame: { capId, nodeId, sig } — no public key, no CBV material.
//
// Pure and transport-agnostic: builds/validates the frame and derives the digest.
// It never sends anything and never stores a flag — the channel layer exchanges
// the frame after base auth, calls verifyCapAttest with its OWN peerKey + cbv,
// and keys the resulting boolean to the live channel (cleared on channel loss,
// never persisted). pickCapableAdjacent (D0) reads only that per-channel flag.
import { verify } from './ed25519.js';
import { hexToBytes, bytesToHex, ID_BYTES } from './ackProof.js';

const _enc = new TextEncoder();

export const CAP_DOMAIN = _enc.encode('AXONA_CAP_ATTEST_V1');       // 19 bytes
export const WRITE_FLIGHT_ACK_V1 = 'write-flight-ack-v1';           // the one capId (19 chars)
const CAP_ID_BYTES = _enc.encode(WRITE_FLIGHT_ACK_V1);             // 19 bytes
const CBV_DIGEST_BYTES = 32;

export class CapAttestError extends Error {
  constructor(code, msg) { super(msg || code); this.name = 'CapAttestError'; this.code = code; }
}

function concat(chunks) {
  let len = 0;
  for (const c of chunks) len += c.length;
  const out = new Uint8Array(len);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.length; }
  return out;
}

// cbvDigest = SHA-256( UTF-8( currentTransportCbvString ) ), exactly 32 bytes.
// The pre-image is the transport's existing CBV string verbatim (node-WS
// `n:<nonce>:<nonce>:<tag>`, bridge + connection id, mesh + `fp:<fp>:<fp>`) — no
// re-canonicalization, so the digest tracks exactly what base auth already bound.
export async function cbvDigest(cbvString) {
  if (typeof cbvString !== 'string' || cbvString.length === 0) {
    throw new CapAttestError('bad_cbv', 'cbvDigest needs the current channel CBV string');
  }
  const buf = await crypto.subtle.digest('SHA-256', _enc.encode(cbvString));
  return new Uint8Array(buf);
}

// The one transcript builder, shared verbatim by signer and verifier. `nodeId`
// is the ATTESTER's id (hex); `digest` is a 32-byte Uint8Array derived LOCALLY.
export function buildCapTranscript(nodeId, digest) {
  // nodeId MUST decode to EXACTLY ID_BYTES (33) — the canonical width idHex()
  // always emits, in every profile (Aster conformance blocker, council seq 538
  // item 2). The module boundary enforces the fixed wire width, not a
  // profile-derived one, matching the D1 serializer.
  const nodeB = (nodeId instanceof Uint8Array) ? nodeId : hexToBytes(String(nodeId), ID_BYTES);
  if (!nodeB || nodeB.length !== ID_BYTES) throw new CapAttestError('bad_nodeid', `nodeId must be ${ID_BYTES} bytes`);
  if (!(digest instanceof Uint8Array) || digest.length !== CBV_DIGEST_BYTES) {
    throw new CapAttestError('bad_digest', `cbvDigest must be ${CBV_DIGEST_BYTES} bytes`);
  }
  return concat([
    CAP_DOMAIN,
    Uint8Array.of(CAP_ID_BYTES.length),   // u8(19)
    CAP_ID_BYTES,
    nodeB,
    digest,
  ]);
}

// ── sign (attester) ────────────────────────────────────────────────────
// `signFn` is the attester's transport signer — identity.sign(bytes). The digest
// is computed here from the attester's OWN current channel CBV string.
export async function signCapAttest(signFn, { nodeId, cbvString }) {
  if (typeof signFn !== 'function') throw new CapAttestError('no_signer', 'signCapAttest needs a sign(bytes) function');
  const digest = await cbvDigest(cbvString);
  const transcript = buildCapTranscript(nodeId, digest);
  const sig = await signFn(transcript);
  if (!(sig instanceof Uint8Array) || sig.length !== 64) throw new CapAttestError('bad_sig_len', 'signer returned a non-64-byte signature');
  return {
    capId:  WRITE_FLIGHT_ACK_V1,
    nodeId: (nodeId instanceof Uint8Array) ? bytesToHex(nodeId) : String(nodeId).toLowerCase(),
    sig:    bytesToHex(sig),
  };
}

// ── verify (channel layer) ──────────────────────────────────────────────
// `opts.peerKey`      — the public key the base handshake proved for THIS peer
//                       (32 raw bytes or a key handle). NEVER taken from the frame.
// `opts.expectedNodeId` — this channel peer's authenticated nodeId (hex). The
//                       frame's nodeId must equal it, and the transcript is built
//                       from it — a frame cannot smuggle a different id.
// `opts.cbvString`    — the VERIFIER's own current channel CBV string.
// Returns { ok, reason }. Fail-closed on anything unexpected: an unknown capId, a
// nodeId that is not this peer, a stale/mismatched cbv, or a bad signature.
export async function verifyCapAttest(frame, opts = {}) {
  const { peerKey, expectedNodeId, cbvString } = opts;
  if (!frame || typeof frame !== 'object') return { ok: false, reason: 'no_frame' };
  if (frame.capId !== WRITE_FLIGHT_ACK_V1) return { ok: false, reason: 'bad_capid' };
  if (peerKey == null) return { ok: false, reason: 'no_peer_key' };
  if (typeof expectedNodeId !== 'string' || expectedNodeId.length === 0) return { ok: false, reason: 'no_expected_nodeid' };

  // Bind to the authenticated channel peer: the attester nodeId is THIS peer.
  if (typeof frame.nodeId !== 'string' || frame.nodeId.toLowerCase() !== expectedNodeId.toLowerCase()) {
    return { ok: false, reason: 'nodeid_mismatch' };
  }
  const sig = hexToBytes(String(frame.sig ?? ''), 64);
  if (!sig) return { ok: false, reason: 'bad_sig_len' };

  let transcript;
  try {
    const digest = await cbvDigest(cbvString);             // verifier's OWN channel
    transcript = buildCapTranscript(expectedNodeId, digest);
  } catch (e) {
    return { ok: false, reason: (e && e.code) || 'bad_transcript' };
  }

  let sigOk = false;
  try { sigOk = await verify(peerKey, transcript, sig); }  // peerKey = base-auth key, NOT the frame
  catch { return { ok: false, reason: 'verify_threw' }; }
  if (!sigOk) return { ok: false, reason: 'bad_signature' };

  return { ok: true, capId: WRITE_FLIGHT_ACK_V1, nodeId: expectedNodeId.toLowerCase() };
}
