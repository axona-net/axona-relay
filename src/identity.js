// identity.js — EPHEMERAL relay identities (design v0.3).
//
// Two keys, two roles (Identity-and-Authorship-Model-v0.3):
//   • NODE identity (connection) — `createNodeIdentity({lat,lng})`. A 264-bit
//     nodeId anchored on the region's S2 prefix; this is the transport key. A
//     relay mints a fresh in-memory one on every start — there is no on-disk
//     identity, no lock, no cross-restart linkage. A restarted relay simply
//     re-joins the mesh as a new node (a different keyspace neighbourhood); the
//     kernel's cold-start anti-entropy drain re-warms its replay caches. The
//     bridge directory + first-party reputation are keyed on the relay/bridge
//     URL, not on the (now-rotating) node id, so nothing downstream depends on
//     a stable id.
//   • AUTHOR identity (authorship) — `createAuthorIdentity()`. Keypair only,
//     NO nodeId / NO region; its public key is the Author ID (== signerPubkey
//     on the wire). This is what `peer.pub(..., { signWith })` signs with. The
//     node key NEVER signs publishes (key separation). The relay/CLI mint a
//     fresh ephemeral author per process — an unlinkable, throwaway signer.

import { createNodeIdentity, createAuthorIdentity }
  from '../vendor/axona-protocol/src/identity/index.js';

/**
 * Mint a fresh, in-memory relay NODE identity in `region`. Never written to
 * disk. Non-extractable signing key (it's never persisted, so it never needs
 * exporting). Each call yields a unique nodeId sharing the region's S2 prefix.
 * @param {{lat:number, lng:number}} region  geo prefix for the nodeId
 * @returns {Promise<object>} node Identity ({ id, region, sign, … })
 */
export async function createEphemeralIdentity(region) {
  return createNodeIdentity({ ...region, extractable: false });
}

/**
 * Mint a fresh, in-memory AUTHOR identity to sign publishes. Ephemeral and
 * unlinkable — a throwaway persona for CLI/relay-originated publishes (the
 * collector's leaderboard, CLI `pub`, selftest beacons). No persistence: a
 * relay never needs a recognizable author across restarts.
 * @returns {Promise<object>} author Identity ({ authorId, pubkeyHex, privateKey, sign, verify })
 */
export async function createEphemeralAuthor() {
  return createAuthorIdentity({ extractable: false });
}
