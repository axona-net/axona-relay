// identity.js — EPHEMERAL relay identity (Phase 2).
//
// Transport ids are never stored. A relay mints a fresh in-memory Ed25519
// keypair / 264-bit nodeId on every start — there is no on-disk identity, no
// lock, and no cross-restart linkage. A restarted relay simply re-joins the
// mesh as a new node (a different keyspace neighbourhood); the kernel's
// cold-start anti-entropy drain re-warms its replay caches. The bridge
// directory + first-party reputation are keyed on the relay/bridge URL, not on
// the (now-rotating) signer, so nothing downstream depends on a stable id.

import { deriveIdentity } from '../vendor/axona-protocol/src/identity/index.js';

/**
 * Mint a fresh, in-memory relay identity in `region`. Never written to disk.
 * Non-extractable signing key (it's never persisted, so it never needs
 * exporting). Each call yields a unique nodeId sharing the region's S2 prefix.
 * @param {{lat:number, lng:number}} region  geo prefix for the nodeId
 * @returns {Promise<object>} Identity
 */
export async function createEphemeralIdentity(region) {
  return deriveIdentity({ ...region, extractable: false });
}
