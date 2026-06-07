// identity.js — a STABLE, persisted relay identity.
//
// A relay's value is being a well-known, always-on node, so its 264-bit
// nodeId must survive restarts. On first run we derive a fresh Ed25519
// keypair (extractable, so it can be persisted) and write the envelope to
// disk; on later runs we reload it. The file holds a private key — it is
// git-ignored and should be chmod 600 in production.

import { readFile, writeFile, chmod } from 'node:fs/promises';
import {
  deriveIdentity, dumpIdentity, loadIdentity,
} from '../vendor/axona-protocol/src/identity/index.js';

/**
 * @param {string} path  identity envelope file
 * @param {{lat:number, lng:number}} region  geo prefix for the nodeId
 * @returns {Promise<{ identity: object, created: boolean }>}
 */
export async function loadOrCreateIdentity(path, region) {
  try {
    const envelope = JSON.parse(await readFile(path, 'utf8'));
    const identity = await loadIdentity(envelope);
    return { identity, created: false };
  } catch (err) {
    if (err && err.code && err.code !== 'ENOENT') {
      // A malformed/corrupt file is worth surfacing rather than silently
      // overwriting someone's key.
      if (!(err.name && err.name.includes('Identity'))) throw err;
    }
    // extractable:true is required so dumpIdentity can export the pkcs8 key.
    const identity = await deriveIdentity({ ...region, extractable: true });
    await writeFile(path, JSON.stringify(await dumpIdentity(identity), null, 2));
    try { await chmod(path, 0o600); } catch { /* non-POSIX fs */ }
    return { identity, created: true };
  }
}
