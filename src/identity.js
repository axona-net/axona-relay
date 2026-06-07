// identity.js — a STABLE, persisted relay identity.
//
// A relay's value is being a well-known, always-on node, so its 264-bit
// nodeId must survive restarts. On first run we derive a fresh Ed25519
// keypair (extractable, so it can be persisted) and write the envelope to
// disk; on later runs we reload it. The file holds a private key — it is
// git-ignored and should be chmod 600 in production.

import { readFile, writeFile, chmod, open, unlink } from 'node:fs/promises';
import {
  deriveIdentity, dumpIdentity, loadIdentity,
} from '../vendor/axona-protocol/src/identity/index.js';

function pidAlive(pid) {
  try { process.kill(pid, 0); return true; }
  catch (e) { return e.code === 'EPERM'; }  // exists but not ours
}

/**
 * Take an exclusive lock on an identity so two relays can't silently run with
 * the SAME keypair/nodeId (which collides in the mesh). Writes `<path>.lock`
 * holding our pid; refuses if a live owner holds it; clears a stale one.
 * @returns {Promise<() => Promise<void>>} release function
 */
export async function acquireIdentityLock(identityPath) {
  const lockPath = identityPath + '.lock';
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const fh = await open(lockPath, 'wx');           // O_CREAT|O_EXCL
      await fh.writeFile(String(process.pid));
      await fh.close();
      return async () => { try { await unlink(lockPath); } catch { /* */ } };
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      let pid = 0;
      try { pid = parseInt(await readFile(lockPath, 'utf8'), 10) || 0; } catch { /* */ }
      if (pid && pid !== process.pid && pidAlive(pid)) {
        const err = new Error(
          `identity "${identityPath}" is already in use by pid ${pid}. ` +
          `To run another relay here, give it a different RELAY_REGION ` +
          `or RELAY_IDENTITY_PATH.`);
        err.code = 'IDENTITY_LOCKED';
        throw err;
      }
      try { await unlink(lockPath); } catch { /* stale; retry */ }
    }
  }
  throw new Error(`could not acquire identity lock for ${identityPath}`);
}

/**
 * Mint a fresh, in-memory identity in `region` — never written to disk. Used
 * for "additional" nodes when the persistent (known) identity is already in
 * use: each gets a unique nodeId sharing the region prefix. Non-extractable
 * signing key (it's never persisted, so it never needs exporting).
 * @param {{lat:number, lng:number}} region
 * @returns {Promise<object>} Identity
 */
export async function createEphemeralIdentity(region) {
  return deriveIdentity({ ...region, extractable: false });
}

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
