// =====================================================================
// smoke_ed25519_fallback.js — software Ed25519 fallback for runtimes
// without native Web Crypto Ed25519 (old Chrome / Samsung Internet /
// WebViews). Before this, deriveIdentity threw "generateKey: Unrecognized
// name" and the device could not join the network at all.
//
//   1. native path round-trips (baseline; Node 20+ has native Ed25519).
//   2. forced-software path round-trips.
//   3. CROSS-IMPL: software-signed verifies natively, and native-signed
//      verifies in software (same RFC 8032 curve).
//   4. deriveIdentity works under forced software → valid id, signs, verifies.
//   5. dump/load persistence round-trips under software.
//   6. PKCS#8 INTEROP: a software-dumped identity loads under native.
//
// Run: node test/smoke_ed25519_fallback.js
// =====================================================================

import {
  generateKeyPair, exportPublicKey, sign, verify,
  nativeEd25519Available, __setForceSoftware, isSoftwareKey,
} from '../src/pubsub/ed25519.js';
import { createNodeIdentity, dumpIdentity, loadIdentity } from '../src/identity/index.js';
const hexToBytes = (h) => new Uint8Array(h.match(/../g).map((b) => parseInt(b, 16)));

let passed = 0, failed = 0;
async function check(label, fn) {
  try { const ok = await fn(); if (ok) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } }
  catch (e) { console.log(`  ✗ ${label} — threw: ${e.message || e}`); failed++; }
}
const MSG = new TextEncoder().encode('axona ed25519 fallback probe ✓');
const LON = { lat: 51.5074, lng: -0.1278 };

async function main() {
  console.log('Axona Ed25519 software-fallback smoke');

  // ── 1. native baseline ──
  __setForceSoftware(false);
  await check('1a. native Ed25519 is available on this runtime', async () => (await nativeEd25519Available()) === true);
  let nPub, nSig, nKp;
  await check('1b. native generate/sign/verify round-trips', async () => {
    nKp = await generateKeyPair();
    nPub = await exportPublicKey(nKp.publicKey);
    nSig = await sign(nKp.privateKey, MSG);
    return (await verify(nPub, MSG, nSig)) === true && nPub.length === 32 && nSig.length === 64;
  });
  await check('1c. native private key is NOT a software handle', async () => isSoftwareKey(nKp.privateKey) === false);

  // ── 2. forced software path ──
  __setForceSoftware(true);
  let sPub, sSig, sKp;
  await check('2a. software generate produces software key handles', async () => {
    sKp = await generateKeyPair();
    return isSoftwareKey(sKp.privateKey) && isSoftwareKey(sKp.publicKey);
  });
  await check('2b. software sign/verify round-trips', async () => {
    sPub = await exportPublicKey(sKp.publicKey);
    sSig = await sign(sKp.privateKey, MSG);
    return (await verify(sPub, MSG, sSig)) === true && sPub.length === 32 && sSig.length === 64;
  });
  await check('2c. software rejects a tampered signature', async () => {
    const bad = Uint8Array.from(sSig); bad[0] ^= 0xff;
    return (await verify(sPub, MSG, bad)) === false;
  });

  // ── 3. cross-implementation interop ──
  await check('3a. software-signed message verifies under NATIVE', async () => {
    __setForceSoftware(false);
    return (await verify(sPub, MSG, sSig)) === true;
  });
  await check('3b. native-signed message verifies under SOFTWARE', async () => {
    __setForceSoftware(true);
    return (await verify(nPub, MSG, nSig)) === true;
  });

  // ── 4. full deriveIdentity under forced software (the real connect path) ──
  let swId;
  await check('4. deriveIdentity works in software-only mode (was: throws, never connects)', async () => {
    __setForceSoftware(true);
    swId = await createNodeIdentity({ lat: LON.lat, lng: LON.lng });
    const sig = await swId.sign(MSG);
    const ok  = await verify(hexToBytes(swId.pubkeyHex), MSG, sig);
    return ok && typeof swId.id === 'string' && swId.id.length === 66;
  });

  // ── 5. persistence round-trip under software ──
  await check('5. dumpIdentity → loadIdentity round-trips under software', async () => {
    __setForceSoftware(true);
    const env = await dumpIdentity(swId);
    const re  = await loadIdentity(env);          // includes the M5 privkey↔pubkey probe
    return re.id === swId.id && re.pubkeyHex === swId.pubkeyHex;
  });

  // ── 6. PKCS#8 interop: a SOFTWARE-dumped identity loads under NATIVE ──
  await check('6. software-dumped identity loads under native (standard PKCS#8)', async () => {
    __setForceSoftware(true);
    const env = await dumpIdentity(swId);
    __setForceSoftware(false);                    // native importer parses the same PKCS#8
    const re  = await loadIdentity(env);
    return re.id === swId.id;
  });

  __setForceSoftware(false);
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
