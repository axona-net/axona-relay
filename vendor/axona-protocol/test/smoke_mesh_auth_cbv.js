// =====================================================================
// smoke_mesh_auth_cbv.js — regression for the WebRTC-mesh axona/4 CBV
// symmetry bug.
//
// The mesh auth handshake binds a peer only if BOTH endpoints derive the
// identical channel-binding value (CBV) and each verifies the other's
// signature over it.  The CBV is cbvFromNonces(myNonce, peerNonce, TAG).
// cbvFromNonces sorts the nonce pair, so the nonces are symmetric — but
// the TAG must be symmetric too.
//
// The bug: the mesh used `meshId` (each side's view of the OTHER peer's
// bridge connId) as the tag.  Those connIds differ per side, so the two
// CBVs disagreed and EVERY mesh-auth signature failed — the WebRTC mesh
// never bound a single peer; all routing fell back to the bridge.  This
// test reproduces that failure and proves the constant-tag fix.
//
// Run: node test/smoke_mesh_auth_cbv.js
// =====================================================================

import { createNodeIdentity } from '../src/identity/index.js';
import { buildAuthHello, verifyAuthHello, makeNonce, cbvFromNonces } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

// Simulate one side proving its identity to the other over a given CBV,
// and the verifier checking it against ITS OWN cbv.  Returns the verify
// result — ok:true means the peer would be bound.
async function proveAndVerify(signerIdentity, signerCbv, verifierCbv) {
  const proof = await buildAuthHello({ identity: signerIdentity, cbv: signerCbv });
  return verifyAuthHello(proof, { cbv: verifierCbv });
}

async function main() {
  console.log('WebRTC-mesh axona/4 CBV symmetry (regression)');

  const alice = await createNodeIdentity({ lat: 40.7, lng: -74.0 });
  const bob   = await createNodeIdentity({ lat: 51.5, lng: -0.1 });
  const nA = makeNonce(), nB = makeNonce();
  // Asymmetric per-side connIds, as the bridge assigns them.
  const connA = 'cA7', connB = 'cB9';

  // ── The OLD (buggy) behaviour: tag = the OTHER peer's connId ────────
  console.log('\n── reproduce the bug: per-side connId tag ──');
  const aliceCbvBug = cbvFromNonces(nA, nB, connB);  // Alice folds Bob's connId
  const bobCbvBug   = cbvFromNonces(nB, nA, connA);  // Bob folds Alice's connId
  check('the two sides derive DIFFERENT CBVs (root cause)', aliceCbvBug !== bobCbvBug);
  const bugAtBob   = await proveAndVerify(alice, aliceCbvBug, bobCbvBug);   // Bob verifies Alice
  const bugAtAlice = await proveAndVerify(bob,   bobCbvBug,   aliceCbvBug); // Alice verifies Bob
  check('Bob CANNOT bind Alice (signature fails)',   bugAtBob.ok === false);
  check('Alice CANNOT bind Bob (signature fails)',   bugAtAlice.ok === false);

  // ── The FIX: constant symmetric tag ────────────────────────────────
  console.log('\n── the fix: constant \'mesh\' tag ──');
  const aliceCbv = cbvFromNonces(nA, nB, 'mesh');
  const bobCbv   = cbvFromNonces(nB, nA, 'mesh');
  check('the two sides derive the SAME CBV', aliceCbv === bobCbv);
  const okAtBob   = await proveAndVerify(alice, aliceCbv, bobCbv);
  const okAtAlice = await proveAndVerify(bob,   bobCbv,   aliceCbv);
  check('Bob binds Alice (signature verifies)',  okAtBob.ok === true && okAtBob.nodeId === alice.id);
  check('Alice binds Bob (signature verifies)',  okAtAlice.ok === true && okAtAlice.nodeId === bob.id);

  // ── Replay protection is unchanged: a different link (fresh nonces)
  //    cannot reuse a captured proof, even with the same tag. ──────────
  console.log('\n── per-link freshness still holds (fresh nonces) ──');
  const nA2 = makeNonce(), nC = makeNonce();
  const linkAC = cbvFromNonces(nA2, nC, 'mesh');
  const replay = await proveAndVerify(alice, aliceCbv, linkAC);  // Alice's A↔B proof onto A↔C
  check('a captured proof does NOT verify on a different link', replay.ok === false);

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
