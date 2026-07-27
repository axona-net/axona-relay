// =====================================================================
// smoke_handshake_auth.js — authenticated peer-identity handshake.
//
// Proves the three-part gate (bind / possess / channel) accepts honest
// peers and rejects every forgery class:
//   - tampered nodeId / pubkey / signature
//   - claiming an id whose key you don't hold (own key, victim's id)
//   - presenting a victim's pubkey you can't sign for
//   - replaying a valid hello onto a different channel (CBV mismatch)
//
// Run: node test/smoke_handshake_auth.js
// =====================================================================

import {
  buildAuthHello, verifyAuthHello, pubkeyMatchesNodeId,
  makeNonce, cbvFromNonces, cbvFromFingerprints, AUTH_PROTO,
} from '../src/transport/handshake-auth.js';
import { createNodeIdentity } from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const NYC = { lat: 40.71, lng: -74.0 };
const LON = { lat: 51.5,  lng: -0.12 };

async function main() {
  console.log('authenticated handshake (axona/4)\n');

  const alice = await createNodeIdentity(NYC);
  const bob   = await createNodeIdentity(LON);

  // ── happy path ─────────────────────────────────────────────────────
  {
    const cbv = cbvFromNonces(makeNonce(), makeNonce(), 'conn-1');
    const hello = await buildAuthHello({ identity: alice, cbv });
    const r = await verifyAuthHello(hello, { cbv });
    check('honest hello verifies', r.ok === true);
    check('verify returns the claimed nodeId', r.nodeId === alice.id);
    check('verify returns the pubkey',         r.pubkey === alice.pubkeyHex);
  }

  // ── bind check in isolation ────────────────────────────────────────
  {
    const ok  = await pubkeyMatchesNodeId(alice.pubkey, alice.id);
    check('pubkey matches its own nodeId suffix', ok === true);
    const bad = await pubkeyMatchesNodeId(bob.pubkey, alice.id);
    check('different pubkey does NOT match nodeId', bad === false);
    // Different geo prefix, same key → suffix still matches (area code
    // is not authenticated).
    const swappedPrefix = 'ff' + alice.id.slice(2);
    const okPrefix = await pubkeyMatchesNodeId(alice.pubkey, swappedPrefix);
    check('geo prefix is NOT part of the binding (area-code semantics)',
      okPrefix === true);
  }

  // ── CBV mismatch (replay onto a different channel) ─────────────────
  {
    const cbv1 = cbvFromNonces(makeNonce(), makeNonce(), 'conn-A');
    const hello = await buildAuthHello({ identity: alice, cbv: cbv1 });
    // Attacker captures alice's hello and replays it on a different
    // channel whose CBV is cbv2.
    const cbv2 = cbvFromNonces(makeNonce(), makeNonce(), 'conn-B');
    const r = await verifyAuthHello(hello, { cbv: cbv2 });
    check('replay onto a different channel is rejected (bad_signature)',
      r.ok === false && r.reason === 'bad_signature');
  }

  // ── tampered fields ────────────────────────────────────────────────
  {
    const cbv = cbvFromNonces(makeNonce(), makeNonce());
    const hello = await buildAuthHello({ identity: alice, cbv });

    const tNode = { ...hello, nodeId: bob.id };
    check('tampered nodeId rejected (pubkey↔nodeId mismatch)',
      (await verifyAuthHello(tNode, { cbv })).reason === 'pubkey_nodeid_mismatch');

    const tProto = { ...hello, proto: 'axona/3' };
    check('wrong proto rejected', (await verifyAuthHello(tProto, { cbv })).reason === 'proto_mismatch');

    const flipped = hello.sig.slice(0, -2) + (hello.sig.endsWith('00') ? '11' : '00');
    const tSig = { ...hello, sig: flipped };
    check('tampered signature rejected (bad_signature)',
      (await verifyAuthHello(tSig, { cbv })).reason === 'bad_signature');

    check('short signature rejected', (await verifyAuthHello({ ...hello, sig: 'ed25519:dead' }, { cbv })).reason === 'bad_sig_length');
    check('missing cbv rejected',     (await verifyAuthHello(hello, { cbv: '' })).reason === 'missing_cbv');
  }

  // ── impersonation A: claim victim's nodeId, sign with own key ──────
  // Attacker presents their OWN pubkey (so PoP could pass) but claims
  // bob's nodeId → bind check fails.
  {
    const cbv = cbvFromNonces(makeNonce(), makeNonce());
    const honest = await buildAuthHello({ identity: alice, cbv });
    const forged = { ...honest, nodeId: bob.id };   // alice's pubkey+sig, bob's id
    const r = await verifyAuthHello(forged, { cbv });
    check('cannot claim another node\'s id with your own key', r.reason === 'pubkey_nodeid_mismatch');
  }

  // ── impersonation B: present victim's pubkey, can't sign for it ────
  // Attacker presents bob's nodeId + bob's (public!) pubkey, but signs
  // with alice's key → bind passes, PoP fails.
  {
    const cbv = cbvFromNonces(makeNonce(), makeNonce());
    const aliceHello = await buildAuthHello({ identity: alice, cbv });
    const forged = { proto: AUTH_PROTO, nodeId: bob.id, pubkey: bob.pubkeyHex, sig: aliceHello.sig };
    const r = await verifyAuthHello(forged, { cbv });
    check('presenting a victim pubkey without its key fails PoP', r.reason === 'bad_signature');
  }

  // ── fingerprint-CBV path (WebRTC channel binding) ──────────────────
  {
    const fpA = 'AA:BB:CC';
    const fpB = 'DD:EE:FF';
    // Both endpoints derive the same CBV regardless of role ordering.
    const cbvAlice = cbvFromFingerprints(fpA, fpB);
    const cbvBob   = cbvFromFingerprints(fpB, fpA);
    check('fingerprint CBV is order-independent', cbvAlice === cbvBob);
    const hello = await buildAuthHello({ identity: alice, cbv: cbvAlice });
    check('fingerprint-bound hello verifies at peer', (await verifyAuthHello(hello, { cbv: cbvBob })).ok === true);
    // A MITM that re-terminated DTLS would present a different fpRemote.
    const cbvMitm = cbvFromFingerprints(fpA, 'MITM:FP');
    check('MITM-substituted fingerprint is rejected',
      (await verifyAuthHello(hello, { cbv: cbvMitm })).ok === false);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
