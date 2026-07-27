// =====================================================================
// smoke_partition.js — the 2026-06 network partition is HERMETIC.
//
// The flag-day separates the old network (kernel ≤2.16, AUTH_PROTO 'axona/4',
// WIRE_VERSION '1.0') from the new one (AUTH_PROTO 'axona/5', WIRE 2.0) so that
// no peer in one can ever form an authenticated relationship with a peer in the
// other — regardless of how they meet (shared bridge, relayed signaling, cached
// state). Two independent layers enforce it:
//
//   1. AUTH_PROTO (load-bearing): the proto tag is hard-checked AND folded into
//      the SIGNED auth transcript. A cross-network hello is rejected at the tag
//      check ('proto_mismatch'); even if the tag is rewritten to match, the
//      signature was computed over the OTHER proto, so the verifier's
//      reconstructed transcript fails ('bad_signature'). This severs BOTH the
//      peer↔peer mesh auth and the peer↔bridge-embedded-node auth.
//   2. WIRE_VERSION (early refusal): major is the hard-compat axis; the peer
//      sends it in client-hello and the bridge gate rejects a mismatched major.
//
// If a future change accidentally lets the two networks interoperate, the
// cross-network checks below flip to ok:true and this fails.
//
// Run: node test/smoke_partition.js
// =====================================================================

import {
  AUTH_PROTO, buildAuthHello, verifyAuthHello, makeNonce, cbvFromNonces,
} from '../src/transport/handshake-auth.js';
import { WIRE_VERSION, wireCompatible, buildClientHello } from '../src/transport/handshake.js';
import { createNodeIdentity } from '../src/identity/index.js';

const OLD_PROTO = 'axona/4';   // the network we are partitioning away from
const OLD_WIRE  = '3.0';       // routing-only flag-day: wire major 3.x is now "old"

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

async function main() {
  console.log('network partition is hermetic (auth-proto + wire-version)\n');

  // Sanity: this build is the NEW network.
  check(`AUTH_PROTO is the new tag (got ${AUTH_PROTO})`, AUTH_PROTO === 'axona/5');
  check(`WIRE_VERSION is the new major (got ${WIRE_VERSION})`, WIRE_VERSION === '4.0');

  const id  = await createNodeIdentity({ lat: 1, lng: 2 });
  const cbv = cbvFromNonces(makeNonce(), makeNonce(), 'mesh');

  // ── 1. same network binds (control) ───────────────────────────────
  console.log('\n── same network (axona/5 ↔ axona/5) ──');
  {
    const hello = await buildAuthHello({ identity: id, cbv });     // proto = AUTH_PROTO
    const v = await verifyAuthHello(hello, { cbv });
    check('honest same-network hello binds', v.ok === true);
  }

  // ── 2. cross network is refused at the tag check ──────────────────
  console.log('\n── old network (axona/4) hello vs new verifier ──');
  {
    const oldHello = await buildAuthHello({ identity: id, cbv, proto: OLD_PROTO });
    const v = await verifyAuthHello(oldHello, { cbv });            // default proto = axona/5
    check('cross-network hello REFUSED', v.ok === false);
    check('  reason is proto_mismatch', v.reason === 'proto_mismatch');
  }
  // symmetric: a new hello presented to an old-network verifier
  {
    const newHello = await buildAuthHello({ identity: id, cbv });  // axona/5
    const v = await verifyAuthHello(newHello, { cbv, proto: OLD_PROTO });
    check('new hello REFUSED by an old-network verifier', v.ok === false && v.reason === 'proto_mismatch');
  }

  // ── 3. tag-rewrite does not help: the transcript binding catches it ─
  console.log('\n── tamper: rewrite the tag to dodge the tag check ──');
  {
    const oldHello = await buildAuthHello({ identity: id, cbv, proto: OLD_PROTO });
    const tampered = { ...oldHello, proto: AUTH_PROTO };           // claim axona/5, sig is over axona/4
    const v = await verifyAuthHello(tampered, { cbv });
    check('tag-rewritten hello still REFUSED', v.ok === false);
    check('  reason is bad_signature (transcript binds the proto)', v.reason === 'bad_signature');
  }

  // ── 4. wire-version is the early refusal axis ─────────────────────
  console.log('\n── wire-version major partition ──');
  {
    check('new wire incompatible with old (4.0 vs 3.0)', wireCompatible(WIRE_VERSION, OLD_WIRE) === false);
    check('new wire compatible within its major (4.0 vs 4.7)', wireCompatible(WIRE_VERSION, '4.7') === true);
    check('client-hello advertises the new wire major', buildClientHello({ version: '1.0.0' }).wireVersion === '4.0');
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('fatal:', e); process.exit(2); });
