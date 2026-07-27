// =====================================================================
// smoke_transport_sim_auth.js — authenticated-identity gate exercised
// end-to-end in the simulator (the same gate the live mesh runs).
//
// Honest peers (real Ed25519 identities) authenticate and connect.
// An impersonator that registers under a victim's nodeId but holds a
// different key is refused.  A peer with no usable identity is refused.
// Legacy (authenticate:false) peers still connect (no regression).
//
// Run: node test/smoke_transport_sim_auth.js
// =====================================================================

import { SimNetwork, simTransport } from '../src/transport/sim/index.js';
import { createNodeIdentity }           from '../src/identity/index.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const NYC = { lat: 40.71, lng: -74.0 };
const LON = { lat: 51.5,  lng: -0.12 };
const TOK = { lat: 35.68, lng: 139.69 };

const net = () => new SimNetwork({ latencyFn: () => 0 });

async function main() {
  console.log('authenticated-identity gate in the simulator\n');

  const aliceId = await createNodeIdentity(NYC);
  const bobId   = await createNodeIdentity(LON);
  const evilId  = await createNodeIdentity(TOK);

  // ── honest authenticated connect ───────────────────────────────────
  {
    const n = net();
    const alice = simTransport({ network: n, identity: aliceId, authenticate: true, heartbeatMs: 0 });
    const bob   = simTransport({ network: n, identity: bobId,   authenticate: true, heartbeatMs: 0 });
    await alice.start(aliceId.id);
    await bob.start(bobId.id);

    const ok = await alice.openConnection(bobId.id);
    check('honest peers authenticate + connect', ok === true);
    check('alice→bob channel open', alice.isConnected(bobId.id));
    check('bob→alice channel open (mutual)', bob.isConnected(aliceId.id));
    await alice.stop(); await bob.stop();
  }

  // ── impersonation: squat a victim's nodeId with a different key ────
  {
    const n = net();
    // Evil registers UNDER bob's nodeId, but signs with its own key.
    const liarIdentity = { ...evilId, id: bobId.id };   // bob's id, evil's keys
    const liar  = simTransport({ network: n, identity: liarIdentity, authenticate: true, heartbeatMs: 0 });
    const alice = simTransport({ network: n, identity: aliceId,      authenticate: true, heartbeatMs: 0 });
    await liar.start(bobId.id);     // squats bob's id in the network
    await alice.start(aliceId.id);

    let rejReason = null;
    alice._onAuthReject = ({ reason }) => { rejReason = reason; };
    const ok = await alice.openConnection(bobId.id);
    check('impersonator is refused (no channel)', ok === false);
    check('alice did NOT bind the impersonator', !alice.isConnected(bobId.id));
    check('rejection reason is the pubkey↔nodeId bind failure',
      typeof rejReason === 'string' && rejReason.includes('pubkey_nodeid_mismatch'));
    await alice.stop(); await liar.stop();
  }

  // ── authenticate-on but counterpart has no signable identity ───────
  {
    const n = net();
    const alice  = simTransport({ network: n, identity: aliceId, authenticate: true, heartbeatMs: 0 });
    const keyless = simTransport({ network: n, identity: { id: bobId.id }, authenticate: true, heartbeatMs: 0 });
    await alice.start(aliceId.id);
    await keyless.start(bobId.id);
    let rej = null;
    alice._onAuthReject = ({ reason }) => { rej = reason; };
    const ok = await alice.openConnection(bobId.id);
    check('peer with no signing key is refused', ok === false && rej === 'missing_identity');
    await alice.stop(); await keyless.stop();
  }

  // ── regression: legacy unauthenticated peers still connect ─────────
  {
    const n = net();
    const a = simTransport({ network: n, heartbeatMs: 0 });           // no identity, authenticate:false
    const b = simTransport({ network: n, heartbeatMs: 0 });
    await a.start('aa' + 'a1'.repeat(32));
    await b.start('bb' + 'b2'.repeat(32));
    const ok = await a.openConnection('bb' + 'b2'.repeat(32));
    check('legacy authenticate:false peers connect unchanged', ok === true);
    await a.stop(); await b.stop();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
