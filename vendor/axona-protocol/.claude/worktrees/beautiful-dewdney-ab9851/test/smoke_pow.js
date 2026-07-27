// =====================================================================
// smoke_pow.js — Stage 2 proof-of-work scaffolding (E-1 placement defense /
//                publish anti-flood anchor).
//
// Verifies the mechanism works at difficulty > 0 (and is pubkey-bound), and is
// INERT (no-op) at the shipped difficulty 0 — including that the pow/signerPow
// fields travel the identity, the envelope, and the auth hello so raising
// difficulty later needs no format flag-day.
//
// Run: node test/smoke_pow.js
// =====================================================================

import { POW_DIFFICULTY, powMint, powVerify, powBits, powCalibrate,
         setPowDifficulty, resetPowDifficulty }   from '../src/pow/pow.js';
import { createNodeIdentity, dumpIdentity, loadIdentity } from '../src/identity/index.js';
import { buildEnvelope, verifyEnvelope } from '../src/pubsub/envelope.js';
import { buildAuthHello, verifyAuthHello } from '../src/transport/handshake-auth.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const PK  = 'aa'.repeat(32);   // 64-hex dummy pubkeys
const PK2 = 'bb'.repeat(32);

async function run() {
  console.log('PoW scaffolding (E-1 / Stage 2)\n');

  console.log('── shipped difficulty is 0 (INERT) ──');
  check('POW_DIFFICULTY.transport === 0', POW_DIFFICULTY.transport === 0);
  check('POW_DIFFICULTY.publish === 0',   POW_DIFFICULTY.publish === 0);
  check('mint at difficulty 0 → "" (no work)',     (await powMint({ pubkeyHex: PK, role: 'transport' })) === '');
  check('verify accepts an empty nonce at 0',      (await powVerify({ pubkeyHex: PK, nonce: '', role: 'transport' })) === true);
  check('verify accepts an absent nonce at 0',     (await powVerify({ pubkeyHex: PK, role: 'transport' })) === true);
  check('verify accepts ANY nonce at 0',           (await powVerify({ pubkeyHex: PK, nonce: 'garbage', role: 'transport' })) === true);

  console.log('\n── mechanism works at difficulty > 0 ──');
  const D = 8;
  const nonce = await powMint({ pubkeyHex: PK, role: 'publish', difficulty: D });
  check('mint finds a nonce',                       typeof nonce === 'string' && nonce.length > 0);
  check('minted nonce carries ≥ D leading-zero bits', (await powBits({ pubkeyHex: PK, nonce, role: 'publish' })) >= D);
  check('verify accepts the minted nonce at D',    (await powVerify({ pubkeyHex: PK, nonce, role: 'publish', difficulty: D })) === true);
  check('verify REJECTS a wrong nonce at D',        (await powVerify({ pubkeyHex: PK, nonce: 'zzz', role: 'publish', difficulty: D })) === false);
  check('verify REJECTS an empty nonce at D',       (await powVerify({ pubkeyHex: PK, nonce: '', role: 'publish', difficulty: D })) === false);
  check('PoW is PUBKEY-BOUND (nonce invalid for another key)', (await powVerify({ pubkeyHex: PK2, nonce, role: 'publish', difficulty: D })) === false);

  console.log('\n── device calibration ──');
  const cal = await powCalibrate({ ms: 150 });
  check('calibrate reports a positive hash rate',  cal.hashesPerSec > 0);

  console.log('\n── fields travel the wire (inert at 0) ──');
  const id = await createNodeIdentity({ lat: 51.5, lng: -0.12 });
  check('identity carries a pow field (=== "")',   id.pow === '');
  const env = await buildEnvelope({ topic: { region: 'useast', owner: null, name: 'cats', write: 'open' }, message: 'hi', identity: id });
  check('signed envelope carries signerPow (=== "")', env.signerPow === '');
  check('envelope still verifies',                 (await verifyEnvelope(env))?.ok === true);
  const cbv   = 'cbv:smoke-pow';
  const hello = await buildAuthHello({ identity: id, cbv });
  check('auth hello carries a pow field (=== "")', hello.pow === '');
  check('auth hello verifies (pow no-op at 0)',    (await verifyAuthHello(hello, { cbv })).ok === true);

  console.log('\n── gate ENFORCEMENT at difficulty > 0 (handshake) ──');
  setPowDifficulty('transport', 10);
  const idH   = await createNodeIdentity({ lat: 51.5, lng: -0.12 });   // mints transport pow at 10
  const cbv2  = 'cbv:enforce';
  const helloH = await buildAuthHello({ identity: idH, cbv: cbv2 });
  check('identity mints a non-empty transport pow at >0', idH.pow.length > 0);
  check('hello with a valid pow is admitted',  (await verifyAuthHello(helloH, { cbv: cbv2 })).ok === true);
  check('hello with an EMPTY pow → bad_pow',   (await verifyAuthHello({ ...helloH, pow: '' }, { cbv: cbv2 })).reason === 'bad_pow');
  check('hello with a WRONG pow → bad_pow',    (await verifyAuthHello({ ...helloH, pow: 'zz' }, { cbv: cbv2 })).reason === 'bad_pow');
  resetPowDifficulty();
  const idR = await createNodeIdentity({ lat: 51.5, lng: -0.12 });
  check('after reset, a no-pow hello is admitted again', (await verifyAuthHello(await buildAuthHello({ identity: idR, cbv: cbv2 }), { cbv: cbv2 })).ok === true);

  console.log('\n── gate ENFORCEMENT at difficulty > 0 (publish signerPow) ──');
  setPowDifficulty('publish', 10);
  const idP  = await createNodeIdentity({ lat: 51.5, lng: -0.12 });
  const envP = await buildEnvelope({ topic: { region: 'useast', owner: null, name: 'cats', write: 'open' }, message: 'hi', identity: idP });
  check('envelope carries a non-empty signerPow at >0', envP.signerPow.length > 0);
  check('valid signerPow verifies',  (await powVerify({ pubkeyHex: envP.signerPubkey, nonce: envP.signerPow, role: 'publish' })) === true);
  check('absent signerPow is rejected', (await powVerify({ pubkeyHex: envP.signerPubkey, nonce: '', role: 'publish' })) === false);
  resetPowDifficulty();

  console.log('\n── nonce persistence (dump → load reuses the puzzle) ──');
  setPowDifficulty('transport', 10);
  const idA  = await createNodeIdentity({ lat: 51.5, lng: -0.12 });
  const envA = await dumpIdentity(idA);
  check('dumped envelope persists the pow',  envA.pow === idA.pow && envA.pow.length > 0);
  const idB  = await loadIdentity(envA);
  check('load REUSES the persisted pow (no re-mint)', idB.pow === idA.pow);
  resetPowDifficulty();
  const env0 = await dumpIdentity(await createNodeIdentity({ lat: 51.5, lng: -0.12 }));
  check('at difficulty 0 the dumped pow is ""', env0.pow === '');

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

run();
