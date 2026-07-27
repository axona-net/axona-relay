// =====================================================================
// smoke_turn_encode.mjs — #343: TURN REST credentials must survive the
// node-datachannel polyfill's URL flattening. Under Node the mesh percent-
// encodes username/credential (libdatachannel percent-decodes userinfo —
// proven empirically against the testnet coturn); in a browser the fields
// pass through untouched (the native stack sends them literally).
// Run: node test/smoke_turn_encode.mjs
// =====================================================================
import { MeshManager } from '../src/transport/web/mesh.js';

let passed = 0, failed = 0;
const check = (label, cond, extra = '') => {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label} ${extra}`); failed++; }
};

const mesh = Object.create(MeshManager.prototype);
mesh._turn = { urls: ['turn:turn.example:3478'], username: '1784673103:probe', credential: 'aB+c/d=' };

const cfg = mesh._iceConfig();
const turn = cfg.iceServers[cfg.iceServers.length - 1];
check('node: username percent-encoded (colon survives the URL flattener)',
  turn.username === '1784673103%3Aprobe', `(got ${turn.username})`);
check('node: credential percent-encoded (base64 +/= safe in userinfo)',
  turn.credential === 'aB%2Bc%2Fd%3D', `(got ${turn.credential})`);
check('node: original cached turn object untouched',
  mesh._turn.username === '1784673103:probe');

// simulate a browser: window.document present → passthrough
globalThis.window = { document: {} };
const cfgB = mesh._iceConfig();
const turnB = cfgB.iceServers[cfgB.iceServers.length - 1];
check('browser: fields pass through raw (native stack sends them literally)',
  turnB.username === '1784673103:probe' && turnB.credential === 'aB+c/d=');
delete globalThis.window;

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
