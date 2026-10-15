#!/usr/bin/env node
// fence_mesh_pools_env.mjs — relay 0.159.0: RELAY_MESH_POOLS → the kernel's
// meshPools profile. Absent = not armed; malformed = refused at start; a
// feasible profile reaches the transport and reads back armed with enforce
// off; an infeasible one is refused by the kernel before any socket exists.
// Mutant: let poolsFromEnv return {} for a malformed string → check 3 fails.
import { poolsFromEnv, ledgerFromEnv } from '../src/relay.js';
import { webTransport } from '../vendor/axona-protocol/src/transport/web/index.js';

let passed = 0, failed = 0;
const check = (l, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${l}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const refuses = (env) => { try { poolsFromEnv(env); return false; } catch (e) { return /arming refused/.test(e.message); } };

console.log('fence_mesh_pools_env: RELAY_MESH_POOLS');
check('1 absent or empty → null (not armed)', poolsFromEnv({}) === null && poolsFromEnv({ RELAY_MESH_POOLS: '' }) === null && poolsFromEnv({ RELAY_MESH_POOLS: '  ' }) === null);
const p = poolsFromEnv({ RELAY_MESH_POOLS: 'M=50,I=50,H=100' });
check('2 the relay row parses to integers', p.M === 50 && p.I === 50 && p.H === 100);
check('3 a malformed string is refused at start, never guessed', refuses({ RELAY_MESH_POOLS: 'M=50;I=50' }) && refuses({ RELAY_MESH_POOLS: 'M=fifty,I=50' }) && refuses({ RELAY_MESH_POOLS: 'M=50,I=50,X=1' }) && refuses({ RELAY_MESH_POOLS: 'H=100' }));
check('4 optional fields ride along', poolsFromEnv({ RELAY_MESH_POOLS: 'M=6,I=6,H=12,pPending=2,cInbound=2,serveFloor=1,promoteMs=1000' }).promoteMs === 1000);
// through the transport (no socket opened: autoHandshake false, never started)
{
  const t = webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: poolsFromEnv({ RELAY_MESH_POOLS: 'M=50,I=50,H=100' }), autoHandshake: false, log: () => {} });
  const prof = t.poolProfile?.();
  check('5 the transport reads back armed with the profile; enforcement is not part of it (the ledger default, false)', prof?.armed === true && prof.M === 50 && prof.I === 50 && prof.H === 100 && t.channelLedgerStats?.()?.bounds?.enforce === false, JSON.stringify(prof));
  let threw = null;
  try { webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: poolsFromEnv({ RELAY_MESH_POOLS: 'M=50,I=50,H=99' }), autoHandshake: false, log: () => {} }); } catch (e) { threw = e; }
  check('6 an infeasible profile is refused by the kernel before any socket exists', threw && /pool profile: H/.test(threw.message), String(threw));
  const u = webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: poolsFromEnv({}), autoHandshake: false, log: () => {} });
  check('7 without the variable the transport is not armed', (u.poolProfile?.() ?? null) === null);
  // ENFORCEMENT (0.160.0)
  check('8 RELAY_LEDGER_ENFORCE=1 → { enforce: true }; anything else → null (the kernel default, false)', ledgerFromEnv({ RELAY_LEDGER_ENFORCE: '1' })?.enforce === true && ledgerFromEnv({}) === null && ledgerFromEnv({ RELAY_LEDGER_ENFORCE: 'yes' }) === null);
  const e = webTransport({ bridgeUrl: 'ws://127.0.0.1:1', identity: { id: 'ab'.repeat(33) }, meshPools: poolsFromEnv({ RELAY_MESH_POOLS: 'M=50,I=50,H=100' }), meshLedger: ledgerFromEnv({ RELAY_LEDGER_ENFORCE: '1' }), autoHandshake: false, log: () => {} });
  const st = e.channelLedgerStats?.();
  check('9 armed + enforce: the ledger reads enforce true with the profile\'s bounds (H as cPhys, pPending 8, cInbound 4) — it refuses from here on', st?.bounds?.enforce === true && st.bounds.cPhys === 100 && st.bounds.pPending === 8 && st.bounds.cInbound === 4 && st.pools?.M === 50, JSON.stringify(st?.bounds));
}
console.log(`\nfence_mesh_pools_env: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
