// =====================================================================
// fence_arming_gate.mjs — the armed-canary arming path, proven end to end
// at the unit level (Aster f9b7bd31 condition 3: an automated test must
// prove the env flags reach the peer; Vega b8a1164d: the old vendor
// understands synaptomeMaintain, so an un-gated flag is the June storm).
//
// Three exported pure pieces from src/relay.js:
//   1. armingFromEnv       — env → the proposal's exact constants, verbatim
//   2. assertArmingSupported — any arming env below kernel 4.67 THROWS
//   3. assertArmedModules  — a requested module missing from the peer THROWS
//                            (version string = claim; peer state = fact)
//
// Run: node test/fence_arming_gate.mjs
// =====================================================================
import { ARM_ENVS, armingFromEnv, assertArmingSupported, assertArmedModules } from '../src/relay.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const throws = (fn) => { try { fn(); return false; } catch { return true; } };

console.log('armed-canary arming fence\n');

// ── 1. env → constants, exact (the proposal table, verbatim) ──────────
{
  const off = armingFromEnv({});
  check('1 default: all four off, nothing armed', off.armedEnvs.length === 0
    && off.armMaintain === null && off.armGate === null && off.armGuard === null && off.armPresence === null);
  const all = armingFromEnv({ RELAY_SYNAPTOME_MAINTAIN: '1', RELAY_ADMISSION_GATE: '1', RELAY_ATTEMPT_GUARD: '1', RELAY_PRESENCE: '1' });
  check('1 all four envs recognized', all.armedEnvs.length === 4 && ARM_ENVS.every((e) => all.armedEnvs.includes(e)));
  check('1 maintenance = the historical configuration, unchanged',
    JSON.stringify(all.armMaintain) === JSON.stringify({ kNear: 5, intervalMs: 15000, maxPerTick: 3 }));
  check('1 gate = council integers + reserve-from-cap lane',
    JSON.stringify(all.armGate) === JSON.stringify({ kNear: 5, sparseFloor: 2, kJoin: 2, laneCooldownMs: 5000, laneWindowMs: 300000 }));
  check('1 guard = live-validated cap + production backoff/pacing',
    JSON.stringify(all.armGuard) === JSON.stringify({ maxAttempts: 4, baseMs: 30000, factor: 2, refillWindowMs: 60000, deficitBaseMs: 30000, deficitFactor: 2 }));
  check('1 presence = announce-on-start + 30s relay rate',
    JSON.stringify(all.armPresence) === JSON.stringify({ announceOnStart: true, relayRateMs: 30000 }));
  const one = armingFromEnv({ RELAY_ATTEMPT_GUARD: '1' });
  check('1 flags are independent', one.armedEnvs.length === 1 && one.armGuard !== null && one.armMaintain === null);
}

// ── 2. the version gate: old vendor + any arming env = refused ────────
{
  const armed = ['RELAY_SYNAPTOME_MAINTAIN'];
  check('2 GATE: 4.62.2 + maintenance env THROWS (the June storm, refused at launch)', throws(() => assertArmingSupported('4.62.2', armed)));
  check('2 GATE: 4.63.0 refused too', throws(() => assertArmingSupported('4.63.0', armed)));
  check('2 GATE: 4.66.1 refused (guard did not exist yet)', throws(() => assertArmingSupported('4.66.1', ['RELAY_ATTEMPT_GUARD'])));
  check('2 GATE: 4.67.0 passes', !throws(() => assertArmingSupported('4.67.0', armed)));
  check('2 GATE: 4.67.1 passes with all four', !throws(() => assertArmingSupported('4.67.1', ARM_ENVS.slice())));
  check('2 GATE: nothing armed = no gate, any version', !throws(() => assertArmingSupported('4.62.2', [])));
}

// ── 3. module landing: a silently-discarded option is a refused start ──
{
  const fullPeer = {
    _maintainCfg: { kNear: 5, intervalMs: 15000, maxPerTick: 3 },
    _gateCfg: { kNear: 5, sparseFloor: 2, kJoin: 2, laneCooldownMs: 5000, laneWindowMs: 300000 },
    _attemptGuard: { maxAttempts: 4, baseMs: 30000, factor: 2, refillWindowMs: 60000 },
    _presenceCfg: { announceOnStart: true, relayRateMs: 30000 },
  };
  const eff = assertArmedModules(fullPeer, ARM_ENVS.slice());
  check('3 LANDED: all four modules present → effective constants returned',
    eff.synaptomeMaintain?.intervalMs === 15000 && eff.admissionGate?.kJoin === 2
    && eff.attemptGuard?.maxAttempts === 4 && eff.presence?.announceOnStart === true);
  // The 4.62.2 shape Aster named: maintenance lands, the other three silently discard.
  const oldVendorPeer = { _maintainCfg: { kNear: 5, intervalMs: 15000, maxPerTick: 3 } };
  check('3 MISSING: the exact 4.62.2 silent-discard shape (maintenance only) THROWS before join',
    throws(() => assertArmedModules(oldVendorPeer, ARM_ENVS.slice())));
  check('3 MISSING: guard alone requested, guard absent → refused', throws(() => assertArmedModules({}, ['RELAY_ATTEMPT_GUARD'])));
  check('3 SCOPED: unrequested modules are not required', !throws(() => assertArmedModules(oldVendorPeer, ['RELAY_SYNAPTOME_MAINTAIN'])));
  check('3 OFF: nothing armed asserts nothing', !throws(() => assertArmedModules({}, [])));
}

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
