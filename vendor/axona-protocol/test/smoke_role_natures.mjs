// smoke_role_natures.mjs — the role-nature table made executable (v4.26.0,
// Phase 7 of the v0.2 program; normative table: Axona-Architecture §VIII).
//
// A role acts in exactly one PRIMARY nature — ROOT, BACKUP, CHILD — plus an
// orthogonal HOLDER flag. Each nature carries obligations and a NAMED eviction
// path; state without an evictor is a leak, obligations without a live
// principal are a storm (I-10; the #333 collapse was a BACKUP whose principal
// was dead — an unmodeled state). This smoke pins:
//   1. nature derivation from ground facts (never a stored copy that drifts)
//   2. BACKUP entry/exit passes through the state machine with role-nature logs
//   3. THE PROMOTION-RESIDUE LEAK (pre-4.26.0): a promoted backup shed its
//      BACKUP nature — backupOf cleared AND out of _backupTopics
//   4. BACKUP eviction path: principal gone + re-homed + BACKUP_EVICT_MS
//   5. nature/holder visible in inspectRoles (I-6 — natures must be observable)
//
// Run: node test/smoke_role_natures.mjs
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { roleNature } from '../src/pubsub/rootClaim.js';
import { BACKUP_EVICT_MS } from '../src/pubsub/constants.js';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m} ${extra}`); fail++; } };
const REG = 0x87n << 248n, idHex = (b) => b.toString(16).padStart(66, '0');
const SELF = REG | 0x011n, TOPIC = REG | 0xabcn;
const NEAR1 = REG | 0xab0n, NEAR2 = REG | 0xabfn;
const PRINCIPAL = idHex(NEAR1).toLowerCase();

function mk({ neighbors = [], bridge = null, replicas = 2 } = {}) {
  const sends = []; const logs = []; const clock = { t: 1_000_000 };
  const dht = {
    getSelfId: () => SELF, onRoutedMessage: () => {},
    routeMessage: (target, type, payload) => sends.push({ target, type, payload }),
    neighbors: () => neighbors, bridgeId: () => bridge,
  };
  const am = new AxonaManager({ dht, now: () => clock.t, rootReplicas: replicas });
  am.nodeId = SELF;
  am.setLogSink((level, type, data) => logs.push({ level, type, data }));
  return { am, sends, logs, clock };
}
const natureLogs = (logs) => logs.filter(l => l.type === 'pubsub:role-nature');

console.log('role natures — the obligation/eviction table, executable\n');

// ── 1+2: REPLICATE ingest enters BACKUP through the state machine ──────────
{
  const { am, logs } = mk({ neighbors: [NEAR1] });
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  const role = am.axonRoles.get(TOPIC);
  ok('REPLICATE ingest → nature BACKUP (derived, not stored)', roleNature(role) === 'backup');
  ok('backup joined _backupTopics (subscribe obligation armed)', am._backupTopics.has(TOPIC));
  const nl = natureLogs(logs);
  ok('BACKUP entry logged exactly once (role-nature, why:replicate)',
    nl.length === 1 && nl[0].data.nature === 'backup' && nl[0].data.why === 'replicate', JSON.stringify(nl.map(l => l.data)));
  // refresh from the SAME principal is bookkeeping, not a transition
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  ok('replica refresh from the same principal logs NO new transition', natureLogs(logs).length === 1);
}

// ── 3: THE PROMOTION-RESIDUE LEAK (regression) ──────────────────────────────
// Pre-4.26.0, promote() left backupOf + _backupTopics membership on the new
// root forever — a ROOT wearing BACKUP state. The _set transition now sheds it.
{
  const { am, logs } = mk({ neighbors: [NEAR1] });
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  const role = am.axonRoles.get(TOPIC);
  // the backup's SUB renewal terminates at self → terminal promotion
  am._rootClaim.promote(role, { via: [] }, { isTerminal: true });
  ok('backup promoted → nature ROOT', roleNature(role) === 'root');
  ok('promotion SHED the backup residue: backupOf cleared', role.backupOf === null);
  ok('…and left _backupTopics (no orphan subscribe obligation)', !am._backupTopics.has(TOPIC));
  const shed = natureLogs(logs).find(l => l.data.why === 'promoted');
  ok('residue shed is a logged nature transition (why:promoted)', !!shed);
}

// ── 4: BACKUP eviction path — principal gone + re-homed + BACKUP_EVICT_MS ──
{
  const { am, logs, clock } = mk({ neighbors: [NEAR1, NEAR2] });
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  const role = am.axonRoles.get(TOPIC);
  // model "re-homed under a live root that isn't us": upstream pinned to NEAR2
  am._upstream.set(TOPIC, [idHex(NEAR2).toLowerCase()]);
  // principal silent past the eviction window
  clock.t += BACKUP_EVICT_MS + 5_000;
  await am.refreshTick();
  ok('stale re-homed backup evicted: backupOf cleared', role.backupOf === null);
  ok('…and out of _backupTopics', !am._backupTopics.has(TOPIC));
  const exit = natureLogs(logs).find(l => l.data.why === 'rehomed-idle');
  ok('eviction is a logged nature transition (why:rehomed-idle)', !!exit && exit.data.nature === 'child');
}

// ── 4b: the eviction NEVER fires while the backup might need to promote ────
{
  const { am, clock } = mk({ neighbors: [NEAR1] });
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  const role = am.axonRoles.get(TOPIC);
  // NOT re-homed (no upstream) — the split-brain-protection case
  clock.t += BACKUP_EVICT_MS + 5_000;
  await am.refreshTick();
  ok('un-rehomed backup is NEVER pruned (it may still win the election)',
    roleNature(role) === 'backup' && am._backupTopics.has(TOPIC));
}

// ── 5: natures are observable (I-6) ─────────────────────────────────────────
{
  const { am } = mk({ neighbors: [NEAR1] });
  await am._onReplicate({ topicId: idHex(TOPIC), from: PRINCIPAL, msgs: [], dels: [] }, { targetId: SELF }); await am._ingestIdle();
  const entry = am.inspectRoles().find(r => r.topicId === idHex(TOPIC));
  ok('inspectRoles carries nature', entry?.nature === 'backup', JSON.stringify(entry));
  ok('inspectRoles carries the holder flag', entry?.holder === false);
  am._hostedTopics.add(TOPIC);
  ok('host() flips holder true (retention, not nature)',
    am.inspectRoles().find(r => r.topicId === idHex(TOPIC))?.holder === true);
}

console.log(`\nResult: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
