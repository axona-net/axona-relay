// council_readonly_fence.mjs — the read-only exemption lets inspection through
// and gates everything else. Run: node test/council_readonly_fence.mjs
//
// (Lives in a FILE, not an inline shell string, because the council PreToolUse
// hook scans the literal command — a test that TYPED "docker compose up" as an
// argument would trip the very gate it is testing. The strings are data here.)
import { deployReasons, isReadOnlyRemote } from '../scripts/council-scope.mjs';

let fail = 0;
const gated = (c) => {
  const r = deployReasons(c);
  if (!r.length) { console.log(`  ✗ SHOULD gate but did not: ${c}`); fail++; }
  else console.log(`  ok gate   - ${c}  → ${r[0]}`);
};
const allowed = (c) => {
  const r = deployReasons(c);
  if (r.length) { console.log(`  ✗ SHOULD allow but gated: ${c}  → ${r[0]}`); fail++; }
  else console.log(`  ok allow  - ${c}`);
};

const UP = 'docker' + ' compose up -d';   // split so THIS file does not trip the hook
const PULL = 'git' + ' pull origin main';
const RESTART = 'systemctl restart coturn';
const PUSH = 'git' + ' push origin testnet:main';

console.log('READ-ONLY on a deploy host — must ALLOW:');
allowed(`ssh axona-bridge 'docker logs coturn'`);
allowed(`ssh axona-bridge 'docker ps'`);
allowed(`ssh axona-bridge 'cat /opt/axona-bridge/docker-compose.yml'`);
allowed(`ssh axona-bridge 'docker compose logs coturn | grep -i relay'`);
allowed(`ssh axona-bridge 'journalctl -u coturn --since -10min'`);
allowed(`ssh axona-bridge 'systemctl status coturn'`);
allowed(`ssh root@64.227.2.28 'tail -n 200 /var/log/coturn.log'`);

console.log('\nMUTATING or ambiguous on a deploy host — must GATE:');
gated('ssh axona-bridge');                                  // interactive: can do anything
gated(`ssh axona-bridge '${UP}'`);
gated(`ssh axona-bridge '${PULL}'`);
gated(`ssh axona-bridge '${RESTART}'`);
gated(`ssh axona-bridge 'docker logs coturn > /tmp/out'`);  // redirect = write
gated(`ssh axona-bridge 'docker logs coturn && rm -rf /opt'`);
gated(`ssh axona-bridge 'journalctl --vacuum-size=1M'`);
gated(`ssh axona-bridge 'curl -X POST http://localhost/admin'`);
gated(`ssh axona-bridge 'docker exec coturn sh'`);          // exec is not read-only

console.log('\nLOCAL deploy paths — must still GATE (unchanged):');
gated(PUSH);

console.log('\nunit: isReadOnlyRemote directly');
const u = (s, want) => { const g = isReadOnlyRemote(s); if (g !== want) { console.log(`  ✗ isReadOnlyRemote(${JSON.stringify(s)}) = ${g}, want ${want}`); fail++; } else console.log(`  ok ${want ? 'RO ' : 'not'} - ${s}`); };
u('docker logs coturn', true);
u('cat a | grep b | tail -5', true);
u('', false);
u('rm x', false);
u('cat a > b', false);
u(`${UP}`, false);

console.log(`\n${fail ? `✗ ${fail} failed` : '✓ all checks passed'}`);
process.exit(fail ? 1 : 0);
