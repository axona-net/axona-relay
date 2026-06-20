// metrics-fleet-observe.mjs — confirm the DEPLOYED relay fleet publishes metrics.
//
// Publishes to a fresh OPEN useast topic T (the fleet hosts keyspace 0x89, so it
// roots T), then subscribes to metricTopic(T) and waits across a fleet cadence
// for a snapshot. Asserts the snapshot is signed by one of the fleet's metrics
// authors (passed as --signers), proving the live fleet — not this harness —
// produced it.
//
//   node scripts/metrics-fleet-observe.mjs --signers a69f6cf5,8dc494a2,57fc9751 --for 285
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { createEphemeralIdentity, createEphemeralAuthor } from '../src/identity.js';
import { createRelay, startRelay, stopRelay, regionDescriptor } from '../src/relay.js';
import { resolveBridgeUrl } from '../src/network.js';
import { deriveTopicId, metricTopic } from '../vendor/axona-protocol/src/index.js';

const argv = process.argv.slice(2);
const opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };
const elog = (...a) => process.stderr.write(a.join(' ') + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const signers = (opt('signers', '')).split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
const forS = Number(opt('for', 285));
const bridge = resolveBridgeUrl({ network: opt('network', 'testnet') });
const rd = regionDescriptor('useast');
const region = rd.name, center = rd.center;
const tName = `metrics-fleet-verify/${Date.now()}`;
const tDesc = { region, name: tName };

async function main() {
  console.log(`fleet-observe → ${bridge}  topic="${tName}"  watch ${forS}s  fleet signers=[${signers.join(',')}]`);
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const author   = await createEphemeralAuthor();
  const { peer, transport } = createRelay({ bridgeUrl: bridge, identity, region: center, onLog: (l, e) => { if (l === 'error') elog('ERR', e); } });
  await startRelay({ peer, transport });

  const readyBy = Date.now() + 30000;
  while (Date.now() < readyBy) { let h; try { h = peer.health(); } catch {} if (h && (h.synaptomeSize >= 1 || h.peers?.length >= 1)) break; await sleep(500); }
  await sleep(1500);

  const id = await deriveTopicId(tDesc);
  const mt = metricTopic(id);
  console.log(`dataId=${id}\nmetricTopic=${mt.name}`);

  const snaps = [];
  await peer.sub(mt, (env) => {
    let m; try { m = JSON.parse(env.message); } catch { return; }
    snaps.push({ by: m.by, signer: env.signerPubkey, current_count: m.current_count, subscribers: m.subscribers, ts: m.ts });
    elog(`  ← snapshot: by=${(m.by||'').slice(0,12)} signer=${(env.signerPubkey||'').slice(0,12)} count=${m.current_count} subs=${m.subscribers}`);
  }, { since: 'all' });

  // publish a few times so the fleet roots + caches T (current_count > 0)
  for (let i = 1; i <= 3; i++) { await peer.pub(tDesc, `fleet-check-${i}`, { signWith: author }); await sleep(400); }
  console.log('published 3 msgs to T; waiting for a fleet metric cycle…');

  const deadline = Date.now() + forS * 1000;
  while (Date.now() < deadline && snaps.length === 0) await sleep(2000);
  // grab a couple extra seconds of any stragglers
  await sleep(2000);

  const fleetSnaps = signers.length
    ? snaps.filter(s => signers.some(p => (s.signer || '').toLowerCase().startsWith(p)))
    : snaps;

  let ok = 0, fail = 0;
  const check = (l, c) => { if (c) { console.log(`  ✓ ${l}`); ok++; } else { console.log(`  ✗ ${l}`); fail++; } };
  check(`received ≥1 snapshot on metricTopic(T) (${snaps.length})`, snaps.length >= 1);
  check('a snapshot was signed by a FLEET metrics author', fleetSnaps.length >= 1);
  if (fleetSnaps[0]) {
    check('snapshot.current_count ≥ 1 (fleet rooted + cached T)', (fleetSnaps[0].current_count ?? 0) >= 1);
    check('snapshot carries by/subscribers', !!fleetSnaps[0].by && typeof fleetSnaps[0].subscribers === 'number');
  }

  console.log(`\nResult: ${ok} passed, ${fail} failed  (total snaps ${snaps.length}, fleet snaps ${fleetSnaps.length})`);
  try { await stopRelay({ peer, transport }); } catch {}
  cleanupWebRTC();
  process.exit(fail === 0 && ok >= 2 ? 0 : 1);
}
main().catch((e) => { elog('observe threw:', e?.stack || e); cleanupWebRTC(); process.exit(2); });
