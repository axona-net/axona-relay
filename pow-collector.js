#!/usr/bin/env node
// =====================================================================
// pow-collector.js — persistent collector for the PoW phone-WASM benchmark.
//
// Subscribes to `pow-bench/results` on the LIVE Axona network and appends every
// result to a JSONL file (dedup by msgId, survives restarts), with a per-device
// running tally. Run it and leave it — testers' results stream in.
//
//   node pow-collector.js                       # prod, → pow-results.jsonl
//   node pow-collector.js --network testnet
//   node pow-collector.js --out /tmp/run1.jsonl --region useast
//
// Each line: { recvTs, msgId, signer, result:{...the published bench result...} }.
// The topic is anchored at the us-east synthetic publisher to match the app's
// reporter, so this receives every tester regardless of their location.
// =====================================================================
import './src/polyfill.js';
import { connectPeer, regionToPublisher } from './src/ops.js';
import { cleanupWebRTC } from './src/polyfill.js';
import { resolveBridgeUrl } from './src/network.js';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';

const argv   = process.argv.slice(2);
const flag   = (n, d) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : d; };
const OUT     = flag('--out', 'pow-results.jsonl');
const REGION  = flag('--region', 'useast');
const NETWORK = flag('--network', undefined);
const TOPIC   = 'pow-bench/results';
const bridge  = resolveBridgeUrl({ network: NETWORK });

// Load already-seen msgIds so restarts don't double-log the replayed backlog.
const seen = new Set();
if (existsSync(OUT)) {
  for (const line of readFileSync(OUT, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try { const r = JSON.parse(line); if (r.msgId) seen.add(r.msgId); } catch { /* */ }
  }
}
const devices = new Map();
const devKey = (r) => (r.device?.deviceLabel || r.device?.deviceId || r.device?.ua || '?');

// Aggregate per (device, candidate, difficulty) for the comparison report we
// publish back so each device can see where it stands.
const LEADERBOARD_TOPIC = 'pow-bench/leaderboard';
const LEADERBOARD_MS    = 15000;
const agg = new Map();

console.log(`pow-collector → ${OUT}\n  topic=${TOPIC} region=${REGION} bridge=${bridge}`);
console.log(`  ${seen.size} result(s) already on file; connecting…`);

const { publisher } = regionToPublisher(REGION);
const h = await connectPeer({ region: REGION, bridge, readyTimeoutSec: 45 });
console.log(`  connected as ${h.nodeId.slice(0, 10)}… — collecting (Ctrl-C to stop)\n`);

await h.peer.sub(TOPIC, (env) => {
  if (!env || env.deleted || !env.message) return;
  const msgId = env.msgId;
  if (!msgId || seen.has(msgId)) return;
  seen.add(msgId);
  let r; try { r = JSON.parse(env.message); } catch { r = { raw: String(env.message).slice(0, 200) }; }
  appendFileSync(OUT, JSON.stringify({ recvTs: new Date().toISOString(), msgId, signer: env.signerPubkey ?? null, result: r }) + '\n');
  const dev = devKey(r);
  devices.set(dev, (devices.get(dev) || 0) + 1);
  // Update the per-(device,candidate,difficulty) aggregate (latest measurement).
  const did = r.device?.deviceId || ('ua:' + (r.device?.ua || '?'));
  const aggKey = `${did}|${r.candidate}|${r.difficulty}`;
  agg.set(aggKey, {
    id:    did,
    label: r.device?.deviceLabel || '',
    ua:    String(r.device?.ua || '').slice(0, 60),
    c:     r.candidate,
    d:     r.difficulty,
    mint:  (r.mint_ms && r.mint_ms.p50 != null) ? Math.round(r.mint_ms.p50) : null,
    mem:   r.peak_wasm_mem_mb ?? null,
    oom:   !!r.oom,
    n:     (agg.get(aggKey)?.n || 0) + 1,
  });
  const mint = (r.mint_ms && r.mint_ms.p50 != null) ? `${Math.round(r.mint_ms.p50)}ms` : '?';
  console.log(`[${seen.size}] ${String(dev).slice(0, 32)} · ${r.candidate} d=${r.difficulty} mint_p50=${mint} mem=${r.peak_wasm_mem_mb}MB oom=${r.oom}`);
}, { publisher, since: 'all' });

function summary() {
  console.log(`\n— ${seen.size} result(s) from ${devices.size} device(s) → ${OUT}`);
  for (const [d, n] of [...devices.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${String(n).padStart(4)}  ${String(d).slice(0, 48)}`);
  }
}
process.on('SIGINT',  async () => { summary(); try { await h.close(); } catch { /* */ } try { cleanupWebRTC(); } catch { /* */ } process.exit(0); });
process.on('SIGTERM', async () => { summary(); try { await h.close(); } catch { /* */ } try { cleanupWebRTC(); } catch { /* */ } process.exit(0); });

// Publish the comparison report back to the devices (keeps the process alive).
setInterval(async () => {
  if (agg.size === 0) return;
  const list = [...agg.values()]
    .filter((e) => e.mint != null)
    .sort((a, b) => (`${a.c}|${a.d}`).localeCompare(`${b.c}|${b.d}`) || a.mint - b.mint)
    .slice(0, 150);
  const report = { ts: new Date().toISOString(), count: list.length, devices: list };
  try {
    const msgId = await h.peer.pub(LEADERBOARD_TOPIC, JSON.stringify(report), { publisher });
    console.log(`  → leaderboard published (${list.length} device-buckets) ${String(msgId).slice(0, 10)}…`);
  } catch (e) {
    console.log('  leaderboard publish failed: ' + (e.message || e));
  }
}, LEADERBOARD_MS);
