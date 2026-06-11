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
// SELF-HEALING: a watchdog checks peer health every 20s and the leaderboard
// publish doubles as a send-path probe; if the bridge connection drops, the
// collector tears down and reconnects with backoff, then re-subscribes
// (since:'all' + the in-memory `seen` set ⇒ it backfills anything missed during
// the outage and never double-logs). Previously a dropped connection silently
// stopped collection until a manual restart.
//
// Each line: { recvTs, msgId, signer, result:{...the published bench result...} }.
// =====================================================================
import './src/polyfill.js';
import { connectPeer, regionToPublisher } from './src/ops.js';
import { cleanupWebRTC } from './src/polyfill.js';
import { resolveBridgeUrl } from './src/network.js';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { exec } from 'node:child_process';

const argv   = process.argv.slice(2);
const flag   = (n, d) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : d; };
const OUT     = flag('--out', 'pow-results.jsonl');
const REGION  = flag('--region', 'useast');
const NETWORK = flag('--network', undefined);
const TOPIC   = 'pow-bench/results';
const bridge  = resolveBridgeUrl({ network: NETWORK });
const sleep   = (ms) => new Promise((r) => setTimeout(r, ms));

// Load already-seen msgIds so restarts/reconnects don't double-log the backlog.
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
const WATCHDOG_MS       = 20000;
const agg = new Map();
const { publisher } = regionToPublisher(REGION);

// ── connection state (rebuilt on reconnect) ─────────────────────────
let h = null;
let reconnecting = false;
let badHealth = 0;

function onMessage(env) {
  if (!env || env.deleted || !env.message) return;
  const msgId = env.msgId;
  if (!msgId || seen.has(msgId)) return;
  seen.add(msgId);
  let r; try { r = JSON.parse(env.message); } catch { r = { raw: String(env.message).slice(0, 200) }; }
  appendFileSync(OUT, JSON.stringify({ recvTs: new Date().toISOString(), msgId, signer: env.signerPubkey ?? null, result: r }) + '\n');
  const dev = devKey(r);
  devices.set(dev, (devices.get(dev) || 0) + 1);
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
}

async function connect() {
  h = await connectPeer({ region: REGION, bridge, readyTimeoutSec: 45,
    onError: (e) => console.log('  peer error: ' + (e?.message || e)) });
  await h.peer.sub(TOPIC, onMessage, { publisher, since: 'all' });
  console.log(`  connected as ${h.nodeId.slice(0, 10)}… — collecting`);
}

function isConnected() {
  try { const hl = h?.peer?.health(); return !!(hl && (hl.synaptomeSize >= 1 || (hl.peers && hl.peers.length >= 1))); }
  catch { return false; }
}

async function reconnect(reason) {
  if (reconnecting) return;
  reconnecting = true;
  console.log(`\n‼ connection lost (${reason}) — reconnecting…`);
  try { await h?.close(); } catch { /* */ }
  try { cleanupWebRTC(); } catch { /* */ }
  h = null;
  for (let attempt = 1; ; attempt++) {
    try { await connect(); console.log(`  ✓ reconnected on attempt ${attempt}\n`); break; }
    catch (e) {
      const wait = Math.min(30000, 2000 * attempt);
      console.log(`  reconnect attempt ${attempt} failed: ${e.message || e} — retry in ${Math.round(wait / 1000)}s`);
      await sleep(wait);
    }
  }
  badHealth = 0;
  reconnecting = false;
}

// ── boot ────────────────────────────────────────────────────────────
console.log(`pow-collector → ${OUT}\n  topic=${TOPIC} region=${REGION} bridge=${bridge}`);
console.log(`  ${seen.size} result(s) already on file; connecting…`);
await connect();
console.log(`  collecting (auto-reconnect armed; Ctrl-C to stop)\n`);

// Watchdog: a dropped bridge connection shows up as no mesh connectivity.
// Require a few consecutive bad checks (~60s) before reconnecting to ride out
// transient blips.
setInterval(() => {
  if (reconnecting) return;
  if (isConnected()) { badHealth = 0; return; }
  if (++badHealth >= 3) reconnect('health check: no mesh connectivity');
}, WATCHDOG_MS);

// Publish the comparison report back to the devices. Doubles as a send-path
// liveness probe: a publish failure means the connection is dead → reconnect.
setInterval(async () => {
  if (reconnecting || !h || agg.size === 0) return;
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
    reconnect('leaderboard publish failed');
  }
}, LEADERBOARD_MS);

// Off-machine backup: gzip + force-push the cumulative history to GitHub every
// few hours so a total machine/disk loss can't take the full history with it.
// Tied to the collector's lifetime (it backs up exactly while it's collecting).
const BACKUP_MS = 2 * 3600 * 1000;
const BACKUP_SH = '/Users/croqueteer/Documents/claude/axona-relay/backup-pow-data.sh';
setInterval(() => {
  exec(BACKUP_SH, (err, stdout, stderr) => {
    console.log(err ? '  backup failed: ' + (stderr || err.message).trim() : '  ' + stdout.trim());
  });
}, BACKUP_MS);

function summary() {
  console.log(`\n— ${seen.size} result(s) from ${devices.size} device(s) → ${OUT}`);
  for (const [d, n] of [...devices.entries()].sort((a, b) => b[1] - a[1])) {
    console.log(`    ${String(n).padStart(4)}  ${String(d).slice(0, 48)}`);
  }
}
process.on('SIGINT',  async () => { summary(); try { await h?.close(); } catch { /* */ } try { cleanupWebRTC(); } catch { /* */ } process.exit(0); });
process.on('SIGTERM', async () => { summary(); try { await h?.close(); } catch { /* */ } try { cleanupWebRTC(); } catch { /* */ } process.exit(0); });
