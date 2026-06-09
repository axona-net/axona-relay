#!/usr/bin/env node
// cli.js — headless pub/sub against a live Axona network, for scripts/agents.
//
// Reuses the relay's connect machinery (createRelay/startRelay) but skips the
// dashboard: it connects with an EPHEMERAL identity, does one job, prints JSON
// to stdout (logs go to stderr), and exits. Built so a tool like Claude Code
// can publish and subscribe over Bash.
//
//   node src/cli.js pub  <topic> <message…>     # publish, print {ok,msgId,…}
//   node src/cli.js sub  <topic> [--for N] [--since all|new]   # stream JSON lines
//   node src/cli.js pull <topic>                # fetch the latest message
//
// Options:
//   --region <name|code>   synthetic-publisher region (default useast / 0x89,
//                          matching the demo's us-east topics so this interops
//                          with axona.net / the kernel demo)
//   --for <seconds>        sub: how long to listen          (default 25)
//   --since <all|new>      sub: replay backlog or live-only  (default all)
//   --network <prod|testnet>  which network to bootstrap from (default prod)
//   --bridge <wss-url>     explicit bridge URL (overrides --network / BRIDGE_URL)
//   --ready-timeout <sec>  max wait for mesh readiness       (default 30)
//
// Topic convention: the topic string is used verbatim (the demo uses
// `us-east/hello-world`). The synthetic region publisher anchors it so
// K-closest routing lands on that region's roots — both pub and sub MUST use
// the same region, or they derive different topic IDs and never meet.

import './polyfill.js';                     // MUST be first — RTCPeerConnection/WebSocket globals
import { cleanupWebRTC } from './polyfill.js';
import { createEphemeralIdentity } from './identity.js';
import { createRelay, startRelay, stopRelay } from './relay.js';
import { regionName, resolveRegion } from './relay.js';
import { resolveBridgeUrl } from './network.js';
import { geoCellId, geoCellCenter } from '../vendor/axona-protocol/src/utils/s2.js';

// ── arg parsing ──────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const cmd  = argv[0];
const opts = { region: 'useast', for: 25, since: 'all', 'ready-timeout': 30 };
const positional = [];
for (let i = 1; i < argv.length; i++) {
  const a = argv[i];
  if (a.startsWith('--')) { opts[a.slice(2)] = argv[++i]; }
  else positional.push(a);
}
const topic = positional[0];
const message = positional.slice(1).join(' ');

const err  = (obj) => { process.stdout.write(JSON.stringify({ ok: false, ...obj }) + '\n'); };
const out  = (obj) => { process.stdout.write(JSON.stringify(obj) + '\n'); };
const elog = (...a) => process.stderr.write(a.join(' ') + '\n');

if (!['pub', 'sub', 'pull'].includes(cmd) || !topic) {
  elog('usage: node src/cli.js <pub|sub|pull> <topic> [message] [--region r] [--for s] [--since all|new] [--network prod|testnet] [--bridge url]');
  process.exit(2);
}

// ── bridge: --bridge › --network › BRIDGE_URL/RELAY_NETWORK env › prod ──
try { opts.bridge = resolveBridgeUrl({ override: opts.bridge, network: opts.network }); }
catch (e) { err({ error: e.message }); process.exit(2); }

// ── region → synthetic publisher (prefix‖0^256), matching axona-peer/demo ──
const regionCode = resolveRegion(opts.region);
if (regionCode == null) { err({ error: `unknown region "${opts.region}"` }); process.exit(1); }
const center      = geoCellCenter(regionCode);
const prefixHex   = geoCellId(center.lat, center.lng, 8).toString(16).padStart(2, '0');
const publisher   = prefixHex + '0'.repeat(64);   // 66-char synthetic nodeId, top byte = region

async function main() {
  const identity = await createEphemeralIdentity({ lat: center.lat, lng: center.lng });
  const onLog = (level, event, ctx) => {
    if (level === 'error') elog('ERR', event, ctx ? JSON.stringify(ctx).slice(0, 160) : '');
  };
  const { peer, transport } = createRelay({ bridgeUrl: opts.bridge, identity, region: center, onLog });
  peer.onError?.((e) => elog('peer-error', e?.code || '', e?.message || ''));
  await startRelay({ peer, transport });

  // ── wait for mesh readiness: bridge bound into the synaptome ──
  const readyBy = Date.now() + Number(opts['ready-timeout']) * 1000;
  let ready = false;
  while (Date.now() < readyBy) {
    let h; try { h = peer.health(); } catch { h = null; }
    if (h && (h.synaptomeSize >= 1 || (h.peers && h.peers.length >= 1))) { ready = true; break; }
    await new Promise(r => setTimeout(r, 500));
  }
  if (!ready) { err({ error: 'timed out waiting for mesh readiness', bridge: opts.bridge }); await done(peer, transport); process.exit(1); }
  await new Promise(r => setTimeout(r, 1500));   // brief settle so roots are reachable

  if (cmd === 'pub') {
    const msgId = await peer.pub(topic, message, { publisher });
    out({ ok: true, action: 'pub', topic, region: opts.region, prefix: '0x' + prefixHex, msgId, nodeId: identity.id });
    await new Promise(r => setTimeout(r, 1500));  // let it propagate to roots
    await done(peer, transport); process.exit(0);
  }

  if (cmd === 'pull') {
    const env = await peer.pull(null, { topic, publisher });   // null msgId → latest
    out({ ok: true, action: 'pull', topic, region: opts.region, message: env ? env.message : null, found: !!env, msgId: env?.msgId ?? null });
    await done(peer, transport); process.exit(0);
  }

  // cmd === 'sub'
  let n = 0;
  await peer.sub(topic, (env) => {
    n++;
    out({ ok: true, action: 'msg', topic, message: env.message, signer: env.signerPubkey ?? null, seq: env.seq ?? null, ts: env.ts ?? null, msgId: env.msgId ?? null });
  }, { publisher, since: opts.since });
  elog(`subscribed to "${topic}" (region ${opts.region}); listening ${opts.for}s, since=${opts.since}…`);
  await new Promise(r => setTimeout(r, Number(opts.for) * 1000));
  out({ ok: true, action: 'sub-done', topic, received: n });
  await done(peer, transport); process.exit(0);
}

async function done(peer, transport) {
  try { await stopRelay({ peer, transport }); } catch { /* */ }
  cleanupWebRTC();
}

main().catch((e) => { err({ error: e?.message || String(e), stack: e?.stack?.split('\n').slice(0,3) }); cleanupWebRTC(); process.exit(1); });
