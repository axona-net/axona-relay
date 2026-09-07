// route-reach.mjs — can we ROUTE-connect to each of the ~20 nodes next door?
//
// The non-pub/sub delivery test: from one fresh driver, route a probe addressed
// to each known fleet node's id and check WHERE routing terminates. A node is
// XOR-closest to its OWN id, so a route that reaches the addressed node
// terminates there -> result.atNode === targetId. If greedy stalls at a
// last-mile local minimum, it terminates one neighbourhood short -> atNode is
// some OTHER node (recorded). No pub/sub, no subscription tree: this isolates
// the routed-delivery path (routeMessage -> route_msg forward) that was
// reaching only ~2 of ~20 before the lookup-assisted escape.
//
//   ROUTE_GREEDY_ESCAPE=0  -> baseline (escape off)   expect ~2/N
//   (default)              -> escape on               expect ~N/N
//
//   BRIDGE=wss://testnet.axona.net SETTLE_MS=25000 node harness/route-reach.mjs
import '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

const env = (k, d) => process.env[k] ?? d;
const REGION = env('REGION', 'eagle'), BRIDGE = env('BRIDGE', 'wss://testnet.axona.net');
const SETTLE = Number(env('SETTLE_MS', '25000'));
const CENSUS = env('CENSUS_FILE', '/private/tmp/claude-501/-Users-croqueteer-Documents-claude/2e2326d1-7e14-4885-9a45-e2230b53efb8/scratchpad/fleet-census.json');
const DISCOVER = Number(env('DISCOVER', '24'));   // random targets to sweep for census
const hex = (x) => (typeof x === 'bigint' ? x.toString(16) : String(x).replace(/^0x/, ''));
const log = (m) => console.error(`[reach] ${m}`);
const escape = process.env.ROUTE_GREEDY_ESCAPE !== '0';

const { peer } = await connectPeer({ region: REGION, bridge: BRIDGE });
log(`connected ${BRIDGE}; escape=${escape ? 'ON' : 'OFF(baseline)'}; settling ${SETTLE / 1000}s`);
await new Promise((r) => setTimeout(r, SETTLE));

const t = peer._node?.transport;
const selfBig = peer._node?.id;
const bridgeBig = t?.bridgeNodeIdBig ?? null;
const isConn = (b) => { try { return t?.isConnected?.(b) === true; } catch { return false; } };

// Build a fleet CENSUS that is the SAME across A/B runs (persist to CENSUS file).
// Discovery uses findKClosest — the iterative lookup, which the escape does NOT
// touch — so the target set is identical whether the escape is on or off. The
// census spreads across the keyspace, so most targets are NOT directly meshed
// with this driver (the multi-hop case where greedy stalls).
let known;
if (existsSync(CENSUS)) {
  known = JSON.parse(readFileSync(CENSUS, 'utf8')).map((h) => BigInt('0x' + h));
  log(`census loaded from file: ${known.length} nodes`);
} else {
  const seen = new Set();
  for (let i = 0; i < DISCOVER; i++) {
    try {
      const target = await deriveTopicIdBig({ region: REGION, name: `harness/census/t${i}` });
      const arr = await peer.findKClosest(target, 8);
      if (Array.isArray(arr)) for (const id of arr) if (typeof id === 'bigint') seen.add(id.toString(16));
    } catch { /* keep sweeping */ }
  }
  seen.delete(hex(selfBig));
  if (bridgeBig !== null) seen.delete(hex(bridgeBig));
  known = [...seen].map((h) => BigInt('0x' + h));
  writeFileSync(CENSUS, JSON.stringify([...seen]));
  log(`census discovered + written: ${known.length} nodes (${DISCOVER} lookups)`);
}
const adjacent = known.filter(isConn).length;
log(`census=${known.length}  directly-connected(next-door)=${adjacent}  non-adjacent(multi-hop)=${known.length - adjacent}`);

let reached = 0, stalled = 0, errored = 0;
const stalls = [];
for (const id of known) {
  try {
    const r = await peer.routeMessage(id, 'local_probe', {});
    const at = (r && typeof r.atNode === 'bigint') ? r.atNode : null;
    if (at === id) {
      reached++;
    } else {
      stalled++;
      stalls.push(`${hex(id).slice(0, 8)} -> stalled at ${at ? hex(at).slice(0, 8) : 'null'} (hops=${r?.hops ?? '?'}${r?.terminal ? ',terminal' : ''}${r?.exhausted ? ',exhausted' : ''})`);
    }
  } catch (e) {
    errored++;
    stalls.push(`${hex(id).slice(0, 8)} -> ERR ${String(e?.message || e).slice(0, 50)}`);
  }
}

log('─'.repeat(60));
for (const s of stalls) log('  ' + s);
log('─'.repeat(60));
log(`RESULT (escape=${escape ? 'ON' : 'OFF'}): reached ${reached}/${known.length}  stalled=${stalled}  errored=${errored}`);
log(`next-door (directly-connected) targets: ${adjacent}; of those the routed probe reached its address: see per-row above`);
try { await peer.leave?.({ timeoutMs: 6000 }); } catch {}
process.exit(0);
