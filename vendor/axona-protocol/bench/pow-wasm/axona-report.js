// axona-report.js — publish bench results to the LIVE Axona network, so a local
// node collects results from testers anywhere via pub/sub (no HTTP collector,
// works across the internet). Lazy-loaded by bench.js only when reporting is on.
//
// v0.19.0: migrated to the v0.3+ identity/topic API (kernel 4.x, wire 4):
//   · deriveIdentity            → createNodeIdentity  (node key: address only)
//   · synthetic-publisher topic → descriptor topic { region, name }
//   · unsigned publish          → signWith a persistent author key
//   · all /src imports carry ?v= cache-busts (a returning device previously
//     reloaded a CACHED old kernel graph → old wire → rejected by the bridge,
//     and results were silently lost — the Howard-morning failure mode).
//
// The collector subscribes with:
//   node ../../axona-relay/pow-collector.js --region useast
// and both sides meet on { region:'useast', name:'pow-bench/results' }.
import {
  AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity, createAuthorIdentity,
} from '/src/index.js?v=4.18.2';
import { webTransport } from '/src/transport/web/index.js?v=4.18.2';

const BRIDGE_URL = new URLSearchParams(location.search).get('bridge')
  || (location.hostname.includes('testnet') ? 'wss://testnet.axona.net'
                                            : 'wss://bridge.axona.net');

// Collection topics — descriptor form; the region NAME anchors convergence
// (v0.3 replaced the synthetic us-east publisher). Must match the collector.
const REGION_NAME   = 'useast';
const TOPIC         = { region: REGION_NAME, name: 'pow-bench/results' };
const LEADERBOARD   = { region: REGION_NAME, name: 'pow-bench/leaderboard' };
const ANCHOR        = { lat: 38.0, lng: -78.0 };   // node placement near the topic region

export const reportTopic  = TOPIC.name;
export const reportBridge = BRIDGE_URL;

/**
 * Persistent reporter — connect ONCE, publish MANY. Use in continuous mode so
 * the (heavy) WebRTC connect isn't repeated every iteration.
 * `onLog` (optional) receives kernel warning/error lines for the caller's
 * diagnostic log — connection problems surface there, not in the console.
 * Returns { nodeId, publish(result)→{msgId}, health(), close() }.
 */
export async function createReporter(onStatus = () => {}, onLeaderboard = null, onLog = null) {
  onStatus(`connecting ${BRIDGE_URL}…`);
  const identity  = await createNodeIdentity({ lat: ANCHOR.lat, lng: ANCHOR.lng });
  // Durable author (localStorage) → a device keeps one stable signer across
  // runs, so the collector can attribute repeat results to the same tester.
  const author    = await createAuthorIdentity({ persistAs: 'pow-bench-author' });
  const transport = webTransport({ bridgeUrl: BRIDGE_URL, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: ANCHOR.lat, lng: ANCHOR.lng });
  node.transport  = transport;
  const domain    = new AxonaDomain({ k: 20 });
  const peer      = new AxonaPeer({ domain, node, identity, transport });

  // Forward kernel warnings/errors into the caller's diagnostic log; skip the
  // chatty debug level. (Copy data exports these lines.)
  if (onLog) {
    const safe = (x) => { try { return typeof x === 'string' ? x : JSON.stringify(x, (k, v) => (typeof v === 'bigint' ? v.toString() : v)); } catch { return String(x); } };
    try { peer.onLog?.((...a) => { const line = a.map(safe).join(' '); if (!/\bdebug\b/.test(a[0])) onLog('kernel: ' + line.slice(0, 200)); }); } catch { /* */ }
    try { peer.onError?.((e) => onLog('kernel error: ' + (e?.message || e))); } catch { /* */ }
  }

  await transport.start(identity.id);
  await peer.start();
  const readyBy = Date.now() + 30000;
  while (Date.now() < readyBy && (node.synaptome?.size ?? 0) < 3) {
    onStatus(`forming mesh… synaptome ${node.synaptome?.size ?? 0}`);
    await new Promise((r) => setTimeout(r, 600));
  }
  await new Promise((r) => setTimeout(r, 1500));            // settle so roots are reachable
  onStatus('Axona connected');

  // Subscribe to the collector's comparison report (replays the latest on
  // connect, then updates live). Lets each device see where it stands.
  if (onLeaderboard) {
    try {
      await peer.sub(LEADERBOARD, (env) => {
        if (!env || !env.message) return;
        try { onLeaderboard(typeof env.message === 'string' ? JSON.parse(env.message) : env.message); } catch { /* */ }
      }, { since: 'all' });
    } catch { /* leaderboard is best-effort */ }
  }

  return {
    nodeId: identity.id,
    async publish(result) {
      const msgId = await peer.pub(TOPIC, JSON.stringify(result), { signWith: author });
      return { ok: true, msgId };
    },
    // Connection-health snapshot for the Copy data export: peer.health()
    // (synaptome size, routing truth, …) + raw transport state if exposed.
    health() {
      let h = null; try { h = peer.health(); } catch (e) { h = { error: String(e?.message || e) }; }
      let t = null; try { t = transport.state ?? transport.getState?.() ?? null; } catch { /* */ }
      return { bridge: BRIDGE_URL, peer: h, transport: t };
    },
    async close() {
      try { await peer.leave?.(); } catch { /* */ }
      try { await transport.stop?.(); } catch { /* */ }
    },
  };
}

/** One-shot: connect, publish, grab the latest comparison, disconnect. */
export async function reportToAxona(result, onStatus = () => {}, onLeaderboard = null) {
  const r = await createReporter(onStatus, onLeaderboard);
  try {
    onStatus('publishing result to Axona…');
    const { msgId } = await r.publish(result);
    // Brief wait: lets the publish propagate AND a replayed leaderboard arrive.
    await new Promise((res) => setTimeout(res, onLeaderboard ? 3500 : 2500));
    onStatus(`published to Axona ✓ (msgId ${String(msgId).slice(0, 12)}…)`);
    return { ok: true, msgId };
  } finally {
    await r.close();
  }
}
