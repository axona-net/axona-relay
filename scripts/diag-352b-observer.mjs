// diag-352b-observer.mjs — a standing subscriber on the axona.bot owner topic,
// with full root-attachment instrumentation. Emulates the "existing live
// subscriber" party from the 2026-07-18 live-delivery gap.
//
// Usage: node scripts/diag-352b-observer.mjs <label> <minutes> [since]
//   since: all | live   (default all)
// Emits JSONL on stdout; run in background with output redirected.
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { instrument, setLabel, log, snapshot, lookupTerminus, topicBig, DESCRIPTOR, managerOf } from './diag-352b-common.mjs';
import { connectPeer } from '../src/ops.js';

const label = process.argv[2] || 'S';
const minutes = Number(process.argv[3]) || 20;
const since = process.argv[4] || 'all';

setLabel(label);
await topicBig();          // resolve before instrument filters
instrument();

const s = await connectPeer({ region: 'useast' });
log('connected', { nodeId: s.nodeId.slice(0, 12), fullNodeId: s.nodeId, since });
managerOf(s.peer);         // force manager creation so snapshots work pre-sub

const received = [];
await s.peer.sub(DESCRIPTOR, (env) => {
  received.push(env.msgId);
  log('app-delivery', { msgId: (env.msgId || '').slice(0, 12), seq: env.seq ?? null, ts: env.ts ?? null, text: env?.message?.text ?? null, deleted: !!env.deleted });
}, { since: since === 'live' ? undefined : since });
log('subscribed', { since });

const endAt = Date.now() + minutes * 60_000;
let lastLookup = 0;
while (Date.now() < endAt) {
  await new Promise(r => setTimeout(r, 5000));
  log('snap', snapshot(s.peer));
  if (Date.now() - lastLookup > 60_000) {
    lastLookup = Date.now();
    log('lookup', await lookupTerminus(s.peer));
  }
}
log('done', { totalReceived: received.length });
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
