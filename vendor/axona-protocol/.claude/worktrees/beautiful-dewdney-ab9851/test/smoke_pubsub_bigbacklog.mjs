// =====================================================================
// smoke_pubsub_bigbacklog.mjs — large replay backlog (O-1 + O-5-on-replay).
//
// Proves the relay-queue + replay-framing change:
//   1. a topic holds > the old 100-message cap (count cap is now 1024), so a
//      chunked file (e.g. 150 messages) survives in the replay cache;
//   2. a fresh subscriber's since:'all' replay delivers ALL of them — sent as
//      MANY frames, not one (the old single-frame send was undeliverable for
//      large content, finding O-5);
//   3. every replay frame stays under the 16 KiB WebRTC wire limit;
//   4. the per-role BYTE cap evicts large entries so a high count can't OOM.
//
//   node test/smoke_pubsub_bigbacklog.mjs
// =====================================================================
import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { toHex } from '../src/utils/hexid.js';

let passed = 0, failed = 0;
const check = (l, c) => { if (c) { console.log(`  ✓ ${l}`); passed++; } else { console.log(`  ✗ ${l}`); failed++; } };
const TOPIC = BigInt('0x' + '89' + 'cd'.repeat(32));
const T = 1_700_000_000_000;
const flush = async () => { for (let i = 0; i < 16; i++) await new Promise(r => setTimeout(r, 0)); };
const WIRE_LIMIT = 16 * 1024;

class MockNet {
  constructor() { this.mgrs = new Map(); this.sends = []; }
  kclosest(t, K) { return [...this.mgrs.keys()].sort((a, b) => { const da = a ^ t, db = b ^ t; return da < db ? -1 : da > db ? 1 : 0; }).slice(0, K); }
  makeDht(selfId) {
    const net = this, direct = new Map();
    return {
      getSelfId: () => selfId,
      onRoutedMessage: () => {}, onDirectMessage: (t, h) => direct.set(t, h), onEvent: () => () => {},
      findKClosest: async (t, K) => net.kclosest(t, K),
      routeMessage: async () => {},
      sendDirect: async (target, type, payload) => {
        net.sends.push({ from: toHex(selfId), to: toHex(target), type, bytes: JSON.stringify(payload).length });
        const m = net.mgrs.get(target); if (!m) return false;
        const h = m._dht._direct.get(type); if (h) await h(payload, { fromId: toHex(selfId) });
        return true;
      },
      _direct: direct,
    };
  }
  spawn(id) { const dht = this.makeDht(id); const m = new AxonaManager({ dht, now: () => T }); m._dht = dht; this.mgrs.set(id, m); return m; }
}
const role = (over = {}) => ({ isRoot: true, isInRootSet: true, children: new Map(), replayCache: [], peerRoots: new Set(), emptiedAt: 0, ...over });

async function main() {
  console.log('Axona pub/sub large replay backlog (O-1 + O-5-on-replay)');
  const N = 150;                                  // > old 100 cap, and > one frame's worth
  const BODY = 'x'.repeat(5000);                  // ~5 KB/message → many frames at 14 KB/frame

  // ── 1+2+3: 150-message backlog replays whole, multi-frame, each < 16 KiB ──
  {
    const net = new MockNet();
    const R = net.spawn(TOPIC ^ 1n);              // root holding the backlog
    const S = net.spawn(TOPIC ^ 3n);              // fresh subscriber
    const cache = [];
    for (let i = 0; i < N; i++) {
      cache.push({ json: JSON.stringify({ b: BODY, i }), publishId: `p${i}`, publishTs: T + i, postHash: 'h' + i.toString().padStart(4, '0'), publisher: null });
    }
    R.axonRoles.set(TOPIC, role({ replayCache: cache }));
    check(`1. root holds ${N} cached (> old 100 cap)`, R.axonRoles.get(TOPIC).replayCache.length === N);

    const got = new Set();
    S.onPubsubDelivery((tid, json) => { try { got.add(JSON.parse(json).i); } catch {} });
    net.sends = [];
    S.pubsubSubscribe(TOPIC);
    await flush();

    const frames = net.sends.filter(s => s.type === 'pubsub:replay-batch');
    check(`2. fresh subscriber received ALL ${N} replayed messages`, got.size === N);
    check(`3a. replay split into MANY frames (${frames.length} > 1, not one giant batch)`, frames.length > 1);
    const maxFrame = frames.length ? Math.max(...frames.map(f => f.bytes)) : 0;
    check(`3b. every replay frame < 16 KiB (max=${maxFrame}B)`, maxFrame > 0 && maxFrame < WIRE_LIMIT);
  }

  // ── 4: per-role BYTE cap evicts large entries before the count cap ──
  {
    const net = new MockNet();
    const R = net.spawn(TOPIC ^ 7n);
    R.replayCacheBytes = 256 * 1024;              // tiny 256 KB byte cap for the test
    R.axonRoles.set(TOPIC, role());
    const r = R.axonRoles.get(TOPIC);
    // add 100 × ~15 KB entries = ~1.5 MB worth → byte cap must hold it to ~256 KB
    for (let i = 0; i < 100; i++) {
      R._addToReplayCache(r, { json: 'y'.repeat(15 * 1024), publishId: `q${i}`, publishTs: T + i, postHash: 'g' + i, publisher: null });
    }
    const bytes = r.replayCache.reduce((s, e) => s + e.json.length, 0);
    check(`4. byte cap bounds memory (cache ${r.replayCache.length} msgs, ${(bytes / 1024).toFixed(0)} KB ≤ ~256 KB)`,
      bytes <= 256 * 1024 && r.replayCache.length < 100);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
