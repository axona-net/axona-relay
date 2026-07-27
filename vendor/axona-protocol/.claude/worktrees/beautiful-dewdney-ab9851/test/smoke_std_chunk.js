// =====================================================================
// smoke_std_chunk.js — @axona/protocol/std/chunk correctness.
//
//   1. byte-exact round-trip (shuffled order)
//   2. every produced message stays under maxMessageBytes (O-5)
//   3. duplicate deliveries tolerated, completes once (CDC-2: distinct index, not receipt count)
//   4. manifest loss tolerated (count learned from any chunk)
//   5. foreign-fileId / malformed garbage ignored (CDC-5)
//   6. publishChunkedBytes refuses files over the replay-cache ceiling (O-1)
//   7. high-level publish→receive round-trip via a mock peer
//   8. receiveChunkedBytes REJECTS on a missing chunk instead of hanging (CDC-1)
//
//   node test/smoke_std_chunk.js
// =====================================================================
import { chunkBytes, createReassembler, stringToBytes, bytesToString,
         publishChunkedBytes, receiveChunkedBytes, rawChunkSize } from '../std/chunk.js';

let passed = 0, failed = 0;
const check = (label, cond) => { if (cond) { console.log(`  ✓ ${label}`); passed++; } else { console.log(`  ✗ ${label}`); failed++; } };
const eqBytes = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);
function shuffle(a) { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = (i * 2654435761) % (i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; }
function makeBytes(n) { const u = new Uint8Array(n); for (let i = 0; i < n; i++) u[i] = (i * 31 + 7) & 0xff; return u; }

// In-memory peer: stores publishes per topic, replays on sub(since:'all').
//   dropIndices  — chunk indices the SUB/replay never delivers (a lost chunk).
//   dropFirst    — chunk indices dropped (not stored) on their FIRST publish but
//                  stored on re-publish — simulates a burst drop that the
//                  publish-side verify+repair pass must detect and heal.
function mockPeer({ dropIndices = new Set(), dropFirst = new Set() } = {}) {
  const store = new Map();
  const droppedOnce = new Set();
  return {
    published: store,
    async pub(topic, message, _opts) {
      if (message?.k === 'c' && dropFirst.has(message.i) && !droppedOnce.has(message.i)) {
        droppedOnce.add(message.i);                 // simulate the first send getting dropped on the wire
        return 'dropped';
      }
      if (!store.has(topic)) store.set(topic, []);
      const msgId = 'm' + (store.get(topic).length);
      store.get(topic).push({ message, msgId });
      return msgId;
    },
    async sub(topic, cb, _opts) {
      for (const e of (store.get(topic) || [])) {
        if (e.message?.k === 'c' && dropIndices.has(e.message.i)) continue;   // simulate a lost chunk
        cb({ message: e.message, msgId: e.msgId });
      }
      return { topic };
    },
    async unsub() {},
  };
}

async function main() {
  console.log('@axona/protocol/std/chunk');
  const MAXMSG = 4096;                            // small to force many chunks in tests

  // ── 1. byte-exact round-trip, shuffled ──
  {
    const input = makeBytes(50_000);
    const { messages, n } = chunkBytes(input, { name: 'x.bin', mime: 'application/octet-stream', maxMessageBytes: MAXMSG });
    let out = null;
    const r = createReassembler((f) => { out = f; });
    for (const m of shuffle(messages)) r.accept(m);
    check(`1. round-trip byte-exact (${n} chunks)`, out && eqBytes(out.bytes, input) && out.name === 'x.bin');
  }

  // ── 2. message size bound (O-5) ──
  {
    const { messages } = chunkBytes(makeBytes(40_000), { maxMessageBytes: MAXMSG });
    const max = Math.max(...messages.map((m) => JSON.stringify(m).length));
    check(`2. every message ≤ maxMessageBytes (max=${max} ≤ ${MAXMSG})`, max <= MAXMSG);
  }

  // ── 3. duplicate tolerance (CDC-2) ──
  {
    const input = makeBytes(20_000);
    const { messages } = chunkBytes(input, { maxMessageBytes: MAXMSG });
    let out = null, fires = 0;
    const r = createReassembler((f) => { out = f; fires++; });
    const dupd = [...messages, ...messages, ...messages.filter((m) => m.k === 'c').slice(0, 3)];  // many dups
    for (const m of shuffle(dupd)) r.accept(m);
    check('3. duplicates tolerated, byte-exact, completes exactly once', out && eqBytes(out.bytes, input) && fires === 1);
  }

  // ── 4. manifest loss tolerated ──
  {
    const input = makeBytes(15_000);
    const { messages } = chunkBytes(input, { maxMessageBytes: MAXMSG });
    let out = null;
    const r = createReassembler((f) => { out = f; });
    for (const m of messages.filter((m) => m.k !== 'm')) r.accept(m);   // drop the manifest
    check('4. completes from chunks alone (manifest lost, n from chunk.n)', out && eqBytes(out.bytes, input));
  }

  // ── 5. garbage ignored (CDC-5) ──
  {
    const input = makeBytes(10_000);
    const { messages, fileId } = chunkBytes(input, { maxMessageBytes: MAXMSG });
    let out = null;
    const r = createReassembler((f) => { out = f; });
    r.accept({ f: 1, k: 'c', id: 'OTHER-FILE', i: 0, n: 1, d: 'AAAA' });   // foreign file
    r.accept({ garbage: true });                                          // malformed
    r.accept(null);
    for (const m of messages) r.accept(m);
    check('5. foreign-fileId + malformed ignored, real file intact', out && eqBytes(out.bytes, input) && out.id === fileId);
  }

  // ── 6. cache-cap refusal (O-1) ──
  {
    const peer = mockPeer();
    let threw = false;
    try { await publishChunkedBytes(peer, makeBytes(5_000_000), { maxMessageBytes: MAXMSG, cacheSize: 100 }); }
    catch { threw = true; }
    check('6. publishChunkedBytes throws when transfer exceeds cache ceiling', threw);
  }

  // ── 7. high-level publish → receive round-trip ──
  {
    const peer = mockPeer();
    const input = stringToBytes('civil-defense sighting payload — '.repeat(2000));   // ~64KB string
    const { topic, n, msgIds } = await publishChunkedBytes(peer, input, { name: 'note.txt', maxMessageBytes: MAXMSG });
    const file = await receiveChunkedBytes(peer, topic, { timeoutMs: 2000 });
    check(`7. publish→receive round-trip (${n} chunks, string intact)`, eqBytes(file.bytes, input) && bytesToString(file.bytes).startsWith('civil-defense'));
    // Receiver-side msgIds: the SAME set the publisher got back — this is how a
    // later session/device (which never saw publishChunkedBytes's return) learns
    // what to peer.kill() to delete the transfer.
    const pub = new Set(msgIds), got = new Set(file.msgIds);
    check(`7b. file.msgIds matches the publisher's set (${got.size}/${pub.size})`,
      got.size === pub.size && [...pub].every((id) => got.has(id)));
  }

  // ── 7c. msgIds are PER FILE — a second file's ids must not leak in ──
  {
    const peer = mockPeer();
    const topic = 'shared-stream';
    const a = await publishChunkedBytes(peer, makeBytes(20_000), { topic, name: 'a.bin', maxMessageBytes: MAXMSG });
    const b = await publishChunkedBytes(peer, makeBytes(15_000), { topic, name: 'b.bin', maxMessageBytes: MAXMSG });
    const file = await receiveChunkedBytes(peer, topic, { timeoutMs: 2000 });   // resolves with the FIRST completed file (a)
    const got = new Set(file.msgIds), bIds = new Set(b.msgIds), aIds = new Set(a.msgIds);
    check('7c. msgIds contain only the resolved file\'s ids (none from the other file)',
      file.name === 'a.bin' && got.size === aIds.size &&
      [...got].every((id) => aIds.has(id)) && [...got].every((id) => !bIds.has(id)));
  }

  // ── 8. no silent hang — rejects on a missing chunk (CDC-1) ──
  {
    const peer = mockPeer({ dropIndices: new Set([2]) });   // chunk index 2 never delivered (replay always drops it)
    const input = makeBytes(30_000);
    // verify:false — this case tests the RECEIVE-side reject on a permanently
    // missing chunk; publish-side verify can't help (the chunk is dropped on
    // every replay) and would just slow the test.
    const { topic } = await publishChunkedBytes(peer, input, { maxMessageBytes: MAXMSG, verify: false });
    let rejected = false, msg = '';
    try { await receiveChunkedBytes(peer, topic, { timeoutMs: 300 }); }
    catch (e) { rejected = true; msg = e.message; }
    check('8. rejects (no hang) on missing chunk, names missing index', rejected && /missing/.test(msg) && /\b2\b/.test(msg));
  }

  // ── 10. PUBLISH verify+repair heals a burst-dropped chunk (Howard's reload bug) ──
  {
    const input = makeBytes(60_000);                       // many chunks
    // Two chunks get dropped on their FIRST publish (burst loss); a reload
    // subscriber would otherwise never reassemble. verify+repair must re-publish.
    const peer = mockPeer({ dropFirst: new Set([1, 4]) });
    const res = await publishChunkedBytes(peer, input, { maxMessageBytes: MAXMSG, throttleMs: 0, verifyWaitMs: 200 });
    check('10a. verify re-published the dropped chunks', res.repaired >= 2);
    // A fresh "reload" subscriber relying only on the cached set reassembles.
    const file = await receiveChunkedBytes(peer, res.topic, { timeoutMs: 2000 });
    check('10b. reload subscriber reassembles byte-exact after repair', eqBytes(file.bytes, input));
  }

  // ── 9. MULTI-FILE: one reassembler, a stream of files (image-channel bug) ──
  {
    const a = makeBytes(20_000), b = makeBytes(33_000);
    const A = chunkBytes(a, { name: 'a.bin', maxMessageBytes: MAXMSG });
    const B = chunkBytes(b, { name: 'b.bin', maxMessageBytes: MAXMSG });
    const got = new Map();                                  // id -> bytes
    const r = createReassembler((f) => { got.set(f.id, f); });
    for (const m of A.messages) r.accept(m);                // first file (manifest + chunks)
    for (const m of B.messages) r.accept(m);                // second file on the SAME reassembler
    const fa = got.get(A.fileId), fb = got.get(B.fileId);
    check('9. two sequential files both reassemble on one reassembler',
      got.size === 2 && fa && fb && eqBytes(fa.bytes, a) && eqBytes(fb.bytes, b) &&
      fa.name === 'a.bin' && fb.name === 'b.bin');
  }

  // ── 11. verify pass must NOT tear down a caller's persistent subscription ──
  // Faithful mock: subscriptions are handler-scoped (a Set per topic) with LIVE
  // delivery, sub() returns a handle whose .stop() removes ONLY that handler, and
  // unsub(topic) stops EVERY handler on the topic — exactly the kernel semantics.
  // axona-share keeps a persistent channel reassembler subscribed to the very
  // topic it publishes images to; the verify pass inside publishChunkedBytes used
  // to peer.unsub(topic) and nuke that subscription. This pins the handle-scoped fix.
  {
    function livePeer() {
      const store = new Map();                 // topic -> [{message,msgId}]
      const subs  = new Map();                 // topic -> Set(cb)
      const setOf = (t) => subs.get(t) || (subs.set(t, new Set()), subs.get(t));
      return {
        _subCount: (t) => (subs.get(t)?.size ?? 0),
        async pub(topic, message) {
          if (!store.has(topic)) store.set(topic, []);
          const msgId = 'm' + store.get(topic).length;
          store.get(topic).push({ message, msgId });
          for (const cb of setOf(topic)) cb({ message, msgId });   // live fan-out
          return msgId;
        },
        async sub(topic, cb, _opts) {
          for (const e of (store.get(topic) || [])) cb({ message: e.message, msgId: e.msgId });  // since:'all' replay
          const set = setOf(topic); set.add(cb);
          return { topic, stop: async () => { set.delete(cb); } };  // handle-scoped stop
        },
        async unsub(topic) { const set = subs.get(topic); const n = set ? set.size : 0; if (set) set.clear(); return { ok: true, removed: n }; },
      };
    }
    const peer = livePeer();
    const TOPIC = { region: 'useast', name: 'axona-share/public-images' };
    // App's persistent channel subscription (a reassembler in real life).
    let liveAfter = 0;
    await peer.sub(TOPIC, (env) => { if (env?.message?.__probe) liveAfter++; }, { since: 'all' });
    const subsBefore = peer._subCount(TOPIC);
    // Publish an image to the SAME topic with verify ON (the default).
    await publishChunkedBytes(peer, makeBytes(40_000), { topic: TOPIC, maxMessageBytes: MAXMSG, throttleMs: 0, verifyWaitMs: 200 });
    const subsAfter = peer._subCount(TOPIC);
    check(`11a. persistent subscription survives a same-topic verify pass (before=${subsBefore}, after=${subsAfter})`, subsAfter === subsBefore && subsAfter === 1);
    // And it still receives a subsequent publish (was torn down by the old unsub).
    await peer.pub(TOPIC, { __probe: true });
    check('11b. persistent subscription still delivers after the publish', liveAfter === 1);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed  (rawChunk@16KB=${rawChunkSize()}B)`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('smoke threw:', e?.stack || e); process.exit(2); });
