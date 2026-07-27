// =====================================================================
// smoke_pubsub_droplog.js — security drop-path logs must actually surface.
//
// AxonaManager funnels 24 drop-path events (bad-signature, stale, oversize,
// posthash-mismatch, unauthorized kill/touch/unpub, …) through
// `this._emitLog?.(level, msg, context)`. For the project's entire life that
// field was NEVER assigned, so every one of those calls was a SILENT no-op:
// a node could be dropping a stream of hostile/malformed messages and the
// operator would see nothing. The peer now attaches its onLog surface via
// AxonaManager.setLogSink at the _requireAxonaManager choke point.
//
// This proves the contract end-to-end at the manager boundary:
//   · with a sink attached, a dropped publish emits its event,
//   · the event carries the documented (level, msg, context),
//   · with NO sink (the default), the same drops are safe no-ops (no throw),
//   · detaching (setLogSink(null)) restores the no-op.
//
// Run: node test/smoke_pubsub_droplog.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { deriveTopicId } from '../src/pubsub/post.js';
import { createAuthorIdentity } from '../src/identity/index.js';

// v0.3: an envelope's topic is the structured DESCRIPTOR object, not a string.
const TOPIC_DESC = (name) => ({ region: 0x89, owner: null, name, write: 'open' });

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}
const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);
const big = (h) => BigInt('0x' + h);

function makeManager() {
  const dht = {
    getSelfId: () => big(hex(0x01)),
    onRoutedMessage: () => {}, onDirectMessage: () => {},
    routeMessage: () => {}, sendDirect: async () => true, findKClosest: undefined,
  };
  return new AxonaManager({ dht });
}

async function main() {
  console.log('pub/sub security drop-path logs surface through the sink\n');

  const id        = await createAuthorIdentity();
  const signed    = await buildEnvelope({ topic: TOPIC_DESC('news'), message: 'hello', identity: id, sign: true });
  const realSigned = signed.msgId;
  const FAKE      = 'dead'.repeat(16);                    // 64-hex, not the real hash
  const publisher = big('a' + id.pubkeyHex);
  // v0.3: the root recomputes the topic id from the SIGNED descriptor and drops
  // a routed-vs-descriptor mismatch — route every publish to the descriptor's id.
  const TID       = await deriveTopicId(TOPIC_DESC('news'));

  // ── a sink set via setLogSink captures drop events ────────────────
  console.log('── setLogSink captures drops ──');
  {
    const m = makeManager();
    const logs = [];
    m.setLogSink((level, msg, context) => logs.push({ level, msg, context }));

    // posthash-mismatch drop (honest signature, tampered postHash)
    await m._onPublishDirect(
      { topicId: TID, publisher, json: JSON.stringify(signed),
        publishId: 'ph:1', publishTs: 1, postHash: FAKE });
    const ph = logs.find(l => l.msg === 'publish-posthash-mismatch-dropped');
    check('posthash-mismatch drop emitted a log', !!ph);
    check('  log carries level "debug"', ph?.level === 'debug');
    check('  log context names the topic', ph?.context?.topicId != null);
    check('  drop took effect (no role created)', m.axonRoles.get(big(TID)) == null);

    // bad-signature drop (corrupt the signature, same topic)
    const forged = { ...signed, signature: '0'.repeat(signed.signature.length) };
    await m._onPublishDirect(
      { topicId: TID, publisher, json: JSON.stringify(forged),
        publishId: 'sig:1', publishTs: 1, postHash: realSigned });
    check('bad-signature drop emitted a log',
      logs.some(l => l.msg === 'publish-bad-signature-dropped'));
    check('drops accumulate (≥2 captured)', logs.length >= 2);
  }

  // ── default (no sink) — the same drops are silent no-ops, never throw ─
  console.log('\n── default has no sink: drops are safe no-ops ──');
  {
    const m = makeManager();                              // never call setLogSink
    check('default _emitLog is null', m._emitLog === null);
    let threw = false;
    try {
      await m._onPublishDirect(
        { topicId: TID, publisher, json: JSON.stringify(signed),
          publishId: 'ph:2', publishTs: 1, postHash: FAKE });
    } catch { threw = true; }
    check('drop path with no sink does not throw', threw === false);
    check('drop still took effect (no role)', m.axonRoles.get(big(TID)) == null);
  }

  // ── detaching the sink restores the no-op ─────────────────────────
  console.log('\n── setLogSink(null) detaches ──');
  {
    const m = makeManager();
    const logs = [];
    m.setLogSink((l, msg, c) => logs.push(msg));
    m.setLogSink(null);
    check('setLogSink(null) clears the sink', m._emitLog === null);
    await m._onPublishDirect(
      { topicId: TID, publisher, json: JSON.stringify(signed),
        publishId: 'ph:3', publishTs: 1, postHash: FAKE });
    check('no logs captured after detach', logs.length === 0);
  }

  // ── the real fix: a PEER wires its onLog to the manager at the choke
  //    point (_requireAxonaManager), so manager drops reach peer.onLog ──
  console.log('\n── peer.onLog receives manager drop-path events ──');
  {
    const manager = makeManager();
    const peer = new AxonaPeer({
      engine: { onEvent: () => () => {} },
      node:   { id: big(hex(0x01)), alive: true },
      axonaManager: manager,
    });
    const seen = [];
    peer.onLog('debug', (msg, context) => seen.push({ msg, context }));
    // Resolving the manager attaches the log sink (idempotent choke point).
    const resolved = peer._requireAxonaManager('test');
    check('peer resolves the same manager', resolved === manager);
    check('choke point attached a sink', typeof manager._emitLog === 'function');
    await manager._onPublishDirect(
      { topicId: TID, publisher, json: JSON.stringify(signed),
        publishId: 'peer:1', publishTs: 1, postHash: FAKE });
    const hit = seen.find(e => e.msg === 'publish-posthash-mismatch-dropped');
    check('a manager drop surfaced through peer.onLog', !!hit);
    check('  forwarded with its context', hit?.context?.topicId != null);
  }

  // ── defensive: a manager WITHOUT setLogSink (older vendored copy) is a
  //    safe no-op at the choke point, never throws ──
  console.log('\n── choke point tolerates a manager without setLogSink ──');
  {
    const legacyMgr = { /* no setLogSink */ inspectRoles: () => [] };
    const peer = new AxonaPeer({
      engine: { onEvent: () => () => {} },
      node:   { id: big(hex(0x01)), alive: true },
      axonaManager: legacyMgr,
    });
    let threw = false;
    try { peer._requireAxonaManager('test'); } catch { threw = true; }
    check('wiring a setLogSink-less manager does not throw', threw === false);
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}
main().catch((e) => { console.error('fatal:', e); process.exit(2); });
