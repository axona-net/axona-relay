// =====================================================================
// smoke_pubsub_c3.js — Wave-A Stage 1 pub/sub abuse hardening.
//
//   C-3 : metrics reflection/amplification + ownership fail-open.
//         · a response is delivered ONLY to the proven channel peer
//           (meta.fromId), never to an attacker-named requesterId;
//         · the owner-gate FAILS CLOSED — when ownership is
//           indeterminate (empty replay cache) the owner-sensitive
//           subscriber count is withheld.
//   SP-10: anonymous (null-signer) publishes share a single quota bucket
//          and can't bypass the per-publisher cap by not signing.
//
// Drives the real AxonaManager handlers directly (same harness shape as
// smoke_pubsub_authz.js).  Run: node test/smoke_pubsub_c3.js
// =====================================================================

import { AxonaManager } from '../src/pubsub/AxonaManager.js';
import { createAuthorIdentity } from '../src/identity/index.js';
import { buildEnvelope } from '../src/pubsub/envelope.js';
import { setPowDifficulty, resetPowDifficulty } from '../src/pow/pow.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

const hex = (b) => 'a' + b.toString(16).padStart(2, '0') + '0'.repeat(63);  // 66-char id
const big = (h) => BigInt('0x' + h);

const ROOT = hex(0x01), SUB = hex(0x02), ATTACKER = hex(0x03), VICTIM = hex(0x04), OWNER = hex(0x06);
const TOPIC = hex(0x05);
const REGION_PUB = '89' + '0'.repeat(64);    // prefix‖0^256 → UNOWNED by construction

function makeManager() {
  const sent = [];
  const dht = {
    getSelfId:       () => big(ROOT),
    onRoutedMessage: () => {}, onDirectMessage: () => {}, routeMessage: () => {},
    sendDirect:      async (to, type, body) => { sent.push({ to, type, body }); return true; },
  };
  return { am: new AxonaManager({ dht }), sent };
}

function setRole(am, { publisher = null, children = [SUB], cacheLen = 1 } = {}) {
  const far = Date.now() + 1e9;
  const cache = [];
  for (let i = 0; i < cacheLen; i++) {
    cache.push({ json: '{}', postHash: 'h' + i, publisher, signerPubkey: null, seq: i, ts: i, expiresAt: far });
  }
  am.axonRoles.set(big(TOPIC), {
    children:    new Map(children.map(c => [big(c), { createdAt: 0, lastRenewed: 0 }])),
    replayCache: cache,
  });
}

let rid = 0;
const req = ({ requesterId, fromId }) =>
  [{ topicId: TOPIC, requesterId, requestId: 'r' + (++rid), postHashes: null }, { fromId }];

async function run() {
  console.log('C-3 metrics authorization + SP-10 quota\n');

  console.log('── C-3: no reflection to a named victim ──');
  {
    const { am, sent } = makeManager();
    setRole(am, { publisher: big(REGION_PUB) });                 // unowned
    am._onMetricsReqDirect(...req({ requesterId: VICTIM, fromId: ATTACKER }));
    check('no response routed to the attacker-named victim', !sent.some(s => s.to === big(VICTIM)));
    check('no metricsResp at all when requesterId ≠ fromId',  !sent.some(s => s.type === 'pubsub:metricsResp'));
  }

  console.log('\n── v3.5.0: open / unowned topics are REFUSED (read them via metricTopic) ──');
  {
    // metrics() is now an OWNER-ONLY reader. An unowned (open / synthetic
    // regional) topic exposes its live state through the published metric topic
    // instead, so the scatter-gather refuses it even for a proven sender —
    // shrinking the fan-out read surface to owner-authenticated requests only.
    const { am, sent } = makeManager();
    setRole(am, { publisher: big(REGION_PUB) });                 // unowned
    am._onMetricsReqDirect(...req({ requesterId: SUB, fromId: SUB }));  // proven, but not an owner
    check('open/unowned topic gets NO metricsResp (owner-only)',
      !sent.some(s => s.type === 'pubsub:metricsResp'));
  }

  console.log('\n── v3.5.0: empty cache → owner indeterminate → REFUSED ──');
  {
    const { am, sent } = makeManager();
    setRole(am, { publisher: null, cacheLen: 0, children: [SUB] });   // subscriber present, cache empty
    am._onMetricsReqDirect(...req({ requesterId: SUB, fromId: SUB }));
    check('empty cache yields NO response (cannot establish the owner)',
      !sent.some(s => s.type === 'pubsub:metricsResp'));
  }

  console.log('\n── C-3: owned-topic gate (keyed on proven requester) ──');
  {
    const a = makeManager(); setRole(a.am, { publisher: big(OWNER) });
    a.am._onMetricsReqDirect(...req({ requesterId: SUB, fromId: SUB }));     // SUB ≠ owner
    check('non-owner denied', !a.sent.some(s => s.type === 'pubsub:metricsResp'));

    const b = makeManager(); setRole(b.am, { publisher: big(OWNER), children: [SUB] });
    b.am._onMetricsReqDirect(...req({ requesterId: OWNER, fromId: OWNER })); // owner
    const r = b.sent.find(s => s.type === 'pubsub:metricsResp');
    check('owner allowed, subscriber count revealed', !!r && r.to === big(OWNER) && r.body.subscribers === 1);
  }

  console.log('\n── SP-10: anonymous publishes share one quota bucket ──');
  {
    const { am } = makeManager();
    const role = { replayCache: [] };
    for (let i = 0; i < 5; i++) {
      am._addToReplayCache(role, { json: '{}', postHash: 'a' + i, publisher: null, publishTs: i }, { quotaPerPublisher: 2 });
    }
    const anon = role.replayCache.filter(e => (e.signerPubkey ?? 'anon') === 'anon');
    check('5 anon publishes, quota 2 → capped at 2', anon.length === 2);
  }

  console.log('\n── publish-ingress PoW gate (B-4 `_publishSignatureOk`, difficulty > 0) ──');
  {
    const { am } = makeManager();
    setPowDifficulty('publish', 10);
    const id  = await createAuthorIdentity();
    const env = await buildEnvelope({ topic: { region: 'useast', name: 'cats' }, message: 'hi', identity: id });   // mints signerPow at 10
    check('ingress ACCEPTS a publish with a valid signerPow', (await am._publishSignatureOk(JSON.stringify(env))) === true);
    check('ingress DROPS a publish with the signerPow stripped',
      (await am._publishSignatureOk(JSON.stringify({ ...env, signerPow: '' }))) === false);
    resetPowDifficulty();
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

run();
