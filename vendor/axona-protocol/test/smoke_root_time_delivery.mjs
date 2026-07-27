// =====================================================================
// smoke_root_time_delivery.mjs — root time is the single ordering authority.
//
// The subscriber's delivered envelope `ts` MUST be the root-assigned monotonic
// stamp (the serialization point), NOT the publisher's signed `ts` (clock-skew-
// prone). A kill is a publish with a delete side-effect and carries the SAME
// root stamp, so apps order messages and kills on one consistent timeline.
// (_dispatchDelivery reads only this._subscriptions, so we drive it with a stub.)
//
// Run: node test/smoke_root_time_delivery.mjs
// =====================================================================
import { AxonaPeer } from '../src/dht/AxonaPeer.js';

let n = 0, fail = 0;
const ok = (m, c) => { if (c) { console.log(`  ok ${++n} - ${m}`); } else { console.log(`  ✗  ${m}`); fail++; } };

const TOPIC = 'aabbcc';
const PUBLISHER_TS = 1000;   // publisher's signed claim (skewed / not authoritative)
const ROOT_TS      = 9999;   // root's monotonic stamp (authoritative)
const ROOT_SEQ     = 42;     // root's dense per-topic counter (gap detection)

function harness() {
  const got = [];
  const sub = { _deliver: (env) => got.push(env) };
  const self = { _subscriptions: new Map([[TOPIC, new Set([sub])]]) };
  return { got, self };
}

// ── 1. a normal message → delivered ts is the ROOT stamp, not the publisher's ──
{
  const { got, self } = harness();
  const signed = JSON.stringify({
    msgId: 'm1', ts: PUBLISHER_TS, topic: { region: 'x', name: 't' },
    message: 'hi', signerPubkey: 'deadbeef',
  });
  AxonaPeer.prototype._dispatchDelivery.call(self, TOPIC, signed, 'm1', ROOT_TS, ROOT_SEQ);
  ok('message delivered once', got.length === 1);
  ok('delivered ts is the ROOT stamp', got[0].ts === ROOT_TS);
  ok('delivered ts is NOT the publisher claim', got[0].ts !== PUBLISHER_TS);
  ok('delivered seq is the root dense counter (gap detection)', got[0].seq === ROOT_SEQ);
  ok('message body preserved', got[0].message === 'hi');
}

// ── 2. a kill → delivered as { deleted:true } carrying the same root stamp ──
{
  const { got, self } = harness();
  const killJson = JSON.stringify({ deleted: true, msgId: 'm1', topic: null });
  AxonaPeer.prototype._dispatchDelivery.call(self, TOPIC, killJson, 'm1', ROOT_TS, ROOT_SEQ);
  ok('kill delivered once', got.length === 1);
  ok('kill marked deleted', got[0].deleted === true);
  ok('kill carries the root stamp (orders with messages)', got[0].ts === ROOT_TS);
  ok('kill carries the dense seq (occupies a slot → detectable)', got[0].seq === ROOT_SEQ);
}

// ── 3. missing stamp never clobbers ts with undefined (defensive) ──
{
  const { got, self } = harness();
  const signed = JSON.stringify({
    msgId: 'm2', ts: PUBLISHER_TS, topic: { region: 'x', name: 't' }, message: 'hey',
  });
  AxonaPeer.prototype._dispatchDelivery.call(self, TOPIC, signed, 'm2', undefined);
  ok('no stamp → ts left as the parsed value (not undefined)', got[0].ts === PUBLISHER_TS);
}

console.log(`\n${fail ? '✗' : '✓'} smoke_root_time_delivery: ${n} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
