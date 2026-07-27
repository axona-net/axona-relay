// smoke_normative_constants.mjs — the doc↔code coherence guard for the timing
// model (v4.25.0, Phase 6c of the v0.2 refactor program).
//
// The architecture doc's §XI timing table is NORMATIVE: every constant appears
// there with its value AND its reason, and the doc promises "a constant whose
// reason you don't know is one you will mis-tune." This smoke pins the code to
// the published table. If it fails, you tuned a constant — which is sometimes
// right, but requires updating the architecture doc's table (and re-versioning
// the doc) IN THE SAME CHANGE. The doc rotting silently was the root failure
// behind the 4.24.0 collapse post-mortem: the stale doc actively recommended
// the removed behavior.
//
// Values mirror: axona-docs/architecture/Axona-Architecture.tex §XI (v4.27.0).
//
// Run: node test/smoke_normative_constants.mjs
import {
  RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS, ROOT_CLAIM_MS,
  BEACON_MS, BEACON_TTL_MS, BEACON_FANOUT, BEACON_LAYERS,
  ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH,
  ROOT_REPLICAS, ROOT_REPLICATE_FULL_MS, BACKUP_EVICT_MS,
  REPLICATE_FULL_BUDGET, INGEST_QUEUE_MAX, INGEST_SLICE_MS,
  MESH_REWARM_MIN, MESH_REWARM_TICKS, MESH_REWARM_COOLDOWN_MS,
  HANDOFF_ACK_MS, HANDOFF_TRIES,
  EMPTY_ROOT_PROBE_DELAY_MS, EMPTY_ROOT_PROBE_INTERVAL_MS,
  EMPTY_ROOT_PROBE_MAX, EMPTY_ROOT_PROBE_FANOUT,
  PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES,
  COLD_BURST_TRIES, COLD_BURST_INTERVAL_MS,
  COLD_BURST_SLOW_TRIES, COLD_BURST_SLOW_INTERVAL_MS,
  FIRST_PUBLISH_RESEND_MS, MAX_DIRECT, DELEGATE_BATCH,
  CACHE_MAX, CACHE_BYTES, TTL_MS,
  METRICS_LEASE_MS, METRICS_PUB_MS, FUTURE_TOLERANCE_MS,
} from '../src/pubsub/constants.js';

let passed = 0, failed = 0;
const pin = (label, actual, normative) => {
  if (actual === normative) { console.log(`  ✓ ${label} = ${normative}`); passed++; }
  else { console.log(`  ✗ ${label}: code=${actual}, doc says ${normative}`); failed++; }
};

console.log('normative-constants guard — code must match the architecture doc §XI\n');

pin('RENEW_FAST_MS', RENEW_FAST_MS, 5_000);
pin('RENEW_MS', RENEW_MS, 60_000);
pin('RENEW_BACKOFF', RENEW_BACKOFF, 1.5);
pin('DROP_MS', DROP_MS, 180_000);
pin('ROOT_CLAIM_MS', ROOT_CLAIM_MS, 6_000);
pin('BEACON_MS', BEACON_MS, 20_000);
pin('BEACON_TTL_MS', BEACON_TTL_MS, 50_000);
pin('BEACON_FANOUT', BEACON_FANOUT, 6);
pin('BEACON_LAYERS', BEACON_LAYERS, 2);
pin('ROOT_VERIFY_FIRST_MS', ROOT_VERIFY_FIRST_MS, 6_000);
pin('ROOT_VERIFY_MS', ROOT_VERIFY_MS, 45_000);
pin('ROOT_VERIFY_BATCH', ROOT_VERIFY_BATCH, 3);
pin('ROOT_REPLICAS', ROOT_REPLICAS, 2);
pin('ROOT_REPLICATE_FULL_MS', ROOT_REPLICATE_FULL_MS, 60_000);
pin('BACKUP_EVICT_MS', BACKUP_EVICT_MS, 60_000);
pin('REPLICATE_FULL_BUDGET', REPLICATE_FULL_BUDGET, 32);
pin('INGEST_QUEUE_MAX', INGEST_QUEUE_MAX, 4096);
pin('INGEST_SLICE_MS', INGEST_SLICE_MS, 8);
pin('MESH_REWARM_MIN', MESH_REWARM_MIN, 3);
pin('MESH_REWARM_TICKS', MESH_REWARM_TICKS, 3);
pin('MESH_REWARM_COOLDOWN_MS', MESH_REWARM_COOLDOWN_MS, 60_000);
pin('HANDOFF_ACK_MS', HANDOFF_ACK_MS, 700);
pin('HANDOFF_TRIES', HANDOFF_TRIES, 2);
pin('EMPTY_ROOT_PROBE_DELAY_MS', EMPTY_ROOT_PROBE_DELAY_MS, 800);
pin('EMPTY_ROOT_PROBE_INTERVAL_MS', EMPTY_ROOT_PROBE_INTERVAL_MS, 5_000);
pin('EMPTY_ROOT_PROBE_MAX', EMPTY_ROOT_PROBE_MAX, 3);
pin('EMPTY_ROOT_PROBE_FANOUT', EMPTY_ROOT_PROBE_FANOUT, 4);
pin('PENDING_PUB_TTL_MS', PENDING_PUB_TTL_MS, 30_000);
pin('PENDING_PUB_MAX_TRIES', PENDING_PUB_MAX_TRIES, 6);
pin('COLD_BURST_TRIES', COLD_BURST_TRIES, 5);
pin('COLD_BURST_INTERVAL_MS', COLD_BURST_INTERVAL_MS, 200);
pin('COLD_BURST_SLOW_TRIES', COLD_BURST_SLOW_TRIES, 5);
pin('COLD_BURST_SLOW_INTERVAL_MS', COLD_BURST_SLOW_INTERVAL_MS, 400);
pin('FIRST_PUBLISH_RESEND_MS', FIRST_PUBLISH_RESEND_MS, 200);
pin('MAX_DIRECT', MAX_DIRECT, 20);
pin('DELEGATE_BATCH', DELEGATE_BATCH, 8);
pin('CACHE_MAX', CACHE_MAX, 1024);
pin('CACHE_BYTES', CACHE_BYTES, 16 * 1024 * 1024);
pin('TTL_MS', TTL_MS, 24 * 60 * 60 * 1000);
pin('METRICS_LEASE_MS', METRICS_LEASE_MS, 70_000);
pin('METRICS_PUB_MS', METRICS_PUB_MS, 20_000);
pin('FUTURE_TOLERANCE_MS', FUTURE_TOLERANCE_MS, 5 * 60 * 1000);

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
