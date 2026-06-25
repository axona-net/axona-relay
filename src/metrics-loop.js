// metrics-loop.js — the PUBLISH side of the derived metric-topic convention.
//
// Background: peer.metrics(topic) is a K-root scatter-gather (~500 ms, O(K) per
// call). Ruinous if every client polls it per topic on a timer. The convention
// (axona-docs/architecture/Pub-Sub-Metrics-Topic-v0.1.md) moves that cost off
// the read path: a topic's primary root PUBLISHES its metrics to a derived topic
// `metricTopic(T)`, and clients sub() that instead — one subscription, latest +
// rolling history for free (snapshots age out at the 48 h hold ceiling).
//
// This is the relay-side loop. The kernel supplies the MECHANISM
// (peer.rootedTopics() + the metricTopic()/isMetricTopic() helpers, all in core
// — no std dependency); this loop supplies the POLICY:
//
//   • cadence       — recompute + republish every ~20 s (a regular publish event,
//                     same as any other topic — clients sub() it like normal);
//   • which topics  — EVERY topic this relay roots, OWNED and OPEN alike
//                     (v4.3.0 decision 2026-06-25: an owned topic's activity
//                     metrics are public too, so anyone can subscribe to an owned
//                     topicID's metrics without owning it). Skipped only:
//       – metric topics            (isMetricTopic → recursion guard: metrics-of-
//                                    metrics never terminates);
//       – roles with no recoverable descriptor (empty/cold — nothing to report);
//   • signer        — an (ephemeral) author; snapshots are signed so a subscriber
//                     can pin trust to a known relay key. Advisory, not
//                     authoritative (the metric topic is open + spoofable).
//
// Self-triggered on the relay's own liveness: it republishes whatever it roots
// right now. When a topic goes cold (no roots), its metrics simply stop and the
// last snapshot ages out — the honest signal.

import { metricTopic, isMetricTopic }
  from '../vendor/axona-protocol/src/index.js';

export const DEFAULT_METRICS_INTERVAL_MS = 20 * 1000;       // ~20 s cadence (regular publish event)
const FIRST_RUN_DELAY_MS = 8000;                            // let caches warm post-mesh

/**
 * Build the signed snapshot payload for one rooted topic (owned or open).
 * Shape mirrors a metricsReq reply plus provenance (ts + computing node).
 */
function snapshotFor(r, nodeId, now) {
  return {
    v:             1,
    topic:         r.topicId,         // the DATA topic this snapshot describes
    ts:            now,
    by:            nodeId,            // computing relay's node id (provenance)
    signer:        nodeId,            // self-asserted provenance; peer.metrics() prefers the envelope's signerPubkey
    current_count: r.current_count,   // live (non-expired) cached posts
    subscribers:   r.subscribers,     // children in this relay's root set
    bytes:         r.bytes,           // live cached envelope bytes
  };
}

/**
 * Start the periodic metric-publish loop. Returns a stop() function.
 *
 * @param {object}   opts
 * @param {object}   opts.peer        a started AxonaPeer (kernel ≥ 3.4.0)
 * @param {object}   opts.author      author identity to sign snapshots ({ signWith })
 * @param {string}   opts.nodeId      this relay's node id (hex, for provenance)
 * @param {number}   [opts.intervalMs]
 * @param {() => number} [opts.now]   clock (testable)
 * @param {(level:string, event:string, ctx?:object)=>void} [opts.log]
 * @returns {() => void} stop
 */
export function startMetricsLoop({
  peer, author, nodeId,
  intervalMs = DEFAULT_METRICS_INTERVAL_MS,
  firstRunDelayMs = FIRST_RUN_DELAY_MS,
  now = () => Date.now(),
  log = () => {},
}) {
  if (!peer || typeof peer.rootedTopics !== 'function') {
    throw new Error('startMetricsLoop: peer.rootedTopics() unavailable (kernel < 3.4.0?)');
  }
  if (!author) throw new Error('startMetricsLoop: an author identity is required to sign snapshots');

  let timer = null, firstTimer = null, busy = false, stopped = false;

  async function cycle() {
    if (busy || stopped) return;       // never overlap a slow cycle with the next tick
    busy = true;
    let published = 0, skipped = 0, failed = 0;
    try {
      const rooted = peer.rootedTopics();
      const ts = now();
      for (const r of rooted) {
        if (stopped) break;
        const d = r.descriptor;
        // Skip: no descriptor (can't guard/derive) and metric topics (recursion
        // guard). Owned AND open data topics both publish (v4.3.0).
        if (!d || isMetricTopic(d)) { skipped++; continue; }
        try {
          await peer.pub(metricTopic(r.topicId),
                         JSON.stringify(snapshotFor(r, nodeId, ts)),
                         { signWith: author });
          published++;
        } catch (e) {
          failed++;
          log('warn', 'metric_publish_failed', { topic: r.topicId, error: String(e?.message || e) });
        }
      }
      if (published || failed) {
        log('debug', 'metrics_cycle', { published, skipped, failed, rooted: rooted.length });
      }
    } catch (e) {
      // A relay is long-lived infra: a bad cycle must never throw out of the
      // timer (which would be an unhandledRejection). Log and wait for the next.
      log('warn', 'metrics_cycle_error', { error: String(e?.message || e) });
    } finally {
      busy = false;
    }
  }

  timer = setInterval(cycle, intervalMs);
  if (timer.unref) timer.unref();              // don't keep the process alive on our account
  firstTimer = setTimeout(cycle, firstRunDelayMs);
  if (firstTimer.unref) firstTimer.unref();

  return function stop() {
    stopped = true;
    if (timer)      { clearInterval(timer);   timer = null; }
    if (firstTimer) { clearTimeout(firstTimer); firstTimer = null; }
  };
}
