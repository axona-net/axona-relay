// relay.js — assemble a full Axona mesh peer that behaves as a relay/supernode.
//
// This is the SAME stack a browser peer runs (webTransport + NeuronNode +
// AxonaDomain + AxonaPeer); the only difference is it runs in Node with the
// node-datachannel RTCPeerConnection polyfill. As a relay it:
//   • forms authenticated WebRTC DataChannels with every other peer,
//   • participates in DHT routing (lookups forward through it),
//   • acts as a pub/sub ROOT AXON for topics it is K-closest to
//     (caches + fans out + replays — no app subscription required), and
//   • relays WebRTC signaling for other peers (meshRelay capability),
//     which is what lets two peers connect with no bridge in the path.
//
// It does NOT run a public WS server, mint TURN, or gate admission — those
// are bridge-only roles.

import { AxonaPeer, AxonaDomain, NeuronNode,
         regionName, resolveRegion, regionCenter, POPULATED_REGIONS }
  from '../vendor/axona-protocol/src/index.js';
import { webTransport }
  from '../vendor/axona-protocol/src/transport/web/index.js';
import { KERNEL_VERSION }
  from '../vendor/axona-protocol/src/transport/handshake.js';
import { WebSocketImpl } from './polyfill.js';

export { KERNEL_VERSION, regionName, resolveRegion, regionCenter, POPULATED_REGIONS };

/**
 * Resolve a region token (name like "eagle" or code like "0x89") to the
 * structured-topic region descriptor used by the v0.3 pub/sub API.
 *
 * v0.3 replaces the old "synthetic publisher = <s2-prefix>‖0^256" anchor with
 * a region NAME carried in the topic descriptor ({ region, name }). The topic
 * id's region byte is derived from this name, so a relay/CLI that opens
 * { region: 'eagle', name: 'foo' } lands in the SAME keyspace as any app that
 * does the same — preserving the old region→keyspace mapping with no synthetic
 * publisher.
 *
 * @param {string} token  region name or code
 * @returns {{ code:number, name:string, center:{lat,lng} } | null}  null if unknown
 */
export function regionDescriptor(token = 'eagle') {
  const code = resolveRegion(token);
  if (code == null) return null;
  return { code, name: regionName(code), center: regionCenter(code) };
}

/**
 * Build (but do not start) the relay peer.
 *
 * @param {object}   opts
 * @param {string}   opts.bridgeUrl   wss:// bridge for bootstrap + signaling
 * @param {object}   opts.identity    ephemeral transport Identity, minted fresh
 *                                    every process start and never persisted (I-ID)
 * @param {{lat:number,lng:number}} opts.region
 * @param {(level:string, event:string, ctx?:object)=>void} [opts.onLog]
 * @param {boolean} [opts.frameRegistry] REF-1.1 M1 canary: arm the four
 *   per-boundary frame-contract registries in SHADOW (observe-only). DEFAULT
 *   OFF. When unset here it falls back to env AXONA_FRAME_REGISTRY==='1', so the
 *   fleet stays inert unless a canary slot opts in. This is CONSTRUCTION-side
 *   arming only; traces still emit only when the runtime gate AXONA_REGISTRY_SHADOW
 *   is also on (kernel shadowEnabled()). Both default-off ⇒ two-part arming.
 */
// ── armed-canary arming (proposal: axona-docs Axona-Armed-Canary-Proposal;
//    hardened per Vega b8a1164d + Aster f9b7bd31) ─────────────────────────
// Three exported, PURE, unit-testable pieces (Aster condition 3: an automated
// test must prove the env flags reach the peer):
//   armingFromEnv(env)        env → the exact constructor options (the
//                             proposal's constants table, verbatim)
//   assertArmingSupported()   vendored-kernel floor: ANY arming env below
//                             4.67 throws — a pre-4.65 kernel already
//                             understands synaptomeMaintain, so that flag
//                             alone against an old vendor is the 2026-06-29
//                             storm with no guard (fail closed, at launch)
//   assertArmedModules()      post-construction, PRE-JOIN proof that every
//                             requested module actually LANDED on the peer
//                             (a version string is a claim; the peer's own
//                             state is the fact) — returns the effective
//                             constants for the armed-modules log line
export const ARM_ENVS = ['RELAY_SYNAPTOME_MAINTAIN', 'RELAY_ADMISSION_GATE', 'RELAY_ATTEMPT_GUARD', 'RELAY_PRESENCE'];

export function armingFromEnv(env = process.env) {
  const armedEnvs = ARM_ENVS.filter((e) => env[e] === '1');
  return {
    armedEnvs,
    armMaintain: env.RELAY_SYNAPTOME_MAINTAIN === '1'
      ? { kNear: 5, intervalMs: 15000, maxPerTick: 3 } : null,
    armGate: env.RELAY_ADMISSION_GATE === '1'
      ? { kNear: 5, sparseFloor: 2, kJoin: 2, laneCooldownMs: 5000, laneWindowMs: 300000 } : null,
    armGuard: env.RELAY_ATTEMPT_GUARD === '1'
      ? { maxAttempts: 4, baseMs: 30000, factor: 2, refillWindowMs: 60000,
          deficitBaseMs: 30000, deficitFactor: 2 } : null,
    armPresence: env.RELAY_PRESENCE === '1'
      ? { announceOnStart: true, relayRateMs: 30000 } : null,
  };
}

export function assertArmingSupported(kernelVersion, armedEnvs) {
  if (!armedEnvs || armedEnvs.length === 0) return;
  const [maj, min] = String(kernelVersion).split('.').map(Number);
  if (!(maj > 4 || (maj === 4 && min >= 67))) {
    throw new Error(
      `arming refused: ${armedEnvs.join(',')} set but vendored kernel is ${kernelVersion} (< 4.67 — ` +
      `no attempt guard exists there; synaptomeMaintain alone is the 2026-06-29 storm). ` +
      `Re-vendor 4.67+ (scripts/sync-protocol.sh) or unset the arming envs.`);
  }
}

export function assertArmedModules(peer, armedEnvs) {
  const missing = [];
  const effective = {};
  if (armedEnvs.includes('RELAY_SYNAPTOME_MAINTAIN')) {
    if (!peer._maintainCfg) missing.push('synaptomeMaintain');
    else effective.synaptomeMaintain = { ...peer._maintainCfg };
  }
  if (armedEnvs.includes('RELAY_ADMISSION_GATE')) {
    if (!peer._gateCfg) missing.push('admissionGate');
    else effective.admissionGate = { ...peer._gateCfg };
  }
  if (armedEnvs.includes('RELAY_ATTEMPT_GUARD')) {
    if (!peer._attemptGuard) missing.push('attemptGuard');
    else effective.attemptGuard = {
      maxAttempts: peer._attemptGuard.maxAttempts, baseMs: peer._attemptGuard.baseMs,
      factor: peer._attemptGuard.factor, refillWindowMs: peer._attemptGuard.refillWindowMs,
    };
  }
  if (armedEnvs.includes('RELAY_PRESENCE')) {
    if (!peer._presenceCfg) missing.push('presence');
    else effective.presence = { ...peer._presenceCfg };
  }
  if (missing.length > 0) {
    throw new Error(
      `arming refused: requested module(s) did not land on the peer: ${missing.join(', ')}. ` +
      `The vendored kernel accepted the option name(s) without building the machinery — ` +
      `do not join the network in this state.`);
  }
  return effective;
}

export function createRelay({ bridgeUrl, identity, region, onLog = () => {},
  frameRegistry = process.env.AXONA_FRAME_REGISTRY === '1' }) {
  const transport = webTransport({
    bridgeUrl,
    identity:    { ...identity, id: identity.id },  // kernel id is already 66-char hex
    // NOTE: peerVersion is left unset on purpose ⇒ webTransport sends
    // KERNEL_VERSION in the client-hello (now 3.0.0 with the v0.3 kernel).
    // The bridge classifies the hello `version` by major: ≥3 → peer-app floor,
    // else kernel floor. Sending the relay's own 0.x version here would be
    // classified kernel-namespace and REJECTED, so we never override it.
    // TODO(deploy): KERNEL_VERSION crossed 2.x→3.0.0 — confirm the live bridge's
    // peer-app floor admits 3.0.0 (a 3.x hello is now classified peer-app, not
    // kernel). Verify against bridge.axona.net / testnet before rollout.
    meshRelay:     true,           // relay signaling for others (bridgeless help)
    reconnect:     true,           // a relay should self-heal the bridge link
    WebSocketImpl,
    log: (event, ctx) => onLog('debug', event, ctx),
  });

  const node = new NeuronNode({
    id:  BigInt('0x' + identity.id),
    lat: region.lat,
    lng: region.lng,
  });
  node.transport = transport;

  const domain = new AxonaDomain({ k: 20 });
  // v0.3: the AxonaPeer takes the NODE identity as `nodeIdentity:` (the
  // transport/connection key). There is NO `publishIdentity:` — publishes name
  // their author per-call via pub(..., { signWith }). The transport factory
  // above keeps `identity:` (it's the same node key, used for the auth hello).
  // Synaptome maintenance: REVERTED to off (2026-06-29) — enabling it on the
  // backbone regressed Howard's suite (0/9/6 failures across 3 runs; connection-count
  // storm + convergence wedge). The fix now exists: the 6522f2f storm was root-caused
  // (never-binding candidates re-probed from churn-reopened deficits) and the kernel
  // 4.67.1 attempt guard bounds it — proven in the live canary correlation
  // (axona-relay 85d6baa, council-closed unanimous 2026-08-24).
  //
  // ARMED-CANARY PLUMBING (proposal: axona-docs Axona-Armed-Canary-Proposal-v0.1;
  // hardened per Vega b8a1164d): env-driven, DEFAULT OFF, and VERSION-GATED.
  // The earlier "inert against an old vendor" claim was FALSE for the one flag
  // that historically mattered: a pre-4.65 kernel already understands
  // synaptomeMaintain, so RELAY_SYNAPTOME_MAINTAIN=1 against the 4.62.2 vendor
  // would run the June storm with no guard. So: ANY arming env against a
  // vendored kernel below 4.67 REFUSES TO START — fail closed, loud, at launch.
  // A canary misconfigured against an old vendor must never come up.
  // Constants per the proposal's table; arming anything is David's explicit
  // call, never a default.
  const { armedEnvs, armMaintain, armGate, armGuard, armPresence } = armingFromEnv(process.env);
  assertArmingSupported(KERNEL_VERSION, armedEnvs);
  const peer   = new AxonaPeer({ domain, node, nodeIdentity: identity, transport,
    ...(frameRegistry === true ? { frameRegistry: true } : {}),
    ...(armMaintain ? { synaptomeMaintain: armMaintain } : {}),
    ...(armGate ? { admissionGate: armGate } : {}),
    ...(armGuard ? { attemptGuard: armGuard } : {}),
    ...(armPresence ? { presence: armPresence } : {}) });

  // JOIN-BLOCKING module assertion (Aster f9b7bd31 condition 2): prove every
  // requested module LANDED on the peer — createRelay runs before any
  // network join (startRelay owns transport.start), so a throw here is a
  // canary that never comes up. On success, log the exact effective
  // constants: the soak's record of what actually ran.
  if (armedEnvs.length > 0) {
    const effective = assertArmedModules(peer, armedEnvs);
    onLog('info', 'armed-modules', { armed: armedEnvs.join(','), effective });
  }

  // Armed-canary ledger (Vega b8a1164d, observation condition (1)): a soak's
  // "quiet" must be tellable apart from "idle". When any arming env is set,
  // log a once-per-minute ledger line: guard counters (refills/coalesced +
  // active/expired candidate entries), presence watermark count, and the
  // count of maintenance refill passes that actually ATTEMPTED something
  // (the 'synaptome-refill' log event = a near-quota deficit REOPENED and was
  // acted on). A 48h soak whose deficitReopens stays 0 has not exercised the
  // guard and discharges nothing — the ledger is what makes that visible.
  if (armedEnvs.length > 0) {
    let deficitReopens = 0;
    const prevEmit = peer._emitLog?.bind(peer);
    if (prevEmit) {
      peer._emitLog = (level, event, ctx) => {
        if (event === 'synaptome-refill' && (ctx?.attempted ?? 0) > 0) deficitReopens++;
        return prevEmit(level, event, ctx);
      };
    }
    const ledger = setInterval(() => {
      const g = peer._attemptGuard;
      const states = g ? [...g._state.values()] : [];
      onLog('info', 'armed-ledger', {
        armed: armedEnvs.join(','),
        guardRefills: g?.refills ?? 0,
        guardCoalesced: g?.coalesced ?? 0,
        guardActive: states.filter((s) => !s.expired).length,
        guardExpired: states.filter((s) => s.expired).length,
        presenceWatermarks: peer._presenceWatermarks?.size ?? 0,
        deficitReopens,
        synaptome: node.synaptome?.size ?? 0,
      });
    }, 60000);
    if (typeof ledger.unref === 'function') ledger.unref();
  }

  return { peer, transport, node, domain };
}

/**
 * Bring the relay up: wire, then peer, then WAIT FOR THE MESH, then integrate.
 *
 * This mirrors kernel `connect()`'s lifecycle EXACTLY and on purpose — it is the
 * one bootstrap site for this repo (src/index.js, src/cli.js and src/ops.js all
 * come through here), so getting the order right here fixes every relay, every
 * CLI call, and every mcp-post/mcp-bot-post publisher at once.
 *
 * THE ORDER IS THE WHOLE POINT (fixed 2026-07-25). This function used to do:
 *
 *     await transport.start(); await peer.start();
 *     peer.integrate().catch(() => {});        // ← fire-and-forget
 *
 * with no `peer.ready()` in between. `integrate()` is `findKClosest(self)` plus
 * authenticated channel opens, so running it before the bridge welcome has
 * seeded ANY peers means it queries a near-empty routing table and does
 * essentially nothing — and because it was never awaited, a pub() could be
 * issued while the node was still unknown to its neighbours. Kernel connect.js
 * states the consequence outright: without effective self-integration a node
 * "sits at the passive-adoption churn floor and self-roots its topics as
 * SINGLETON roots in a sparse region — the cross-region pub/sub loss (fresh
 * subscribers read 0, publishers strand on leave)". That matches the live
 * #axona.bot symptom: K-closest populated, publishes vanish, reads empty.
 *
 * So: ready() FIRST (the welcome seeds peers to query), integrate() SECOND, and
 * AWAIT it, so a caller that got a resolved startRelay() is genuinely woven in.
 *
 * `peer.ready()` never throws — on timeout it returns { ready, peers, ms,
 * reason:'timeout' } — so awaiting it cannot wedge start-up. Pass `ready:false`
 * to skip the warm-up (integration then heals in the background, connect()'s
 * own semantics); pass an object to tune { minPeers, timeoutMs, stableMs }.
 *
 * Returns the readiness status plus what integration did, so a caller can log or
 * refuse to publish instead of guessing. Integration failure is non-fatal but no
 * longer INVISIBLE — the old `.catch(() => {})` swallowed it completely.
 */
export async function startRelay({ peer, transport, ready = {} }) {
  await transport.start();
  await peer.start();
  const status = (ready === false) ? null : await peer.ready(ready);

  let integrated = null;
  let integrateError = null;
  if (typeof peer.integrate === 'function') {
    const integrating = Promise.resolve(peer.integrate())
      .then((r) => { integrated = r ?? true; })
      .catch((e) => { integrateError = e; });
    if (ready !== false) await integrating;
  }
  return { ...(status ?? {}), integrated, integrateError };
}

/** Best-effort graceful shutdown. */
export async function stopRelay({ peer, transport }) {
  try { await peer.leave?.(); } catch { /* ignore */ }
  try { await peer.stop?.(); }  catch { /* ignore */ }
  try { await transport.stop?.(); } catch { /* ignore */ }
}
