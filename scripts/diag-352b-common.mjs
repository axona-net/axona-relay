// diag-352b-common.mjs — shared instrumentation for the warm-topic
// live-delivery-gap investigation (adjacent to task #352, 2026-07-18).
//
// Patches AxonaManager/RootClaim PROTOTYPES (must be imported BEFORE any peer
// is created) so every probe logs, for ONE topic of interest:
//   · root-claim transitions (become/demote/promote) with reasons
//   · every DELIVER (who served us → the upstream pin), incl. empty repin pings
//   · every PULLRESP (definitive null-answer vs. timer expiry is distinguished
//     by whether a resp event ever appears for the corrId)
//   · every ROOTBEACON naming the topic
// plus a snapshot() of the manager's attachment state and an iterative
// lookup() of the true routing terminus.
import AxonaManager from '../vendor/axona-protocol/src/pubsub/AxonaManager.js';
import { RootClaim } from '../vendor/axona-protocol/src/pubsub/rootClaim.js';
import { idHex, idBig, lc } from '../vendor/axona-protocol/src/pubsub/ids.js';
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';

export const OWNER = '83866c66598304ed57767cf66b42b7a33b1884a47d8124317d3ad557995bb8df';
export const DESCRIPTOR = { region: 'useast', name: 'axona.bot', owner: OWNER, write: 'owner' };

let TOPIC_BIG = null;
export async function topicBig() {
  if (TOPIC_BIG === null) TOPIC_BIG = await deriveTopicIdBig(DESCRIPTOR);
  return TOPIC_BIG;
}

const short = (h) => (typeof h === 'string' ? h.slice(0, 12) : h == null ? null : String(h).slice(0, 12));
let LABEL = 'probe';
export function setLabel(l) { LABEL = l; }
export function log(ev, ctx = {}) {
  process.stdout.write(JSON.stringify({ t: Date.now(), label: LABEL, ev, ...ctx }) + '\n');
}

// ── prototype patches (call before connectPeer) ─────────────────────────
export function instrument() {
  const isOurs = (topicIdish) => {
    try { return TOPIC_BIG !== null && idBig(topicIdish) === TOPIC_BIG; } catch { return false; }
  };

  const origDeliver = AxonaManager.prototype._onDeliver;
  AxonaManager.prototype._onDeliver = function (payload, meta) {
    if (meta.targetId === this.nodeId && isOurs(payload.topicId)) {
      log('deliver', {
        from: short(payload.from),
        n: Array.isArray(payload.msgs) ? payload.msgs.length : 0,
        msgIds: (payload.msgs || []).map(m => short(m?.msgId)),
      });
    }
    return origDeliver.call(this, payload, meta);
  };

  const origPullResp = AxonaManager.prototype._onPullResp;
  AxonaManager.prototype._onPullResp = function (payload, meta) {
    log('pullresp', { corrId: payload?.corrId, hasJson: !!payload?.json, fromMeta: short(meta?.fromId != null ? idHex(idBig(meta.fromId)) : null) });
    return origPullResp.call(this, payload, meta);
  };

  const origBeacon = AxonaManager.prototype._onRootBeacon;
  AxonaManager.prototype._onRootBeacon = function (payload, meta) {
    if (payload && Array.isArray(payload.topics) && TOPIC_BIG !== null) {
      for (const tHex of payload.topics) {
        if (isOurs(tHex)) { log('beacon', { root: short(payload.root), layer: payload.layer }); break; }
      }
    }
    return origBeacon.call(this, payload, meta);
  };

  const origBecome = RootClaim.prototype.become;
  RootClaim.prototype.become = function (tBig, why) {
    if (TOPIC_BIG !== null && tBig === TOPIC_BIG) log('root-become', { why });
    return origBecome.call(this, tBig, why);
  };
  const origDemote = RootClaim.prototype.demote;
  RootClaim.prototype.demote = function (tBig, rootHex, why) {
    if (TOPIC_BIG !== null && tBig === TOPIC_BIG) log('root-demote', { to: short(rootHex), why });
    return origDemote.call(this, tBig, rootHex, why);
  };

  // Log the kernel's own pubsub warn/info stream for our probes too.
  const origLog = AxonaManager.prototype._log;
  AxonaManager.prototype._log = function (level, event, ctx) {
    log('kernel', { level, event, ctx });
    return origLog.call(this, level, event, ctx);
  };
}

// ── live-state introspection ─────────────────────────────────────────────
export function managerOf(peer) {
  return peer._axonaManager
    ?? (typeof peer._requireAxonaManager === 'function' ? peer._requireAxonaManager('diag') : null);
}

export function snapshot(peer) {
  const am = managerOf(peer);
  if (!am || TOPIC_BIG === null) return null;
  const t = TOPIC_BIG;
  const role = am.axonRoles.get(t);
  const sub = am.mySubscriptions.get(t);
  const hint = am._rootHint.get(t);
  const beacon = am._rootBeacons.get(t);
  return {
    me: short(lc(idHex(am.nodeId))),
    isRoot: role ? !!role.isRoot : false,
    roleCache: role ? role.cache.length : null,
    roleSubs: role ? role.subscribers.size : null,
    upstream: (am._upstream.get(t) || []).map(short),
    hint: hint ? { via: short(hint.via), ageMs: Date.now() - hint.at } : null,
    beacon: beacon ? { root: short(beacon.root), expInMs: beacon.exp - Date.now() } : null,
    subscribed: !!sub,
    subInterval: sub ? sub.interval : null,
    neighbors: (typeof am.dht.neighbors === 'function') ? (am.dht.neighbors() || []).length : null,
  };
}

// Iterative lookup of the topic's routing terminus (the emergent root as the
// network resolves it from THIS vantage). Slow (multi-round) — call sparingly.
export async function lookupTerminus(peer) {
  const am = managerOf(peer);
  if (!am || typeof am.dht.lookup !== 'function') return null;
  try {
    const r = await am.dht.lookup(await topicBig());
    const path = (r && Array.isArray(r.path)) ? r.path : [];
    return {
      terminus: path.length ? short(lc(idHex(idBig(path[path.length - 1])))) : null,
      hops: r?.hops ?? null,
      path: path.map(p => short(lc(idHex(idBig(p))))),
    };
  } catch (e) { return { error: String(e?.message || e) }; }
}
