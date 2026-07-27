// region.js — resolve the anchor region for an Axona app instance.
//
// In the v0.3 topic model, REGION IS A FIRST-CLASS PART OF THE TOPIC DESCRIPTOR
// ({ region, name }), not something an app keys in by hand. So this helper no
// longer builds a synthetic publisher (the old "prefix + 64 zeros" anchor that
// faked region into a publisher-keyed topic-id); it simply resolves a ?region=
// token to the canonical kernel region (name/code/center) and hands the app a
// region token to drop straight into its topic descriptors.
//
// THE ORIGINAL FIX still holds: an app derives BOTH the peer's own NODE identity
// AND its topic region from the SAME resolved region, so a co-located deployment
// is a self-contained keyspace — local nodes root local topics and the bridge is
// only the WebSocket rendezvous, never a data root.
//
// Precedence:  ?region=<name|hex>  ›  `fallback`  ›  DEFAULT_REGION
// The token is a region name ("uswest") or an 8-bit hex code ("0x80").
//
// CONVERGENCE RULE: all participants of a topic MUST resolve to the SAME region,
// or they compute different topic-ids and never find each other. For a regional
// deployment everyone shares the same URL (so the same ?region=); for a shared
// channel, put the region in the link so a joiner adopts it (see buildShareUrl).

import { resolveRegion, regionName, regionCenter } from '/src/utils/region-names.js';

// Backward-compatible default: the existing live network + the bench collector
// live in us-east, so an app with no ?region= keeps converging with them.
export const DEFAULT_REGION = 'useast';

/**
 * @param {object} [opts]
 * @param {string} [opts.search]    location.search to read ?region= from.
 * @param {string} [opts.fallback]  region used when no ?region= is present.
 * @returns {{ token:string, code:number, name:string, center:{lat,lng} }}
 *          `token` → the `region` field of a `{ region, name }` topic descriptor
 *          AND the placement for this peer's own node identity;
 *          `center` → createNodeIdentity({lat,lng}) when no real geolocation.
 */
export function resolveAnchor({ search = (typeof location !== 'undefined' ? location.search : ''),
                                fallback = DEFAULT_REGION } = {}) {
  const param = new URLSearchParams(search).get('region');
  const token = (param || fallback || DEFAULT_REGION);
  let code = resolveRegion(token);
  if (code == null) code = resolveRegion(DEFAULT_REGION);   // unknown token → safe default
  const name   = regionName(code);
  const center = regionCenter(code);
  // `token` is normalized to the canonical region name so it round-trips through
  // resolveRegion identically for every participant (a topic's region field must
  // resolve to the same code everywhere or topic-ids diverge).
  return { token: name, code, name, center };
}

/**
 * A share/QR URL that carries the region, so whoever opens it resolves to the
 * SAME keyspace as the creator (and thus the same topic-ids). Preserves any
 * existing query params and merges in region (+ optional extras like ?join=).
 */
export function buildShareUrl(regionToken, extraParams = {}) {
  const url = new URL(location.origin + location.pathname);
  const p = new URLSearchParams(location.search);
  p.set('region', regionToken);
  for (const [k, v] of Object.entries(extraParams)) p.set(k, v);
  url.search = p.toString();
  return url.toString();
}
