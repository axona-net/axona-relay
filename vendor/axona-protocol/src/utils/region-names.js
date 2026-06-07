/**
 * region-names.js — canonical human-readable names for all 192 Axona regions.
 *
 * Every Axona node/topic carries an 8-bit S2 region code in the top byte of
 * its 264-bit id (see utils/s2.js — codes [0,192), 192..255 reserved). This
 * module gives each of the 192 codes a stable, short, human-readable name so
 * you can see at a glance which region a node claims to live within, and so a
 * region name can be used interchangeably with its numeric code as a prefix.
 *
 * The names are CURATED from each cell's geographic center (the nearest named
 * land region or ocean basin), generated once by scripts/gen-region-names.mjs
 * and frozen here as the canonical list — part of the protocol. Properties
 * guaranteed (and enforced by test/smoke_region_names.js):
 *
 *   • exactly 192 entries, index === the 8-bit region code
 *   • each name matches /^[a-z0-9]{1,8}$/  (lowercase, no spaces, ≤ 8 chars)
 *   • all 192 names are unique  ⇒  name ↔ code is a bijection
 *
 * Coarse by nature: an 8-bit cell spans ~2000 km, so a name marks the broad
 * region a cell falls in, not a precise town. Ocean/polar cells take basin
 * names with a compass suffix (e.g. npacw, sthne).
 */

import { geoCellId, isValidCellId, S2_CELL_COUNT } from './s2.js';

/** Canonical region name for every 8-bit code, indexed by code [0,192). */
export const REGION_NAMES = Object.freeze([
  'satlw', 'satlw2', 'satlans', 'satlan2', 'satlan', 'wafrics',
  'brazile', 'brazil', 'brazilne', 'natlanse', 'natlan', 'natles',
  'iberiaw', 'iberia', 'wafric2', 'wafric', 'sahels', 'sahel',
  'iberiae', 'medsea', 'egypt', 'mideast', 'redsea', 'eafric',
  'eafric2', 'congo', 'angola2', 'angola', 'safricw', 'safricsw',
  'safric', 'madags', 'madag', 'indianw', 'sindoc', 'indian',
  'indiann', 'nindia', 'madagne', 'madagn', 'horn', 'arabsea',
  'gulf', 'pakist2', 'pakist', 'tibet', 'sindia', 'sindia2',
  'malay', 'seasia', 'chinaw', 'china', 'taiwan', 'taiwane',
  'philip', 'philips', 'ausnth', 'indones', 'malay2', 'malays',
  'sindocne', 'sindoce', 'auswest', 'auscen', 'blacks', 'caspia',
  'sibwest', 'ural', 'uraln', 'arctic', 'nordic', 'germany',
  'britain', 'natlen', 'natle', 'labrad', 'cannef', 'cannefw',
  'grnland', 'grnlande', 'arcticsw', 'alaskae', 'canada', 'canadaw',
  'uswestn', 'uswestw', 'alaska', 'bering', 'kamchke', 'kamchk',
  'yakutian', 'yakutia', 'sibest', 'mongol', 'korea', 'japan2',
  'japan', 'japans', 'pngn', 'pngn2', 'pngne', 'npacws',
  'npacw2', 'npacw', 'npacwe', 'npacif', 'npacife', 'hawaii',
  'hawaiie', 'hawaiise', 'hawaiis', 'hawaiisw', 'fiji2', 'spacw',
  'polynn', 'polyn', 'polyns', 'spacifsw', 'spacws', 'nzeale',
  'nzeal', 'fijisw', 'fiji', 'corale', 'coral', 'png',
  'auseast2', 'auseast', 'uswest', 'npace', 'npaces', 'npacese',
  'mexicos', 'camer2', 'mexico', 'uscen', 'useast2', 'useast',
  'natlw2', 'natlw', 'venez', 'amazon2', 'colomb', 'camer',
  'peruw', 'peru', 'amazon', 'brazilw', 'saopau', 'argent',
  'chile', 'spacee', 'space2', 'space', 'peruw2', 'spacenw',
  'spacifne', 'spacifn', 'spacif', 'spacifs', 'spacifs2', 'spacifs3',
  'nzeale2', 'antross2', 'antrossn', 'antross', 'antpennw', 'spaces',
  'patagosw', 'patago', 'sthnwnw', 'sthnw', 'sthnwe', 'sthnw2',
  'antpene', 'antpen', 'anteast', 'antarc', 'sthn', 'sthne',
  'sthnne', 'madags2', 'sthnew', 'sthne2', 'sthnee', 'sthnee2',
  'anteaste', 'sthnps', 'sthnp', 'tasman', 'sthnpnw', 'auscens',
]);

/** name (lowercase) → code. Built once. */
const NAME_TO_CODE = (() => {
  const m = new Map();
  REGION_NAMES.forEach((name, code) => m.set(name, code));
  return m;
})();

/**
 * Region name for an 8-bit region code.
 * @param {number} code  integer in [0,192)
 * @returns {string|null}  the name, or null for an invalid/reserved code
 */
export function regionName(code) {
  return isValidCellId(code) ? REGION_NAMES[code] : null;
}

/**
 * Region code for a region name (case-insensitive).
 * @param {string} name
 * @returns {number|null}  code in [0,192), or null if unknown
 */
export function regionCode(name) {
  if (typeof name !== 'string') return null;
  const code = NAME_TO_CODE.get(name.trim().toLowerCase());
  return code === undefined ? null : code;
}

/**
 * Resolve a region token — a NAME or a numeric code — to its 8-bit code.
 * This is what lets a region name be used as a prefix interchangeably with the
 * raw code: accepts 'useast', 'USEAST', '0x89', '137', or the number 137.
 * @param {string|number} token
 * @returns {number|null}  code in [0,192), or null if unresolvable
 */
export function resolveRegion(token) {
  if (typeof token === 'number') return isValidCellId(token) ? token : null;
  if (typeof token !== 'string') return null;
  const t = token.trim();
  const byName = regionCode(t);
  if (byName !== null) return byName;
  // numeric forms: 0x.. hex, or plain decimal
  const n = /^0x[0-9a-f]+$/i.test(t) ? parseInt(t, 16)
          : /^\d+$/.test(t)          ? parseInt(t, 10)
          : NaN;
  return isValidCellId(n) ? n : null;
}

/**
 * Convenience: the region name for a (lat,lng) coordinate.
 * @param {number} lat
 * @param {number} lng
 * @returns {string}
 */
export function regionNameForLatLng(lat, lng) {
  return REGION_NAMES[geoCellId(lat, lng, 8)];
}

/** Sanity: the list length must match the S2 cell count. */
if (REGION_NAMES.length !== S2_CELL_COUNT) {
  throw new Error(
    `region-names.js: have ${REGION_NAMES.length} names, expected ${S2_CELL_COUNT}`);
}
