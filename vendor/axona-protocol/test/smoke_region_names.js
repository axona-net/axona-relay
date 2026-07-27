// =====================================================================
// smoke_region_names.js — canonical-region model: 84 major regions with
// neutral animal names, every ocean/sparse cell folds to its nearest major,
// and every legacy name still resolves (as a hidden alias) to its canonical
// region so nothing built on the old names breaks.
//
// Run: node test/smoke_region_names.js
// =====================================================================

import {
  REGION_NAMES, regionNames, regionName, regionCode, resolveRegion,
  regionNameForLatLng, MAJORS, canonicalRegion, CANONICAL_REGIONS, POPULATED_REGIONS,
} from '../src/utils/region-names.js';
import { S2_CELL_COUNT, geoCellId } from '../src/utils/s2.js';

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

console.log('Axona region-names smoke (canonical fold + animal names)');

console.log('\n── MAJORS: 84 canonical regions, well-formed, unique ──');
const majorCodes = Object.keys(MAJORS).map(Number);
const majorNames = majorCodes.map(c => MAJORS[c]);
check('84 major regions', majorCodes.length === 84);
check('CANONICAL_REGIONS has one entry per major', CANONICAL_REGIONS.length === 84);
check('POPULATED_REGIONS aliases CANONICAL_REGIONS', POPULATED_REGIONS === CANONICAL_REGIONS);
const fmt = /^[a-z0-9]{2,16}$/;   // full common animal names, no length squeeze
const badFmt = majorNames.filter(n => !fmt.test(n));
check('all animal names match /^[a-z0-9]{2,16}$/', badFmt.length === 0);
if (badFmt.length) console.log('     offenders:', badFmt);
const dupes = majorNames.filter((n, i) => majorNames.indexOf(n) !== i);
check('all animal names unique', dupes.length === 0);
if (dupes.length) console.log('     dupes:', [...new Set(dupes)]);

console.log('\n── REGION_NAMES: 192 cells, each shows its canonical animal ──');
check(`exactly ${S2_CELL_COUNT} entries`, REGION_NAMES.length === S2_CELL_COUNT);
check('every entry is a major animal name',
  REGION_NAMES.every(n => majorNames.includes(n)));
check('list frozen', Object.isFrozen(REGION_NAMES));
check('a major cell shows its own animal (0x89 → eagle)', REGION_NAMES[0x89] === 'eagle');
check('regionName(code) === REGION_NAMES[code]', regionName(0x2c) === REGION_NAMES[0x2c]);

console.log('\n── canonicalRegion: fold is idempotent, always lands on a major ──');
let idem = true, allMajor = true;
for (let c = 0; c < S2_CELL_COUNT; c++) {
  const canon = canonicalRegion(c);
  if (!majorCodes.includes(canon)) allMajor = false;
  if (canonicalRegion(canon) !== canon) idem = false;
}
check('every cell folds to a MAJOR code', allMajor);
check('canonicalRegion is idempotent (canon of canon = canon)', idem);
check('a major folds to itself (0x2c → 0x2c)', canonicalRegion(0x2c) === 0x2c);

console.log('\n── the political fix: no country name on the contested cell ──');
check('0x2c (NW-India/E-Pakistan) is "chinkara", not "pakistn"', REGION_NAMES[0x2c] === 'chinkara');
check('legacy "pakistn" still resolves → 0x2c', resolveRegion('pakistn') === 0x2c);
check('"chinkara" resolves → 0x2c', resolveRegion('chinkara') === 0x2c);

console.log('\n── the hotspot fix: ocean/sparse cells fold to real regions ──');
check('ocean cell 0x68 (pac_68) is NOT its own region', REGION_NAMES[0x68] !== 'pac_68');
check('resolveRegion("pac_68") folds off the ocean cell', resolveRegion('pac_68') !== 0x68);
check('resolveRegion(0x68 numeric) folds off the ocean cell', resolveRegion(0x68) !== 0x68);
check('mid-Pacific (30,-160) mints into a major', majorCodes.includes(canonicalRegion(geoCellId(30, -160, 8))));
check('Antarctica (-80,100) mints into a major', majorCodes.includes(canonicalRegion(geoCellId(-80, 100, 8))));
// Greenland override: both cells (west 0x4e, north/east 0x4f) fold to caribou (0x4c).
check('W Greenland 0x4e folds to caribou 0x4c', canonicalRegion(0x4e) === 0x4c);
check('NE Greenland 0x4f folds to caribou 0x4c (override, not reindeer)', canonicalRegion(0x4f) === 0x4c);

console.log('\n── wire-compat: the live relay regions keep their exact codes ──');
check('legacy "useast" → 0x89 (unchanged; eagle is a major)', resolveRegion('useast') === 0x89);
check('legacy "uswest" → 0x80 (unchanged; grizzly is a major)', resolveRegion('uswest') === 0x80);
check('legacy "uscentlw" → 0x87 (unchanged; bison is a major)', resolveRegion('uscentlw') === 0x87);

console.log('\n── resolveRegion: name OR numeric, always canonical ──');
check("resolveRegion('EAGLE') case-insensitive → 0x89", resolveRegion('EAGLE') === 0x89);
check('resolveRegion("137") === 0x89', resolveRegion('137') === 0x89);
check('resolveRegion(137) === 0x89', resolveRegion(137) === 0x89);
check('resolveRegion(192) === null (reserved)', resolveRegion(192) === null);
check('resolveRegion("nope") === null', resolveRegion('nope') === null);

console.log('\n── regionNames shim + regionNameForLatLng ──');
check("regionNames(0x89) → ['eagle']", (() => {
  const a = regionNames(0x89); return Array.isArray(a) && a.length === 1 && a[0] === 'eagle';
})());
check('regionNameForLatLng(Kansas 39,-98) === bison', regionNameForLatLng(39, -98) === 'bison');
check('regionNameForLatLng(lat,lng) === REGION_NAMES[geoCellId(...)]',
  regionNameForLatLng(37, -122) === REGION_NAMES[geoCellId(37, -122, 8)]);

console.log('\n── invalid / reserved ──');
check('regionName(192) === null', regionName(192) === null);
check('regionName(255) === null', regionName(255) === null);
check('regionCode("nope") === null', regionCode('nope') === null);

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
