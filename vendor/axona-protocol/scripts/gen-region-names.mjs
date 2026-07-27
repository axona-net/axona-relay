// gen-region-names.mjs — generate TWO names per region (the cell's two halves).
//
// Rules:
//   • Each 8-bit cell = two S2 level-3 sub-cells (its halves). Name each half
//     from the nearest anchor; both names resolve to one code. Prefer DISTINCT
//     real places (country, else large city). Homogeneous cells share one name.
//   • All names ≤ 8 chars, /^[a-z0-9_]{1,8}$/.
//   • LAND anchors may span cells with a single-letter COMPASS suffix
//     (russiae) — never a number. ISLAND anchors are CLAIM-ONCE (name only the
//     single closest cell).
//   • Open water → "<oce3>_<hex>" (pac_68, atl_0a, ind_22, sou_a3, arc_44).
//   • OVERRIDES below hand-fix specific cells.
//
// Output: literal [a,b] pairs (stdout) + face-grouped review (stderr).

import { geoCellSubCenters, S2_CELL_COUNT } from '../src/utils/s2.js';

const LAND_DIST = 10, ISLAND_DIST = 8, COMPASS_EPS = 2;

// [name(≤7 for land that may span; ≤8 for points/islands), lat, lng, 'land'|'island'].
const ANCHORS = [
  // North America
  ['uswest', 40, -121, 'land'], ['ussw', 33, -108, 'land'], ['uscentr', 41, -98, 'land'],
  ['useast', 41, -78, 'land'], ['usse', 32, -83, 'land'], ['canada', 58, -100, 'land'],
  ['quebec', 53, -72, 'land'], ['ontario', 50, -85, 'land'], ['alberta', 54, -114, 'land'],
  ['vancouvr', 50, -123, 'land'], ['alaska', 64, -150, 'land'], ['grnland', 72, -42, 'land'],
  ['mexico', 24, -103, 'land'], ['guatml', 15, -90, 'land'],
  // South America
  ['colombia', 4, -73, 'land'], ['caracas', 8, -65, 'land'], ['guyana', 5, -59, 'land'],
  ['amazon', -4, -62, 'land'], ['brazil', -9, -45, 'land'], ['saopalo', -23, -47, 'land'],
  ['bolivia', -17, -64, 'land'], ['paragwy', -24, -58, 'land'], ['argentn', -35, -64, 'land'],
  ['chile', -33, -71, 'land'], ['peru', -10, -76, 'land'], ['patagon', -45, -69, 'land'],
  // Europe
  ['uk', 54, -2, 'land'], ['iberia', 40, -4, 'land'], ['france', 47, 2, 'land'],
  ['germany', 51, 10, 'land'], ['europe', 48, 16, 'land'], ['italy', 42, 13, 'land'],
  ['sweden', 62, 16, 'land'], ['finland', 64, 27, 'land'], ['baltic', 56, 24, 'land'],
  ['poland', 52, 19, 'land'], ['balkans', 43, 21, 'land'], ['ukraine', 49, 32, 'land'],
  ['greece', 39, 22, 'land'], ['turkey', 39, 35, 'land'], ['moscow', 56, 40, 'land'],
  // Russia / North & Central Asia
  ['urals', 60, 64, 'land'], ['siberia', 62, 95, 'land'], ['eastsib', 66, 130, 'land'],
  ['mongolia', 47, 104, 'land'], ['kamchtk', 56, 159, 'land'], ['kazakh', 48, 67, 'land'],
  // Middle East
  ['levant', 33, 38, 'land'], ['arabia', 23, 45, 'land'], ['iraq', 33, 43, 'land'],
  ['iran', 32, 53, 'land'], ['afghan', 34, 66, 'land'], ['caucasus', 42, 45, 'land'],
  // Africa
  ['morocco', 31, -7, 'land'], ['algeria', 28, 2, 'land'], ['tunisia', 34, 9, 'land'],
  ['libya', 27, 17, 'land'], ['egypt', 27, 30, 'land'], ['mali', 18, -3, 'land'],
  ['niger', 17, 9, 'land'], ['chad', 15, 18, 'land'], ['sudan', 15, 30, 'land'],
  ['senegal', 14, -15, 'land'], ['westafr', 9, -6, 'land'], ['nigeria', 9, 8, 'land'],
  ['ethiopia', 9, 39, 'land'], ['kenya', 1, 38, 'land'], ['somalia', 6, 47, 'land'],
  ['tanzana', -6, 35, 'land'], ['congo', -2, 22, 'land'], ['angola', -12, 18, 'land'],
  ['zambia', -14, 27, 'land'], ['namibia', -22, 17, 'land'], ['botswan', -22, 24, 'land'],
  ['safrica', -30, 24, 'land'],
  // South / SE Asia
  ['pakistn', 30, 68, 'land'], ['india', 22, 78, 'land'], ['nepal', 28, 84, 'land'],
  ['bengal', 24, 89, 'land'], ['tibet', 32, 86, 'land'], ['china', 35, 105, 'land'],
  ['schina', 24, 110, 'land'], ['korea', 37, 127, 'land'], ['japan', 37, 138, 'land'],
  ['vietnam', 16, 107, 'land'], ['thailnd', 15, 101, 'land'], ['myanmar', 21, 96, 'land'],
  ['malaysa', 4, 102, 'land'], ['indonsa', -2, 118, 'land'],
  // Oceania
  ['ozwest', -26, 119, 'land'], ['ozcentr', -25, 134, 'land'], ['oznorth', -15, 133, 'land'],
  ['ozeast', -32, 147, 'land'],
  // Poles
  ['arctic', 86, 0, 'land'], ['antarc', -84, 0, 'land'], ['antpen', -68, -63, 'land'],
  ['eastant', -74, 90, 'land'], ['rossant', -82, 175, 'land'],
  // Islands (claim-once)
  ['iceland', 65, -18, 'island'], ['hawaii', 21, -157, 'island'], ['galapgs', 0, -91, 'island'],
  ['easter', -27, -109, 'island'], ['tahiti', -17, -149, 'island'], ['samoa', -14, -172, 'island'],
  ['fiji', -17, 178, 'island'], ['tonga', -21, -175, 'island'], ['newcald', -21, 165, 'island'],
  ['solomon', -9, 160, 'island'], ['guam', 13, 145, 'island'], ['marshl', 7, 168, 'island'],
  ['kiribti', 2, -157, 'island'], ['azores', 38, -28, 'island'], ['canary', 28, -16, 'island'],
  ['capevrd', 16, -24, 'island'], ['bermuda', 32, -65, 'island'], ['falklnd', -52, -59, 'island'],
  ['sthelna', -16, -6, 'island'], ['ascensn', -8, -14, 'island'], ['maldive', 3, 73, 'island'],
  ['mauritu', -20, 57, 'island'], ['seychel', -5, 55, 'island'], ['kerguel', -49, 69, 'island'],
  ['svalbrd', 78, 18, 'island'], ['faroe', 62, -7, 'island'], ['cuba', 21, -78, 'island'],
  ['bahamas', 24, -76, 'island'], ['caribbn', 15, -71, 'island'], ['srilnka', 7, 81, 'island'],
  ['taiwan', 24, 121, 'island'], ['madagas', -19, 47, 'island'], ['nzealnd', -42, 172, 'island'],
  ['borneo', 1, 114, 'island'], ['sumatra', 0, 101, 'island'], ['philipin', 13, 122, 'island'],
];

// Exact hand-fixes (code → {half: name}). Applied last; authoritative.
const OVERRIDES = {
  0x00: { 1: 'rio' }, 0x0f: { 0: 'mru', 1: 'ghana' },
  0x12: { 1: 'sardinia' }, 0x17: { 1: 'kenya' },
  0x18: { 0: 'tanzania', 1: 'mozambiq' }, 0x19: { 1: 'congos' }, 0x1e: { 1: 'botswana' },
  0x20: { 1: 'madagass' },
  0x27: { 0: 'madagasn' }, 0x2b: { 0: 'afghan' },
  0x2d: { 0: 'tibet', 1: 'india_n' }, 0x2e: { 0: 'india_s', 1: 'india_s' },
  0x34: { 1: 'chinase' }, 0x38: { 0: 'westpap' },
  0x41: { 0: 'moscow' }, 0x46: { 0: 'scan' }, 0x47: { 0: 'easteu', 1: 'westeu' },
  0x48: { 1: 'iceland' },
  0x4d: { 0: 'ontario' }, 0x4e: { 0: 'grnlands', 1: 'grnlands' }, 0x4f: { 0: 'grnlandn' },
  0x52: { 0: 'manitoba', 1: 'manitoba' }, 0x53: { 1: 'alberta' }, 0x54: { 0: 'britcol', 1: 'usnw' },
  0x5e: { 0: 'chinane', 1: 'khabaro' }, 0x6b: { 1: 'pac_6b' }, 0x6c: { 0: 'hawaii', 1: 'hawaii' },
  0x7d: { 0: 'papnewgn', 1: 'papnewgn' },
  0x86: { 0: 'uss', 1: 'mexico' }, 0x8d: { 1: 'venezuel' },
};

// Single-name overrides (code → name), applied AFTER the two halves collapse to
// ONE name per region. Authoritative. Used where the collapsed default isn't
// the wanted label — a city for a multi-country cell, or an area that spans
// cells (the same name may legitimately cover more than one code).
const SINGLE_OVERRIDES = {
  // North America
  0x80: 'uswest', 0x81: 'mexico', 0x84: 'mexicow', 0x8f: 'centrlam',
  0x87: 'uscentlw', 0x88: 'uscentle', 0x89: 'useast',
  0x86: 'usmex', 0x54: 'usswbc',
  // Europe / Mid-East / Asia / Pacific
  0x48: 'britisle', 0x2a: 'arabia', 0x2e: 'india', 0x31: 'indochin',
  0x34: 'chinatw', 0x7c: 'bougain', 0xbd: 'nzealnds',
};

const DEG = Math.PI / 180;
const unit = (lat, lng) => { const a = lat*DEG, b = lng*DEG, c = Math.cos(a); return [c*Math.cos(b), c*Math.sin(b), Math.sin(a)]; };
const angDeg = (u, v) => Math.acos(Math.max(-1, Math.min(1, u[0]*v[0]+u[1]*v[1]+u[2]*v[2]))) / DEG;
const A = ANCHORS.map(([name, lat, lng, type]) => ({ name, lat, lng, type, v: unit(lat, lng) }));
const LAND = A.filter(a => a.type === 'land');
const ISLAND = A.filter(a => a.type === 'island');
const hex2 = (c) => c.toString(16).padStart(2, '0');
const cap8 = (s) => s.slice(0, 8);

function basin(lat, lng) {
  let L = lng; while (L > 180) L -= 360; while (L < -180) L += 360;
  if (lat >= 66) return 'arc';
  if (lat <= -55) return 'sou';
  if (L >= -70 && L <= 20) return 'atl';
  if (L > 20 && L <= 110 && lat < 31) return 'ind';
  return 'pac';
}
function nearest(list, u) { let best = null, bd = Infinity; for (const a of list) { const d = angDeg(u, a.v); if (d < bd) { bd = d; best = a; } } return { anchor: best, dist: bd }; }
function compass(lat, lng, a) {
  let dLng = lng - a.lng; while (dLng > 180) dLng -= 360; while (dLng < -180) dLng += 360;
  const dLat = lat - a.lat;
  if (Math.abs(dLat) < COMPASS_EPS && Math.abs(dLng) < COMPASS_EPS) return '';
  return Math.abs(dLat) >= Math.abs(dLng) ? (dLat >= 0 ? 'n' : 's') : (dLng >= 0 ? 'e' : 'w');
}

const halves = [];
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const [c0, c1] = geoCellSubCenters(code);
  halves.push({ code, idx: 0, c: c0, u: unit(c0.lat, c0.lng) });
  halves.push({ code, idx: 1, c: c1, u: unit(c1.lat, c1.lng) });
}
const key = (code, idx) => code * 2 + idx;
const NAMES = Array.from({ length: S2_CELL_COUNT }, () => [null, null]);
const usedNames = new Set();
const islandClaimed = new Set();

// Pass 1: island claim-once.
const islandCand = ISLAND.map(isl => {
  const cs = halves.map(h => ({ h, d: angDeg(h.u, isl.v) })).filter(x => x.d <= ISLAND_DIST).sort((a, b) => a.d - b.d);
  return { isl, cs, best: cs.length ? cs[0].d : Infinity };
}).filter(x => x.cs.length).sort((a, b) => a.best - b.best);
for (const { isl, cs } of islandCand) {
  const first = cs.find(x => !islandClaimed.has(key(x.h.code, x.h.idx)));
  if (!first) continue;
  const winner = first.h.code;
  for (const x of cs) if (x.h.code === winner && !islandClaimed.has(key(x.h.code, x.h.idx))) {
    NAMES[x.h.code][x.h.idx] = isl.name; islandClaimed.add(key(x.h.code, x.h.idx));
  }
  usedNames.add(isl.name);
}

// Pass 2: classify remaining; ocean now, collect land.
const landEntries = [];
for (let code = 0; code < S2_CELL_COUNT; code++) {
  const [c0, c1] = geoCellSubCenters(code);
  const cls = [{ idx: 0, c: c0 }, { idx: 1, c: c1 }].map(h => {
    if (NAMES[code][h.idx] !== null) return { idx: h.idx, kind: 'island' };
    const land = nearest(LAND, unit(h.c.lat, h.c.lng));
    if (land.anchor && land.dist <= LAND_DIST) return { idx: h.idx, kind: 'land', base: land.anchor.name, anchor: land.anchor, c: h.c };
    return { idx: h.idx, kind: 'ocean', c: h.c };
  });
  for (const c of cls) if (c.kind === 'ocean') { const nm = `${basin(c.c.lat, c.c.lng)}_${hex2(code)}`; NAMES[code][c.idx] = nm; usedNames.add(nm); }
  const lands = cls.filter(c => c.kind === 'land');
  if (lands.length === 2 && lands[0].base === lands[1].base)
    landEntries.push({ slots: [[code, 0], [code, 1]], base: lands[0].base, anchor: lands[0].anchor, code, center: { lat: (c0.lat+c1.lat)/2, lng: (c0.lng+c1.lng)/2 } });
  else for (const c of lands) landEntries.push({ slots: [[code, c.idx]], base: c.base, anchor: c.anchor, code, center: c.c });
}

// Pass 3: land naming — closest entry keeps bare name; rest get compass; _hex last.
const byBase = new Map();
for (const e of landEntries) { if (!byBase.has(e.base)) byBase.set(e.base, []); byBase.get(e.base).push(e); }
for (const [base, group] of byBase) {
  group.forEach(e => { e.dist = angDeg(unit(e.center.lat, e.center.lng), e.anchor.v); });
  group.sort((a, b) => a.dist - b.dist);
  group.forEach((e, i) => {
    const cands = [];
    if (i === 0) cands.push(base);
    const oc = compass(e.center.lat, e.center.lng, e.anchor);
    if (oc) cands.push(cap8(base + oc));
    cands.push(cap8(`${base}_${hex2(e.code)}`));
    const name = cands.find(c => !usedNames.has(c)) ?? cap8(`${base}_${hex2(e.code)}`);
    usedNames.add(name);
    for (const [code, half] of e.slots) NAMES[code][half] = name;
  });
}

// Pass 4: apply overrides.
for (const [code, m] of Object.entries(OVERRIDES))
  for (const [half, name] of Object.entries(m)) NAMES[+code][+half] = name;

// Pass 5: collapse each cell's two halves to ONE name.
//   • both halves identical → that name
//   • one ocean half + one land half → the LAND name (ocean takes the landmass)
//   • both halves ocean → the (shared) ocean name
//   • two different lands → keep half 0; pick the wanted city/area via SINGLE_OVERRIDES
const isOcean = (s) => /^(pac|atl|ind|sou|arc)_[0-9a-f]{2}$/.test(s);
const SINGLE = NAMES.map(([a, b]) => {
  if (a === b) return a;
  if (isOcean(a) && !isOcean(b)) return b;
  if (isOcean(b) && !isOcean(a)) return a;
  return a;                                   // both ocean, or two lands → half 0
});
for (const [code, name] of Object.entries(SINGLE_OVERRIDES)) SINGLE[+code] = name;

// ── Review → stderr ──
const pad = (s, n) => String(s).padEnd(n);
for (let f = 0; f < 6; f++) {
  process.stderr.write(`\n── face ${f} ──\n`);
  for (let code = f * 32; code < (f + 1) * 32; code++) {
    const [c0, c1] = geoCellSubCenters(code);
    process.stderr.write(`0x${hex2(code)}  ${pad(SINGLE[code], 9)}` +
      `  (${c0.lat.toFixed(0)},${c0.lng.toFixed(0)} | ${c1.lat.toFixed(0)},${c1.lng.toFixed(0)})\n`);
  }
}
const over = SINGLE.filter(n => n.length > 8);
const dups = [...new Set(SINGLE.filter((n, i) => SINGLE.indexOf(n) !== i))];
process.stderr.write(`\nnames=${SINGLE.length}  distinct=${new Set(SINGLE).size}  maxlen=${Math.max(...SINGLE.map(n => n.length))}` +
  (over.length ? `  OVER-8=${[...new Set(over)].join(',')}` : '  (all ≤8)') +
  (dups.length ? `  multi-cell=${dups.join(',')}` : '') + '\n');

// ── Literal → stdout ──  (one name per region)
let out = '';
for (let code = 0; code < S2_CELL_COUNT; code++) out += `  '${SINGLE[code]}',  // 0x${hex2(code)}\n`;
process.stdout.write(out);
