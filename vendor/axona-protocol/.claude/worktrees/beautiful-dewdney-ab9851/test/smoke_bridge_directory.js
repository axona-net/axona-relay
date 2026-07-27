// =====================================================================
// smoke_bridge_directory.js — bridge directory spec: build, validate,
//                             haversine, and the layered failover ranking.
// Run: node test/smoke_bridge_directory.js
// =====================================================================

import {
  BRIDGE_DIRECTORY_TOPIC,
  BRIDGE_ENTRY_MAX_AGE_MS,
  buildBridgeEntry,
  validateBridgeEntry,
  rankBridges,
  haversineKm,
} from '../src/bridgeDirectory.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const NOW = 1_750_000_000_000;   // fixed clock for determinism
const SF  = { lat: 37.77, lng: -122.42 };

console.log('\n── topic + entry build ──');
check('topic is the public directory name', BRIDGE_DIRECTORY_TOPIC === 'axona:bridge-directory');
const e1 = buildBridgeEntry({ url: 'wss://bridge.axona.net', lat: 38, lng: -77, label: 'us-east', ver: '2.42.0', ts: NOW });
check('buildBridgeEntry keeps fields', e1.url === 'wss://bridge.axona.net' && e1.label === 'us-east' && e1.ts === NOW);
check('buildBridgeEntry defaults ts when omitted', typeof buildBridgeEntry({ url: 'wss://x.net', lat: 0, lng: 0 }).ts === 'number');

console.log('\n── validate ──');
check('accepts a well-formed wss entry', !!validateBridgeEntry({ url: 'wss://b.example', lat: 10, lng: 20, ts: NOW }));
check('rejects non-wss (ws://) url',  validateBridgeEntry({ url: 'ws://b.example', lat: 10, lng: 20 }) === null);
check('rejects https url',            validateBridgeEntry({ url: 'https://b.example', lat: 10, lng: 20 }) === null);
check('rejects missing url',          validateBridgeEntry({ lat: 10, lng: 20 }) === null);
check('rejects out-of-range lat',     validateBridgeEntry({ url: 'wss://b', lat: 999, lng: 0 }) === null);
check('rejects non-number lng',       validateBridgeEntry({ url: 'wss://b', lat: 0, lng: '5' }) === null);
check('rejects non-object',           validateBridgeEntry('nope') === null);
check('clamps label length', validateBridgeEntry({ url: 'wss://b', lat: 0, lng: 0, label: 'x'.repeat(200) }).label.length === 64);

console.log('\n── turn endpoint ──');
const eTurn = buildBridgeEntry({ url: 'wss://b.net', lat: 0, lng: 0, turn: 'turn:b.net:3478,turns:b.net:5349' });
check('buildBridgeEntry parses comma turn → array', Array.isArray(eTurn.turn) && eTurn.turn.length === 2);
check('buildBridgeEntry accepts turn array', buildBridgeEntry({ url:'wss://b', lat:0, lng:0, turn:['turn:b:3478'] }).turn[0] === 'turn:b:3478');
check('no turn → no turn field', buildBridgeEntry({ url:'wss://b', lat:0, lng:0 }).turn === undefined);
check('validate passes turn through', validateBridgeEntry({ url:'wss://b', lat:0, lng:0, turn:['turns:b:5349'] }).turn[0] === 'turns:b:5349');
check('validate drops non-turn scheme', validateBridgeEntry({ url:'wss://b', lat:0, lng:0, turn:['http://evil','turn:ok:3478'] }).turn.length === 1);
check('validate omits turn when none valid', validateBridgeEntry({ url:'wss://b', lat:0, lng:0, turn:['nope'] }).turn === undefined);

console.log('\n── haversine ──');
check('zero distance to self', haversineKm(SF, SF) < 1);
const nyc = { lat: 40.71, lng: -74.0 };
const km = haversineKm(SF, nyc);
check('SF↔NYC ≈ 4130 km', km > 4000 && km < 4300);
check('missing point → Infinity', haversineKm(null, SF) === Infinity);

console.log('\n── rankBridges: roots first, never displaced ──');
{
  const entries = [
    validateBridgeEntry({ url: 'wss://far.example',  lat: 35.7, lng: 139.7, ts: NOW }),   // Tokyo
    validateBridgeEntry({ url: 'wss://near.example', lat: 37.8, lng: -122.3, ts: NOW }),   // ~SF
  ];
  const r = rankBridges({ roots: ['wss://primary.example'], entries, self: SF, now: NOW });
  check('root is first', r[0].url === 'wss://primary.example' && r[0].source === 'root');
  check('nearer fresh bridge outranks farther', r[1].url === 'wss://near.example');
  check('farther fresh bridge last', r[2].url === 'wss://far.example');
  check('all three present', r.length === 3);
}

console.log('\n── rankBridges: known-good (prior success) beats fresh ──');
{
  const entries = [
    validateBridgeEntry({ url: 'wss://near-fresh.example', lat: 37.8, lng: -122.3, ts: NOW }),
    validateBridgeEntry({ url: 'wss://far-known.example',  lat: 35.7, lng: 139.7, ts: NOW }),
  ];
  const reputation = { 'wss://far-known.example': { okCount: 3, lastOkAt: NOW - 1000, lastTimeToMeshMs: 900 } };
  const r = rankBridges({ roots: [], entries, reputation, self: SF, now: NOW });
  check('known-good (even if far) ranks before fresh', r[0].url === 'wss://far-known.example' && r[0].source === 'known');
  check('fresh follows', r[1].url === 'wss://near-fresh.example' && r[1].source === 'fresh');
}

console.log('\n── rankBridges: known ordered by recency then latency ──');
{
  const mk = (u) => validateBridgeEntry({ url: u, lat: 38, lng: -77, ts: NOW });
  const entries = [mk('wss://a'), mk('wss://b'), mk('wss://c')];
  const reputation = {
    'wss://a': { okCount: 1, lastOkAt: NOW - 5000, lastTimeToMeshMs: 500 },
    'wss://b': { okCount: 1, lastOkAt: NOW - 1000, lastTimeToMeshMs: 2000 },  // most recent
    'wss://c': { okCount: 1, lastOkAt: NOW - 1000, lastTimeToMeshMs: 300 },   // tie on recency, faster
  };
  const r = rankBridges({ roots: [], entries, reputation, self: SF, now: NOW }).map((x) => x.url);
  check('most-recent success first', r[0] === 'wss://c' || r[0] === 'wss://b');
  check('recency tie broken by lower latency (c before b)', r.indexOf('wss://c') < r.indexOf('wss://b'));
  check('older success last among known', r[2] === 'wss://a');
}

console.log('\n── rankBridges: prior-failure sinks below fresh; stale dropped ──');
{
  const entries = [
    validateBridgeEntry({ url: 'wss://failed.example', lat: 37.8, lng: -122.3, ts: NOW }),
    validateBridgeEntry({ url: 'wss://fresh.example',  lat: 35.7, lng: 139.7, ts: NOW }),
    validateBridgeEntry({ url: 'wss://stale.example',  lat: 37.8, lng: -122.3, ts: NOW - BRIDGE_ENTRY_MAX_AGE_MS - 1 }),
  ];
  const reputation = { 'wss://failed.example': { failCount: 2 } };
  const r = rankBridges({ roots: [], entries, reputation, self: SF, now: NOW }).map((x) => x.url);
  check('stale entry dropped', !r.includes('wss://stale.example'));
  check('fresh outranks prior-failure', r.indexOf('wss://fresh.example') < r.indexOf('wss://failed.example'));
  check('prior-failure still listed (last resort)', r.includes('wss://failed.example'));
}

console.log('\n── rankBridges: dedupe + empty ──');
{
  const entries = [validateBridgeEntry({ url: 'wss://primary.example', lat: 38, lng: -77, ts: NOW })];
  const r = rankBridges({ roots: ['wss://primary.example'], entries, self: SF, now: NOW });
  check('a directory entry equal to a root is not duplicated', r.length === 1 && r[0].source === 'root');
  check('empty inputs → empty list', rankBridges({}).length === 0);
}

console.log(`\nResult: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
