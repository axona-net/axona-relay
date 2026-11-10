#!/usr/bin/env node
// =====================================================================
// fence_peer_dump.mjs — the `peer-dump` line (0.198.0, David 2026-11-09:
// "do you also list the connected nodes to each node you query? Please do
// that").
//
// buildPeerDump is pure: health() and the transport go in, one JSON-able
// object comes out. These checks drive it with a faithful transport, with
// transports that are missing or throw, and with hostile values in every
// field it reads.
//   1  idHex: bigint and 66-hex pass; negative, oversized, number, object,
//      upper-case and short strings are null
//   2  faithful: one row per bound channel, link kind from the candidate
//      types (webrtc, turn, mesh, ws, null), rtt, state, class, open age
//   3  unbound = the COUNT of synaptome ids with no bound channel; self and
//      bound ids out; no such id appears anywhere in the line (0.200.0)
//   4  no transport, a throwing boundPeers: complete false, count NULL (a
//      list that could not be read is never an empty list)
//   5  throwing getPeers / meshIdFor / classOf / getLatency: rows survive
//   6  hostile bound list: self, duplicates, junk types, bad bigints dropped
//   7  hostile mesh row: non-string and oversized candidate types, NaN and
//      negative rtt, future and zero openedAt all become null
//   8  a 120-peer dump renders whole through renderCtx and stays under the
//      journald line limit (49152 bytes); the same object under another
//      event name is cut (control: the exemption is what keeps it whole)
// Run: node test/fence_peer_dump.mjs
// =====================================================================
import { buildPeerDump, idHex } from '../src/peerdump.js';
import { renderCtx } from '../src/logctx.js';

let pass = 0, fail = 0;
const ok = (m, c, x = '') => { if (c) { pass++; console.log(`  ✓ ${m}`); } else { fail++; console.log(`  ✗ ${m} ${x}`); } };
const big = (n) => (0x89n << 256n) | BigInt(n);
const hx = (n) => big(n).toString(16).padStart(66, '0');
const NOW = 1_800_000_000_000;

const SELF = big(1), A = big(10), B = big(11), C = big(12), D = big(13), BR = big(99), E = big(20);

function transport(over = {}) {
  const meshIds = new Map([[A, 'mA'], [B, 'mB'], [C, 'mC']]);
  return {
    boundPeers: () => [A, B, C, D, BR],
    getLatency: (id) => (id === D ? 41.26 : id === BR ? 7 : -1),
    bridgeNodeIdBig: BR,
    mesh: { getPeers: () => [
      { peerId: 'mA', state: 'open', openedAt: NOW - 5000, rttLast: 12.34, localCand: 'host', remoteCand: 'srflx' },
      { peerId: 'mB', state: 'open', openedAt: NOW - 100, rttLast: 80, localCand: 'relay', remoteCand: 'host' },
      { peerId: 'mC', state: 'datachannel-opening', openedAt: 0, rttLast: null, localCand: null, remoteCand: null },
      { peerId: 'mZ', state: 'open', openedAt: NOW - 1, rttLast: 1, localCand: 'host', remoteCand: 'host' },
    ] },
    webrtc: {
      meshIdFor: (id) => meshIds.get(id) ?? null,
      classOf: (id) => (id === A ? 'M' : id === B ? 'I' : null),
    },
    ...over,
  };
}
const health = (peers) => ({ nodeId: hx(1), synaptomeSize: peers.length, peers });
const row = (d, n) => d.peers.find((p) => p.id === hx(n));

// 1 ---------------------------------------------------------------------
ok('1 idHex: a bigint becomes 66 hex', idHex(A) === hx(10) && idHex(0n) === '0'.repeat(66));
ok('1b idHex: a 66-hex string passes through', idHex(hx(10)) === hx(10));
ok('1c idHex: negative, oversized, number, object, upper-case, short are null',
  [-1n, 1n << 264n, 42, {}, null, undefined, hx(10).toUpperCase(), 'ab', hx(10) + '0'].every((v) => idHex(v) === null));

// 2 ---------------------------------------------------------------------
{ const d = buildPeerDump(health([hx(10), hx(11), hx(12), hx(13), hx(99), hx(20), hx(1)]), transport(), { relay: '0.198.0', kernel: '4.131.0', now: NOW });
  ok('2 header: own id, versions, complete, count 5, synaptome', d.nodeId === hx(1) && d.relay === '0.198.0' && d.kernel === '4.131.0' && d.complete === true && d.count === 5 && d.synaptome === 7, JSON.stringify({ ...d, peers: d.peers.length }));
  const a = row(d, 10), b = row(d, 11), c = row(d, 12), dd = row(d, 13), br = row(d, 99);
  ok('2a host/srflx channel: link webrtc, rtt 12.3, open, class M, age 5000', a && a.link === 'webrtc' && a.rttMs === 12.3 && a.state === 'open' && a.cls === 'M' && a.openMs === 5000, JSON.stringify(a));
  ok('2b one end relay: link turn, class I', b && b.link === 'turn' && b.rttMs === 80 && b.cls === 'I' && b.openMs === 100, JSON.stringify(b));
  ok('2c candidates not yet read: link mesh, rtt null, state carried, age null', c && c.link === 'mesh' && c.rttMs === null && c.state === 'datachannel-opening' && c.openMs === null && c.cls === null, JSON.stringify(c));
  ok('2d bound, no mesh channel, not the bridge: link null, rtt from the transport', dd && dd.link === null && dd.rttMs === 41.3 && dd.state === null, JSON.stringify(dd));
  ok('2e the bridge socket: link ws, bridge true', br && br.link === 'ws' && br.bridge === true && br.rttMs === 7, JSON.stringify(br));
  ok('2f only the bridge row carries the bridge flag once serialised', JSON.parse(JSON.stringify(d)).peers.filter((p) => 'bridge' in p).length === 1);
  // 3
  ok('3 unbound: a count of 1, the synaptome id with no channel', d.unbound === 1, JSON.stringify(d.unbound));
  ok('3a the unbound id itself is nowhere in the line; a bound id is (control)', !JSON.stringify(d).includes(hx(20)) && !renderCtx('peer-dump', d).includes(hx(20)) && renderCtx('peer-dump', d).includes(hx(10)));
  ok('3b a mesh row with no bound node (mZ) adds no peer', d.peers.length === 5);
}

// 4 ---------------------------------------------------------------------
{ const h = health([hx(10), hx(20), hx(1), hx(10)]);
  for (const [name, t] of [['no transport', null], ['undefined transport', undefined], ['a transport without boundPeers', {}],
    ['a throwing boundPeers', transport({ boundPeers: () => { throw new Error('boom'); } })],
    ['a boundPeers that returns a non-array', transport({ boundPeers: () => 'nope' })]]) {
    let d = null, threw = null; try { d = buildPeerDump(h, t, { now: NOW }); } catch (e) { threw = e; }
    ok(`4 ${name}: complete false, count NULL, no rows, unbound NULL (nothing to subtract from), synaptome size kept, no id printed`,
      !threw && d.complete === false && d.count === null && d.peers.length === 0 && d.unbound === null && d.synaptome === 4 && !JSON.stringify(d).includes(hx(10)) && !JSON.stringify(d).includes(hx(20)), String(threw || JSON.stringify(d)));
  }
  let d = null, threw = null; try { d = buildPeerDump(null, null); } catch (e) { threw = e; }
  ok('4b no health object either: every field null or empty, nothing thrown', !threw && d.nodeId === null && d.complete === false && d.count === null && d.synaptome === null && d.peers.length === 0 && d.unbound === null && d.relay === null && d.kernel === null, String(threw || JSON.stringify(d)));
  const e = buildPeerDump(health([]), transport({ boundPeers: () => [] }), { now: NOW });
  ok('4c control: a READ empty list is complete true, count 0, unbound 0', e.complete === true && e.count === 0 && e.unbound === 0);
  const np = buildPeerDump({ nodeId: hx(1), synaptomeSize: 3, peers: 'nope' }, transport(), { now: NOW });
  ok('4d a synaptome list that is not an array: unbound NULL, never 0; rows survive', np.unbound === null && np.count === 5);
}

// 5 ---------------------------------------------------------------------
{ const boom = () => { throw new Error('boom'); };
  const d1 = buildPeerDump(health([]), transport({ mesh: { getPeers: boom } }), { now: NOW });
  ok('5 throwing getPeers: 5 rows survive; mesh channels read as link mesh', d1.complete && d1.count === 5 && row(d1, 10).link === 'mesh' && row(d1, 10).rttMs === null && row(d1, 99).link === 'ws', JSON.stringify(d1.peers.map((p) => p.link)));
  const d2 = buildPeerDump(health([]), transport({ webrtc: { meshIdFor: boom, classOf: boom } }), { now: NOW });
  ok('5b throwing meshIdFor: 5 rows survive; the bridge is still ws, the rest null', d2.count === 5 && row(d2, 99).link === 'ws' && row(d2, 10).link === null, JSON.stringify(d2.peers.map((p) => p.link)));
  const d3 = buildPeerDump(health([]), transport({ webrtc: { meshIdFor: (id) => (id === A ? 'mA' : null), classOf: boom }, getLatency: boom }), { now: NOW });
  ok('5c throwing classOf and getLatency: class null, rtt null, rows survive', d3.count === 5 && row(d3, 10).cls === null && row(d3, 10).link === 'webrtc' && row(d3, 13).rttMs === null, JSON.stringify(d3.peers));
  const t4 = transport(); Object.defineProperty(t4, 'bridgeNodeIdBig', { get() { throw new Error('boom'); } });
  const d4 = buildPeerDump(health([]), t4, { now: NOW });
  ok('5d a throwing bridge id getter: the bridge row is link null, nothing thrown', d4.count === 5 && row(d4, 99).link === null && row(d4, 99).bridge === undefined);
}

// 6 ---------------------------------------------------------------------
{ const d = buildPeerDump(health([hx(1), 'junk', 42, null, hx(20), hx(20)]),
    transport({ boundPeers: () => [SELF, A, A, hx(10), 'junk', 42, null, undefined, -1n, 1n << 264n, {}, [], B] }), { now: NOW });
  ok('6 hostile bound list: self, duplicates (bigint and hex), junk and bad bigints dropped', d.count === 2 && d.peers.map((p) => p.id).join() === [hx(10), hx(11)].join(), JSON.stringify(d.peers.map((p) => p.id.slice(-4))));
  ok('6b hostile synaptome list: junk dropped, duplicates collapsed, self out: count 1', d.unbound === 1, JSON.stringify(d.unbound));
  ok('6c every id in the dump is 66 lower-case hex', [d.nodeId, ...d.peers.map((p) => p.id)].every((s) => /^[0-9a-f]{66}$/.test(s)));
  ok('6d the line holds exactly the ids it should: own id + one per bound row', (JSON.stringify(d).match(/[0-9a-f]{66}/g) || []).length === 1 + d.peers.length);
}

// 7 ---------------------------------------------------------------------
{ const rows = (r) => transport({ boundPeers: () => [A], mesh: { getPeers: () => [null, 7, { peerId: 9 }, { peerId: 'mA', ...r }] } });
  const cases = [
    ['object candidate types', { localCand: { type: 'relay' }, remoteCand: ['relay'] }],
    ['a 5000-character candidate type', { localCand: 'x'.repeat(5000), remoteCand: 'host' }],
    ['an empty candidate type', { localCand: '', remoteCand: 'host' }],
  ];
  for (const [name, r] of cases) {
    const p = buildPeerDump(health([]), rows(r), { now: NOW }).peers[0];
    ok(`7 ${name}: link mesh, never a guessed kind`, p && p.link === 'mesh', JSON.stringify(p));
  }
  for (const v of [NaN, -1, Infinity, '5', null, undefined, {}]) {
    const p = buildPeerDump(health([]), rows({ rttLast: v }), { now: NOW }).peers[0];
    ok(`7b rttLast ${String(v)}: null`, p.rttMs === null, JSON.stringify(p));
  }
  for (const v of [0, -5, NOW + 1, '1', null]) {
    const p = buildPeerDump(health([]), rows({ openedAt: v }), { now: NOW }).peers[0];
    ok(`7c openedAt ${String(v)}: age null`, p.openMs === null, JSON.stringify(p));
  }
  const p = buildPeerDump(health([]), rows({ state: 'y'.repeat(4000) }), { now: NOW }).peers[0];
  ok('7d an oversized state string: null', p.state === null);
  const m = buildPeerDump(health([]), transport(), { relay: 5, kernel: 'k'.repeat(99), now: 'soon' });
  ok('7e non-string and oversized versions: null; a non-numeric clock falls back', m.relay === null && m.kernel === null && m.count === 5);
}

// 8 ---------------------------------------------------------------------
{ const ids = Array.from({ length: 120 }, (_, i) => big(1000 + i));
  const t = {
    boundPeers: () => ids,
    getLatency: () => -1,
    bridgeNodeIdBig: null,
    mesh: { getPeers: () => ids.map((_, i) => ({ peerId: `mesh-${i}`, state: 'datachannel-opening', openedAt: NOW - 123456789, rttLast: 1234.5, localCand: 'srflx', remoteCand: 'prflx' })) },
    webrtc: { meshIdFor: (id) => `mesh-${ids.indexOf(id)}`, classOf: () => 'M' },
  };
  const d = buildPeerDump(health(Array.from({ length: 50 }, (_, i) => hx(5000 + i))), t, { relay: '0.198.0', kernel: '4.131.0', now: NOW });
  const line = renderCtx('peer-dump', d);
  const bytes = Buffer.byteLength(`[2026-11-10T00:00:00.000Z] peer-dump ${line}`);
  let back = null; try { back = JSON.parse(line); } catch { /* */ }
  ok('8 120 bound + 50 unbound renders whole: every row parses back, unbound is the number 50', back && back.peers.length === 120 && back.unbound === 50 && back.count === 120, `len ${line.length}`);
  ok('8a the rendered line holds 121 ids: own id + 120 bound, no unbound id', (line.match(/[0-9a-f]{66}/g) || []).length === 121 && !line.includes(hx(5000)));
  ok(`8b the line is under the journald limit of 49152 bytes (${bytes})`, bytes < 49152, String(bytes));
  const cut = renderCtx('peer-dump-like', d);
  let cutBack = null; try { cutBack = JSON.parse(cut); } catch { /* */ }
  ok('8c control: the same object under another event name is reduced', cut.length < line.length && !(cutBack && Array.isArray(cutBack.peers) && cutBack.peers.length === 120), `cut ${cut.length} vs ${line.length}`);
}

console.log(`\nfence_peer_dump: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
