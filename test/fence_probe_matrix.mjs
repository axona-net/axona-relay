#!/usr/bin/env node
// fence_probe_matrix — the relay-side probe facility (src/probe.js, 0.150.0).
//
//   A. classifyRouted: consumed AT the target → ok; consumed elsewhere →
//      misrouted; terminal / exhausted / nothing → their names.
//   B. runProbeMatrix against a fake peer+transport: one routed row per
//      sample per target, a direct row only where a channel exists, the
//      node's own id skipped, errors recorded not thrown, summary counts.
//   C. the file-drop worker: a request file produces <run>.<self12>.jsonl +
//      .done; a second poll runs nothing; a .done left by an earlier process
//      is honoured; a malformed request is ignored and never retried.
// Mutants: classifyRouted returning ok for any consumed → A2/B4 fail;
// the worker's existsSync(done) check removed → C3 fails.
import { mkdtempSync, writeFileSync, readFileSync, existsSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { classifyRouted, runProbeMatrix, parseRequest, startProbeWorker } from '../src/probe.js';

let passed = 0, failed = 0;
const check = (label, ok, extra = '') => { console.log(`  ${ok ? '✓' : '✗'} ${label}${ok ? '' : ' ' + extra}`); ok ? passed++ : failed++; };
const H = (n) => n.toString(16).padStart(66, '0');
const SELF = BigInt('0x' + H(0x89aa));
const T_OK = H(0x8901), T_TERM = H(0x8902), T_EXH = H(0x8903), T_MIS = H(0x8904), T_ERR = H(0x8905);

function fakePeer() {
  return {
    _node: { synaptome: new Map([[1n, { peerId: BigInt('0x' + T_OK) }]]) },
    async routeMessage(target, type, payload) {
      const h = target.toString(16).padStart(66, '0');
      if (type !== '__tunneled_direct__' || payload?.innerType !== 'local_probe' || payload?.targetId !== h) throw new Error('wrong envelope');
      if (h === T_OK)   return { consumed: true,  atNode: target, hops: 2 };
      if (h === T_TERM) return { consumed: false, atNode: 7n, hops: 2, terminal: true };
      if (h === T_EXH)  return { consumed: false, atNode: SELF, hops: 0, exhausted: true };
      if (h === T_MIS)  return { consumed: true,  atNode: 9n, hops: 3 };
      throw new Error('route failed: channel closed');
    },
  };
}
function fakeTransport() {
  const sent = [];
  return { sent, isConnected: (id) => id === BigInt('0x' + T_OK), async send(id, type) { sent.push([id, type]); return []; } };
}

(async () => {
  console.log('fence_probe_matrix: the relay-side probe facility');

  // A ─ classification
  check('A1 consumed at the target → ok with hops', classifyRouted({ consumed: true, atNode: BigInt('0x' + T_OK), hops: 2 }, T_OK).outcome === 'ok');
  check('A2 consumed elsewhere → misrouted', classifyRouted({ consumed: true, atNode: 9n, hops: 3 }, T_OK).outcome === 'misrouted');
  check('A3 terminal / exhausted / empty → named', classifyRouted({ terminal: true }, T_OK).outcome === 'terminal' && classifyRouted({ exhausted: true }, T_OK).outcome === 'exhausted' && classifyRouted(null, T_OK).outcome === 'unconsumed');

  // B ─ the matrix row
  {
    const peer = fakePeer(), transport = fakeTransport(), rows = [];
    const targets = [{ id: T_OK, label: 'ok' }, { id: T_TERM, label: 'term' }, { id: T_EXH, label: 'exh' }, { id: T_MIS, label: 'mis' }, { id: T_ERR, label: 'err' }, { id: H(0x89aa), label: 'self' }, { id: 'nothex', label: 'bad' }];
    const s = await runProbeMatrix({ peer, transport, selfId: SELF, targets, samples: 2, gapMs: 0, from: 'v', emit: (r) => rows.push(r) });
    const routed = rows.filter((r) => r.mode === 'routed');
    check('B1 one routed row per sample per valid target (self and malformed skipped)', routed.length === 10 && !rows.some((r) => r.to === 'self' || r.to === 'bad'), String(routed.length));
    check('B2 direct rows only where a channel exists', rows.filter((r) => r.mode === 'direct').length === 2 && rows.filter((r) => r.mode === 'direct').every((r) => r.to === 'ok' && r.outcome === 'ok'));
    check('B3 outcomes by target', ['ok', 'terminal', 'exhausted', 'misrouted', 'error'].every((o, i) => routed.filter((r) => r.to === ['ok', 'term', 'exh', 'mis', 'err'][i]).every((r) => r.outcome === o)), JSON.stringify(routed.map((r) => [r.to, r.outcome])));
    check('B4 a misrouted consume is not counted ok', s.routedOk === 2 && s.routedFail === 8, JSON.stringify(s));
    check('B5 the error row carries the message and hops stay null', routed.find((r) => r.to === 'err').err.includes('channel closed') && routed.find((r) => r.to === 'err').hops === null);
    check('B6 the ok row carries hops and the target as atNode', routed.find((r) => r.to === 'ok').hops === 2 && routed.find((r) => r.to === 'ok').atNode === T_OK.slice(0, 12));
    check('B7 summary direct counts', s.directOk === 2 && s.directFail === 0 && s.targets === 5);
  }

  // C ─ the file-drop worker
  {
    const dir = mkdtempSync(join(tmpdir(), 'probe-'));
    const peer = fakePeer(), transport = fakeTransport(), logs = [];
    const w = startProbeWorker({ peer, transport, selfId: SELF, dir, pollMs: 3600000, log: (l, e, c) => logs.push([l, e, c]), label: 'relay-x' });
    writeFileSync(join(dir, 'run1.json'), JSON.stringify({ targets: [{ id: T_OK, label: 'ok' }, { id: T_TERM, label: 'term' }], samples: 1, gapMs: 0 }));
    writeFileSync(join(dir, 'junk.json'), '{"not": "a request"}');
    await w.poll();
    const self12 = SELF.toString(16).padStart(66, '0').slice(0, 12);
    const out = join(dir, `run1.${self12}.jsonl`), done = join(dir, `run1.${self12}.done`);
    check('C1 a request produces <run>.<self12>.jsonl and .done', existsSync(out) && existsSync(done));
    const rows = readFileSync(out, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
    check('C1b the file holds run, pair rows (from = label) and done', rows[0].kind === 'run' && rows.filter((r) => r.kind === 'pair').every((r) => r.from === 'relay-x') && rows.at(-1).kind === 'done');
    await w.poll();   // junk is next in sort order? 'junk' < 'run1' — poll handles one per call; run both
    await w.poll();
    check('C2 the malformed request is ignored (no output) and logged once', !readdirSync(dir).some((n) => n.startsWith('junk.') && n !== 'junk.json') && logs.filter((x) => x[1] === 'probe-request-ignored').length === 1);
    const before = readFileSync(out, 'utf8');
    await w.poll();
    check('C2b a second poll runs nothing again', readFileSync(out, 'utf8') === before && logs.filter((x) => x[1] === 'probe-run-start').length === 1);
    // a fresh worker (restart) honours a .done left behind
    writeFileSync(join(dir, 'run2.json'), JSON.stringify({ targets: [{ id: T_OK }], samples: 1, gapMs: 0 }));
    writeFileSync(join(dir, `run2.${self12}.done`), '{"run":"run2"}\n');
    const w2 = startProbeWorker({ peer, transport, selfId: SELF, dir, pollMs: 3600000, log: (l, e, c) => logs.push([l, e, c]) });
    const run1Before = readFileSync(out, 'utf8');
    await w2.poll(); await w2.poll(); await w2.poll();   // enough polls to walk past junk, run1 and run2
    check('C3 .done files from an earlier process are honoured: run1 not re-run, run2 not run', readFileSync(out, 'utf8') === run1Before && !existsSync(join(dir, `run2.${self12}.jsonl`)) && logs.filter((x) => x[1] === 'probe-run-start').length === 1);
    check('C4 parseRequest defaults and bounds', parseRequest('{"targets":["' + T_OK + '"]}').samples === 3 && parseRequest('{"targets":[],"samples":-1}').samples === 3 && parseRequest('nope') === null);
    w.stop(); w2.stop();
  }

  console.log(`\nfence_probe_matrix: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
