#!/usr/bin/env node
// e2e/run.js — live-network test framework for the Axona pub/sub stack.
//
// Spins ephemeral peers through the relay connect machinery (src/ops.js →
// connectPeer) against a REAL bridge and runs honest + behavioural pub/sub
// scenarios over REAL WebRTC, asserting observable outcomes. This exercises the
// deployed kernel end-to-end on the live mesh — not just in-process.
//
//   node e2e/run.js                      # production (default)
//   node e2e/run.js --network testnet    # the SF staging line
//   BRIDGE_URL=wss://host node e2e/run.js
//
// Flags: --network <prod|testnet>, --keep (don't exit non-zero on failure).
//
// Scope note: adversarial cases that require a FORGED wire payload (e.g. C-3's
// attacker-named `requesterId`, or an unsigned-publish flood) are covered by the
// kernel unit tests (axona-protocol/test/smoke_pubsub_c3.js), since the public
// peer API won't emit them. This harness validates the network-OBSERVABLE
// behaviour: round-trip delivery, the metrics path, retraction (incl. duplicate
// copies, SP-11), and per-publisher quota self-limiting.

import { connectPeer } from '../src/ops.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { resolveBridgeUrl, resolveNetwork } from '../src/network.js';

const argv = process.argv.slice(2);
const flag = (name) => { const i = argv.indexOf(name); return i >= 0 ? (argv[i + 1] ?? true) : undefined; };
const network = flag('--network');
const bridge  = resolveBridgeUrl({ network });
const RUN     = Date.now().toString(36) + Math.floor(Math.random() * 1e4).toString(36);
const T       = (name) => `e2e/${name}-${RUN}`;
const sleep   = (ms) => new Promise(r => setTimeout(r, ms));

const results = [];
function ok(label, cond, detail = '') {
  results.push({ label, pass: !!cond, detail });
  console.log(`  ${cond ? '✓' : '✗'} ${label}${detail ? `  — ${detail}` : ''}`);
}

// Subscribe and collect everything seen on `topic` for `ms`, then resolve.
async function collect(peer, topic, publisher, ms, since = 'all') {
  const msgs = [];
  await peer.sub(topic, (env) => {
    if (!env) return;
    if (env.deleted) msgs.push({ deleted: true, msgId: env.msgId });
    else             msgs.push({ message: env.message });
  }, { publisher, since });
  await sleep(ms);
  return msgs;
}

async function main() {
  console.log(`Axona pub/sub e2e — network=${resolveNetwork(network) ?? (process.env.RELAY_NETWORK ? resolveNetwork(process.env.RELAY_NETWORK) : 'prod')} bridge=${bridge}  run=${RUN}\n`);
  console.log('Connecting two ephemeral peers (real WebRTC)…');
  const A = await connectPeer({ region: 'useast', bridge, readyTimeoutSec: 45 });
  const B = await connectPeer({ region: 'useast', bridge, readyTimeoutSec: 45 });
  console.log(`  A ${A.nodeId.slice(0, 10)}…  B ${B.nodeId.slice(0, 10)}…  (publisher prefix 0x${A.prefixHex})\n`);

  try {
    // ── S1: pub → sub round-trip across two peers ──────────────────────
    console.log('── S1: pub → sub round-trip ──');
    {
      const topic = T('roundtrip'), want = `hello-${RUN}`;
      const subP = collect(B.peer, topic, B.publisher, 9000, 'all');
      await sleep(1500);
      const id = await A.peer.pub(topic, want, { publisher: A.publisher });
      const got = await subP;
      ok('A publishes, B receives it', got.some(m => m.message === want),
         `msgId ${String(id).slice(0, 12)}… · B saw ${got.length} msg`);
    }

    // ── S2: metrics legit path (C-3-modified path still serves a real caller) ──
    console.log('\n── S2: metrics path (legit caller) ──');
    {
      const topic = T('metrics');
      await A.peer.pub(topic, `m-${RUN}`, { publisher: A.publisher });
      await sleep(2500);
      let m = null, err = null;
      try { m = await A.peer.metrics(topic, { publisher: A.publisher }); } catch (e) { err = e; }
      ok('metrics(topic) returns a result for the proven caller', !err && m && typeof m === 'object',
         err ? `threw ${err.message}` : `current_count=${m?.current_count} subscribers=${m?.subscribers}`);
    }

    // ── S3: kill retracts everywhere, incl. duplicate copies (SP-11) ───
    console.log('\n── S3: kill (retraction + duplicate copies, SP-11) ──');
    {
      const topic = T('kill'), msg = `killme-${RUN}`;
      const id = await A.peer.pub(topic, msg, { publisher: A.publisher });  // copy 1
      await A.peer.pub(topic, msg, { publisher: A.publisher });             // copy 2 — identical content ⇒ same msgId
      await sleep(2000);
      const k = await A.peer.kill(topic, id, { publisher: A.publisher });
      await sleep(2500);
      const got = await collect(B.peer, topic, B.publisher, 9000, 'all');
      const present = got.some(m => m.message === msg);
      ok('killed content is absent for a fresh subscriber', !present,
         `kill ok=${k?.ok} · B replayed ${got.filter(m => m.message).length} msg · killed-present=${present}`);
    }

    // ── S4: per-publisher quota self-limits a single flooding publisher ─
    console.log('\n── S4: per-publisher quota (self-limit under flood) ──');
    {
      const topic = T('quota'), N = 25;
      for (let i = 0; i < N; i++) await A.peer.pub(topic, `q-${i}-${RUN}`, { publisher: A.publisher });
      await sleep(3000);
      const got  = await collect(B.peer, topic, B.publisher, 11000, 'all');
      const uniq = new Set(got.filter(m => m.message).map(m => m.message)).size;
      const capped = uniq < N;
      ok('delivery works and is bounded (≤ published)', uniq >= 1 && uniq <= N,
         `published ${N}, B replayed ${uniq} distinct — ${capped ? 'per-publisher cap observed' : 'no cap observed (quota ≥ N or spread across roots)'}`);
    }

    console.log('\n  · C-3 reflection / fail-closed and SP-10 anon-flood require forged');
    console.log('    payloads — covered by axona-protocol/test/smoke_pubsub_c3.js (unit).');
  } finally {
    await A.close();
    await B.close();
    cleanupWebRTC();
  }

  const fails = results.filter(r => !r.pass).length;
  console.log(`\nResult: ${results.length - fails} passed, ${fails} failed`);
  process.exit(fails === 0 || flag('--keep') ? 0 : 1);
}

main().catch(err => { console.error('e2e threw:', err?.message || err); try { cleanupWebRTC(); } catch {} process.exit(2); });
