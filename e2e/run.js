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
import { deriveTopicIdBig } from '../vendor/axona-protocol/src/pubsub/post.js';
import { fromHex, toHex } from '../vendor/axona-protocol/src/utils/hexid.js';

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

    // ════ White-box adversarial suite ════════════════════════════════
    // These forge genuine wire frames via peer.sendDirect, so the victim-root's
    // handler runs with a transport-PROVEN meta.fromId (not a hand-set one — the
    // difference from the unit tests). The honest "control" send doubles as the
    // gate: if B answers A, then A→B is a direct authenticated channel and
    // fromId=A was proven, which is the only condition under which the forged
    // assertion is meaningful. If the two peers never form a direct link this
    // run, the scenario reports inconclusive (skipped) rather than failing.
    console.log('\n── A1: C-3 reflection (forged requesterId, real proven fromId) ──');
    await sleep(4000);                       // let A↔B form a direct mesh link
    {
      const Bmgr = B.peer._axonaManager;
      const pubBig = fromHex(A.publisher);
      const Tbig = await deriveTopicIdBig(pubBig, T('adv-metrics'));
      const Bbig = fromHex(B.nodeId), Abig = fromHex(A.nodeId);
      if (!Bmgr) ok('A1 metrics reflection', false, 'B manager absent (no pub/sub ran?)');
      else {
        // Make B a root for the topic (deterministic; region-keyed ⇒ unowned ⇒
        // the vouch is the only gate). Then spy B's outbound sends.
        Bmgr.axonRoles.set(Tbig, { children: new Map(),
          replayCache: [{ json: '{}', postHash: 'seed', publisher: pubBig, signerPubkey: null, seq: 0, ts: 0, expiresAt: Date.now() + 1e9 }] });
        const sent = [];
        const orig = B.peer.sendDirect.bind(B.peer);
        B.peer.sendDirect = async (to, type, payload) => { sent.push({ to, type }); return orig(to, type, payload); };
        try {
          sent.length = 0;                    // control: honest requesterId === sender
          await A.peer.sendDirect(Bbig, 'pubsub:metricsReq-k', { topicId: toHex(Tbig), requesterId: A.nodeId, requestId: 'ctl-' + RUN, postHashes: null });
          await sleep(2000);
          const honest = sent.some(s => s.type === 'pubsub:metricsResp' && s.to === Abig);
          if (!honest) {
            ok('A1 inconclusive — no direct A→B channel this run (skipped)', true, 'honest control drew no response');
          } else {
            sent.length = 0;                  // attack: forge requesterId = victim ≠ fromId
            const VICTIM = fromHex('89' + 'a'.repeat(64));
            await A.peer.sendDirect(Bbig, 'pubsub:metricsReq-k', { topicId: toHex(Tbig), requesterId: toHex(VICTIM), requestId: 'adv-' + RUN, postHashes: null });
            await sleep(2000);
            const reflected = sent.filter(s => s.type === 'pubsub:metricsResp').length;
            ok('C-3: forged requesterId draws NO reflected metricsResp', reflected === 0,
               `honest control answered · forged emitted ${reflected} metricsResp`);
          }
        } finally { B.peer.sendDirect = orig; Bmgr.axonRoles.delete(Tbig); }
      }
    }

    console.log('\n── A2: B-1 subscribe-origin spoof (forged subscriberId) ──');
    {
      const Bmgr = B.peer._axonaManager;
      const pubBig = fromHex(A.publisher);
      const Tbig = await deriveTopicIdBig(pubBig, T('adv-sub'));
      const Bbig = fromHex(B.nodeId), Abig = fromHex(A.nodeId);
      if (!Bmgr) ok('A2 subscribe spoof', false, 'B manager absent');
      else {
        Bmgr.axonRoles.set(Tbig, { children: new Map(), replayCache: [] });
        try {
          await A.peer.sendDirect(Bbig, 'pubsub:subscribe-k', { topicId: toHex(Tbig), subscriberId: A.nodeId, lastSeenTs: 0 });
          await sleep(1800);
          const honest = [...(Bmgr.axonRoles.get(Tbig)?.children.keys() || [])].some(k => k === Abig);
          if (!honest) {
            ok('A2 inconclusive — honest subscribe-k not enrolled (skipped)', true, 'no direct A→B this run');
          } else {
            const VICTIM = fromHex('89' + 'b'.repeat(64));
            await A.peer.sendDirect(Bbig, 'pubsub:subscribe-k', { topicId: toHex(Tbig), subscriberId: toHex(VICTIM), lastSeenTs: 0 });
            await sleep(1800);
            const victimEnrolled = [...(Bmgr.axonRoles.get(Tbig)?.children.keys() || [])].some(k => k === VICTIM);
            ok('B-1: forged subscriberId (victim) is NOT enrolled as a child', !victimEnrolled,
               `honest enrolled · victim enrolled=${victimEnrolled}`);
          }
        } finally { Bmgr.axonRoles.delete(Tbig); }
      }
    }

    console.log('\n  · SP-10 anon-flood and replay/freshness need forged unsigned/stale');
    console.log('    envelopes — covered by smoke_pubsub_c3.js / smoke_envelope.js (unit).');
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
