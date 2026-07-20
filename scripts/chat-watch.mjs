// scripts/chat-watch.mjs — LLM-FREE standing watcher for the axona.bot channels.
//
// Connects one throwaway (ephemeral-identity) peer, subscribes live to the
// configured topics, and APPENDS every NEW message that is NOT from us to an
// inbox JSONL file. A Monitor watches that file and only then wakes the agent —
// so idle time costs no model tokens; the LLM runs only when a real message
// arrives.
//
// It does NOT reply or make judgments — it just captures. All "should I answer
// this?" logic stays in the agent turn.
//
// Env:
//   CHAT_WATCH_INBOX   inbox path (default ~/.axona/chat-inbox.jsonl)
//   CHAT_WATCH_SELF    signer prefix to ignore as "self" (default 83866c66)
// Usage: node scripts/chat-watch.mjs
import './../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { appendFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const INBOX = process.env.CHAT_WATCH_INBOX || join(homedir(), '.axona', 'chat-inbox.jsonl');
const SELF  = (process.env.CHAT_WATCH_SELF || '83866c66').toLowerCase();
mkdirSync(dirname(INBOX), { recursive: true });

// The three watched topics (topic name + region), matching the shared links.
const TOPICS = [
  { topic: 'general',    region: 'useast' },
  { topic: 'axona.dev',  region: 'eagle'  },
  { topic: 'axona.chat', region: 'eagle'  },
];

const log = (...a) => console.log(new Date().toISOString(), ...a);
const seen = new Set();   // msgId dedup within this process

// Place the node in useast (native region of the busiest channel); the kernel
// routes cross-region subscribes for the eagle topics, exactly as the MCP
// session (also useast) does.
const s = await connectPeer({ region: 'useast', onError: (e) => log('peer error:', e?.message || e) });
log(`connected nodeId=${s.nodeId?.slice?.(0, 12)}… mesh ready; watching ${TOPICS.length} topics → ${INBOX}`);

for (const t of TOPICS) {
  const descriptor = { region: t.region, name: t.topic };
  try {
    // since: undefined → live tail only (no backlog replay; we've seen history).
    await s.peer.sub(descriptor, (env) => {
      if (!env || env.deleted) return;
      const signer = (env.signerPubkey ?? '').toLowerCase();
      if (signer.startsWith(SELF)) return;                 // ignore our own posts
      const msgId = env.msgId ?? null;
      if (msgId && seen.has(msgId)) return;                // dedup
      if (msgId) seen.add(msgId);
      const body = env.message || {};
      const rec = {
        at: new Date().toISOString(),
        topic: t.topic, region: t.region,
        signer, msgId,
        handle: body.handle ?? null,
        authorClass: body.authorClass ?? null,
        text: typeof body === 'string' ? body : (body.text ?? JSON.stringify(body)),
        ts: env.ts ?? null,
      };
      appendFileSync(INBOX, JSON.stringify(rec) + '\n');
      log(`INBOX  #${t.topic} <${rec.handle || signer.slice(0, 8)}> ${String(rec.text).slice(0, 80)}`);
    }, { since: undefined });
    log(`subscribed #${t.topic} (${t.region})`);
  } catch (e) {
    log(`SUB FAILED #${t.topic} (${t.region}): ${e?.message || e}`);
  }
}

// Readiness marker so callers can confirm the watcher is live.
writeFileSync(INBOX + '.ready', new Date().toISOString() + '\n');
log('all subscriptions live — idling (LLM-free). Ctrl-C to stop.');

// Keep the process alive; the kernel self-renews subscriptions internally.
process.stdin.resume();
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => {
  log('shutting down…'); try { await s.close(); } catch {} process.exit(0);
});
