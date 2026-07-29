// scripts/mcp-post.mjs — one-shot chat-compatible publish signed by the MCP's
// durable author identity, WITHOUT the MCP server. Connects a throwaway peer
// (ephemeral nodeId — never reuses the running server's durable node identity,
// which would collide on the DHT), signs with the same persisted author key,
// and publishes the std/message shape with an in-payload §6.5 declaration so
// chat clients render it.
//
// Usage: node scripts/mcp-post.mjs "<topic>" "<message>" [handle] [region]
//                                  [--owner=self|<authorId>] [--write=owner|open]
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { deriveTopicId } from '../vendor/axona-protocol/src/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

// ─────────────────────────────────────────────────────────────────────────
// OWNED CHANNELS — the bug this table exists to prevent (2026-07-28).
//
// owner and write policy FOLD INTO the topic id, so one name is two
// addresses:
//     name only                        -> 89ba78c0…   (open, anyone writes)
//     name + owner + write:'owner'     -> 89f7f877…   (the bot's channel)
//
// This script had no owner/write arguments, so it could only ever address
// the bare one. Every axona.bot post for weeks published successfully to a
// topic nobody reads, reported ok, and looked from the outside like a bot
// that had stopped posting. Nothing was broken; both sides addressed a flat
// namespace correctly and meant different things by the same word.
//
// So the mapping is a visible table, not a default buried in a call site,
// and the resolved topic id is printed on every post — the address you send
// to is the address you can see.
// ─────────────────────────────────────────────────────────────────────────
const OWNED_CHANNELS = { 'axona.bot': { owner: 'self', write: 'owner' } };

const argv  = process.argv.slice(2);
const flags = Object.fromEntries(argv.filter(a => a.startsWith('--'))
  .map(a => { const [k, ...v] = a.slice(2).split('='); return [k, v.join('=') || true]; }));
const [topic, text, handle = 'axona.bot', region = 'eagle'] = argv.filter(a => !a.startsWith('--'));
if (!topic || !text) {
  console.error('usage: node scripts/mcp-post.mjs "<topic>" "<message>" [handle] [region] [--owner=self] [--write=owner]');
  process.exit(2);
}

// Explicit flags beat the table; the table beats bare. An explicit
// --write=open is how you deliberately address the open topic of a name
// that has an owned channel.
const policy = {
  owner: flags.owner ?? OWNED_CHANNELS[topic]?.owner,
  write: flags.write ?? OWNED_CHANNELS[topic]?.write,
};

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};

const author = await createAuthorIdentity({ persistAs: 'claude', store });   // the MCP's durable Author ID
const s = await connectPeer({ region, author });                             // ephemeral node identity

// "self" is resolvable only once the author exists, which is why the table
// stores the word rather than an id.
const descriptor = { region: s.regionName, name: topic };
if (policy.write) descriptor.write = policy.write;
if (policy.owner) descriptor.owner = policy.owner === 'self' ? author.authorId : policy.owner;

// Print the address BEFORE publishing. A post to the wrong topic is not a
// failure anyone can see downstream — it succeeds, and is simply absent.
const topicId = await deriveTopicId(descriptor);
console.error(`[mcp-post] ${topic} -> ${topicId}` +
  (descriptor.write ? `  (write:${descriptor.write}, owner:${String(descriptor.owner).slice(0, 8)}…)` : '  (open)'));
// A freshly-connected peer's K-closest estimate is built from a barely-warmed
// table (readiness gate = synaptome ≥ 1); publishing immediately can distribute
// to the wrong cohort and strand the message. Warm the route with a lookup-read
// of the target topic first, then let the mesh settle before publishing.
try { await s.peer.pull(null, { topic: descriptor }); } catch { /* warming only */ }
await new Promise(r => setTimeout(r, 5000));
const body = { v: 1, text, handle, authorClass: 'agent' };
const msgId = await s.peer.pub(descriptor, body, { signWith: author });

// HOLD-UNTIL-CONFIRMED (the mcp-bot-post v0.59.0 pattern, 2026-07-21). A
// die-fast publisher's sole-copy publish can die with its own departure (the
// prod 4.29.0 leave-order bug destroyed two of axona.bot's posts this way —
// including, delightfully, the post announcing that very bug). Publish, then
// hold the publisher alive until an INDEPENDENT fresh probe session — seeing
// exactly what a real subscriber sees — replays the message; republish
// (idempotent msgId) every 45s while unconfirmed; give up at 150s with exit 1
// so callers know to retry.
// TWO independent confirmations, because they fail differently and knowing
// WHICH one answered is the diagnosis (#393).
//
//   replay — a fresh subscriber asks for history (since:'all')
//   pull   — a fresh reader asks for the latest value
//
// On 2026-07-28 an owned-topic post reached a STANDING subscriber (David saw
// it live in axona.chat) while this probe's replay never fired. A standing
// subscriber gets the live push; a fresh one needs replay. That points at
// replay-to-a-new-subscriber on owned topics, not at delivery — so the probe
// now records which path answered instead of collapsing both into a boolean
// and losing the distinction.
const probe = await connectPeer({ region: s.regionName });
let via = null;
await probe.peer.sub(descriptor, (env) => {
  if (!via && (env?.msgId === msgId || env?.message?.text === text)) via = 'replay';
}, { since: 'all' });

const seen = (r) => r && (r.msgId === msgId || r.text === text || r.message?.text === text);
const deadline = Date.now() + 150_000;
let lastPub = Date.now(), lastPull = 0;
while (Date.now() < deadline && !via) {
  await new Promise(r => setTimeout(r, 1000));
  if (!via && Date.now() - lastPull >= 5000) {
    lastPull = Date.now();
    try { if (seen(await probe.peer.pull(null, { topic: descriptor }))) via = 'pull'; }
    catch { /* pull is one of two paths; its failure is not the answer */ }
  }
  if (!via && Date.now() - lastPub >= 45_000) {
    try { await s.peer.pub(descriptor, body, { signWith: author }); lastPub = Date.now(); } catch { /* retry next round */ }
  }
}
const confirmed = via !== null;
console.log(JSON.stringify({
  ok: confirmed, topic, topicId, region: s.regionName, msgId,
  signer: author.authorId, confirmed, via,
  ...(descriptor.write ? { write: descriptor.write, owner: descriptor.owner } : {}),
}));
try { await probe.close(); } catch { /* */ }
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(confirmed ? 0 : 1);
