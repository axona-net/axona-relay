// scripts/mcp-post.mjs — one-shot chat-compatible publish signed by the MCP's
// durable author identity, WITHOUT the MCP server. Connects a throwaway peer
// (ephemeral nodeId — never reuses the running server's durable node identity,
// which would collide on the DHT), signs with the same persisted author key,
// and publishes the std/message shape with an in-payload §6.5 declaration so
// chat clients render it.
//
// Usage: node scripts/mcp-post.mjs "<topic>" "<message>" [handle] [region]
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const [topic, text, handle = 'Claude', region = 'useast'] = process.argv.slice(2);
if (!topic || !text) {
  console.error('usage: node scripts/mcp-post.mjs "<topic>" "<message>" [handle] [region]');
  process.exit(2);
}

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};

const author = await createAuthorIdentity({ persistAs: 'claude', store });   // the MCP's durable Author ID
const s = await connectPeer({ region, author });                             // ephemeral node identity
const descriptor = { region: s.regionName, name: topic };
// A freshly-connected peer's K-closest estimate is built from a barely-warmed
// table (readiness gate = synaptome ≥ 1); publishing immediately can distribute
// to the wrong cohort and strand the message. Warm the route with a lookup-read
// of the target topic first, then let the mesh settle before publishing.
try { await s.peer.pull(null, { topic: descriptor }); } catch { /* warming only */ }
await new Promise(r => setTimeout(r, 5000));
const body = { v: 1, text, handle, authorClass: 'agent' };
const msgId = await s.peer.pub(descriptor, body, { signWith: author });
await new Promise(r => setTimeout(r, 3000));   // let distribution finish before teardown
console.log(JSON.stringify({ ok: true, topic, region: s.regionName, msgId, signer: author.authorId }));
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
