// scripts/mcp-bot-post.mjs — post to the OWNER-ONLY "axona.bot" topic as
// Claude's durable MCP author, which is the topic's owner. Only this author's
// key can publish there (write:'owner' folds into the topic id and roots
// enforce it statelessly), so the channel is tamper-proof by construction.
//
// The signing key lives OUTSIDE every git repo at ~/.axona/claude-mcp-identity.json
// (override: MCP_AUTHOR_PATH). Never commit it.
//
// Usage:
//   node scripts/mcp-bot-post.mjs "<message>"
//   node scripts/mcp-bot-post.mjs "<message>" --advertise "<blurb>"
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { deriveTopicId } from '../vendor/axona-protocol/src/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const TOPIC_NAME = 'axona.bot';
const REGION = 'useast';

const args = process.argv.slice(2);
const adIdx = args.indexOf('--advertise');
const blurb = adIdx >= 0 ? args[adIdx + 1] : null;
const text = args.filter((a, i) => i !== adIdx && i !== adIdx + 1)[0];
if (!text) { console.error('usage: node scripts/mcp-bot-post.mjs "<message>" [--advertise "<blurb>"]'); process.exit(2); }

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};

const author = await createAuthorIdentity({ persistAs: 'claude', store });   // the owner key
const s = await connectPeer({ region: REGION, author });                     // ephemeral node
const descriptor = { region: s.regionName, name: TOPIC_NAME, owner: author.authorId, write: 'owner' };

// Warm the route toward the topic before publishing (fresh peers otherwise
// distribute into a bad cohort and strand the message — see task #352), then
// publish and CONFIRM by reading the message back; retry if it stranded.
try { await s.peer.pull(null, { topic: descriptor }); } catch { /* warming only */ }
await new Promise(r => setTimeout(r, 5000));

const body = { v: 1, text, handle: 'Claude', authorClass: 'agent' };
let msgId = null, confirmed = false;
for (let attempt = 1; attempt <= 3 && !confirmed; attempt++) {
  msgId = await s.peer.pub(descriptor, body, { signWith: author });
  await new Promise(r => setTimeout(r, 4000));
  try {
    const env = await s.peer.pull(null, { topic: descriptor });
    confirmed = !!env && (env.msgId === msgId || env?.message?.text === text);
  } catch { confirmed = false; }
  if (!confirmed) console.error(`attempt ${attempt}: publish not readable yet, ${attempt < 3 ? 'retrying' : 'giving up on confirm (msgId may still propagate)'}`);
}

let ad = null;
if (blurb) {
  const topicId = await deriveTopicId(descriptor);
  ad = {
    type: 'topic.ad',
    name: TOPIC_NAME,
    blurb,
    topicId,
    network: 'production',
    region: descriptor.region,
    mode: 'controlled',
    owner: author.authorId,
    write: 'owner',
    postedAt: Date.now()
  };
  await s.peer.pub({ region: s.regionName, name: 'advertised-topics' }, ad, { signWith: author });
}

console.log(JSON.stringify({ ok: true, topic: TOPIC_NAME, owner: author.authorId, write: 'owner', msgId, confirmed, advertised: !!ad }));
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
