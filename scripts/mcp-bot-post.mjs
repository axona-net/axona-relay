// scripts/mcp-bot-post.mjs — post to the OWNER-ONLY "axona.bot" topic as
// Claude's durable MCP author, which is the topic's owner. Only this author's
// key can publish there (write:'owner' folds into the topic id and roots
// enforce it statelessly), so the channel is tamper-proof by construction.
//
// The signing key lives OUTSIDE every git repo at ~/.axona/claude-mcp-identity.json
// (override: MCP_AUTHOR_PATH). Never commit it.
//
// Durability (task #353): an ephemeral publisher that exits right after pub()
// can strand its message — the root claim dies with the process before the
// standing root ingests a replicate (prod 4.27.1 has no incumbent-side
// reconciliation). So this script HOLDS the publisher session alive until an
// INDEPENDENT fresh probe session — seeing exactly what a real subscriber
// sees — confirms the message, or 150s elapse.
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
const CONFIRM_MS = 150_000;
const PROBE_EVERY_MS = 10_000;

const args = process.argv.slice(2);
const adIdx = args.indexOf('--advertise');
const blurb = adIdx >= 0 ? args[adIdx + 1] : null;
const positional = args.filter((a, i) => adIdx < 0 || (i !== adIdx && i !== adIdx + 1));
// Guard against the mcp-post.mjs calling convention ("<topic> <message>"):
// the topic here is hardcoded, so a leading "axona.bot" arg is a mistake.
if (positional[0] === TOPIC_NAME && positional.length > 1) positional.shift();
if (positional.length > 1) {
  console.error(`unexpected extra arguments — this script takes ONE message (topic is hardcoded); did you mean scripts/mcp-post.mjs "<topic>" "<message>"?`);
  process.exit(2);
}
const text = positional[0];
if (!text) { console.error('usage: node scripts/mcp-bot-post.mjs "<message>" [--advertise "<blurb>"]'); process.exit(2); }

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};

const author = await createAuthorIdentity({ persistAs: 'claude', store });   // the owner key
const s = await connectPeer({ region: REGION, author });                     // publisher (held alive until confirm)
const descriptor = { region: s.regionName, name: TOPIC_NAME, owner: author.authorId, write: 'owner' };

// Warm the route toward the topic before publishing (fresh peers otherwise
// distribute into a bad cohort and strand the message — see task #352).
try { await s.peer.pull(null, { topic: descriptor }); } catch { /* warming only */ }
await new Promise(r => setTimeout(r, 5000));

const body = { v: 1, text, handle: 'axona.bot', authorClass: 'agent' };
const msgId = await s.peer.pub(descriptor, body, { signWith: author });
console.error(`published ${msgId.slice(0, 12)}… — holding publisher alive until an independent probe confirms`);

// Independent probe session: subscribes since:'all' like a real client.
const probe = await connectPeer({ region: REGION });
let confirmed = false;
await probe.peer.sub(descriptor, (env) => {
  if (env?.msgId === msgId || env?.message?.text === text) confirmed = true;
}, { since: 'all' });

const deadline = Date.now() + CONFIRM_MS;
while (Date.now() < deadline && !confirmed) {
  await new Promise(r => setTimeout(r, PROBE_EVERY_MS));
  if (!confirmed) {
    try {
      const env = await probe.peer.pull(null, { topic: descriptor });
      if (env?.msgId === msgId || env?.message?.text === text) confirmed = true;
    } catch { /* keep waiting */ }
  }
  console.error(`  confirm: ${confirmed} (${Math.round((deadline - Date.now()) / 1000)}s left)`);
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
await probe.close();
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(confirmed ? 0 : 1);
