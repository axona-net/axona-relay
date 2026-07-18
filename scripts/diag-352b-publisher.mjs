// diag-352b-publisher.mjs — instrumented replica of mcp-bot-post.mjs: fresh
// ephemeral node, route-warm pull, settle, publish a DIAGNOSTIC message to the
// owner-only axona.bot topic, then confirm-read with varying pull timeouts.
// Logs the root hint / lookup terminus at every stage.
//
// Usage: node scripts/diag-352b-publisher.mjs "<text>"
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { instrument, setLabel, log, snapshot, lookupTerminus, topicBig, DESCRIPTOR, managerOf } from './diag-352b-common.mjs';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const text = process.argv[2] || `diag-352b probe ${new Date().toISOString()}`;

setLabel('P');
await topicBig();
instrument();

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};
const author = await createAuthorIdentity({ persistAs: 'claude', store });
if (author.authorId !== DESCRIPTOR.owner) { log('fatal', { error: 'author mismatch', got: author.authorId }); process.exit(1); }

const s = await connectPeer({ region: 'useast', author });
log('connected', { nodeId: s.nodeId.slice(0, 12), fullNodeId: s.nodeId });
managerOf(s.peer);
log('snap-preWarm', snapshot(s.peer));

// Route-warm exactly like the real post script (pull latest, default 1s timeout).
let warmPull = null;
try { warmPull = await s.peer.pull(null, { topic: DESCRIPTOR }); } catch (e) { log('warmPullErr', { e: String(e?.message || e) }); }
log('warm-pull', { found: !!warmPull, msgId: warmPull?.msgId?.slice?.(0, 12) ?? null });
log('snap-postWarmPull', snapshot(s.peer));
await new Promise(r => setTimeout(r, 5000));
log('snap-postSettle', snapshot(s.peer));
log('lookup-prePub', await lookupTerminus(s.peer));
log('snap-prePub', snapshot(s.peer));

const body = { v: 1, text, handle: 'axona.bot', authorClass: 'agent' };
const msgId = await s.peer.pub(DESCRIPTOR, body, { signWith: author });
log('published', { msgId: msgId?.slice(0, 12), fullMsgId: msgId });
log('snap-postPub', snapshot(s.peer));

// Watch the pending-publish confirm state + attempt confirm pulls.
const am = managerOf(s.peer);
for (let i = 1; i <= 6; i++) {
  await new Promise(r => setTimeout(r, 4000));
  const pending = am._pendingPub ? [...am._pendingPub.keys()].map(k => k.slice(0, 12)) : [];
  log('pending-pub', { attempt: i, pending });
  const timeoutMs = i <= 3 ? 1000 : 10_000;      // first like prod script, then generous
  let env = null;
  try { env = await s.peer.pull(null, { topic: DESCRIPTOR, timeoutMs }); } catch (e) { log('pullErr', { e: String(e?.message || e) }); }
  log('confirm-pull', { attempt: i, timeoutMs, found: !!env, msgId: env?.msgId?.slice?.(0, 12) ?? null, matches: env?.msgId === msgId });
  log('snap', snapshot(s.peer));
  if (env?.msgId === msgId && i >= 4) break;
}
log('lookup-final', await lookupTerminus(s.peer));
log('done', {});
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
