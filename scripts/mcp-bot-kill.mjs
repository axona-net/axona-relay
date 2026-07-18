// scripts/mcp-bot-kill.mjs — retract a message from the owner-only "axona.bot"
// topic (owner-signed kill). Usage: node scripts/mcp-bot-kill.mjs <msgId>
import '../src/polyfill.js';
import { cleanupWebRTC } from '../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { createAuthorIdentity } from '../vendor/axona-protocol/src/identity/index.js';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';

const [msgId] = process.argv.slice(2);
if (!msgId) { console.error('usage: node scripts/mcp-bot-kill.mjs <msgId>'); process.exit(2); }

const STORE_PATH = process.env.MCP_AUTHOR_PATH || join(homedir(), '.axona', 'claude-mcp-identity.json');
const read = () => { try { return JSON.parse(readFileSync(STORE_PATH, 'utf8')); } catch { return {}; } };
const store = {
  get: (k) => read()[k] ?? null,
  set: (k, v) => { const o = read(); o[k] = v; mkdirSync(dirname(STORE_PATH), { recursive: true }); writeFileSync(STORE_PATH, JSON.stringify(o, null, 2)); },
};

const author = await createAuthorIdentity({ persistAs: 'claude', store });
const s = await connectPeer({ region: 'useast', author });
const descriptor = { region: s.regionName, name: 'axona.bot', owner: author.authorId, write: 'owner' };
try { await s.peer.pull(null, { topic: descriptor }); } catch { /* route warming */ }
await new Promise(r => setTimeout(r, 4000));
await s.peer.kill(descriptor, msgId, { signWith: author });
await new Promise(r => setTimeout(r, 3000));
console.log(JSON.stringify({ ok: true, killed: msgId }));
await s.close();
try { cleanupWebRTC(); } catch { /* */ }
process.exit(0);
