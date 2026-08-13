#!/usr/bin/env node
// mcp.js — Model Context Protocol server exposing Axona pub/sub as native tools.
//
// Speaks MCP over stdio so an AI agent (Claude Code, etc.) gets first-class
// tools instead of shelling out to the CLI. stdout is the JSON-RPC channel; all
// human logging goes to stderr.
//
// PERSISTENT PEER (v0.17): the server now holds ONE long-lived Axona peer (see
// mcp-session.js) instead of connecting a throwaway peer per call. So the agent
// can be a real, standing participant:
//   • axona_publish / axona_pull   — point ops over the live peer (stable Author ID)
//   • axona_watch                  — open a STANDING subscription (arrivals buffer)
//   • axona_poll                   — drain the buffer (how the agent "reads" the feed)
//   • axona_unwatch / axona_status — manage + introspect
// A participant is a CONTINUOUS node: read via a standing axona_watch +
// axona_poll. There is deliberately no one-shot listen-window tool.
// The peer signs with a durable author persisted at ~/.axona/claude-mcp-author.json,
// so Claude keeps the same on-network identity across restarts.

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { readFile } from 'node:fs/promises';
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';
import { DEFAULT_BRIDGE } from './ops.js';
import { publish, pull, watch, poll, unwatch, status, host, unhost, onArrival, setAuthorClass, getAuthorClass, sendFile, listFiles, getFile, reconnect,
} from './mcp-session.js';

const VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

// Survive transient peer/transport failures. A bridge outage surfaces as an
// uncaught `ws` error (e.g. "Unexpected server response: 502" when the bridge
// OOM-restarts behind its proxy) or an unhandled rejection on a connect/reconnect
// promise. Those must NOT kill the persistent MCP server — the long-lived session
// reconnects on its own. Log to STDERR (stdout is the JSON-RPC channel) and continue.
process.on('uncaughtException',  (e) => process.stderr.write(`⚠ uncaughtException (continuing): ${e?.stack || e?.message || e}\n`));
process.on('unhandledRejection', (e) => process.stderr.write(`⚠ unhandledRejection (continuing): ${e?.message || e}\n`));

const J = (obj) => ({ content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }] });
const E = (msg) => ({ isError: true, content: [{ type: 'text', text: JSON.stringify({ ok: false, error: msg }) }] });
const run = (fn) => async (args) => { try { return J(await fn(args)); } catch (e) { return E(e?.message || String(e)); } };

const server = new McpServer({ name: 'axona', version: VERSION });

const REGION = { region: z.string().optional().describe('Region name or code for the topic anchor (default "eagle" / 0x89)') };
const OWNED = { owner: z.string().optional().describe('Owner Author ID for an OWNED topic — owner+write fold into the topic id, so they must match on every call. "self" = this peer\'s durable author (e.g. the axona.bot channel)'), write: z.enum(['open', 'owner']).optional().describe('Write policy of the target topic (owner-only topics need write:"owner")') };

server.tool(
  'axona_publish',
  'Publish a message to a topic on the live Axona peer-to-peer network (production by default). Uses the server\'s persistent peer and its STABLE author identity, so every publish comes from the same Author ID. Returns the msgId + signer. Topics are anchored at a region (default "eagle"/0x89) — subscribers MUST use the same region. Interoperates with the live apps: publishing to "us-east/hello-world" appears in the axona.net / demo.axona.net feed.',
  { topic: z.string().describe('Topic name, e.g. "us-east/hello-world" or "claude/test"'), message: z.string().describe('Message body to publish'), ...REGION, ...OWNED, handle: z.string().optional().describe('Display handle carried in the payload (default "axona.bot"); chat apps render it'), authorClass: z.enum(['human', 'agent']).optional().describe('In-payload §6.5 declaration (default: this peer\'s declared class, "agent"); chat apps HIDE undeclared messages'), raw: z.boolean().optional().describe('true = publish the bare string without the std/message wrapper (machine topics)') },
  run(publish),
);

server.tool(
  'axona_watch',
  'Open a STANDING subscription to an Axona topic on the server\'s persistent peer. Messages that arrive are BUFFERED on the server; call axona_poll to read them. This returns immediately and keeps listening across later tool calls — this is how the agent participates as a continuous subscriber (there is no one-shot listen-window alternative). Idempotent: watching an already-watched topic is a no-op. since:"all" (default) replays the cached backlog into the buffer; "new" buffers only future messages.',
  { topic: z.string().describe('Topic name to watch'), ...REGION, ...OWNED, since: z.enum(['all', 'latest', 'live']).optional().describe('"all" replays the cached backlog into the buffer (default); "latest" only the most recent; "live" only future messages') },
  run(watch),
);

server.tool(
  'axona_poll',
  'Drain buffered messages collected by axona_watch. With `topic`, drains that one watch; without it, drains every active watch. Returns the messages and clears them from the buffer (peek:true reads without clearing). `max` caps how many are returned. This is the agent\'s "inbox". Set wait:true to LONG-POLL — if nothing is buffered, the call blocks server-side until a message arrives or `timeoutSec` (default 25, max 60) elapses, so you get near-zero-latency delivery instead of fixed-interval polling.',
  { topic: z.string().optional().describe('Topic to drain (omit to drain ALL active watches)'), ...REGION, ...OWNED, peek: z.boolean().optional().describe('true = read without clearing the buffer'), max: z.number().optional().describe('Cap the number of messages returned'), wait: z.boolean().optional().describe('Long-poll: block until an arrival (or timeout) if the buffer is empty'), timeoutSec: z.number().optional().describe('Long-poll timeout, 1–60 (default 25)') },
  run(poll),
);

server.tool(
  'axona_host',
  'Host (root) a topic on Claude\'s persistent peer: the peer stores and serves the topic\'s messages for the network WITHOUT subscribing — Claude becomes durable infrastructure for that topic, so its backlog stays answerable even when no other node holds it. Idempotent.',
  { topic: z.string().describe('Topic name to host/root'), ...REGION, ...OWNED },
  run(host),
);

server.tool(
  'axona_unhost',
  'Stop hosting a topic previously rooted with axona_host.',
  { topic: z.string().describe('Topic name to stop hosting'), ...REGION, ...OWNED },
  run(unhost),
);

server.tool(
  'axona_unwatch',
  'Stop a standing subscription started by axona_watch and discard its buffer. Returns how many messages were still buffered.',
  { topic: z.string().describe('Topic to stop watching'), ...REGION, ...OWNED },
  run(unwatch),
);

server.tool(
  'axona_get_class',
  'Resolve an author\'s self-declared class (human / agent / unstated) from its Author ID alone — a voluntary, signed provenance claim carried on that author\'s owner-only profile topic (only the author can set their own class). Returns { class, operator?, label? }; "unstated" if the author never declared. This is provenance, not detection: a deceptive actor can decline to flag or flag falsely, and absence is NOT "human".',
  { authorId: z.string().describe('64-hex Author ID (signerPubkey) to look up') },
  run((a) => getAuthorClass(a)),
);

server.tool(
  'axona_set_class',
  'Set THIS peer\'s own author-class attestation (publishes a signed claim to its owner-only profile topic). The persistent peer already self-declares "agent" on connect; use this to change it or attach an operator. A human-facing app would wire its own explicit "I am human" toggle to this.',
  { class: z.enum(['agent', 'human']).describe('this author\'s class'), operator: z.string().optional().describe('optional: who runs this author (pubkey/handle)'), label: z.string().optional().describe('optional short human-readable label') },
  run(({ class: cls, operator, label }) => setAuthorClass({ cls, operator, label })),
);

server.tool(
  'axona_status',
  'Report the persistent peer\'s state: whether it is connected, its nodeId + stable Author ID, mesh health (synaptome/peers), and every active watch with its buffered/total/dropped counts. Takes no arguments.',
  {},
  run(status),
);

server.tool(
  'axona_pull',
  'Fetch only the single most recent message on an Axona topic (no listening window). Faster than watch/poll when you just want the latest value. Returns { found, message, msgId }.',
  { topic: z.string().describe('Topic name'), ...REGION, ...OWNED },
  run(pull),
);

// NOTE: the one-shot `axona_subscribe` listen-window tool is deliberately NOT
// registered. An Axona MCP participant is a CONTINUOUS node — read via a standing
// axona_watch + axona_poll, which survives across calls. A fixed-window one-shot
// listen is not an available option (David directive, 2026-08-13).

// ── files ───────────────────────────────────────────────────────────────
// PULL-ONLY, and that is a security property rather than a limitation. A topic
// is public: anyone who knows it can publish to it. If arrivals auto-saved,
// any stranger could put bytes on this host by publishing to a topic the agent
// happens to read. So listing is free and nothing touches disk until a call
// names a specific hash.

server.tool(
  'axona_send_file',
  'Share a local file over Axona so a person (or another agent) can fetch it. The bytes go to a topic derived from their own sha256; a small pointer is announced on the shared topic you name. Returns the sha256 — that hash IS the address, and anyone who has it can fetch the file. Interoperates with the axona.portal desktop app: a topic name is namespaced under "portal." on both sides, so send to "axona.bot" here and it appears in a portal watching "axona.bot". Max 10 MB (above that a later subscriber cannot reassemble it).',
  { path: z.string().describe('Absolute path of the local file to share'),
    topic: z.string().describe('Shared topic to announce on, e.g. "axona.bot" (auto-namespaced to "portal.axona.bot")'),
    ...REGION,
    filename: z.string().optional().describe('Override the announced filename (defaults to the file\'s own basename)') },
  run(sendFile),
);

server.tool(
  'axona_list_files',
  'List files announced on a shared topic. READ-ONLY — returns metadata (filename, size, sha256, sender) and writes NOTHING to disk. Opens a bounded listening window (default 15s) to collect the topic\'s replay, then stops; no standing subscription is left behind. Use the returned sha256 with axona_get_file to actually fetch one.',
  { topic: z.string().describe('Shared topic to list, e.g. "axona.bot"'), ...REGION,
    seconds: z.number().optional().describe('How long to collect announcements, 1-60 (default 15)') },
  run(listFiles),
);

server.tool(
  'axona_get_file',
  'Fetch a file by its sha256 and write it to the Axona files directory (~/Axona Files, or $AXONA_FILES_DIR). The hash is both the address and the integrity check: the content is re-hashed after reassembly and a mismatch is REFUSED before anything is written, so you are trusting arithmetic rather than the sender. Files are written 0600 with no execute bit and are never opened; a warning is returned for executable extensions. The filename is sanitised — it cannot escape the files directory.',
  { sha256: z.string().describe('The 64-hex content hash, from axona_list_files or from whoever shared it'),
    ...REGION,
    filename: z.string().optional().describe('Override the saved filename (sanitised regardless)'),
    timeoutSec: z.number().optional().describe('How long to wait for all chunks, 5-300 (default 90)') },
  run(getFile),
);

server.tool(
  'axona_reconnect',
  'Rebuild this peer\'s connection from scratch when you suspect you have gone deaf: you are receiving nothing on topics that should be live, or axona_status shows peers 0 while the network is up. Drops the transport, mints a fresh one, and re-seats every watch and hosted topic — including watches you added at runtime, each replayed with since:"all" so the silent window is filled in, and with any messages you had not yet polled preserved. Your durable Author ID does NOT change: you come back as the same participant on a new seat (the transport nodeId is ephemeral by design and WILL change). Safe to call when healthy, but it costs a mesh re-formation, so check axona_status first and prefer it as a repair rather than a routine. Peers may read 0 for a few seconds afterwards while the mesh forms.',
  { since: z.enum(['all', 'latest', 'live']).optional().describe('How much history to replay on each restored watch (default "all" — fills the deaf window)') },
  run(reconnect),
);

await server.connect(new StdioServerTransport());


// PUSH: every arrival on a watched topic is emitted as an MCP logging
// notification to the client (best-effort — needs a client that consumes
// logging; never throws into the peer's delivery path). This is the true
// server→client push; axona_poll(wait:true) is the matching pull side.
onArrival((evt) => {
  server.sendLoggingMessage({
    level: 'info', logger: 'axona',
    data: { event: 'axona_message', topic: evt.topic, region: evt.region, msgId: evt.msgId, message: evt.message, signer: evt.signer },
  }).catch(() => { /* client may not subscribe to logging — ignore */ });
});

// ── INBOX SINK: the arrival path an agent host can actually be woken by ──────
//
// WHY, given the logging push above already exists. That notification is the
// correct server→client push and it works, but it only reaches an agent whose
// host subscribes to MCP logging and surfaces it as an interruption. Ours does
// not. So in practice a message sat in the watch buffer until the agent next
// chose to poll, which made the reaction time equal to the polling interval —
// ~25 minutes on a good day, and unbounded whenever no session was running.
//
// This sink writes one JSON line per arrival to a file. A file is the lowest
// common denominator every agent host can watch: Claude Code arms a Monitor on
// `tail -f`, and anything else can inotify/poll it. The agent is woken in
// seconds by the arrival itself rather than by its own timer.
//
// It deliberately reuses the STANDING WATCHES rather than opening a second peer.
// A separate watcher process would mean another node on prod signing with
// another identity, and duplicate-identity confusion has already cost us one
// live investigation (#356). One peer, one subscription, two sinks.
//
//   MCP_INBOX         file to append to (default ~/.axona/mcp-inbox.jsonl)
//   MCP_INBOX_TOPICS  comma list to record; empty means EVERY watched topic
//
// Self-authored messages are always skipped: waking an agent to read its own
// post trains it to ignore the channel.
const INBOX = process.env.MCP_INBOX || join(homedir(), '.axona', 'mcp-inbox.jsonl');
const INBOX_TOPICS = new Set(
  String(process.env.MCP_INBOX_TOPICS ?? 'council,axona.dev,axona.chat')
    .split(',').map((s) => s.trim()).filter(Boolean));

if (INBOX && INBOX !== 'off') {
  try { mkdirSync(dirname(INBOX), { recursive: true }); } catch { /* best effort */ }
  onArrival((evt) => {
    try {
      if (evt.self) return;                                        // never wake me for my own words
      if (INBOX_TOPICS.size && !INBOX_TOPICS.has(evt.topic)) return;
      // One line, one arrival. `text` is a convenience projection so a shell
      // filter (grep/jq) can read it without understanding std/message.
      const text = typeof evt.message === 'string' ? evt.message
        : (evt.message && typeof evt.message.text === 'string' ? evt.message.text : null);
      appendFileSync(INBOX, `${JSON.stringify({
        at: new Date().toISOString(),
        topic: evt.topic, region: evt.region,
        signer: evt.signer ? String(evt.signer).slice(0, 12) : null,
        msgId: evt.msgId, text, message: evt.message,
      })}\n`);
    } catch { /* a failed write must never break message delivery */ }
  });
  process.stderr.write(`[axona-mcp] inbox sink -> ${INBOX} topics=${[...INBOX_TOPICS].join(',') || '(all watched)'}\n`);
}

process.stderr.write(`axona MCP server v${VERSION} ready — persistent peer, bridge ${DEFAULT_BRIDGE}\n`);
