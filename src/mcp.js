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
//   • axona_subscribe              — back-compat one-shot listen window
// The peer signs with a durable author persisted at ~/.axona/claude-mcp-author.json,
// so Claude keeps the same on-network identity across restarts.

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { readFile } from 'node:fs/promises';
import { DEFAULT_BRIDGE } from './ops.js';
import { publish, pull, watch, poll, unwatch, status, subscribeWindow, host, unhost, onArrival } from './mcp-session.js';

const VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

const J = (obj) => ({ content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }] });
const E = (msg) => ({ isError: true, content: [{ type: 'text', text: JSON.stringify({ ok: false, error: msg }) }] });
const run = (fn) => async (args) => { try { return J(await fn(args)); } catch (e) { return E(e?.message || String(e)); } };

const server = new McpServer({ name: 'axona', version: VERSION });

const REGION = { region: z.string().optional().describe('Region name or code for the topic anchor (default "useast" / 0x89)') };

server.tool(
  'axona_publish',
  'Publish a message to a topic on the live Axona peer-to-peer network (production by default). Uses the server\'s persistent peer and its STABLE author identity, so every publish comes from the same Author ID. Returns the msgId + signer. Topics are anchored at a region (default "useast"/0x89) — subscribers MUST use the same region. Interoperates with the live apps: publishing to "us-east/hello-world" appears in the axona.net / demo.axona.net feed.',
  { topic: z.string().describe('Topic name, e.g. "us-east/hello-world" or "claude/test"'), message: z.string().describe('Message body to publish'), ...REGION },
  run(publish),
);

server.tool(
  'axona_watch',
  'Open a STANDING subscription to an Axona topic on the server\'s persistent peer. Messages that arrive are BUFFERED on the server; call axona_poll to read them. Unlike axona_subscribe (which blocks for a fixed window), this returns immediately and keeps listening across later tool calls — this is how the agent participates as a continuous subscriber. Idempotent: watching an already-watched topic is a no-op. since:"all" (default) replays the cached backlog into the buffer; "new" buffers only future messages.',
  { topic: z.string().describe('Topic name to watch'), ...REGION, since: z.enum(['all', 'new']).optional().describe('"all" replays backlog into the buffer (default); "new" is live-only') },
  run(watch),
);

server.tool(
  'axona_poll',
  'Drain buffered messages collected by axona_watch. With `topic`, drains that one watch; without it, drains every active watch. Returns the messages and clears them from the buffer (peek:true reads without clearing). `max` caps how many are returned. This is the agent\'s "inbox". Set wait:true to LONG-POLL — if nothing is buffered, the call blocks server-side until a message arrives or `timeoutSec` (default 25, max 60) elapses, so you get near-zero-latency delivery instead of fixed-interval polling.',
  { topic: z.string().optional().describe('Topic to drain (omit to drain ALL active watches)'), ...REGION, peek: z.boolean().optional().describe('true = read without clearing the buffer'), max: z.number().optional().describe('Cap the number of messages returned'), wait: z.boolean().optional().describe('Long-poll: block until an arrival (or timeout) if the buffer is empty'), timeoutSec: z.number().optional().describe('Long-poll timeout, 1–60 (default 25)') },
  run(poll),
);

server.tool(
  'axona_host',
  'Host (root) a topic on Claude\'s persistent peer: the peer stores and serves the topic\'s messages for the network WITHOUT subscribing — Claude becomes durable infrastructure for that topic, so its backlog stays answerable even when no other node holds it. Idempotent.',
  { topic: z.string().describe('Topic name to host/root'), ...REGION },
  run(host),
);

server.tool(
  'axona_unhost',
  'Stop hosting a topic previously rooted with axona_host.',
  { topic: z.string().describe('Topic name to stop hosting'), ...REGION },
  run(unhost),
);

server.tool(
  'axona_unwatch',
  'Stop a standing subscription started by axona_watch and discard its buffer. Returns how many messages were still buffered.',
  { topic: z.string().describe('Topic to stop watching'), ...REGION },
  run(unwatch),
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
  { topic: z.string().describe('Topic name'), ...REGION },
  run(pull),
);

server.tool(
  'axona_subscribe',
  'Subscribe to an Axona topic, collect messages for a fixed window, then return them. Blocks for `seconds` (default 20, max 120). Convenience for a one-shot listen; for ongoing participation prefer axona_watch + axona_poll (no blocking, survives across calls). since:"all" (default) replays the cached backlog; "new" is live-only. Must use the same region as the publisher.',
  { topic: z.string().describe('Topic name to subscribe to'), ...REGION, seconds: z.number().optional().describe('How long to listen, 1–120 (default 20)'), since: z.enum(['all', 'new']).optional().describe('"all" replays backlog (default); "new" is live-only') },
  run(subscribeWindow),
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

process.stderr.write(`axona MCP server v${VERSION} ready — persistent peer, bridge ${DEFAULT_BRIDGE}\n`);
