#!/usr/bin/env node
// mcp.js — Model Context Protocol server exposing Axona pub/sub as native tools.
//
// Speaks MCP over stdio so an AI agent (Claude Code, etc.) gets first-class
// tools — axona_publish / axona_subscribe / axona_pull — instead of shelling
// out to the CLI. Each call connects a fresh ephemeral peer through the testnet
// bridge (see ops.js), does one job, tears down, and returns JSON.
//
// stdout is the JSON-RPC channel; all human logging goes to stderr.

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { readFile } from 'node:fs/promises';
import { publish, subscribe, pull, DEFAULT_BRIDGE } from './ops.js';

const VERSION = JSON.parse(
  await readFile(new URL('../package.json', import.meta.url), 'utf8')).version;

const J = (obj) => ({ content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }] });
const E = (msg) => ({ isError: true, content: [{ type: 'text', text: JSON.stringify({ ok: false, error: msg }) }] });

const server = new McpServer({ name: 'axona', version: VERSION });

server.tool(
  'axona_publish',
  'Publish a message to a topic on the live Axona peer-to-peer network. Connects an ephemeral peer through the testnet bridge, publishes a signed envelope, and returns its msgId. Topics are anchored at a region (default "useast"/0x89) — subscribers MUST use the same region. Interoperates with the live apps: publishing to "us-east/hello-world" appears in the axona.net / demo-testnet feed.',
  {
    topic:   z.string().describe('Topic name, e.g. "us-east/hello-world" or "claude/test"'),
    message: z.string().describe('Message body to publish'),
    region:  z.string().optional().describe('Region name or code for the topic anchor (default "useast" / 0x89)'),
  },
  async ({ topic, message, region }) => {
    try { return J(await publish({ topic, message, region })); }
    catch (e) { return E(e?.message || String(e)); }
  }
);

server.tool(
  'axona_subscribe',
  'Subscribe to an Axona topic, collect messages for a fixed window, then return them. Connects an ephemeral peer, subscribes, listens for `seconds` (default 20, max 120), and returns every message received. since:"all" (default) replays the cached backlog so you see recent history; "new" is live-only. Must use the same region as the publisher.',
  {
    topic:   z.string().describe('Topic name to subscribe to'),
    region:  z.string().optional().describe('Region name or code (default "useast")'),
    seconds: z.number().optional().describe('How long to listen, 1–120 (default 20)'),
    since:   z.enum(['all', 'new']).optional().describe('"all" replays backlog (default); "new" is live-only'),
  },
  async ({ topic, region, seconds, since }) => {
    try { return J(await subscribe({ topic, region, seconds, since })); }
    catch (e) { return E(e?.message || String(e)); }
  }
);

server.tool(
  'axona_pull',
  'Fetch only the single most recent message on an Axona topic (no listening window). Faster than subscribe when you just want the latest value. Returns { found, message, msgId }.',
  {
    topic:  z.string().describe('Topic name'),
    region: z.string().optional().describe('Region name or code (default "useast")'),
  },
  async ({ topic, region }) => {
    try { return J(await pull({ topic, region })); }
    catch (e) { return E(e?.message || String(e)); }
  }
);

await server.connect(new StdioServerTransport());
process.stderr.write(`axona MCP server v${VERSION} ready (bridge ${DEFAULT_BRIDGE})\n`);
