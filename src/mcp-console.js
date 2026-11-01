// mcp-console.js — stdout is the MCP JSON-RPC wire; NOTHING else may write it.
//
// MCP-STDOUT-POLLUTION (Orion b91784d9, 2026-11-01): src/polyfill.js logs one
// "[ts] ice-pair {...}" line with console.log on every RTCPeerConnection state
// change. In Node, console.log writes process.stdout, which under this server
// is the JSON-RPC channel to the host. Claude Code's client logged 64 of them
// in 22 minutes on one seat ("Ignoring non-JSON line on stdout") and carried
// on; Antigravity's client dropped the stdio pipe at the first one. The relay
// daemon NEEDS that line on its stdout (ops/negotiation-triage.py reads it), so
// polyfill.js is untouched: this module rebinds the four stdout-bound console
// methods to stderr instead, and mcp.js imports it FIRST. ESM evaluates imports
// in order before the importing module's body, so every later import — ops.js,
// polyfill.js, the kernel — already sees the rebound console when it loads.
// console.error and console.trace already write stderr and are left alone.
import { format } from 'node:util';

const toStderr = (...args) => { process.stderr.write(format(...args) + '\n'); };
for (const name of ['log', 'info', 'debug', 'warn']) console[name] = toStderr;
