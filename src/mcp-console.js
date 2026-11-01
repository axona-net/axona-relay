// mcp-console.js — stdout is the MCP JSON-RPC wire; NOTHING else may write it.
//
// MCP-STDOUT-POLLUTION (Orion b91784d9, 2026-11-01): src/polyfill.js logs one
// "[ts] ice-pair {...}" line with console.log on every RTCPeerConnection state
// change. In Node, console.log writes process.stdout, which under this server
// is the JSON-RPC channel to the host. Claude Code's client logged 64 of them
// in 22 minutes on one seat ("Ignoring non-JSON line on stdout") and carried
// on; Antigravity's client dropped the stdio pipe at the first one. The relay
// daemon NEEDS that line on its stdout (ops/negotiation-triage.py reads it), so
// polyfill.js is untouched: this module points the whole console at stderr
// instead, and mcp.js imports it FIRST. ESM evaluates imports in order before
// the importing module's body, so every later import — ops.js, polyfill.js,
// the kernel — already sees this console when it loads.
//
// 0.189.0 rebound console.log, info, debug and warn. On Node 24 console.dir
// (and dirxml) write the Console's own stdout stream without going through
// log, so they slipped past that form (Orion 11c49d2f, MCPSC-1); table, count,
// time, group and assert do go through log or warn. 0.190.0 replaces the
// global console with a Console whose stdout AND stderr are process.stderr,
// which covers every method at once; nothing in this server's import graph
// calls dir today, so this closes a door nobody has walked through yet.
import { Console } from 'node:console';

globalThis.console = new Console({ stdout: process.stderr, stderr: process.stderr });
