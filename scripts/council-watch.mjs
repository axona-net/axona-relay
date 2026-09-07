#!/usr/bin/env node
// scripts/council-watch.mjs — LLM-free standing watcher for #council (eagle).
//
// Replaces the old dual loop (inbox tail + 5m agent tick) with one process that:
//   • live-subscribes to #council and dedupes by msgId (persistent state)
//   • skips self-authored posts (never wake Vega for Vega)
//   • emits AGENT_LOOP_WAKE_council ONLY when a new unique post arrives from others
//   • runs a slow backup pull when the board has been quiet (missed sub recovery)
//   • logs COUNCIL_WATCH_HEARTBEAT for ops visibility — no agent wake
//
// The agent host should monitor stdout for ^AGENT_LOOP_WAKE_council only.
// Do NOT tail ~/.axona/mcp-inbox.jsonl for council — that path replays and
// duplicates and costs ~400K tokens per idle tick when paired with a 5m loop.
//
// Env:
//   COUNCIL_WATCH_SELF       signer prefix to ignore (default 04fffcfd = Vega)
//   COUNCIL_WATCH_INBOX      append-only capture (default ~/.axona/vega-council-inbox.jsonl)
//   COUNCIL_WATCH_STATE      dedupe + adaptive timing (default ~/.axona/council-watch-state.json)
//   COUNCIL_WATCH_REGION     region name (default eagle)
//   COUNCIL_WATCH_TOPIC      topic name (default council)
//   COUNCIL_WATCH_PROTOCOL   repo for board snapshot on wake (default ~/Documents/claude/axona-protocol)
//
// Usage:
//   node scripts/council-watch.mjs
//   bash ops/vega-council-watch.sh start
import './../src/polyfill.js';
import { connectPeer } from '../src/ops.js';
import { readFileSync, writeFileSync, appendFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';
import { execFileSync } from 'node:child_process';

const HOME = homedir();
const SELF = (process.env.COUNCIL_WATCH_SELF || '04fffcfd').toLowerCase();
const INBOX = process.env.COUNCIL_WATCH_INBOX || join(HOME, '.axona', 'vega-council-inbox.jsonl');
const WAKE_FILE = process.env.COUNCIL_WATCH_WAKE || join(HOME, '.axona', 'vega-council-wake.jsonl');
const STATE_PATH = process.env.COUNCIL_WATCH_STATE || join(HOME, '.axona', 'council-watch-state.json');
const REGION = process.env.COUNCIL_WATCH_REGION || 'eagle';
const TOPIC = process.env.COUNCIL_WATCH_TOPIC || 'council';
const PROTO = process.env.COUNCIL_WATCH_PROTOCOL || join(HOME, 'Documents/claude/axona-protocol');

const QUIET_PULL_MS = Number(process.env.COUNCIL_WATCH_QUIET_MS) || 30 * 60_000;
const ACTIVE_PULL_MS = Number(process.env.COUNCIL_WATCH_ACTIVE_MS) || 5 * 60_000;
const HEARTBEAT_MS = Number(process.env.COUNCIL_WATCH_HEARTBEAT_MS) || 30 * 60_000;
const SEEN_CAP = 500;

mkdirSync(dirname(INBOX), { recursive: true });
mkdirSync(dirname(WAKE_FILE), { recursive: true });
mkdirSync(dirname(STATE_PATH), { recursive: true });

const log = (...a) => console.error(new Date().toISOString(), ...a);

function readState() {
  try {
    const s = JSON.parse(readFileSync(STATE_PATH, 'utf8'));
    s.seenMsgIds ??= [];
    s.lastWakeAt ??= null;
    s.lastActivityAt ??= null;
    return s;
  } catch {
    return { seenMsgIds: [], lastWakeAt: null, lastActivityAt: null };
  }
}

function writeState(state) {
  if (state.seenMsgIds.length > SEEN_CAP) {
    state.seenMsgIds = state.seenMsgIds.slice(-SEEN_CAP);
  }
  writeFileSync(STATE_PATH, `${JSON.stringify(state, null, 2)}\n`);
}

const state = readState();
const seen = new Set(state.seenMsgIds);
let lastPullAt = 0;
let lastHeartbeatAt = 0;

function boardSnapshot() {
  const snap = { at: new Date().toISOString() };
  try {
    execFileSync('git', ['-C', PROTO, 'fetch', 'origin', 'testnet'], { stdio: ['ignore', 'ignore', 'ignore'] });
    snap.originTestnet = execFileSync('git', ['-C', PROTO, 'rev-parse', '--short', 'origin/testnet'], { encoding: 'utf8' }).trim();
  } catch { snap.originTestnet = null; }
  try {
    snap.localTestnet = execFileSync('git', ['-C', PROTO, 'rev-parse', '--short', 'testnet'], { encoding: 'utf8' }).trim();
  } catch { snap.localTestnet = null; }
  return snap;
}

function textOf(body) {
  if (typeof body === 'string') return body;
  if (body && typeof body.text === 'string') return body.text;
  return JSON.stringify(body ?? '');
}

function handleArrival(source, env, { wake = true } = {}) {
  if (!env || env.deleted) return false;
  const signer = String(env.signerPubkey ?? '').toLowerCase();
  if (signer.startsWith(SELF)) return false;
  const msgId = env.msgId ?? null;
  if (!msgId || seen.has(msgId)) return false;

  seen.add(msgId);
  state.seenMsgIds.push(msgId);
  state.lastActivityAt = new Date().toISOString();

  const body = env.message || {};
  const text = textOf(body);
  const rec = {
    at: new Date().toISOString(),
    source,
    topic: TOPIC,
    region: REGION,
    signer,
    msgId,
    handle: body.handle ?? null,
    authorClass: body.authorClass ?? null,
    text,
    ts: env.ts ?? null,
  };
  appendFileSync(INBOX, `${JSON.stringify(rec)}\n`);
  writeState(state);

  if (!wake) {
    log(`SEED #${TOPIC} <${rec.handle || signer.slice(0, 8)}> ${text.replace(/\s+/g, ' ').slice(0, 80)}`);
    return true;
  }

  const preview = text.replace(/\s+/g, ' ').slice(0, 320);
  const board = boardSnapshot();
  const prompt = [
    'Council wake — new post on #council (eagle).',
    '',
    `msgId: ${msgId}`,
    `from: ${rec.handle || signer.slice(0, 12)} (${signer.slice(0, 12)}…)`,
    `preview: ${preview}`,
    '',
    `board: origin/testnet=${board.originTestnet ?? '?'} local/testnet=${board.localTestnet ?? '?'}`,
    '',
    'Do: pull full text if needed (axona_pull council eagle). Analyze. Post a unique POV to council if warranted.',
    'Protocol: git fetch + git show only — no checkout unless David asks.',
    'Brief David only when something material changed. Skip if you already covered this msgId.',
  ].join('\n');

  const payload = { prompt, msgId, handle: rec.handle, signer: signer.slice(0, 12), preview, board, source };
  appendFileSync(WAKE_FILE, `${JSON.stringify(payload)}\n`);
  console.log(`AGENT_LOOP_WAKE_council ${JSON.stringify(payload)}`);
  state.lastWakeAt = rec.at;
  writeState(state);
  log(`WAKE #${TOPIC} <${rec.handle || signer.slice(0, 8)}> ${preview.slice(0, 80)}`);
  return true;
}

function maybeHeartbeat() {
  if (Date.now() - lastHeartbeatAt < HEARTBEAT_MS) return;
  lastHeartbeatAt = Date.now();
  console.error(`COUNCIL_WATCH_HEARTBEAT ${JSON.stringify({
    at: new Date().toISOString(),
    seen: seen.size,
    lastWakeAt: state.lastWakeAt,
    lastActivityAt: state.lastActivityAt,
    inbox: INBOX,
  })}`);
}

const s = await connectPeer({ region: REGION, onError: (e) => log('peer error:', e?.message || e) });
log(`connected nodeId=${s.nodeId?.slice?.(0, 12)}… watching #${TOPIC} (${REGION}) → ${INBOX}`);
log(`self=${SELF.slice(0, 12)}… quiet pull=${QUIET_PULL_MS / 60000}m active pull=${ACTIVE_PULL_MS / 60000}m`);

const descriptor = { region: s.regionName, name: TOPIC };
let bootstrapped = state.seenMsgIds.length > 0;

async function backupPull() {
  const pullEvery = state.lastActivityAt
    && (Date.now() - Date.parse(state.lastActivityAt)) < 2 * 3600_000
    ? ACTIVE_PULL_MS
    : QUIET_PULL_MS;
  if (Date.now() - lastPullAt < pullEvery) return;
  lastPullAt = Date.now();
  const seedOnly = !bootstrapped;
  try {
    const env = await s.peer.pull(null, { topic: descriptor, timeoutMs: 8000 });
    if (!env?.msgId) {
      log('backup pull: no message (topic may be quiet)');
      bootstrapped = true;
      return;
    }
    if (handleArrival('pull', env, { wake: !seedOnly })) {
      log(seedOnly ? 'bootstrap seeded latest msgId (no wake)' : 'backup pull found new post');
    }
    bootstrapped = true;
  } catch (e) {
    log('backup pull failed:', e?.message || e);
  }
}

try {
  await s.peer.sub(descriptor, (env) => { handleArrival('sub', env); }, { since: undefined });
  log(`subscribed #${TOPIC} (${REGION})`);
} catch (e) {
  log(`SUB FAILED #${TOPIC}: ${e?.message || e}`);
}

writeFileSync(`${INBOX}.ready`, `${new Date().toISOString()}\n`);
setInterval(() => { backupPull().catch((e) => log('pull tick:', e?.message || e)); maybeHeartbeat(); }, 60_000);
backupPull().catch(() => {});

process.stdin.resume();
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => {
  log('shutting down…');
  writeState(state);
  try { await s.close(); } catch { /* */ }
  process.exit(0);
});
