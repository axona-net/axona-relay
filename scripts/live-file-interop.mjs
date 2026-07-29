// =====================================================================
// live-file-interop.mjs — the two implementations must agree.
//
// axona-portal/src/transfer/ and axona-relay/src/file-transfer.js implement
// manifest v1 SEPARATELY. Unit tests pin the constants on each side, but only
// this proves the thing that matters: a file sent by the desktop portal is
// findable and fetchable by an agent, over the real network, with neither side
// importing the other's code.
//
// If this fails, a human drags a file in and the agent simply never sees it —
// no error anywhere, because a pointer that does not validate is indistinguishable
// from a topic with nothing on it.
//
//   node scripts/live-file-interop.mjs
// =====================================================================

import { connect, resolveRegion, regionCenter } from '../vendor/axona-protocol/src/index.js';
import { sendFile as portalSend } from '../../axona-portal/src/transfer/index.js';
import { listPointers, fetchFileToDisk, namespaced, FILES_DIR, hashBytes } from '../src/file-transfer.js';
import { randomBytes } from 'node:crypto';
import { readFile, rm } from 'node:fs/promises';

const BRIDGE = process.env.BRIDGE_URL || 'wss://bridge.axona.net';
const REGION = 'eagle';
const SHARE  = namespaced(`interop.${Date.now()}`);

const center = regionCenter(resolveRegion(REGION));
const dial = async (label) => {
  const r = await connect({ bridge: BRIDGE, location: { lat: center.lat, lng: center.lng },
                            author: true, ready: { timeoutSec: 45 } });
  console.log(`  ${label} connected`);
  return { peer: r.peer ?? r, author: r.author ?? null };
};

const t0 = Date.now();
console.log(`bridge ${BRIDGE} · shared topic ${SHARE}\n`);

const HUMAN = await dial('portal (human)');
const AGENT = await dial('mcp (agent)  ');

const payload  = randomBytes(120 * 1024);
const expect   = hashBytes(payload);
const filename = 'interop-report.pdf';
console.log(`\npayload ${payload.length} bytes · sha256 ${expect.slice(0, 16)}…`);

// ── the human's portal sends, using the PORTAL implementation ──────────
console.log('\nportal: sending (axona-portal/src/transfer)…');
const sent = await portalSend(HUMAN.peer, {
  bytes: payload, filename, mime: 'application/pdf',
  shareTopic: { region: REGION, name: SHARE }, region: REGION, signWith: HUMAN.author,
});
console.log(`  sha256 ${sent.sha256.slice(0,16)}… chunks=${sent.chunks} repaired=${sent.repaired}`);
const hashesAgree = sent.sha256 === expect;

// ── the agent lists, using the RELAY implementation ────────────────────
console.log('\nagent: listing (axona-relay/src/file-transfer)…');
const listed = await listPointers(AGENT.peer, { region: REGION, name: SHARE }, { seconds: 20 });
console.log(`  saw ${listed.length} file(s): ${listed.map(f => `${f.filename} ${f.bytes}B`).join(', ') || '(none)'}`);
const found = listed.find(f => f.sha256 === expect);

let ok = false, saved = null;
if (found) {
  console.log('\nagent: fetching by hash + verifying before write…');
  saved = await fetchFileToDisk(AGENT.peer, { sha256: found.sha256, region: REGION, filename: found.filename, timeoutMs: 90_000 });
  const onDisk = new Uint8Array(await readFile(saved.path));
  ok = Buffer.compare(Buffer.from(payload), Buffer.from(onDisk)) === 0;
  console.log(`  wrote ${saved.path}`);
  console.log(`  bytes match what the portal sent: ${ok}`);
  console.log(`  filename preserved across implementations: ${saved.filename === filename}`);
  console.log(`  contained inside ${FILES_DIR}: ${saved.path.startsWith(FILES_DIR)}`);
}

const pass = hashesAgree && !!found && ok && saved?.path.startsWith(FILES_DIR);
console.log(`\n  portal hash == expected: ${hashesAgree}`);
console.log(`  agent found the pointer: ${!!found}`);
console.log(`\n${pass ? '✓ INTEROP PASS' : '✗ INTEROP FAIL'} · ${((Date.now()-t0)/1000).toFixed(1)}s`);

if (saved) { try { await rm(saved.path); } catch { /* */ } }
try { await HUMAN.peer.leave?.(); await AGENT.peer.leave?.(); } catch { /* */ }
process.exit(pass ? 0 : 1);
