// =====================================================================
// file-transfer.js — send and fetch files over Axona, for the MCP peer.
//
// ─── THE WIRE FORMAT IS SHARED; THIS CODE IS NOT ──────────────────────
// axona-portal/src/transfer/ is the other implementation. The two MUST agree
// on manifest v1 exactly — MANIFEST_V, the pointer field set, and the
// `portal.f.<sha256>` topic name. They are deliberately NOT one package yet:
// the format is young (folder drops will add an `encoding`) and publishing a
// package whose shape is about to move is how you end up supporting two. The
// SCHEMA is the contract; smoke_file_transfer.mjs fences this side of it.
//
// ─── WHY MCP IS PULL-ONLY ─────────────────────────────────────────────
// The desktop portal auto-saves arrivals, which is fine for an app a human is
// watching. Here it would be a hole: a topic is PUBLIC, so anyone who knows it
// can publish to it, and auto-saving would mean any stranger can put bytes on
// the agent's host by publishing to a topic the agent happens to read.
//
// So listFiles() only ever returns metadata, and nothing touches disk until an
// explicit fetchFile() names a hash. Every write is additionally:
//   · verified — sha256 recomputed over what reassembled, mismatch REFUSED
//   · contained — filename reduced to one component, no traversal, no escape
//   · inert — mode 0600, no execute bit, never opened
// =====================================================================

import { createHash } from 'node:crypto';
import { writeFile, mkdir, readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve, basename, extname, dirname } from 'node:path';
import { publishChunkedBytes, receiveChunkedBytes } from '../vendor/axona-protocol/std/chunk.js';

// ── manifest v1 — MUST match axona-portal/src/transfer/manifest.js ──
export const MANIFEST_V   = 1;
export const TOPIC_PREFIX = 'portal.';
export const ENCODINGS    = new Set(['raw']);

/** Ceiling. Above ~10.4 MB a transfer cannot be reassembled by anyone who
 *  subscribes after it was sent — the replay cache no longer holds every
 *  chunk. Refusing here is better than shipping something unreadable. */
export const MAX_FILE_BYTES = 10 * 1024 * 1024;

/** Where fetched files land. Never caller-supplied: a tool argument that could
 *  name an absolute path is a tool that can write anywhere the process can. */
export const FILES_DIR = resolve(process.env.AXONA_FILES_DIR || join(homedir(), 'Axona Files'));

export const hashBytes = (b) => createHash('sha256').update(b).digest('hex');

export function fileTopicName(sha256) {
  if (!/^[0-9a-f]{64}$/.test(sha256)) throw new Error('fileTopicName: expected a lowercase 64-hex sha256');
  return `${TOPIC_PREFIX}f.${sha256}`;
}

/** Put a shared-topic name under the portal namespace, idempotently — so an
 *  agent and a human portal addressing "axona.bot" reach the SAME topic. */
export function namespaced(name) {
  const s = String(name ?? '').trim();
  return s.startsWith(TOPIC_PREFIX) ? s : TOPIC_PREFIX + s;
}

export function makePointer({ sha256, filename, bytes, mime = 'application/octet-stream', encoding = 'raw' }) {
  if (!/^[0-9a-f]{64}$/.test(sha256))         throw new Error('makePointer: bad sha256');
  if (!Number.isInteger(bytes) || bytes < 0)  throw new Error('makePointer: bad bytes');
  if (!ENCODINGS.has(encoding))               throw new Error(`makePointer: unknown encoding ${encoding}`);
  return { v: MANIFEST_V, kind: 'file', sha256, filename: String(filename ?? 'file'), bytes, mime: String(mime), encoding };
}

/** Never throws, never partially trusts — a public topic delivers anything. */
export function readPointer(body) {
  if (!body || typeof body !== 'object')                 return null;
  if (body.v !== MANIFEST_V || body.kind !== 'file')     return null;
  if (!/^[0-9a-f]{64}$/.test(String(body.sha256 ?? ''))) return null;
  if (!Number.isInteger(body.bytes) || body.bytes < 0)   return null;
  if (!ENCODINGS.has(body.encoding))                     return null;
  return {
    v: body.v, kind: 'file', sha256: body.sha256,
    filename: String(body.filename ?? 'file'), bytes: body.bytes,
    mime: String(body.mime ?? 'application/octet-stream'), encoding: body.encoding,
  };
}

// ── the trust boundary ────────────────────────────────────────────────
// A filename from the network is hostile text, not a path. Mirrors
// axona-portal/src/paths.js, which carries the 62-assertion fence.

/** Executables are never handed to the OS and never marked runnable. The
 *  decision to run something a stranger sent belongs to the user, taken
 *  deliberately in their own shell — not implied by a fetch. */
const NO_RUN = new Set(['.exe', '.app', '.sh', '.command', '.bat', '.cmd', '.com', '.scr',
                        '.msi', '.pkg', '.dmg', '.jar', '.ps1', '.vbs', '.scpt', '.bin', '.run']);

export function safeFilename(raw) {
  let s = String(raw ?? '').trim();
  s = basename(s);                                  // strip every directory component
  // eslint-disable-next-line no-control-regex
  s = s.replace(/[\x00-\x1f\x7f]/g, '');            // control chars, incl. the NUL truncation trick
  s = s.replace(/[/\\:*?"<>|]/g, '_');              // separators + Windows-reserved
  s = s.replace(/^\.+/, '');                        // no dotfiles, no "..", no "."
  if (!s) s = 'file';
  if (s.length > 120) {                             // keep the extension when truncating
    const ext = extname(s).slice(0, 16);
    s = s.slice(0, 120 - ext.length) + ext;
  }
  return s;
}

/** Resolve inside FILES_DIR, or throw. The check is on the RESOLVED path, so
 *  symlinks and `..` cannot smuggle a write out of the directory. */
export function containedPath(filename) {
  const p = resolve(join(FILES_DIR, safeFilename(filename)));
  const root = FILES_DIR.endsWith('/') ? FILES_DIR : FILES_DIR + '/';
  if (p !== FILES_DIR && !p.startsWith(root)) throw new Error(`refusing to write outside ${FILES_DIR}`);
  return p;
}

/** Never overwrite. A second file of the same name gets " (2)". */
function uniquePath(p) {
  if (!existsSync(p)) return p;
  const ext = extname(p), stem = p.slice(0, p.length - ext.length);
  for (let i = 2; i < 1000; i++) { const c = `${stem} (${i})${ext}`; if (!existsSync(c)) return c; }
  return `${stem} (${Date.now()})${ext}`;
}

// ── operations ────────────────────────────────────────────────────────

/**
 * Publish a file: bytes to their own hash topic, pointer to the shared one.
 * Bytes first — a pointer that arrives before its bytes are cached invites a
 * fetch that times out, which reads to a user as "the network lost my file".
 */
export async function sendFileBytes(peer, { bytes, filename, mime, shareTopic, region, signWith }) {
  if (bytes.length > MAX_FILE_BYTES) {
    throw new Error(`file is ${(bytes.length / 1048576).toFixed(1)} MB; the reload-safe ceiling is ` +
                    `${(MAX_FILE_BYTES / 1048576).toFixed(0)} MB (above it, a later subscriber cannot reassemble it)`);
  }
  const sha256 = hashBytes(bytes);
  const res = await publishChunkedBytes(peer, bytes, {
    topic: { region, name: fileTopicName(sha256) }, signWith, name: filename, mime,
  });
  const pointer = makePointer({ sha256, filename, bytes: bytes.length, mime, encoding: 'raw' });
  const pointerMsgId = await peer.pub(shareTopic, pointer, { signWith });
  return { sha256, pointer, pointerMsgId, chunks: res?.n ?? null, repaired: res?.repaired ?? 0 };
}

/**
 * List what has been announced on a shared topic. **Reads only.**
 *
 * A bounded window rather than a standing subscription, deliberately: nothing
 * accumulates server-side, there is no buffer to grow, and no path exists by
 * which an arrival can trigger a write. Newest first, deduped by hash —
 * identical content is one file no matter how many times it was announced.
 */
export async function listPointers(peer, shareTopic, { seconds = 15 } = {}) {
  const byHash = new Map();
  const handle = await peer.sub(shareTopic, (env) => {
    const p = readPointer(env?.message);
    if (!p) return;                                   // chat messages, junk, other apps — expected
    const prev = byHash.get(p.sha256);
    if (!prev || (env.ts ?? 0) > (prev.ts ?? 0)) {
      byHash.set(p.sha256, { ...p, msgId: env.msgId, signer: env.signerPubkey ?? null, ts: env.ts ?? null });
    }
  }, { since: 'all' });
  await new Promise((r) => setTimeout(r, Math.max(1, Math.min(60, seconds)) * 1000));
  try { await handle?.stop?.(); } catch { /* */ }
  return [...byHash.values()].sort((a, b) => (b.ts ?? 0) - (a.ts ?? 0));
}

/**
 * Fetch by hash, VERIFY, then write. In that order, and the order is the point.
 *
 * The sha256 is recomputed over what actually reassembled and compared to what
 * was asked for. A mismatch throws before anything reaches disk, so the caller
 * is not trusting the sender, the pointer, or the network — only arithmetic.
 */
export async function fetchFileToDisk(peer, { sha256, region, filename, timeoutMs = 90_000 }) {
  if (!/^[0-9a-f]{64}$/.test(String(sha256 ?? ''))) throw new Error('fetch: expected a lowercase 64-hex sha256');

  const file = await receiveChunkedBytes(peer, { region, name: fileTopicName(sha256) }, { timeoutMs });

  const got = hashBytes(file.bytes);
  if (got !== sha256) {
    throw new Error(`content does not match its address — asked for ${sha256.slice(0, 12)}…, ` +
                    `reassembled ${got.slice(0, 12)}… (${file.bytes.length} bytes). Refusing to write.`);
  }

  const name = safeFilename(filename || file.name || 'file');
  const path = uniquePath(containedPath(name));
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, file.bytes, { mode: 0o600 });   // inert: no execute bit, never opened

  return {
    path, filename: basename(path), bytes: file.bytes.length, sha256,
    mime: file.mime ?? 'application/octet-stream',
    executable: NO_RUN.has(extname(path).toLowerCase()),
  };
}

/** Read a local file for sending. Refuses a directory or an over-size file. */
export async function readLocalFile(path) {
  const p = resolve(String(path));
  const bytes = new Uint8Array(await readFile(p));
  if (bytes.length > MAX_FILE_BYTES) {
    throw new Error(`${basename(p)} is ${(bytes.length / 1048576).toFixed(1)} MB; ceiling is ${(MAX_FILE_BYTES / 1048576).toFixed(0)} MB`);
  }
  return { bytes, filename: basename(p) };
}
