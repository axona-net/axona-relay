// =====================================================================
// smoke_file_transfer.mjs — the MCP file boundary.
//
// Two distinct jobs:
//
//  A. SCHEMA AGREEMENT. axona-portal implements manifest v1 independently.
//     If these constants drift, a human portal and an agent stop being able to
//     exchange files and NOTHING ERRORS — the pointer simply never validates,
//     or the file topic resolves elsewhere. Pin them here so drift on this side
//     is a test failure rather than a silent incompatibility.
//
//  B. THE TRUST BOUNDARY. A filename arriving from a public topic is hostile
//     text. Anyone can publish to a topic an agent reads, so every one of these
//     inputs is a thing a stranger can actually send, not a hypothetical.
//
// Run: node test/smoke_file_transfer.mjs
// =====================================================================

import {
  MANIFEST_V, TOPIC_PREFIX, ENCODINGS, MAX_FILE_BYTES, FILES_DIR,
  hashBytes, fileTopicName, namespaced, makePointer, readPointer,
  safeFilename, containedPath,
} from '../src/file-transfer.js';
import { basename, sep } from 'node:path';

let n = 0, fail = 0;
const ok = (m, c, extra = '') => {
  if (c) console.log(`  ok ${++n} - ${m}`);
  else { console.log(`  ✗  ${m}${extra ? '  ' + extra : ''}`); fail++; }
};
const throws = (fn) => { try { fn(); return false; } catch { return true; } };
const b = (s) => new TextEncoder().encode(s);

console.log('file-transfer — schema agreement + the trust boundary\n');

// ── A. schema, pinned against axona-portal/src/transfer/manifest.js ──
{
  ok('MANIFEST_V is 1',        MANIFEST_V === 1);
  ok('TOPIC_PREFIX is portal.', TOPIC_PREFIX === 'portal.');
  ok('only raw is encodable',  ENCODINGS.size === 1 && ENCODINGS.has('raw'));
  ok('ceiling is 10 MB',       MAX_FILE_BYTES === 10 * 1024 * 1024);

  const h = hashBytes(b('hello'));
  ok('file topic is portal.f.<sha256>', fileTopicName(h) === `portal.f.${h}`, fileTopicName(h));
  ok('same bytes → same address', hashBytes(b('hello')) === h);
  ok('different bytes → different address', hashBytes(b('hellp')) !== h);

  // The exact pointer field set. An extra or missing key breaks the other side.
  const p = makePointer({ sha256: h, filename: 'a.txt', bytes: 5, mime: 'text/plain' });
  ok('pointer keys are exactly the agreed set',
    Object.keys(p).sort().join(',') === 'bytes,encoding,filename,kind,mime,sha256,v',
    Object.keys(p).sort().join(','));

  // Shared topics must namespace, so an agent and a human portal addressing
  // "axona.bot" land on the SAME topic rather than two that look alike.
  ok('namespaced() adds the prefix',   namespaced('axona.bot') === 'portal.axona.bot');
  ok('namespaced() is idempotent',     namespaced(namespaced('x')) === 'portal.x');
}

// ── B1. hostile filenames ────────────────────────────────────────────
{
  ok('traversal is stripped to one component', safeFilename('../../etc/passwd') === 'passwd',
    safeFilename('../../etc/passwd'));
  ok('absolute path is stripped',              safeFilename('/etc/shadow') === 'shadow');
  ok('windows path is stripped',               !safeFilename('C:\\Windows\\system32\\cmd.exe').includes('\\'));
  ok('a bare .. cannot survive',               safeFilename('..') === 'file', safeFilename('..'));
  ok('a dotfile cannot be created',            !safeFilename('.bashrc').startsWith('.'), safeFilename('.bashrc'));
  ok('empty becomes a name',                   safeFilename('') === 'file');
  ok('whitespace-only becomes a name',         safeFilename('   ') === 'file');
  ok('null/undefined becomes a name',          safeFilename(undefined) === 'file');

  // The NUL truncation trick: a naive writer sees "safe.txt" and the OS sees
  // more. Control characters are removed, not replaced.
  const nul = safeFilename('safe.txt\u0000.sh');
  ok('NUL is removed, not truncated at',       nul === 'safe.txt.sh', nul);
  ok('other control chars are removed',        safeFilename('a\u0007b\u001fc.txt') === 'abc.txt');
  ok('newline is removed',                     safeFilename('a\nb.txt') === 'ab.txt');

  ok('separators are neutralised',             !safeFilename('a/b\\c.txt').match(/[/\\]/));
  ok('long names keep their extension',        safeFilename('x'.repeat(400) + '.pdf').endsWith('.pdf'));
  ok('long names are bounded',                 safeFilename('x'.repeat(400) + '.pdf').length <= 120);
  ok('a normal name survives intact',          safeFilename('Quarterly Report (final).pdf') === 'Quarterly Report (final).pdf');
}

// ── B2. containment — the resolved path never leaves FILES_DIR ───────
{
  for (const hostile of ['../../../etc/passwd', '/etc/passwd', '..\\..\\win.ini', '....//....//x', '/']) {
    const p = containedPath(hostile);
    ok(`contained: ${JSON.stringify(hostile)}`, p.startsWith(FILES_DIR + sep) || p === FILES_DIR, p);
  }
  ok('a plain name lands directly inside', containedPath('a.txt') === `${FILES_DIR}${sep}a.txt`);
  ok('the resolved path has no .. left',   !containedPath('../x').includes('..'));
  ok('basename is preserved for normal input', basename(containedPath('report.pdf')) === 'report.pdf');
}

// ── B3. pointer validation — everything a public topic can deliver ───
{
  const good = makePointer({ sha256: hashBytes(b('z')), filename: 'a', bytes: 1 });
  ok('accepts a valid pointer',   readPointer(good) !== null);
  ok('rejects null',              readPointer(null) === null);
  ok('rejects a bare string',     readPointer('hello') === null);
  ok('rejects an array',          readPointer([1, 2, 3]) === null);
  ok('rejects a std/message',     readPointer({ v: 1, text: 'hi', handle: 'x' }) === null);
  ok('rejects a future version',  readPointer({ ...good, v: 2 }) === null);
  ok('rejects a bad hash',        readPointer({ ...good, sha256: 'xyz' }) === null);
  ok('rejects an UPPERCASE hash', readPointer({ ...good, sha256: good.sha256.toUpperCase() }) === null);
  ok('rejects a negative size',   readPointer({ ...good, bytes: -1 }) === null);
  ok('rejects a float size',      readPointer({ ...good, bytes: 2.5 }) === null);
  ok('rejects an unknown encoding', readPointer({ ...good, encoding: 'zip' }) === null);
  ok('strips unexpected fields',  Object.keys(readPointer({ ...good, evil: 1 })).indexOf('evil') === -1);
  ok('bad sha refused at construction', throws(() => makePointer({ sha256: 'nope', filename: 'a', bytes: 1 })));
}

console.log(`\n${fail === 0 ? '✓' : '✗'} file-transfer: ${n} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
