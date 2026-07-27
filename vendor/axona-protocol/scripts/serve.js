#!/usr/bin/env node
// Minimal static file server for local @axona/protocol development.
// Serves the parent directory (axona-protocol/) so src/ + examples/ load
// from the browser.  Used by .claude/launch.json to drive the preview pane
// for examples/minimal-pubsub-browser/index.html.
//
// Usage: node scripts/serve.js [port]
//
// Port defaults to 8765 (matches launch.json).

import http        from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { fileURLToPath }  from 'node:url';
import { dirname, join, resolve, normalize } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT      = resolve(__dirname, '..');
const PORT      = Number(process.argv[2] ?? 8765);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.mjs':  'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg':  'image/svg+xml',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.ico':  'image/x-icon',
  '.map':  'application/json; charset=utf-8',
};

function ext(p) {
  const i = p.lastIndexOf('.');
  return i >= 0 ? p.slice(i).toLowerCase() : '';
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    let pathname = decodeURIComponent(url.pathname);
    // Root redirects to the browser demo.
    if (pathname === '/') pathname = '/index.html';
    if (pathname.endsWith('/')) pathname += 'index.html';
    const filePath = normalize(join(ROOT, pathname));
    if (!filePath.startsWith(ROOT)) {                 // path-traversal guard
      res.writeHead(403); res.end('forbidden'); return;
    }
    const s = await stat(filePath);
    if (s.isDirectory()) {
      res.writeHead(404); res.end('not found'); return;
    }
    const body = await readFile(filePath);
    res.writeHead(200, {
      'Content-Type':  MIME[ext(filePath)] ?? 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    res.end(body);
  } catch (err) {
    if (err.code === 'ENOENT') { res.writeHead(404); res.end('not found'); }
    else                       { res.writeHead(500); res.end('server error: ' + err.message); }
  }
});

server.listen(PORT, () => {
  console.log(`axona-protocol static: http://localhost:${PORT}`);
});
