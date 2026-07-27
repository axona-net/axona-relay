#!/usr/bin/env node
// =====================================================================
// collector.js — serve the bench page (cross-origin isolated) + collect results.
//
//   node bench/pow-wasm/collector.js [--port 8099]
//
// Run it from any machine on the LAN (e.g. your PC), then open
//   http://<this-machine-LAN-IP>:<port>/bench/pow-wasm/
// on each phone, run the benchmark, and put the same URL (minus the path) in the
// page's "collector URL" box. Each device's result POSTs back here, is appended
// to results.jsonl, and a live tally prints to the console.
//
//   • Serves the REPO ROOT so the page's `../../../src/pow/pow.js` import resolves.
//   • Sets COOP/COEP so `crossOriginIsolated` is true → the accurate
//     performance.measureUserAgentSpecificMemory() API and WASM threads work.
//   • Zero dependencies (node:http / node:fs).
//
// Axona dogfood (optional): pipe results.jsonl to a topic via the relay CLI, e.g.
//   tail -f results.jsonl | while read l; do \
//     node ../../../axona-relay/src/cli.js pub "axona/pow-bench" "$l"; done
// and aggregate from any device with `node ../../../axona-relay/src/cli.js sub
// "axona/pow-bench" --for 600`.
// =====================================================================
import { createServer } from 'node:http';
import { readFile, appendFile } from 'node:fs/promises';
import { join, normalize, extname, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = normalize(join(HERE, '..', '..'));        // axona-protocol/ — so /src/... resolves
const RESULTS = join(HERE, 'results.jsonl');
const PORT = Number(process.argv[(process.argv.indexOf('--port') + 1) || -1]) || 8099;

const MIME = {
  '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript',
  '.json': 'application/json', '.wasm': 'application/wasm', '.css': 'text/css',
  '.map': 'application/json',
};

let count = 0;
const tally = new Map();   // device-ish key → latest one-line summary

const server = createServer(async (req, res) => {
  // Cross-origin isolation for the accurate memory API + threads.
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  res.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type');

  if (req.method === 'OPTIONS') { res.writeHead(204).end(); return; }

  if (req.method === 'POST' && req.url === '/submit') {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 1e6) req.destroy(); });
    req.on('end', async () => {
      try {
        const r = JSON.parse(body);
        await appendFile(RESULTS, JSON.stringify(r) + '\n');
        count++;
        const key = `${(r.device?.ua || '?').slice(0, 40)}…`;
        tally.set(key, `${r.candidate} d=${r.difficulty} mint_p50=${r.mint_ms?.p50?.toFixed?.(0)}ms mem=${r.peak_wasm_mem_mb?.toFixed?.(1)}MB oom=${r.oom}`);
        console.log(`\n[#${count}] ${key}\n    ${tally.get(key)}`);
        res.writeHead(200, { 'content-type': 'application/json' }).end('{"ok":true}');
      } catch (e) {
        res.writeHead(400, { 'content-type': 'application/json' }).end(`{"ok":false,"error":${JSON.stringify(String(e.message))}}`);
      }
    });
    return;
  }

  // Static file serving from the repo root (path-traversal guarded).
  const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  let rel = normalize(urlPath).replace(/^(\.\.[/\\])+/, '');
  let abs = join(ROOT, rel);
  if (!abs.startsWith(ROOT)) { res.writeHead(403).end('forbidden'); return; }
  if (urlPath.endsWith('/')) abs = join(abs, 'index.html');
  try {
    const data = await readFile(abs);
    res.writeHead(200, { 'content-type': MIME[extname(abs)] || 'application/octet-stream' }).end(data);
  } catch {
    res.writeHead(404).end('not found');
  }
});

server.listen(PORT, () => {
  console.log(`pow-bench collector on :${PORT}`);
  console.log(`  open  http://<this-machine-LAN-IP>:${PORT}/bench/pow-wasm/  on each device`);
  console.log(`  results → ${RESULTS}  (cross-origin isolated: yes)`);
});
