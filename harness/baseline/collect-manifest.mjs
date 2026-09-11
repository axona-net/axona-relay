#!/usr/bin/env node
// =============================================================================
// collect-manifest.mjs — Task 1 (AX-T1-D1 §5, D2-DISPATCH-01 T1.1) baseline
// manifest collector. READ-ONLY. It runs git/ps/ssh/curl and writes JSON; it
// never builds, launches, restarts, deploys or writes outside its output dir.
//
// What it answers, per component and per host, keeping FOUR identities apart
// because they have disagreed with each other on this fleet before:
//
//   checkout        what `git` says is on disk here
//   builtArtifact   what a consumer actually bundles (vendor/, node_modules/)
//   runningProcess  what a live process loaded, from ITS OWN start banner
//   remoteReport    what a service says about itself over the network
//
// Every field is a value, or {status:"not_observable"|"missing"|"inferred",
// reason}. Unknown is never healthy and never zero.
//
//   node harness/baseline/collect-manifest.mjs            # full run
//   node harness/baseline/collect-manifest.mjs --local    # no ssh/curl
//   OUT=/path node harness/baseline/collect-manifest.mjs  # output dir override
//
// Secrets: no environment dump. The only configuration copied is the explicit
// allowlist in CONFIG_ALLOWLIST. Diffs are never copied, only counts and names.
// =============================================================================
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { hostname, loadavg, platform, release, arch, totalmem, uptime } from 'node:os';

const HERE = dirname(fileURLToPath(import.meta.url));
const RELAY = resolve(HERE, '../..');            // axona-relay
const WS = resolve(RELAY, '..');                 // workspace root
const LOCAL_ONLY = process.argv.includes('--local');
const T0 = new Date();
const DAY = T0.toISOString().slice(0, 10);
const OUT = process.env.OUT || join(HERE, 'manifest', DAY);
const SCHEMA = 'axona-baseline-manifest/0.1';

const NOT = (reason, status = 'not_observable') => ({ status, reason });
const commands = [];                             // every external command, for the note
// Both repos are PUBLIC. Nothing that names a person or a machine leaves this
// script: workspace paths become <ws>, any other home directory becomes <home>,
// and the collector's hostname is reported as a hash. Remote hosts are named by
// their ssh alias only.
const redact = (s) => s.replace(new RegExp(WS.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), '<ws>').replace(/(\/Users|\/home|\/c\/Users)\/[^/\s'"]+/g, '<home>');
function sh(file, args, { cwd, timeout = 60_000, input } = {}) {
  const line = [file, ...args].join(' ');
  commands.push({ at: new Date().toISOString(), cwd: cwd ? redact(cwd) : undefined, cmd: redact(line).slice(0, 400) });
  try {
    // maxBuffer: the default 1 MiB silently fails on the 1.27 MB chat bundle
    const out = execFileSync(file, args, { cwd, timeout, input, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: 16 * 1024 * 1024 });
    // PowerShell emits CRLF; a stray \r stops `.` in a JS regex, and only the
    // final (unterminated) Windows process row parsed on the first run.
    return { ok: true, out: out.replace(/\r/g, '').trimEnd() };
  } catch (e) {
    return { ok: false, out: (e.stdout || '').toString().trimEnd(), err: (e.stderr || e.message || '').toString().trim().slice(0, 300) };
  }
}
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');
const fileSha = (p) => existsSync(p) ? sha256(readFileSync(p)) : NOT('file absent', 'missing');
const readJson = (p) => existsSync(p) ? JSON.parse(readFileSync(p, 'utf8')) : null;
const kernelConst = (dir) => {
  const p = join(dir, 'src/transport/handshake.js');
  if (!existsSync(p)) return NOT('handshake.js absent', 'missing');
  const m = readFileSync(p, 'utf8').match(/KERNEL_VERSION\s*=\s*'([0-9.]+)'/);
  return m ? m[1] : NOT('KERNEL_VERSION constant not found');
};
// elapsed "[[dd-]hh:]mm:ss" → seconds; used so a start time never depends on a host's local clock/tz
function etimeToSec(s) {
  s = s.trim(); let d = 0;
  if (s.includes('-')) { const [dd, rest] = s.split('-'); d = +dd; s = rest; }
  const parts = s.split(':').map(Number);
  while (parts.length < 3) parts.unshift(0);
  return d * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2];
}
const startFromEtime = (sampledAt, etime) => new Date(sampledAt.getTime() - etimeToSec(etime) * 1000).toISOString();

// ── checkout identity ────────────────────────────────────────────────────────
function checkout(dir) {
  if (!existsSync(join(dir, '.git'))) return NOT('no .git at ' + dir.replace(WS, '<ws>'), 'missing');
  const g = (...a) => sh('git', a, { cwd: dir });
  const head = g('log', '-1', '--format=%H%n%cI');
  const [commit, headDate] = head.ok ? head.out.split('\n') : [];
  const st = g('status', '--porcelain');
  const lines = st.ok && st.out ? st.out.split('\n') : [];
  const tracked = lines.filter(l => !l.startsWith('??')).map(l => l.slice(3));
  const untracked = lines.filter(l => l.startsWith('??')).map(l => l.slice(3));
  const ab = g('rev-list', '--left-right', '--count', 'HEAD...@{u}');
  const tags = g('describe', '--tags', '--exact-match', 'HEAD');
  const pkg = readJson(join(dir, 'package.json'));
  const lock = ['package-lock.json', 'pnpm-lock.yaml', 'yarn.lock'].find(f => existsSync(join(dir, f)));
  return {
    path: dir.replace(WS, '<ws>'),
    commit: commit || NOT('git log failed: ' + head.err),
    headDate: headDate || NOT('git log failed'),
    branch: g('rev-parse', '--abbrev-ref', 'HEAD').out,
    remote: g('remote', 'get-url', 'origin').ok ? sh('git', ['remote', 'get-url', 'origin'], { cwd: dir }).out : NOT('no origin remote', 'missing'),
    aheadBehindUpstream: ab.ok ? { ahead: +ab.out.split('\t')[0], behind: +ab.out.split('\t')[1] } : NOT('no upstream'),
    exactTag: tags.ok ? tags.out : null,
    dirty: { trackedModified: tracked.length, trackedFiles: tracked, untracked: untracked.length, untrackedNames: untracked },
    packageName: pkg?.name ?? null, packageVersion: pkg?.version ?? null,
    lockfile: lock ? { name: lock, sha256: fileSha(join(dir, lock)) } : NOT('no lockfile', 'missing'),
  };
}
// count files that differ between two src trees (never copies content)
function treeDiffCount(a, b) {
  const r = sh('diff', ['-rq', a, b]);
  if (r.ok) return 0;
  if (r.out) return r.out.split('\n').filter(Boolean).length;
  return NOT('diff failed: ' + r.err);
}
function consumerKernel(dir) {
  const pkg = readJson(join(dir, 'package.json'));
  const spec = pkg?.dependencies?.['@axona/protocol'] ?? null;
  const inst = join(dir, 'node_modules/@axona/protocol');
  return {
    dependencySpec: spec ?? NOT('no @axona/protocol dependency', 'missing'),
    installedVersion: readJson(join(inst, 'package.json'))?.version ?? NOT('not installed', 'missing'),
    installedKernelConst: kernelConst(inst),
  };
}

// ── local running processes: MCP peers (full nodes pinned at start) ──────────
function localMcpProcesses(vendorBumps) {
  const ps = sh('ps', ['-axo', 'pid=,ppid=,etime=,rss=,args=']);
  if (!ps.ok) return NOT('ps failed');
  const now = new Date();
  return ps.out.split('\n').filter(l => /axona-relay\/src\/mcp\.js/.test(l)).map(l => {
    const m = l.trim().match(/^(\d+)\s+(\d+)\s+(\S+)\s+(\d+)\s+(.*)$/);
    const [, pid, ppid, etime, rss] = m;
    const parent = sh('ps', ['-o', 'comm=', '-p', ppid]).out.split('/').pop();
    const startedAt = startFromEtime(now, etime);
    // The kernel a long-lived MCP peer runs is whatever vendor/ held when it
    // STARTED, not what the checkout holds now. Read off the vendor bump history;
    // that is an inference from git dates, so it is labelled one.
    const bump = vendorBumps.find(b => b.date <= startedAt);
    return {
      pid: +pid, parentPid: +ppid, parentApp: parent, startedAt, rssKB: +rss,
      kernelAtStart: bump ? { status: 'inferred', value: bump.kernel, reason: `vendor bump ${bump.commit} at ${bump.date} precedes process start` } : NOT('no vendor bump precedes start'),
      note: parent === 'launchd' ? 'orphan: parent is launchd, no live app' : undefined,
    };
  });
}
function vendorBumpHistory() {
  const r = sh('git', ['log', '--format=%h %cI %s', '--', 'vendor/axona-protocol/package.json'], { cwd: RELAY });
  if (!r.ok) return [];
  return r.out.split('\n').map(l => {
    const m = l.match(/^(\w+) (\S+) .*?kernel ([0-9.]+)/);
    return m ? { commit: m[1], date: new Date(m[2]).toISOString(), kernel: m[3] } : null;
  }).filter(Boolean);
}

// ── remote reports ───────────────────────────────────────────────────────────
function healthz(host) {
  const r = sh('curl', ['-sS', '-m', '10', `https://${host}/healthz`]);
  if (!r.ok) return NOT(`curl failed: ${r.err}`);
  try { const j = JSON.parse(r.out); return { version: j.version, kernelVersion: j.kernelVersion, status: j.status, observedAt: new Date().toISOString() }; }
  catch { return NOT('non-JSON healthz body'); }
}
function chatDeployed() {
  const idx = sh('curl', ['-sS', '-m', '10', 'https://axona.chat/']);
  if (!idx.ok) return NOT('index fetch failed: ' + idx.err);
  const asset = idx.out.match(/assets\/index-[^"]+\.js/)?.[0];
  if (!asset) return NOT('no index-*.js asset in index.html');
  const js = sh('curl', ['-sS', '-m', '30', `https://axona.chat/${asset}`], { timeout: 40_000 });
  if (!js.ok) return NOT('bundle fetch failed');
  const lits = {};
  for (const m of js.out.matchAll(/\b(4\.\d{2}\.\d+|0\.\d{2}\.\d+)\b/g)) lits[m[1]] = (lits[m[1]] || 0) + 1;
  return { asset, bundleSha256: sha256(js.out), bundleBytes: Buffer.byteLength(js.out, 'utf8'), versionLiterals: lits,
    note: 'literals are strings found in the minified bundle; the KERNEL_VERSION export is a getter, so the constant is attributed by literal count, not by symbol' };
}
function bridgeContainers(label, sshArgs, dir) {
  const script = `cd ${dir} 2>/dev/null || { echo NODIR; exit 0; }; echo HEAD=$(git rev-parse HEAD); echo DIRTY=$(git status --porcelain | wc -l); for c in $(docker ps -q); do docker inspect -f '{{.Name}} {{.Config.Image}} {{.Image}} {{.State.StartedAt}}' $c; done`;
  const r = sh('ssh', ['-o', 'ConnectTimeout=20', ...sshArgs, script], { timeout: 45_000 });
  if (!r.ok) return NOT(`ssh ${label} failed: ${r.err}`);
  if (r.out.startsWith('NODIR')) return NOT(`${dir} absent on ${label}`, 'missing');
  const o = { checkoutCommit: r.out.match(/HEAD=(\w+)/)?.[1], dirtyEntries: +(r.out.match(/DIRTY=(\d+)/)?.[1] ?? -1), containers: [] };
  for (const l of r.out.split('\n')) { const m = l.match(/^\/(\S+) (\S+) (\S+) (\S+)$/); if (m) o.containers.push({ name: m[1], image: m[2], imageId: m[3], startedAt: m[4] }); }
  return o;
}

// ── fleet census (imports relay-census.sh, the single count definition) ──────
const HOSTS = [   // mirrors ops/fleet.sh HOSTS + DROPLETS; keep in step by hand.
  // Repo paths are ~-relative on purpose: the ssh alias resolves the account,
  // so no host's username has to appear in a public file.
  { host: 'air', flavour: 'mac', repo: '~/Documents/claude/axona-relay', target: 6, region: 'eagle' },
  { host: 'm1', flavour: 'mac', repo: '~/Documents/claude/axona-relay', target: 8, region: 'eagle' },
  { host: 'axona-linux', flavour: 'linux', repo: '~/Documents/claude/axona-relay', target: 5, region: 'eagle' },
  { host: 'axona-win', flavour: 'win', repo: '~/github/axona-relay', target: 20, region: 'eagle' },
  { host: '143.110.224.247', flavour: 'droplet', repo: '/opt/axona-relay', target: 3, region: 'useast/uswest=eagle, grizzly1=grizzly' },
  { host: '167.71.106.63', flavour: 'droplet', repo: '/opt/axona-relay', target: 3, region: 'useast/uswest=eagle, grizzly1=grizzly' },
  { host: '159.203.46.28', flavour: 'droplet', repo: '/opt/axona-relay', target: 3, region: 'useast/uswest=eagle, grizzly1=grizzly' },
];
const DKEY = join(process.env.HOME, '.ssh/id_ed25519_axona');
const WINBASH = 'C:\\Program Files\\Git\\bin\\bash.exe';

const POSIX_SCRIPT = (repo) => `cd ${repo} || { echo NODIR; exit 0; }
echo HEAD=$(git rev-parse --short HEAD); echo DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
echo RELAY=$(grep -m1 '"version"' package.json | tr -dc '0-9.'); echo VENDOR=$(grep -m1 '"version"' vendor/axona-protocol/package.json | tr -dc '0-9.')
echo LOAD=$(uptime | sed 's/.*load average[s]*: *//')
echo COUNT=$(bash relay-census.sh count)
bash relay-census.sh --kernels | sed 's/^/KERNELS /'
for p in $(bash relay-census.sh --pids); do echo PID $(ps -o pid=,etime=,rss= -p $p); done`;

const DROPLET_SCRIPT = `cd /opt/axona-relay || { echo NODIR; exit 0; }
echo HEAD=$(git rev-parse --short HEAD); echo DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
echo RELAY=$(grep -m1 '"version"' package.json | tr -dc '0-9.'); echo VENDOR=$(grep -m1 '"version"' vendor/axona-protocol/package.json | tr -dc '0-9.')
echo LOAD=$(uptime | sed 's/.*load average: *//'); echo MEM=$(free -m | awk 'NR==2{print $2" "$3" "$7}')
echo COUNT=$(systemctl list-units 'axona-relay@*' --state=running --no-legend | wc -l)
for u in $(systemctl list-units 'axona-relay@*' --state=running --no-legend | awk '{print $1}'); do
  pid=$(systemctl show $u -p MainPID --value); since=$(systemctl show $u -p ActiveEnterTimestamp --value)
  reg=$(systemctl show $u -p Environment --value | tr ' ' '\\n' | grep '^RELAY_REGION=' | cut -d= -f2)
  k=$( { journalctl -u $u --no-pager --since "$since" 2>/dev/null | grep -a -m1 -oE 'kernel v[0-9.]+'; } || true)
  echo "UNIT $u pid=$pid etime=$(ps -o etime= -p $pid | tr -d ' ') rss=$(ps -o rss= -p $pid | tr -d ' ') region=$reg banner=[$k]"
done`;

// Windows: no /proc, no lsof, stdout is buffered until the ssh exits, tasklist
// counts every node.exe. Process start comes from CIM (host-local clock, tz recorded).
const WIN_SCRIPT = `cd ~/github/axona-relay || { echo NODIR; exit 0; }
echo HEAD=$(git rev-parse --short HEAD); echo DIRTY=$(git status --porcelain | wc -l | tr -d ' ')
echo RELAY=$(grep -m1 '"version"' package.json | tr -dc '0-9.'); echo VENDOR=$(grep -m1 '"version"' vendor/axona-protocol/package.json | tr -dc '0-9.')
echo COUNT=$(bash relay-census.sh count)
bash relay-census.sh --kernels | sed 's/^/KERNELS /'
echo TZ=$(powershell -NoProfile -Command "Get-Date -Format o")
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \\"name='node.exe'\\" | Sort-Object CreationDate | ForEach-Object { 'PROC {0} {1} {2} {3}' -f \\$_.ProcessId, \\$_.CreationDate.ToString('yyyy-MM-ddTHH:mm:ss'), [int](\\$_.WorkingSetSize/1KB), \\$_.CommandLine }"`;

function censusHost(h) {
  const sampledAt = new Date();
  let r;
  if (h.flavour === 'droplet') r = sh('ssh', ['-o', 'ConnectTimeout=25', '-i', DKEY, `root@${h.host}`, DROPLET_SCRIPT], { timeout: 90_000 });
  else if (h.flavour === 'win') r = sh('ssh', ['-o', 'ConnectTimeout=20', h.host, `"${WINBASH}" -l -s`], { input: WIN_SCRIPT, timeout: 120_000 });
  else r = sh('ssh', ['-o', 'ConnectTimeout=20', h.host, 'bash -l -s'], { input: POSIX_SCRIPT(h.repo), timeout: 90_000 });
  const rec = { host: h.host, flavour: h.flavour, regionConfig: h.region, sampledAt: sampledAt.toISOString(), targetRelays: h.target };
  if (!r.ok && !r.out) return { ...rec, reachable: false, observedRelays: NOT('ssh failed: ' + r.err), error: r.err };
  if (r.out.startsWith('NODIR')) return { ...rec, reachable: true, observedRelays: NOT('repo dir absent', 'missing') };
  const get = (k) => r.out.match(new RegExp(`^${k}=(.*)$`, 'm'))?.[1]?.trim();
  rec.reachable = true;
  rec.checkoutCommit = get('HEAD'); rec.dirtyEntries = +get('DIRTY');
  rec.relayPackageVersion = get('RELAY'); rec.vendorKernelVersion = get('VENDOR');
  rec.load = get('LOAD') ?? NOT('uptime not collected on this platform');
  if (get('MEM')) { const [t, u, a] = get('MEM').split(' ').map(Number); rec.memoryMB = { total: t, used: u, available: a }; }
  rec.observedRelays = +get('COUNT');
  rec.runningKernels = r.out.split('\n').filter(l => l.startsWith('KERNELS ')).map(l => l.slice(8).trim());
  rec.processes = [];
  for (const l of r.out.split('\n')) {
    let m;
    if ((m = l.match(/^PID\s+(\d+)\s+(\S+)\s+(\d+)/))) rec.processes.push({ pid: +m[1], startedAt: startFromEtime(sampledAt, m[2]), rssKB: +m[3] });
    else if ((m = l.match(/^UNIT (\S+) pid=(\d+) etime=(\S+) rss=(\d+) region=(\S*) banner=\[(.*)\]/)))
      rec.processes.push({ unit: m[1], pid: +m[2], startedAt: startFromEtime(sampledAt, m[3]), rssKB: +m[4], region: m[5], bannerKernel: m[6].replace('kernel v', '') || NOT('no banner in journal since unit start') });
    else if ((m = l.match(/^PROC (\d+) (\S+) (\d+) (.*)$/)))
      rec.processes.push({ pid: +m[1], startedAtHostLocal: m[2], hostClock: get('TZ'), rssKB: +m[3], commandLine: m[4].trim(), bannerKernel: NOT('Windows: stdout fd not resolvable from git-bash; banner not read') });
  }
  if (h.flavour === 'win') rec.note = 'tasklist counts EVERY node.exe; commandLine per process shows whether each is src/index.js. '
    + 'relay-census.sh --kernels on Windows is a PROXY that reads relay-logs/roll-*.log generations; current logs are named win-*.log, so the proxy reports a stale generation. Finding against the tool, not corrected here.';
  if (rec.observedRelays !== h.target) rec.finding = `observed ${rec.observedRelays} vs target ${h.target}`;
  return rec;
}

// ── assemble ─────────────────────────────────────────────────────────────────
const K = join(WS, 'axona-protocol'), R = RELAY, C = join(WS, 'axona-chat'), B = join(WS, 'axona-bridge'), D = join(WS, 'axona-docs'), M = join(WS, 'axona-mcp'), WEB = join(WS, 'axona-web');
const vendorBumps = vendorBumpHistory();
const mcpJson = readJson(join(WS, '.mcp.json'));
const CONFIG_ALLOWLIST = ['BRIDGE_URL', 'MCP_REGION', 'RELAY_NETWORK', 'MCP_STANDING_WATCHES', 'MCP_HANDLE'];
const mcpServers = mcpJson ? Object.fromEntries(Object.entries(mcpJson.mcpServers).map(([n, s]) => [n, {
  entry: s.args?.[0]?.replace(WS, '<ws>'), env: Object.fromEntries(Object.entries(s.env || {}).filter(([k]) => CONFIG_ALLOWLIST.includes(k))),
  envKeysWithheld: Object.keys(s.env || {}).filter(k => !CONFIG_ALLOWLIST.includes(k)) }])) : NOT('.mcp.json absent', 'missing');

const manifest = {
  schema: SCHEMA, task: 'AX-T1-D1 §5 / AX-T1-D2-DISPATCH-01 T1.1', collectedAt: T0.toISOString(),
  collector: { script: 'harness/baseline/collect-manifest.mjs', scriptSha256: sha256(readFileSync(fileURLToPath(import.meta.url))), hostSha256_12: sha256(hostname()).slice(0, 12), os: `${platform()} ${release()} ${arch()}`, node: process.version, localOnly: LOCAL_ONLY },
  components: {
    kernel: { repo: 'axona-protocol', note: 'the kernel package IS the SDK (@axona/protocol); no separate SDK artifact exists',
      checkout: checkout(K), kernelConst: kernelConst(K),
      adce81cResolves: sh('git', ['rev-parse', '--verify', '-q', 'adce81c^{commit}'], { cwd: K }).out || NOT('adce81c not in this repo', 'missing') },
    relay: { repo: 'axona-relay', checkout: checkout(R),
      builtArtifact: { vendorKernelVersion: readJson(join(R, 'vendor/axona-protocol/package.json'))?.version, vendorKernelConst: kernelConst(join(R, 'vendor/axona-protocol')),
        vendorSrcFilesDifferingFromKernelCheckout: treeDiffCount(join(K, 'src'), join(R, 'vendor/axona-protocol/src')), vendorBumpHistory: vendorBumps.slice(0, 6) } },
    mcp: { repo: 'axona-relay (src/mcp.js, src/mcp-session.js)', note: 'the live MCP is the relay repo; <ws>/axona-mcp is a July checkout with no remote and is NOT what runs',
      staleRepoCheckout: checkout(M), servers: mcpServers,
      runningProcess: LOCAL_ONLY ? NOT('--local') : localMcpProcesses(vendorBumps) },
    chat: { repo: 'axona-chat', checkout: checkout(C), builtArtifact: consumerKernel(C), remoteReport: LOCAL_ONLY ? NOT('--local') : chatDeployed() },
    bridge: { repo: 'axona-bridge', checkout: checkout(B), builtArtifact: consumerKernel(B),
      remoteReport: LOCAL_ONLY ? NOT('--local') : { east: healthz('bridge.axona.net'), west: healthz('bridge-west.axona.net') },
      runningProcess: LOCAL_ONLY ? NOT('--local') : { east: bridgeContainers('east', ['axona-bridge'], '/opt/axona-bridge'), west: bridgeContainers('west', ['-i', DKEY, 'root@24.199.98.119'], '/opt/axona-bridge-docker') },
      configNote: 'bridge .env (STRICT_MIN_KERNEL, HEALTHZ_TOKEN, TURN secret) is unversioned and not copied; STRICT_MIN_KERNEL=4.84.0 set by hand on both 2026-09-10 per ops/STATE.md' },
    web: { repo: 'axona-web', checkout: checkout(WEB) },
    docs: { repo: 'axona-docs', checkout: checkout(D) },
  },
  localHost: { hostSha256_12: sha256(hostname()).slice(0, 12), load1_5_15: loadavg(), uptimeSec: Math.round(uptime()), totalMemMB: Math.round(totalmem() / 1048576) },
  fleet: LOCAL_ONLY ? NOT('--local') : HOSTS.map(censusHost),
  unknowns: [
    'running-artifact identity: a start banner proves what a process LOADED at start, not that its files are unchanged since; no in-process attestation exists',
    'Windows relays: per-process kernel banner not readable from git-bash; SIGUSR1 health-dump has no Windows path; count is every node.exe',
    'droplet relays: kernel read from journald since unit ActiveEnterTimestamp; if the journal rotated, banner is not_observable',
    'MCP peers: kernel-at-start is INFERRED from vendor bump dates, not read from the process',
    'storm-window comparability: no retained per-host time series before 2026-09-09; Task 8a owns this',
    'soak contamination: axona-stress/soak-v3.sh was running on axona-linux against prod during collection (~1.8 fresh nodes/min)',
    'bridge .env and droplet region drop-ins are host-local configuration; drop-ins are versioned at axona-relay/deploy/droplet, .env is not',
  ],
  collectionCommands: commands,
  finishedAt: null,
};
manifest.finishedAt = new Date().toISOString();
manifest.durationSec = Math.round((new Date(manifest.finishedAt) - T0) / 1000);

mkdirSync(OUT, { recursive: true });
const mf = join(OUT, 'baseline-manifest.json');
writeFileSync(mf, JSON.stringify(manifest, null, 2) + '\n');
if (!LOCAL_ONLY) writeFileSync(join(OUT, 'fleet-census.jsonl'), manifest.fleet.map(h => JSON.stringify(h)).join('\n') + '\n');
const summary = {
  out: OUT.replace(WS, '<ws>'), manifestSha256: sha256(readFileSync(mf)), durationSec: manifest.durationSec,
  fleet: LOCAL_ONLY ? 'skipped' : manifest.fleet.map(h => `${h.host}:${h.reachable ? h.observedRelays : 'UNREACHABLE'}/${h.targetRelays}${h.finding ? ' !' : ''}`).join(' '),
};
console.log(JSON.stringify(summary, null, 2));
