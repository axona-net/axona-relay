// tui.js — a live blessed dashboard rendered from peer.health() + events.
//
// Two presenters share one interface { update(state), logLine(line), destroy() }:
//   • makeDashboard() — full-screen blessed panels (interactive TTY)
//   • makePlainLog()  — timestamped lines (no TTY / RELAY_TUI=0 / systemd)
// index.js picks one based on stdout.isTTY and the RELAY_TUI env.

import blessed from 'blessed';

const short = (hex, head = 8, tail = 4) =>
  (typeof hex === 'string' && hex.length > head + tail + 1)
    ? `${hex.slice(0, head)}…${hex.slice(-tail)}` : String(hex ?? '—');

const fmtDur = (ms) => {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), ss = s % 60;
  return (h ? `${h}h` : '') + (h || m ? `${m}m` : '') + `${ss}s`;
};

const stateColor = (st) =>
  st === 'open' ? '{green-fg}' : st === 'connecting' ? '{yellow-fg}' : '{red-fg}';

// ── Full-screen dashboard ────────────────────────────────────────────
export function makeDashboard({ version, kernelVersion, bridgeUrl, nodeId, region, regionLabel, regionName }) {
  const regionOf = (peerId) => {
    const b = parseInt(String(peerId).slice(0, 2), 16);
    return (Number.isFinite(b) && regionName ? regionName(b) : null) || `0x${String(peerId).slice(0, 2)}`;
  };
  const screen = blessed.screen({ smartCSR: true, title: 'axona-relay', fullUnicode: true });

  const header = blessed.box({
    top: 0, left: 0, width: '100%', height: 6,
    tags: true, border: 'line',
    style: { border: { fg: 'cyan' } },
  });
  const peersBox = blessed.box({
    top: 6, left: 0, width: '50%', bottom: 9,
    label: ' Mesh peers ', tags: true, border: 'line',
    scrollable: true, alwaysScroll: true, scrollbar: { ch: ' ' },
    style: { border: { fg: 'gray' } },
  });
  const statsBox = blessed.box({
    top: 6, left: '50%', width: '50%', height: 11,
    label: ' Status ', tags: true, border: 'line',
    style: { border: { fg: 'gray' } },
  });
  const rolesBox = blessed.box({
    top: 17, left: '50%', width: '50%', bottom: 9,
    label: ' Pub/sub roles (root axon) ', tags: true, border: 'line',
    scrollable: true, alwaysScroll: true,
    style: { border: { fg: 'gray' } },
  });
  const logBox = blessed.log({
    bottom: 0, left: 0, width: '100%', height: 9,
    label: ' Log ', tags: true, border: 'line',
    scrollable: true, scrollback: 500,
    style: { border: { fg: 'gray' } },
  });

  screen.append(header);
  screen.append(peersBox);
  screen.append(statsBox);
  screen.append(rolesBox);
  screen.append(logBox);

  const quit = () => { screen.destroy(); process.emit('relay:quit'); };
  screen.key(['q', 'C-c'], quit);
  screen.render();

  return {
    update(s) {
      const t = s.health.transport || {};
      const bs = t.bridgeState || 'down';
      header.setContent(
        `{bold}{cyan-fg}axona-relay{/} v${version}  ·  kernel v${kernelVersion}  ·  up ${fmtDur(s.uptimeMs)}\n` +
        `node  {bold}${short(nodeId, 10, 6)}{/}   region {bold}${regionLabel}{/} ` +
        `${region.lat.toFixed(2)},${region.lng.toFixed(2)} (0x${s.regionCode})\n` +
        `bridge ${bridgeUrl}   state ${stateColor(bs)}${bs}{/}` +
        (s.health.meshDegraded ? '   {red-fg}{bold}MESH DEGRADED{/}' : ''));

      const peers = s.health.peers || [];
      peersBox.setLabel(` Mesh peers (${peers.length}) `);
      peersBox.setContent(peers.length
        ? peers.map((p, i) => `${String(i + 1).padStart(2)}. {green-fg}${short(p, 12, 6)}{/}  {cyan-fg}${regionOf(p)}{/}`).join('\n')
        : '{gray-fg}— no mesh peers yet —{/}');

      statsBox.setContent(
        `synaptome     {bold}${s.health.synaptomeSize}{/}\n` +
        `mesh channels {bold}${t.meshChannels ?? '—'}{/}  (open ${t.meshOpen ?? '—'})\n` +
        `mesh bound    {bold}${t.meshBound ?? '—'}{/}  {gray-fg}(authenticated)\n` +
        `bound (total) {bold}${t.boundCount ?? '—'}{/}\n` +
        `subscriptions {bold}${s.health.subscriptions}{/}\n` +
        `wire          ${s.health.wireVersion ?? '—'}\n` +
        `started       ${s.health.started ? '{green-fg}yes{/}' : '{red-fg}no{/}'}`);

      const roles = s.health.axonRoles || [];
      rolesBox.setLabel(` Pub/sub roles (${roles.length}) `);
      rolesBox.setContent(roles.length
        ? roles.map(r => `${short(r.topic, 10, 4)} ${r.isRoot ? '{cyan-fg}root{/}' : 'leaf'}  ` +
                         `subs ${r.children}  cache ${r.cacheSize}`).join('\n')
        : '{gray-fg}— not rooting any topics yet —{/}');

      screen.render();
    },
    logLine(line) { logBox.log(line); },
    destroy() { try { screen.destroy(); } catch { /* */ } },
  };
}

// ── Plain log presenter (no TTY) ─────────────────────────────────────
export function makePlainLog({ version, kernelVersion, bridgeUrl, nodeId, region, regionLabel }) {
  const ts = () => new Date().toISOString().slice(11, 19);
  console.log(`[${ts()}] axona-relay v${version} (kernel v${kernelVersion})`);
  console.log(`[${ts()}] node ${nodeId}`);
  console.log(`[${ts()}] region ${regionLabel ?? '?'} (${region.lat},${region.lng})  bridge ${bridgeUrl}`);
  return {
    update(s) {
      const t = s.health.transport || {};
      console.log(
        `[${ts()}] state=${t.bridgeState ?? 'down'} ` +
        `peers=${(s.health.peers || []).length} ` +
        `synaptome=${s.health.synaptomeSize} ` +
        `mesh(open/bound)=${t.meshOpen ?? '—'}/${t.meshBound ?? '—'} ` +
        `roles=${(s.health.axonRoles || []).length} ` +
        `subs=${s.health.subscriptions}` +
        (s.health.meshDegraded ? ' DEGRADED' : ''));
    },
    logLine(line) { console.log(`[${ts()}] ${stripTags(line)}`); },
    destroy() {},
  };
}

// blessed inline tags ({red-fg}, {bold}, {/}) are dashboard-only; strip them
// for the plain-log presenter so lines read cleanly in a terminal / journald.
function stripTags(s) { return String(s).replace(/\{\/?[a-z0-9-]*\}/gi, ''); }
