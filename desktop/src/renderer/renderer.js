// renderer.js — dashboard logic. Talks to main only through window.relay
// (preload). Reads a one-shot snapshot on load, then lives on the pushed
// status/health/log streams.

(function () {
  const $ = (id) => document.getElementById(id);
  const api = window.relay;

  const els = {
    light: $('light'), statusText: $('statusText'),
    netProd: $('netProd'), netTestnet: $('netTestnet'),
    load: $('load'), loadNow: $('loadNow'),
    hBridge: $('hBridge'), hMesh: $('hMesh'), hSyn: $('hSyn'),
    hRoles: $('hRoles'), hSubs: $('hSubs'), hNode: $('hNode'),
    openAtLogin: $('openAtLogin'),
    log: $('log'),
    verRegion: $('verRegion'), verText: $('verText'),
  };

  const cssVar = (n, f) => getComputedStyle(document.documentElement).getPropertyValue(n).trim() || f;
  const spark = window.makeSparkline(els.load, { capacity: 120 });

  function applyStatus(s) {
    if (!s) return;
    els.light.className = 'indicator indicator-' + (s.color || 'red');
    els.statusText.textContent = s.state || '';
    spark.setColor(s.color === 'orange' ? cssVar('--orange', '#e88c20')
                 : s.color === 'red' ? cssVar('--red', '#d05050')
                 : cssVar('--green', '#50c060'));
  }

  function renderHealth(h) {
    if (!h) return;
    const t = h.transport || {};
    els.hBridge.textContent = t.bridgeState ?? '—';
    els.hMesh.textContent = `${t.meshOpen ?? '—'} / ${t.meshBound ?? '—'}`;
    els.hMesh.classList.toggle('degraded', h.meshDegraded === true);
    els.hSyn.textContent = h.synaptomeSize ?? (h.peers?.length ?? '—');
    els.hRoles.textContent = Array.isArray(h.axonRoles) ? h.axonRoles.length : '—';
    els.hSubs.textContent = h.subscriptions ?? '—';
    els.hNode.textContent = h.nodeId ? h.nodeId.slice(0, 14) + '…' : '—';

    const bound = t.meshBound ?? 0;
    els.loadNow.textContent = `${bound} bound`;
    spark.push(bound);
    spark.draw();
  }

  function setNetButtons(network) {
    els.netProd.classList.toggle('active', network === 'prod');
    els.netTestnet.classList.toggle('active', network === 'testnet');
  }

  function renderMeta(meta) {
    if (!meta) return;
    setNetButtons(meta.network);
    els.verRegion.textContent = 'region ' + (meta.region || '—');
    els.verText.textContent =
      `app ${meta.appVersion || '—'} · relay ${meta.relayVersion || '—'} · kernel ${meta.kernelVersion || '—'}`;
  }

  let logCount = 0;
  function appendLog(l) {
    if (!l) return;
    const div = document.createElement('div');
    div.className = 'line ' + (l.level === 'error' ? 'error' : l.level === 'warn' ? 'warn' : '');
    const ctx = l.ctx ? ' ' + JSON.stringify(l.ctx) : '';
    const ts = new Date(l.ts || Date.now()).toLocaleTimeString();
    div.textContent = `${ts}  ${l.event || ''}${ctx}`;
    els.log.appendChild(div);
    if (++logCount > 300) { els.log.removeChild(els.log.firstChild); logCount--; }
    els.log.scrollTop = els.log.scrollHeight;
  }

  async function switchTo(network) {
    setNetButtons(network);
    els.statusText.textContent = 'switching…';
    try { renderMeta(await api.switchNetwork(network)); }
    catch (e) { appendLog({ level: 'error', event: 'switch-failed', ctx: { message: String(e && e.message || e) } }); }
  }

  // ── wire UI ──
  els.netProd.addEventListener('click', () => switchTo('prod'));
  els.netTestnet.addEventListener('click', () => switchTo('testnet'));
  els.openAtLogin.addEventListener('change', async () => {
    els.openAtLogin.checked = await api.setOpenAtLogin(els.openAtLogin.checked);
  });

  // ── streams ──
  api.onStatus(applyStatus);
  api.onHealth(renderHealth);
  api.onLog(appendLog);
  api.onError((e) => appendLog({ level: 'error', event: e && e.code || 'error', ctx: { message: e && e.message } }));

  // ── initial snapshot ──
  (async () => {
    try {
      const [meta, settings, status, health] = await Promise.all([
        api.getMeta(), api.getSettings(), api.getStatus(), api.getHealth(),
      ]);
      renderMeta({ ...meta, appVersion: settings && settings.appVersion });
      if (settings) els.openAtLogin.checked = !!settings.openAtLogin;
      applyStatus(status);
      renderHealth(health);
    } catch (e) {
      appendLog({ level: 'error', event: 'init-failed', ctx: { message: String(e && e.message || e) } });
    }
  })();
})();
