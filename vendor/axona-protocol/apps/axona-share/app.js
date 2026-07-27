// Axona-share — share images over Axona pub/sub. Proof of concept.
// Channels are pub/sub topics; images are compressed to <1MB then sent as a set
// of chunk-messages (@axona/protocol/std/chunk) and reassembled on every subscriber.
import { connectAxona, KERNEL_VERSION, REGION } from './axona.js?v=0.20.0';
// ?v= cache-busts the TRANSITIVE module imports too, not just the app.js entry in
// index.html: a static host serves /std/chunk.js with a ~10-min max-age, so after
// a kernel/std deploy a browser would otherwise keep running the stale chunk lib.
// Bump the token (= APP_VERSION) whenever std/chunk or lib/image changes.
// publishChunkedBytes = the reliable send path (paces + verifies what the mesh
// cached and re-publishes any gaps), so a RELOAD subscriber reassembles without
// the app tuning a throttle. createReassembler stays for the RECEIVE side — a
// channel is a STREAM of images over time (publishChunkedBytes/receiveChunkedBytes
// is one-shot per file), so we keep one persistent reassembler per channel.
import { createReassembler, publishChunkedBytes } from '/std/chunk.js?v=0.20.0';
import { compressImage } from '../lib/image.js?v=0.20.0';

const APP_VERSION = '0.20.0';     // chunks via kernel std/chunk v3.6.0 (reliable publish: verify+repair).
                                  // Transitive module imports are now ?v=-versioned so a std/chunk deploy
                                  // is picked up immediately (was: stale cached chunk lib after deploy).
const DEFAULT_CHANNEL = { id: 'axona-share/public-images', name: 'Public Images' };
const MAX_IMAGE_BYTES = 1_000_000;
const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── state ───────────────────────────────────────────────────────────
let axona = null;
let channels = loadChannels();
let activeId = channels[0].id;
const feeds  = new Map();         // channelId → [{ id, url, mime, caption, ts }]
const seen   = new Map();         // channelId → Set(fileId)   (dedup replay + own echo)
const reasm  = new Map();         // channelId → reassembler
const unread = new Map();         // channelId → count of images arrived while not viewing
let pendingFile = null;           // composer: chosen File/Blob awaiting Share

function loadChannels() {
  try {
    const saved = JSON.parse(localStorage.getItem('axonashare-channels') || '[]');
    const byId = new Map(saved.map((c) => [c.id, c]));
    byId.set(DEFAULT_CHANNEL.id, byId.get(DEFAULT_CHANNEL.id) || DEFAULT_CHANNEL);   // always present
    return [DEFAULT_CHANNEL, ...[...byId.values()].filter((c) => c.id !== DEFAULT_CHANNEL.id)];
  } catch { return [DEFAULT_CHANNEL]; }
}
function saveChannels() {
  localStorage.setItem('axonashare-channels', JSON.stringify(channels.filter((c) => c.id !== DEFAULT_CHANNEL.id)));
}
const feedOf = (id) => { if (!feeds.has(id)) feeds.set(id, []); return feeds.get(id); };
const seenOf = (id) => { if (!seen.has(id)) seen.set(id, new Set()); return seen.get(id); };

// ── incoming: a fully reassembled image for a channel ───────────────
function onImage(channelId, { id, mime, bytes, meta }) {
  const s = seenOf(channelId);
  if (s.has(id)) { console.log('[axona-share] duplicate image ignored', id); return; }
  s.add(id);
  const url = URL.createObjectURL(new Blob([bytes], { type: mime || 'image/jpeg' }));
  feedOf(channelId).push({ id, url, mime, caption: (meta && meta.caption) || '', ts: (meta && meta.ts) || Date.now() });
  console.log(`[axona-share] display image ${id} on ${channelId} (active=${activeId})`);
  if (channelId === activeId) {
    renderFeed();
  } else {
    unread.set(channelId, (unread.get(channelId) || 0) + 1);   // arrived on a channel you're not viewing
    renderChannels();
  }
}

async function subscribeChannel(ch) {
  if (reasm.has(ch.id)) return;
  const r = createReassembler(
    (file) => { console.log(`[axona-share] reassembled ${file.id} · ${file.bytes.length} bytes on ${ch.id}`); onImage(ch.id, file); },
    { onProgress: (p) => console.log(`[axona-share] recv chunk ${p.have}/${p.total} (${p.id}) on ${ch.id}`) },
  );
  reasm.set(ch.id, r);
  try {
    // publishChunkedBytes sends OBJECT messages on the { region, name } topic, so
    // feed env.message straight to the reassembler. Tolerate the legacy JSON-string
    // form too (older app builds wrapped each chunk in JSON.stringify) so a channel
    // still mid-transition reassembles either way.
    await axona.peer.sub(axona.topicOf(ch.id), (env) => {
      if (!env || env.deleted || !env.message) return;
      const m = typeof env.message === 'string'
        ? (() => { try { return JSON.parse(env.message); } catch { return null; } })()
        : env.message;
      if (m) r.accept(m);
    }, { since: 'all' });
    console.log('[axona-share] subscribed', ch.id);
  }
  catch (e) { console.error('[axona-share] subscribe failed', ch.id, e); setStatus('subscribe failed: ' + (e.message || e)); }
}

// ── publish an image to the active channel ──────────────────────────
async function shareImage(file, caption) {
  if (!axona) { setStatus('not connected yet'); return; }
  setStatus('compressing…');
  let blob;
  try { blob = await compressImage(file, { maxBytes: MAX_IMAGE_BYTES }); }
  catch (e) { console.error('[axona-share] compress failed', e); setStatus('image error: ' + (e.message || e)); return; }
  const bytes = new Uint8Array(await blob.arrayBuffer());
  const meta = { caption: caption || '', ts: Date.now() };
  const channelId = activeId;
  // Optimistic local card (instant). The published fileId is added to the seen-set
  // after publish so a self-echo (if the channel returns our own publish) dedups.
  onImage(channelId, { id: 'self-' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), mime: 'image/jpeg', bytes, meta });
  setStatus(`sending ${(bytes.length / 1024).toFixed(0)} KB…`);
  try {
    // RELIABLE publish (std/chunk, kernel ≥3.6.0): paces the chunks and then
    // verifies the mesh cached every one, re-publishing any gaps — so a RELOAD
    // subscriber reassembles. No manual throttle to guess.
    const { fileId, n, repaired } = await publishChunkedBytes(axona.peer, bytes, {
      topic: axona.topicOf(channelId), signWith: axona.author,
      name: file.name || 'image.jpg', mime: 'image/jpeg', meta,
    });
    seenOf(channelId).add(fileId);                  // suppress our own echo
    console.log(`[axona-share] published ${fileId} · ${bytes.length} bytes · ${n} chunk(s)` +
      `${repaired ? ` · repaired ${repaired}` : ''} → ${channelId}`);
    setStatus(`shared ✓${repaired ? ` (repaired ${repaired})` : ''}`);
  } catch (e) { console.error('[axona-share] publish failed', e); setStatus('share failed: ' + (e.message || e)); }
}

// ── channels ────────────────────────────────────────────────────────
async function addChannel(ch, activate = true) {
  if (!channels.find((c) => c.id === ch.id)) { channels.push(ch); saveChannels(); renderChannels(); }
  if (axona) await subscribeChannel(ch);
  if (activate) setActive(ch.id);
}
function createChannel() {
  const name = prompt('Name your new channel:');
  if (!name) return;
  const id = 'axona-share/c/' + Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 6);
  addChannel({ id, name: name.trim() });
}
function joinChannel() {
  const id = prompt('Paste the channel ID to join:');
  if (!id || !id.trim()) return;
  const clean = id.trim();
  addChannel({ id: clean, name: clean.startsWith('axona-share/') ? clean.split('/').pop() : clean });
}
function setActive(id) {
  activeId = id;
  unread.delete(id);                                  // viewing it clears its unread badge
  $('activeName').textContent = (channels.find((c) => c.id === id) || {}).name || id;
  renderChannels(); renderFeed();
  closeSidebarMobile();
}

// ── rendering ───────────────────────────────────────────────────────
function renderChannels() {
  $('channels').innerHTML = channels.map((c) => {
    const u = unread.get(c.id) || 0;
    return `
    <div class="chan ${c.id === activeId ? 'active' : ''}" data-id="${c.id}">
      <span class="chan-name" title="${esc(c.id)}">${esc(c.name)}</span>
      ${u ? `<span class="badge" title="${u} new image(s)">${u}</span>` : ''}
      <button class="icon" data-qr="${esc(c.id)}" title="Show QR — scan to join this channel">QR</button>
      <button class="icon" data-copy="${esc(c.id)}" title="Copy channel ID to share">Copy</button>
    </div>`;
  }).join('');
}
function renderFeed() {
  const items = [...feedOf(activeId)].sort((a, b) => b.ts - a.ts);   // newest first
  $('feed').innerHTML = items.length ? items.map((it) => `
    <div class="card">
      <img src="${it.url}" alt="">
      ${it.caption ? `<div class="cap">${esc(it.caption)}</div>` : ''}
    </div>`).join('') : '<div class="empty">No images yet. Share one above — it goes to everyone in this channel.</div>';
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
function setStatus(m) { $('status').textContent = m; }

// A scannable link that opens the app with this channel pre-joined (id + name).
// Carries the region so the joiner resolves to the SAME keyspace/topic-id as us
// (a channel shared across regions otherwise wouldn't converge).
function joinUrl(ch) {
  return location.origin + location.pathname +
    '?region=' + encodeURIComponent(REGION.token) +
    '&join='   + encodeURIComponent(ch.id) +
    '&name='   + encodeURIComponent(ch.name);
}
function showQR(ch) {
  const u = joinUrl(ch);
  $('qrTitle').textContent = `Scan to join “${ch.name}”`;
  $('qrImg').src = 'https://api.qrserver.com/v1/create-qr-code/?size=260x260&margin=12&data=' + encodeURIComponent(u);
  $('qrUrl').textContent = u; $('qrUrl').href = u;
  $('qrModal').classList.add('show');
}

// composer preview
function setPending(file) {
  pendingFile = file;
  const url = URL.createObjectURL(file);
  $('preview').src = url; $('preview').style.display = 'block';
  $('shareBtn').disabled = false;
  $('composerHint').textContent = file.name || 'image ready';
}
function clearComposer() {
  pendingFile = null;
  $('preview').src = ''; $('preview').style.display = 'none';
  $('caption').value = ''; $('shareBtn').disabled = true;
  $('composerHint').textContent = 'Choose, snap, or drag an image here';
  $('fileInput').value = ''; $('camInput').value = '';
}

// ── mobile sidebar ──────────────────────────────────────────────────
const openSidebar  = () => { $('sidebar').classList.add('open'); $('overlay').classList.add('show'); };
const closeSidebar = () => { $('sidebar').classList.remove('open'); $('overlay').classList.remove('show'); };
const closeSidebarMobile = () => { if (window.matchMedia('(max-width:760px)').matches) closeSidebar(); };

// ── wiring ──────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', async () => {
  $('ver').textContent = `app v${APP_VERSION} · kernel v${KERNEL_VERSION} · region ${REGION.name}`;

  // Joined via a scanned QR / shared link (?join=<id>&name=<name>): add the
  // channel and make it active. If the app was already open in another tab on
  // this device, the storage listener below pulls it in there too.
  const params = new URLSearchParams(location.search);
  const joinId = (params.get('join') || '').trim();
  if (joinId) {
    const name = params.get('name') || (joinId.startsWith('axona-share/') ? joinId.split('/').pop() : joinId);
    if (!channels.find((c) => c.id === joinId)) { channels.push({ id: joinId, name }); saveChannels(); }
    activeId = joinId;
    history.replaceState(null, '', location.pathname);     // clean URL so a refresh doesn't re-join
  }

  renderChannels(); setActive(activeId); clearComposer();

  // Another tab on this device joined/created a channel → reflect it here.
  window.addEventListener('storage', (e) => {
    if (e.key !== 'axonashare-channels') return;
    const before = new Set(channels.map((c) => c.id));
    channels = loadChannels(); renderChannels();
    if (axona) channels.filter((c) => !before.has(c.id)).forEach((c) => subscribeChannel(c));
  });

  $('addChannel').addEventListener('click', () => {
    const j = confirm('OK = create a new channel\nCancel = join an existing one by ID');
    if (j) createChannel(); else joinChannel();
  });
  $('channels').addEventListener('click', (e) => {
    const qr = e.target.closest('[data-qr]');
    if (qr) { const ch = channels.find((c) => c.id === qr.dataset.qr); if (ch) showQR(ch); return; }
    const copy = e.target.closest('[data-copy]');
    if (copy) { navigator.clipboard?.writeText(copy.dataset.copy); setStatus('channel ID copied — send it to a friend'); return; }
    const chan = e.target.closest('.chan'); if (chan) setActive(chan.dataset.id);
  });
  $('qrClose').addEventListener('click', () => $('qrModal').classList.remove('show'));
  $('qrModal').addEventListener('click', (e) => { if (e.target === $('qrModal')) $('qrModal').classList.remove('show'); });

  $('pickBtn').addEventListener('click', () => $('fileInput').click());
  $('camBtn').addEventListener('click', () => $('camInput').click());
  $('fileInput').addEventListener('change', (e) => { if (e.target.files[0]) setPending(e.target.files[0]); });
  $('camInput').addEventListener('change', (e) => { if (e.target.files[0]) setPending(e.target.files[0]); });
  $('shareBtn').addEventListener('click', async () => {
    if (!pendingFile) return;
    const f = pendingFile, cap = $('caption').value;
    $('shareBtn').disabled = true;
    await shareImage(f, cap);
    clearComposer();
  });

  const main = $('main');
  ['dragover', 'dragenter'].forEach((ev) => main.addEventListener(ev, (e) => { e.preventDefault(); main.classList.add('drag'); }));
  ['dragleave', 'drop'].forEach((ev) => main.addEventListener(ev, (e) => { e.preventDefault(); main.classList.remove('drag'); }));
  main.addEventListener('drop', (e) => {
    const f = [...(e.dataTransfer?.files || [])].find((x) => x.type.startsWith('image/'));
    if (f) setPending(f);
  });

  $('menuBtn').addEventListener('click', () => ($('sidebar').classList.contains('open') ? closeSidebar() : openSidebar()));
  $('overlay').addEventListener('click', closeSidebar);

  // connect + subscribe everything
  try {
    axona = await connectAxona(setStatus);
    for (const c of channels) await subscribeChannel(c);
    setStatus('connected — ' + channels.length + ' channel(s)');
  } catch (e) { setStatus('Axona connect failed: ' + (e.message || e)); }
});
