// Axona Minimal — the smallest useful Axona app: publish to a topic, subscribe
// to a topic, show what arrives. ~70 lines, no framework. This is the artifact
// the programmer-intro talk builds.

// ?v= cache-busts these module URLs (GitHub Pages serves them max-age=600); bump
// the token (= APP_VERSION) each release to pull fresh kernel exports on reload.
// (Deeper kernel internals refresh on the Pages cache expiry — query strings
// can't bust an unbundled module's transitive imports.)
import { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity, createAuthorIdentity, deriveTopicId, KERNEL_VERSION } from '/src/index.js?v=0.23.0';
import { webTransport } from '/src/transport/web/index.js?v=0.23.0';
import { regionName }   from '/src/utils/region-names.js?v=0.23.0';
import { resolveAnchor } from '../lib/region.js?v=0.23.0';
import { makeMessage, readMessage } from '/std/message.js?v=0.23.0';

const $ = (id) => document.getElementById(id);
const status = (t) => { $('status').textContent = t; };

// The TOPIC is rooted at a fixed region (us-east), so every participant derives
// the SAME topic-id no matter where they are. The user's OWN identity, by
// contrast, is rooted at their REAL location (see whereAmI below) — so a message
// shows where its sender actually sits, while the topic stays one shared keyspace.
const APP_VERSION = '0.23.0';
// Bridge selection (same as axona-share): ?bridge=<wss url> → ?net=testnet|prod
// shortcut → default by hostname. Lets one build run against either network.
const KNOWN_BRIDGES = { prod: 'wss://bridge.axona.net', testnet: 'wss://testnet.axona.net' };
const _params = new URLSearchParams(location.search);
function resolveBridge() {
  const explicit = (_params.get('bridge') || '').trim();
  if (/^wss?:\/\//.test(explicit)) return explicit;
  const net = (_params.get('net') || '').trim().toLowerCase();
  if (KNOWN_BRIDGES[net]) return KNOWN_BRIDGES[net];
  return location.hostname.includes('testnet') ? KNOWN_BRIDGES.testnet : KNOWN_BRIDGES.prod;
}
const BRIDGE = resolveBridge();
const NETWORK = BRIDGE === KNOWN_BRIDGES.testnet ? 'testnet' : BRIDGE === KNOWN_BRIDGES.prod ? 'prod' : 'custom';
const ANCHOR = resolveAnchor({ search: '', fallback: 'useast' });   // topic region — pinned to us-east
const TOPIC_REGION = ANCHOR.token;                                   // the `region` field of every { region, name }

let peer, node$identity, author, currentTopic = null, currentSub = null;
let myClass = 'unstated';     // our OWN declared author-class — stays 'unstated' until the user opts in

// Durable AUTHOR identity (v0.3 key separation): the node identity authenticates the
// connection and is ephemeral; this AUTHOR key signs your posts and is persisted
// (persistAs) so authorship is stable across reloads. Every publish names it via
// { signWith: author }. It has NO location/node-id — authorship is not a place.
async function loadOrCreateAuthor() {
  return createAuthorIdentity({ persistAs: 'axona-minimal:author' });   // load-or-create against localStorage
}
const seen = new Set();                                  // msgIds we've already shown (dedup own echo)

// "region:userID" read off a 264-bit NODE id: top byte = the sender's S2 region
// cell, the rest = SHA-256(pubkey).
//
// NOTE: the protocol does NOT reveal a publisher's location — a signed envelope
// carries only signerPubkey/the Author ID (who signed), never the node-id's S2
// region (where they are), and the region can't be derived from the author key.
// That's a deliberate publisher-privacy property. THIS APP opts in: it chooses to
// share the sender's region by putting its own NODE id in the message payload
// below. That's an application-layer example of voluntary location disclosure,
// not a protocol feature. (Anonymous posts carry nothing → 'anon'.)
const idLabel = (nodeId) =>
  (typeof nodeId === 'string' && nodeId.length >= 10)
    ? `${regionName(parseInt(nodeId.slice(0, 2), 16))}:${nodeId.slice(2, 10)}`
    : 'anon';

// Real geolocation → the user's actual S2 cell. Denied / unavailable → us-east.
function whereAmI() {
  const fallback = { lat: ANCHOR.center.lat, lng: ANCHOR.center.lng };
  if (!navigator.geolocation) return Promise.resolve(fallback);
  return new Promise((resolve) => {
    navigator.geolocation.getCurrentPosition(
      (p) => resolve({ lat: p.coords.latitude, lng: p.coords.longitude }),
      ()  => resolve(fallback),                          // permission denied → default region
      { timeout: 8000, maximumAge: 600000 },
    );
  });
}

// Connect: locate the user (real S2 cell → their node-id), open the web
// transport, build the peer, wait briefly for the mesh to form.
async function connect() {
  status('locating…');
  const here = await whereAmI();
  status('connecting…');
  node$identity   = await createNodeIdentity({ lat: here.lat, lng: here.lng });       // ephemeral connection key
  author          = await loadOrCreateAuthor();                                       // durable author key
  const transport = webTransport({ bridgeUrl: BRIDGE, identity: node$identity });     // transport factory keeps `identity:`
  const node      = new NeuronNode({ id: BigInt('0x' + node$identity.id), lat: here.lat, lng: here.lng });
  node.transport  = transport;
  peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: node$identity, transport });

  await transport.start(node$identity.id);
  await peer.start();
  // Weave into the mesh, then wait until it's wide enough to pub/sub reliably.
  // peer.ready() (kernel v4.8.2) resolves on synaptome>=minPeers OR a stable
  // synaptome OR timeout — so a solo tab converges fast instead of polling for
  // a count it can't reach. Subscribing before this strands the SUB in a
  // not-yet-formed mesh (slow first delivery).
  status('forming mesh…');
  peer.integrate().catch(() => {});
  const ready = await peer.ready({ minPeers: 4, timeoutMs: 8000 });
  status(`connected (${ready.peers} peer${ready.peers === 1 ? '' : 's'}${ready.ready ? '' : ', mesh thin'})`);
  $('ver').textContent = `app v${APP_VERSION} · kernel v${KERNEL_VERSION} · ${NETWORK} · you ${idLabel(node$identity.id)}`;
  $('send').disabled = false;
}

// Subscribe to a topic (the user types its NAME; region is the pinned anchor).
// Re-subscribing to a new topic drops the old one.
async function ensureSubscribed(topic) {
  if (topic === currentTopic) return;
  if (currentSub) { try { await currentSub.stop(); } catch {} }
  currentTopic = topic;
  currentSub = await peer.sub({ region: TOPIC_REGION, name: topic }, async (env) => {
    if (!env || env.deleted || seen.has(env.msgId)) return;   // skip our own already-shown echo
    seen.add(env.msgId);
    // CONSUME the author-class flag: resolve the SENDER'S declared class from the
    // signed envelope's signerPubkey (the Author ID), independent of any payload.
    // Agent-authored messages get highlighted + badged so they stand out at a glance.
    const cls = await classOf(env.signerPubkey);
    const m = env.message;
    const text = readMessage(m);                                 // canonical std/message body
    const node = (m && typeof m === 'object') ? m.node : null;   // the location WE chose to share
    render(text, idLabel(node), false, topic, cls);
  }, { since: 'all' });
}

// Publish to the current topic. The protocol signs the post with our key but
// does NOT carry our location; THIS app voluntarily includes its publish ID
// (node-id, which encodes our S2 region) in the payload so subscribers can show
// where the message came from. Own publishes may not echo back, so we render
// optimistically and let the seen-set dedup if they do.
async function send() {
  const topic = $('topic').value.trim(), text = $('message').value;
  if (!topic || !text) return;
  await ensureSubscribed(topic);
  // Every publish names its signer: { signWith: author } (v0.3). We voluntarily share
  // our NODE id so subscribers can show the sender's region (idLabel) — an app choice,
  // not a protocol disclosure (the signed envelope never carries location).
  const msgId = await peer.pub({ region: TOPIC_REGION, name: topic }, makeMessage(text, { node: node$identity.id }), { signWith: author });
  seen.add(msgId);
  render(text, idLabel(node$identity.id), true, topic, { class: myClass, operatorVerified: false });
  $('message').value = '';
}

// Resolve a sender's declared author-class from their Author ID (signerPubkey).
// Cached per author — getAuthorClass is a network pull of their owner-only profile
// topic. Absent/unverifiable → 'unstated' (NEVER silently 'human').
const classCache = new Map();                            // signerPubkey → Promise<{ class, operatorVerified }>
function classOf(signer) {
  if (!signer) return Promise.resolve({ class: 'unstated', operatorVerified: false });   // anonymous post
  if (classCache.has(signer)) return classCache.get(signer);                             // dedup in-flight + cache positives
  const p = (async () => {
    try {
      const r = await peer.getAuthorClass(signer, { timeoutMs: 3000 });   // cold cross-peer pull needs > the 1s default
      const res = { class: r.class, operatorVerified: !!r.operatorVerified };
      if (res.class === 'unstated') classCache.delete(signer);            // transient miss — let a later message retry
      return res;
    } catch { classCache.delete(signer); return { class: 'unstated', operatorVerified: false }; }
  })();
  classCache.set(signer, p);
  return p;
}

// Self-asserted author-class badge; a verified ring means the operator
// countersigned. Covers every kernel class — a future/unknown value falls back
// to its raw name rather than being mislabeled as 'agent'.
const CLASS_LABEL = {
  human:   '🧑 human',
  agent:   '🤖 agent',
  service: '⚙️ service',
  bridge:  '🌉 bridge',
  relay:   '📡 relay',
};
function classBadge(cls) {
  if (!cls || cls.class === 'unstated') return null;
  const el = document.createElement('span');
  el.className = `badge ${cls.class}${cls.operatorVerified ? ' verified' : ''}`;
  el.textContent = CLASS_LABEL[cls.class] || `· ${cls.class}`;
  el.title = 'signed author-class attestation' + (cls.operatorVerified ? ', operator-countersigned' : '');
  return el;
}

function render(text, who, self, topic, cls) {
  const out = $('out');
  if (out.querySelector('.empty')) out.innerHTML = '';
  const el = document.createElement('div');
  const clsTag = cls && cls.class && cls.class !== 'unstated' ? ' ' + cls.class : '';
  el.className = 'msg' + (self ? ' self' : '') + clsTag;   // any known class → row class hook
  el.innerHTML = `<div class="text"><span class="topic"></span><span class="body"></span></div><div class="meta"></div>`;
  el.querySelector('.topic').textContent = topic ? `${topic}: ` : '';
  el.querySelector('.body').textContent = text;
  const meta = el.querySelector('.meta');
  const badge = classBadge(cls);
  if (badge) meta.appendChild(badge);
  meta.appendChild(document.createTextNode(`${who} · ${new Date().toLocaleTimeString()}`));
  out.appendChild(el);
  out.scrollTop = out.scrollHeight;
}

// Show the topic-id beneath the field, prefixed with the topic's region (us-east,
// pinned here). deriveTopicId is pure, so this works before we even connect.
async function showTopicId(topic) {
  $('topicId').textContent = topic
    ? `${ANCHOR.name} : ${await deriveTopicId({ region: TOPIC_REGION, name: topic })}`
    : '';
}

// "I am human" — opt-in. Checking it signs a `human` author-class attestation with
// the durable author key and publishes it to our owner-only profile topic, so any
// peer can resolve our class from our Author ID alone. DEFAULT OFF = unstated; we
// never silently declare 'human'. The minimal app declares but does not retract.
$('human').addEventListener('change', async (e) => {
  if (!peer) { e.target.checked = false; status('connect first'); return; }
  if (!e.target.checked) { status('note: unchecking does not retract the published attestation'); return; }
  try {
    status('declaring human…');
    await peer.setAuthorClass('human', { signWith: author });
    myClass = 'human';
    classCache.delete(author.pubkeyHex);                 // re-resolve our own badge fresh
    status('declared: human');
  } catch (err) { e.target.checked = false; status('declare failed: ' + (err.message || err)); }
});

$('send').addEventListener('click', () => send().catch((e) => status('send failed: ' + (e.message || e))));
$('message').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('send').click(); });
$('topic').addEventListener('input',  () => showTopicId($('topic').value.trim()).catch(() => {}));
$('topic').addEventListener('change', () => ensureSubscribed($('topic').value.trim()).catch(() => {}));

showTopicId($('topic').value.trim()).catch(() => {});
connect()
  .then(() => ensureSubscribed($('topic').value.trim()))
  .catch((e) => status('connect failed: ' + (e.message || e)));
