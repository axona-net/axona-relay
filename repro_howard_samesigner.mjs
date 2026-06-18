// repro_howard_samesigner.mjs — Howard's EXACT shape against the LIVE bridge:
// ONE peer (one signer) publishes 5 chunks ~1s apart to a region topic, then a
// SECOND fresh peer subscribes since:'all'. Mirrors axona-share.
//
//   node repro_howard_samesigner.mjs
import './src/polyfill.js';
import { AxonaPeer, AxonaDomain, NeuronNode, createNodeIdentity, createAuthorIdentity, KERNEL_VERSION } from './vendor/axona-protocol/src/index.js';
import { webTransport } from './vendor/axona-protocol/src/transport/web/index.js';

const BRIDGE = 'wss://bridge.axona.net';
const CENTER = { lat: 35.7721, lng: -78.6386 };       // useast, like the anchor
const REGION = 'useast';                              // structured-topic region (v0.3)
const TOPIC  = { region: REGION, name: 'claude/howard-samesigner-' + Math.floor(Date.now() / 1000) };
const sleep  = (ms) => new Promise(r => setTimeout(r, ms));

async function makePeer(label) {
  const identity  = await createNodeIdentity(CENTER);         // connection/node key
  const transport = webTransport({ bridgeUrl: BRIDGE, identity });
  const node      = new NeuronNode({ id: BigInt('0x' + identity.id), lat: CENTER.lat, lng: CENTER.lng });
  node.transport  = transport;
  const peer = new AxonaPeer({ domain: new AxonaDomain({ k: 20 }), node, nodeIdentity: identity, transport });
  await transport.start(identity.id);
  await peer.start();
  const until = Date.now() + 30000;
  while (Date.now() < until && (node.synaptome?.size ?? 0) < 3) { await sleep(500); }
  await sleep(1500);
  console.log(`  [${label}] ${identity.id.slice(0,12)}… synaptome=${node.synaptome?.size ?? 0}`);
  return { peer, transport, identity };
}

async function main() {
  console.log(`kernel v${KERNEL_VERSION} · topic ${TOPIC.region}/${TOPIC.name}`);

  console.log('— publisher peer connecting —');
  const A = await makePeer('PUB');
  // v0.3: publishes are signed by an AUTHOR identity. Howard's "same signer"
  // shape ⇒ ONE durable author signs every chunk (its Author ID is the
  // signerPubkey both peers see). A throwaway-but-stable author for this run.
  const author = await createAuthorIdentity();
  console.log(`  signer (Author ID) ${author.authorId.slice(0,12)}…`);
  const MSGS = ['count:5', 'chunk-1', 'chunk-2', 'chunk-3', 'chunk-4'];
  for (let i = 0; i < MSGS.length; i++) {
    const id = await A.peer.pub(TOPIC, JSON.stringify({ ft: 1, id: 'F1', i, n: 5, data: MSGS[i] }), { signWith: author });
    console.log(`  published ${i}: ${MSGS[i]} → ${id.slice(0,12)}…`);
    await sleep(1000);                                   // Howard waits ~1s between
  }
  await sleep(2000);

  console.log('— fresh subscriber peer connecting (reload) —');
  const B = await makePeer('SUB');
  const got = [];
  await B.peer.sub(TOPIC, (env) => {
    if (!env || env.deleted || !env.message) return;
    let m; try { m = JSON.parse(env.message); } catch { return; }
    got.push(m.data);
  }, { since: 'all' });

  await sleep(20000);
  console.log(`\nRESULT: subscriber received ${got.length}/5 → [${got.join(', ')}]`);
  try { await A.peer.leave?.(); await A.transport.stop?.(); } catch {}
  try { await B.peer.leave?.(); await B.transport.stop?.(); } catch {}
  process.exit(0);
}
main().catch(e => { console.error('threw:', e?.stack || e); process.exit(2); });
