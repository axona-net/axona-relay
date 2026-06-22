// author-class-smoke.mjs — verify the agent self-declaration end-to-end on a live
// network: the persistent session auto-declares class:"agent" on connect; a
// SEPARATE peer resolves that class from the Author ID alone (pinned region +
// owner-only profile topic); and a NON-owner is rejected from writing the
// victim's profile topic (owner-only enforcement). Defaults to testnet.
//
//   RELAY_NETWORK=testnet node scripts/author-class-smoke.mjs
process.env.RELAY_NETWORK   = process.env.RELAY_NETWORK   || 'testnet';
process.env.MCP_AUTHOR_PATH = process.env.MCP_AUTHOR_PATH || '/tmp/claude-mcp-identity.classsmoke.json';
process.env.MCP_OPERATOR    = process.env.MCP_OPERATOR    || 'ed25519:david-demo-operator';

const S = await import('../src/mcp-session.js');
const { connectPeer } = await import('../src/ops.js');
const { cleanupWebRTC } = await import('../src/polyfill.js');

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
let pass = true;
const check = (n, ok, d = '') => { console.log(`${ok ? 'PASS' : 'FAIL'}  ${n}${d ? ' — ' + d : ''}`); if (!ok) pass = false; };

let B;
try {
  // 1. session connects and auto-declares agent
  const st = await S.status();   // triggers nothing; declare happens in ensureSession
  await S.ensureSession();
  const st1 = await S.status();
  const authorId = st1.authorId;
  check('session auto-declared agent on connect', st1.declaredClass === 'agent', `class=${st1.declaredClass} operator=${st1.operator}`);
  console.log(`[cls] author ${authorId.slice(0,12)}… profile topic region pinned, owner=author`);

  await wait(5000);   // let the attestation propagate to roots

  // 2. a SEPARATE peer resolves the class from the Author ID ALONE
  B = await connectPeer({ region: 'useast' });
  const topic = S.authorClassTopic(authorId);
  const env = await B.peer.pull(null, { topic });
  let resolved = 'unstated', operator = null;
  if (env && env.message && (env.signerPubkey || '').toLowerCase() === authorId.toLowerCase()) {
    const att = JSON.parse(env.message);
    if (att.kind === 'axona:author-class:v1') { resolved = att.class; operator = att.operator ?? null; }
  }
  check('separate peer resolved class from Author ID alone', resolved === 'agent', `resolved=${resolved}`);
  check('operator field carried through', operator === process.env.MCP_OPERATOR, `operator=${operator}`);
  check('binding: attestation signer === author', (env?.signerPubkey || '').toLowerCase() === authorId.toLowerCase());

  // 3. a NON-owner CANNOT write the victim's profile topic (owner-only)
  let blocked = false, errmsg = '';
  try {
    await B.peer.pub(topic, JSON.stringify({ kind: 'axona:author-class:v1', class: 'human', ts: Date.now(), author: authorId }), { signWith: B.author });
  } catch (e) { blocked = true; errmsg = String(e?.code || e?.message || e); }
  check('non-owner write rejected (owner-only)', blocked, errmsg.slice(0, 60));

  // 4. session.getAuthorClass self-lookup
  const g = await S.getAuthorClass({ authorId });
  check('getAuthorClass(self) returns agent', g.ok && g.class === 'agent');
} catch (e) {
  check('no exception', false, String(e?.stack || e?.message || e).slice(0, 300));
} finally {
  try { if (B) await B.close(); } catch {}
  await S.shutdown();
  try { cleanupWebRTC(); } catch {}
}
console.log(pass ? '\n✓ ALL PASS' : '\n✗ FAILURES');
process.exit(pass ? 0 : 1);
