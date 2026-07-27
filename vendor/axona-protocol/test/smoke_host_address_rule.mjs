// smoke_host_address_rule.mjs — HOSTING IS DECIDED BY ADDRESS, NEVER BY OWNERSHIP.
//
// A node may host a topic only if its own nodeId puts it in that topic's keyspace
// neighbourhood. Owning the topic, publishing to it, or being its only publisher
// are NOT reasons to host it. Owning and hosting are disjoint properties
// (David, 2026-07-25).
//
// Why this needs a fence: pubsubHost() joins the topic tree and creates a ROLE,
// and a role changes routing — wireHandlers hands a via-routed packet to a node
// BECAUSE it holds a role, and the PUB path stamps with an existing role instead
// of resolving the true root. So hosting a distant topic is precisely how an app
// mints an interloper root that captures writes readers never see. This codebase's
// own MCP peer did exactly that to its own channel, for weeks, because it owned it.
//
// Direct unit test of the guard (_hostNeighbourhoodCheck) — no mesh needed. The
// guard must be CONSERVATIVE: refuse only on positive evidence of exclusion, so
// small/cold/sim networks keep hosting legitimately.
import { AxonaPeer } from '../src/dht/AxonaPeer.js';
import { ErrorCodes } from '../src/errors.js';

let failed = 0;
const ok = (label, cond) => { console.log(`${cond ? 'PASS' : 'FAIL'}  ${label}`); if (!cond) failed++; };

// Minimal harness: drive the guard with a controlled findKClosest + self id.
function probe({ selfId, topicId, closest }) {
  const p = Object.create(AxonaPeer.prototype);
  p._node = { id: selfId };
  p._rootReplicas = 2;                       // K = (2+1)*2 = 6
  p.findKClosest = async () => closest;
  return p._hostNeighbourhoodCheck(topicId);
}

const TOPIC = 0x89f7f877n;
// Distances are XOR against TOPIC; pick ids that are unambiguously near/far.
const near = (n) => TOPIC ^ BigInt(n);        // small xor  → close
const far  = (n) => TOPIC ^ (BigInt(n) << 200n); // huge xor → distant

// ── refusal requires POSITIVE evidence ──────────────────────────────────────
{
  // 6 others all strictly closer than a distant self → provably excluded.
  const r = await probe({ selfId: far(1), topicId: TOPIC, closest: [near(1), near(2), near(3), near(4), near(5), near(6)] });
  ok('distant node with K closer peers is REFUSED', r.ok === false);
  ok('refusal reports how many were closer', r.closer === 6);
}
{
  // Same crowd, but self is the closest of all → allowed.
  const r = await probe({ selfId: near(0), topicId: TOPIC, closest: [near(1), near(2), near(3), near(4), near(5), near(6)] });
  ok('closest node is ALLOWED even in a crowded neighbourhood', r.ok === true && r.closer === 0);
}
{
  // Self sits mid-pack: fewer than K strictly closer → allowed (conservative).
  const r = await probe({ selfId: near(4), topicId: TOPIC, closest: [near(1), near(2), near(3), far(9), far(8), far(7)] });
  ok('mid-neighbourhood node is ALLOWED (only 3 closer, K=6)', r.ok === true && r.closer === 3);
}

// ── the conservative escapes: absence of evidence is not evidence ────────────
{
  const r = await probe({ selfId: far(1), topicId: TOPIC, closest: [near(1), near(2)] });
  ok('sparse table (fewer than K others) is ALLOWED', r.ok === true);
}
{
  const r = await probe({ selfId: far(1), topicId: TOPIC, closest: [] });
  ok('empty table (cold start / sim) is ALLOWED', r.ok === true);
}
{
  const p = Object.create(AxonaPeer.prototype);
  p._node = { id: far(1) }; p._rootReplicas = 2;
  p.findKClosest = async () => { throw new Error('lookup down'); };
  const r = await p._hostNeighbourhoodCheck(TOPIC);
  ok('lookup failure is ALLOWED (cannot prove exclusion)', r.ok === true);
}
{
  const p = Object.create(AxonaPeer.prototype);
  p._node = {}; p._rootReplicas = 2; p.findKClosest = async () => [near(1)];
  const r = await p._hostNeighbourhoodCheck(TOPIC);
  ok('node with no address yet is ALLOWED', r.ok === true);
}

// ── self must never count against itself ────────────────────────────────────
{
  // findKClosest(target) can return our own id; it must be excluded from the
  // closer-count AND from the "enough others to judge" tally.
  const self = near(0);
  const r = await probe({ selfId: self, topicId: TOPIC, closest: [self, near(1), near(2), near(3), near(4), near(5), near(6)] });
  ok('self in the candidate list is not counted as a closer peer', r.closer === 0 && r.ok === true);
}
{
  // 6 candidates of which one is self → only 5 others → cannot judge → allow.
  const self = far(1);
  const r = await probe({ selfId: self, topicId: TOPIC, closest: [self, near(1), near(2), near(3), near(4), near(5)] });
  ok('self does not inflate the others-count toward a refusal', r.ok === true);
}

// ── hex-string candidate ids (the wire/adapter shape) are understood ─────────
{
  const hex = (b) => b.toString(16).padStart(66, '0');
  const r = await probe({
    selfId: far(1), topicId: TOPIC,
    closest: [near(1), near(2), near(3), near(4), near(5), near(6)].map(hex),
  });
  ok('hex-string candidates are compared correctly (still refused)', r.ok === false && r.closer === 6);
}

// ── the error surface an app actually sees ───────────────────────────────────
ok('HOST_NOT_IN_NEIGHBOURHOOD error code is exported', ErrorCodes.HOST_NOT_IN_NEIGHBOURHOOD === 'HOST_NOT_IN_NEIGHBOURHOOD');

console.log(failed ? `\n${failed} check(s) failed` : '\nsmoke_host_address_rule: all checks passed');
process.exit(failed ? 1 : 0);
