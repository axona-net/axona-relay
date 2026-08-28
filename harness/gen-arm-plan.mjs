// =============================================================================
// harness/gen-arm-plan.mjs — emit the §6 churn action plan for an Arm run.
//
//   node harness/gen-arm-plan.mjs <durationMs>   →   JSON plan on stdout
//
// §6 schedule (spec v0.3, all seats concurring):
//   - rolling relay restart every 15–30 min across ROTATING hosts, ≥95%
//     census. relay-roll starts a replacement BEFORE stopping one old, so
//     census never dips — safe on every host including the small ones.
//   - abrupt single-relay kill/restart per host every 2h, ≥3min heir window.
//     relay-kill removes one first, so it only fits where a host has headroom
//     against its 95% floor: m4 (26→25 ok) and axona-win (20→19 ok). On m1
//     (12) and axona-linux (4) a kill would breach the floor, so kills are
//     scheduled ONLY on the big hosts and the driver floor-guards the rest.
//
// Deterministic: interval jitter comes from a seeded LCG on the duration, so
// the same duration yields the same plan (§7 attribution — Arm B replays the
// identical churn script).
// =============================================================================
const DURATION = Number(process.argv[2]);
if (!Number.isInteger(DURATION) || DURATION <= 0) { console.error('gen-arm-plan: durationMs required'); process.exit(1); }

// host, fleet size (for the 95% floor)
const HOSTS = [
  { host: 'm4', size: 26, killable: true },
  { host: 'm1', size: 12, killable: false },
  { host: 'axona-linux', size: 4, killable: false },
  { host: 'axona-win', size: 20, killable: true },
];
let s = (DURATION % 2147483647) || 1;
const rnd = () => (s = (s * 48271) % 2147483647) / 2147483647;

const plan = [];
// Rolling restart every 15–30 min, rotating hosts. First roll no earlier than
// 5 min in (let the workload settle and produce baseline traffic first).
let at = 5 * 60_000, hi = 0;
while (at < DURATION - 60_000) {
  const h = HOSTS[hi % HOSTS.length];
  plan.push({ atMs: at, kind: 'relay-roll', host: h.host, hostSize: h.size, floorPct: 95, heirMs: 20_000 });
  hi++;
  at += 15 * 60_000 + Math.floor(rnd() * 15 * 60_000);   // 15–30 min
}
// Abrupt kill/restart per killable host every 2h, ≥3min heir window, offset so
// kills don't land on the same minute as a roll.
for (const h of HOSTS.filter((x) => x.killable)) {
  let k = 2 * 60 * 60_000 + Math.floor(rnd() * 10 * 60_000);   // first ~2h, staggered
  while (k < DURATION - 5 * 60_000) {
    plan.push({ atMs: k, kind: 'relay-kill', host: h.host, hostSize: h.size, floorPct: 95, heirMs: 180_000 });
    k += 2 * 60 * 60_000;
  }
}
plan.sort((a, b) => a.atMs - b.atMs);
process.stdout.write(JSON.stringify(plan));
