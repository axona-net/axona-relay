# Armed-canary RUN-LOG — two-relay soak (proposal v0.4)

Operation class: ADDITIVE CANARY (sanctioned by David in-session, 2026-08-24
~22:25Z, "Let's go" — after the unanimous 4-seat execution-readiness round).
Governing document: axona-docs architecture/Axona-Armed-Canary-Proposal-v0.4.md
(main aaf2b43, sha256 3624444d8e629f9c71ceaf0affceecbec688811da83d024f5c625c4d7471864a).

## Prepare — 2026-08-24T22:29Z, all green

| Step | Evidence |
|---|---|
| Dedicated checkout | `~/Documents/claude/axona-relay-canary/` — clone of axona-relay, pinned `8e14ab1` ("arming gate: floor = the pin (4.67.1) + guardMaxAttempts in the ledger") |
| Deps | `npm ci` exit 0 |
| Kernel source | axona-protocol checkout, clean, HEAD `7fc3e563cc4803c909182f10a0cc0ad15e97c6f0` (= 4.67.1 slice-3, council-approved) |
| Vendor sync | `PROTOCOL_SRC=…/axona-protocol/src bash scripts/sync-protocol.sh` exit 0 — includes the script's own npm-test gate (green); full log: scratchpad sync-canary.log |
| Vendored version print | `node -p "require('./vendor/axona-protocol/package.json').version"` → `4.67.1` |
| Arming fence | `node test/fence_arming_gate.mjs` exit 0 — 19 passed, 0 failed; output retained at `artifacts/fence-arming-19.txt`, sha256 `2bd6423921080e57060d21042065400f08dacced91b6d08e3b65da3968628440` |
| Fleet untouched | axona-relay (fleet checkout) and its 4.62.2 vendor not modified; census during canary window = 28 `src/index.js` processes (EXPECT=28 for any roll) |

### Pin advance — 2026-08-24T22:42Z

Launch-blocking defect caught in pre-launch review: the plain-log renderer
capped event ctx at 120 chars, which would truncate the armed-ledger line —
and a truncated ledger is a threshold not evidenced. Fix committed to
axona-relay testnet `5a40e72` (armed-* events log full ctx JSON; rendering
only, no arming semantics). Canary checkout advanced to `5a40e72`;
re-verified: vendored version print `4.67.1`, fence exit 0, vendor sync
intact. Council notified in the launch report.

## Launch — 2026-08-24T22:57–22:59Z, all green

| Check | Evidence |
|---|---|
| Canary 1 | started 22:57:37Z, identity suffix `…2815e4` (truncated; full id lives only in its own log, never persisted elsewhere) |
| Canary 2 | started 22:58:57Z (+80 s stagger), suffix `…a74cc1` |
| Version gate | passed on both (no `arming refused`; vendored 4.67.1) |
| `armed-modules` | both logs, constants byte-for-byte the proposal table |
| Census | 28 `src/index.js` processes (26 fleet + 2 canaries) — EXPECT=28 for any roll during the window |
| Bond (valve part 1) | mutual: `auth-mesh-complete` + `mesh-cap-attested` in BOTH logs at 22:58:59Z, each naming the other canary |
| First ledger (canary 1) | 22:58:37Z full-JSON line: guardActive 2, guardMaxAttempts 1, deficitReopens 1, synaptome 22→23 — the ledger moves from minute one |
| First ledger (canary 2) | due on its first 60 s tick; confirmed at next watch pass |

## Mid-soak valve test — 2026-08-25T10:54:54Z (authorized by David 2026-08-25)

| Step | Evidence |
|---|---|
| Graceful stop | SIGINT to canary-2 node (identified via lsof on its log); exit in 1 s |
| Relaunch | 10:54:55Z, same envs, same checkout; `armed-modules` logged, no refusals; fresh identity suffix `…b5c5a9` (I-ID) |
| Rebond (≤10 min) | **PASS at +3 s** — canary-1 `auth-mesh-complete` + `mesh-cap-attested` with the new suffix at 10:54:57Z |
| Watermark (≤5 min) | **NOT DISCHARGED** — canary-1 `presenceWatermarks` still 0 at +40 min; no presence delivery in either log |
| Census | 28 throughout (27 during the 1 s gap) |

Mechanism: `announceOnStart` fired at 10:54:55 — 2 s BEFORE the first bond
(10:54:57). Delivery is direct-neighbour-only against the 4.62.2 fleet, and a
just-restarted node has no bonded neighbours at announce time; there is no
re-announce after bonding. The one-shot structurally precedes every bond on a
restart. Filed as a kernel design issue (axona-protocol; see council report
for the number). Per the v0.4 table: valve prerequisite stays OPEN; the soak's
other criteria are unaffected and the soak continues.

Verification-harness defects (disclosed, no canary impact): the first
restart script's `tail -60` window missed the `armed-modules` line behind
~20 peers of bootstrap chatter and reported a false launch failure; the
second watch script hard-coded a wrong epoch and never terminated (killed).
The evidence above is read from the canary logs directly.

## Soak — RUNNING from 2026-08-24T22:59Z

Arming envs (all four): RELAY_SYNAPTOME_MAINTAIN=1 RELAY_ADMISSION_GATE=1
RELAY_ATTEMPT_GUARD=1 RELAY_PRESENCE=1 · RELAY_NETWORK=testnet · region eagle
· RELAY_TUI=0 · staggered by one minute. Required at launch: version gate
passes (4.67.1), `armed-modules` line with effective constants, canary↔canary
bond verified in both synaptomes.

## Soak — pending

24–48 h against the v0.4 numeric table. armed-ledger sampled every 60 s,
retained as artifacts with sha256s. Mid-soak: canary 2 restart (valve test:
watermark advance ≤5 min, rebond ≤10 min).

## Soak end — 2026-08-25T13:32Z (planned machine shutdown, David's call)

Elapsed 14 h 33 m of the 24–48 h window. Both canaries stopped by the
runbook abort: SIGINT, graceful leave, census back to 26. Ledgers
checkpointed: canary-1 873 lines (sha256 57c3c2e6…13fb52), canary-2 871
lines (sha256 80ca478a…a69e15) — ≈100% minute-coverage of the window.

Against the v0.4 table at end: guardMaxAttempts = 1 THROUGHOUT (over-budget
zero — PASS); ledger presence ~100% (PASS); attempt rates far under 100/h
(PASS); fleet baseline unaffected, census correct at every reading (PASS);
valve rebond +3 s (PASS), watermark NOT discharged (kernel finding #57 —
prerequisite stays open); deficitReopens = 1 per canary — BELOW the
≥5/24 h exercise bar, so this soak EXTENDS rather than passes by its own
rule. Verdict: no misbehavior in 14.5 h; the guard-exercise quota and the
valve remain undischarged. Resumption or a fresh soak is David's call.
