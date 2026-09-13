# pilot-p1-canary-reads — commands, budgets, classes (offline read tool, AX-PILOT-P1)

Companion to ops/pilot-p1-canary-reads.mjs (David-approved offline implementation, decision
5431979d…). This tool READS only. It never rolls, never writes a stop file, never removes one,
never copies a capture. Its one output that looks like a decision, `stopfile-removal-permitted`,
is an advisory string; acting on it is a separately approved human command.

## Invocation

    node ops/pilot-p1-canary-reads.mjs --plan canary --target lin --phase B2 --baseline  --expect-sha <PRE_SHA>    --min-free-kb <int>
    node ops/pilot-p1-canary-reads.mjs --plan canary --target lin --phase B2 --candidate --expect-sha <CANARY_SHA> --min-free-kb <int> --run-id <id>
    node ops/pilot-p1-canary-reads.mjs --plan canary --target lin --phase DURING --gen <YYYYMMDD-HHMMSS> --launch-pids p1,p2,...
    node ops/pilot-p1-canary-reads.mjs --plan canary --target lin --phase A1 --gen <…> --launch-pids <…> --launch-complete --run-id <same id> --expect-sha <CANARY_SHA> --inventory <B2 candidate inventory.json>

Default is DRY RUN: prints every command string and its cap, spawns nothing. `--live` requires
PILOT_P1_AUTHORIZED=yes in the environment (an interlock, not an approval) and a separate
operational approval per invocation. Importing the module spawns nothing; the tests never spawn
ssh (the local-shell test redirects the transport's ssh invocation to `bash -c` on owned
fixtures and asserts that it was ssh that was requested).

## Inputs are validated before any command string exists

| input | rule |
|---|---|
| --target | exactly `lin` |
| --phase | B2 (with exactly one of --baseline / --candidate), DURING, A1 |
| --expect-sha | 40 hex; baseline = the pre-run HEAD, candidate = 29003c23… |
| --min-free-kb | positive integer; below it → HOLD |
| --gen | `^\d{8}-\d{6}$`, copied from the roll output's `generation <GEN>` line |
| --launch-pids | 1..5 unique positive integers, from the roll output and the post-roll census |
| --run-id | operator-supplied run identifier (1..64 of A-Za-z0-9._-), given to B2 --candidate and REQUIRED again at A1; the inventory's runId must equal it (same host + time + version do not identify a run) |
| --launch-complete | required at A1: the operator's explicit assertion that --launch-pids is the full attempted set; absence is a refusal, never a default |
| --inventory | the B2 --candidate run's inventory.json: host lin, complete = true, safe names only, runId, takenAt, expectSha equal to A1's --expect-sha |
| --binding-incomplete | when the roll output, census and logs disagree; A1 REFUSES (HOLD) |
Host-derived names are accepted only if they match the recorder's basename grammar
`disc-relay-<pid>-<id>.jsonl` (id ≤ 32 alphanumerics), contain no path separator and no shell
character; the listing is `find -maxdepth 1 -type f`, so symlinks are listed separately as
not-regular and never read. Historic captures (present in the B2 inventory) and foreign-pid
captures are excluded BEFORE any read. Launch completeness is operator-supplied; the census is
compared against it and any census pid outside the launch set is a HOLD.

## Commands (literal; every member its own command; Linux `timeout -k 5 60 bash -c` wrapper)

B2: `git -C <co> rev-parse HEAD` · (candidate only) `cd <co> && shasum -a 256 <four files>` ·
`cd <co> && bash relay-census.sh count` · `… --pids` · `df -k /` ·
`cd <co> && find relay-logs -maxdepth 1 -name "disc-relay-*" -type f -print` ·
`… -type l -print` · `… -name .trace-stop -print` ·
`cd <co> && LAT_TRACE=1 LAT_TRACE_MAX_MS=900000 node -p "JSON.stringify({a:…,b:…})"`.
DURING: `ps -o pid=,lstart=,etime=,rss=,%cpu= -p <launch-pids>` · five commands
`cd <co> && tail -c 131072 relay-logs/relay-<GEN>-<slot>.log` (slot 1..5, named, never a glob) ·
`df -k /`.
A1: `cd <co> && bash relay-census.sh --pids` · `ps …` · the two listings · then, only if the
listing has no unsafe names and no pid ambiguity, one command per bound candidate (≤ 5):
`cd <co> && ls -la relay-logs/<name> && sha256sum relay-logs/<name> && head -c 8192 relay-logs/<name> && printf "\n==TAIL==\n" && tail -c 4096 relay-logs/<name>`
· the five tails · `df -k /`.
A `cd` failure stops the command (`&&`); a missing log fails its own tail; nothing is echoed
over a failure.

## Budgets (retained output and dispatch, not remote work or memory)

| item | value |
|---|---|
| per command deadline | 60 s (transport process-group kill; shrinks to the remaining wall) |
| tail cap | 131072 + 4096 bytes |
| capture read cap | 8192 + 4096 + 2048 bytes |
| small reads | 4096; listings 65536 |
| phase retained | 2 MiB |
| phase wall | 5 min |
Every command is admitted against the remaining phase bytes and time before it runs; when the
budget is exhausted the row is `skipped` with reason `phase budget` and the transport is not
called. Truncated, clipped, timed-out, errored or skipped rows are NOT OBSERVED. The phase is
classed by its WORST member; a phase that is not `complete` carries the hold
"phase class <x> (worst member): NOT OBSERVED" and yields no advisory.

## What A1 concludes, and what it does not

`stopfile-removal-permitted` only when: the phase is complete; the listing had no unsafe names
and no ambiguity; the census read completed, is non-empty, has no malformed lines and no
duplicates, contains every launch pid and no other pid; every launch pid has exactly one bound
capture whose ls and sha lines name that file and whose FIRST FILE LINE is the armed row (the
first complete line within the 8 KiB head — the accepted armed row exceeds 512 bytes) carrying
the filename's pid and captureId; every bound capture's tail ends in a terminal row with the
same pid and captureId, a known reason (capped | stopped | window-elapsed | closed) and finite
non-negative elapsedMs / rowsWritten / bytesWritten. Otherwise `HOLD: unresolved` with named
holds. A launch pid absent from the census is a HOLD even when its capture ended `closed`: that
reason proves recorder closure, not process exit or launch provenance; it is reported beside
the hold, never used as a waiver. Invalid terminal fields are omitted from the summary (null)
and hold the phase; no host-supplied string reaches summary.json.
A tail's first line is usually a fragment: reported as `tailFragment`, never as a torn-line
count for the file. sha256 and size come from the host's `sha256sum` and `ls -la`.

## Outputs

`<out>/summary.json` — the allowlisted summary (numbers, classes, booleans, validated names,
digests, reasons, holds); `manifest.json` — one row per command with the ACTUAL budgets of this
plan (the accepted collector's writer is not used because it hard-codes its own budget);
`<step>.txt` — raw stdout per command, mode 0600 in a 0700 directory, never posted;
`inventory.json` (B2) — the name inventory A1 requires. Council receives summary.json content.

The output directory is created EXCLUSIVELY: if anything exists at `<out>` — a directory of any
mode, a file, a symlink whether or not its target exists — the writer refuses before touching it,
so no pre-existing mode is inherited and no file is ever overwritten (files open `wx`, 0600; the
directory is chmod'd 0700 after creation so the umask cannot widen it). The CLI's default `<out>`
carries a millisecond timestamp and is therefore fresh; a repeated `--out` is a refusal, not a
merge.

The tail summary's `lastState` is a closed vocabulary: the kernel's bridge states `connecting`,
`open`, `stale`, `disconnected`, `upgrade-required`, `graduated`, the tui's `down`, the literal
`unrecognized` for any other `state=` token, or null when no `state=` token was seen. The token
itself is never copied; the raw tail stays in the 0600 `<step>.txt`.
