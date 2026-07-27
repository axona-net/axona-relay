# PoW phone-WASM benchmark

The **go/no-go gate** for the Stage-4 memory-hard PoW function pick
(`axona-docs/architecture/Stage4-MemoryHard-PoW-v0.1.md`). It measures, on a real
device, for a candidate at a chosen memory parameter:

- **mint time** (p50 / p90 / p99 over trials),
- **verify time**,
- **peak WASM memory** + an **OOM** flag,
- whether the tab survives (the real phone constraint).

The decision rule: *at a memory parameter that fits the weakest supported phone,
does the candidate fit without OOM, give an acceptable foreground mint, and keep
verify at µs scale?* If neither Equihash nor Cuckoo clears it → lower difficulty,
go background-only, or fall back to Argon2id.

## Share it via a URL (deployed)

The page is served by GitHub Pages at the repo root, so once this is on `main`
it's live at:

> **<https://demo.axona.net/bench/pow-wasm/>**  (testnet: `demo-testnet.axona.net/bench/pow-wasm/`)

Share that link with anyone (a **QR code** on the page makes phone-joining a
scan). They open it on their phone/PC, hit **Run**, and — with **auto-publish to
Axona** on (default) — the result is published over the **live Axona network** to
the topic `pow-bench/results`. No server to run, works across the internet.
(`?bridge=wss://…` overrides the bridge.)

**Continuous mode** (user-initiated via **Run continuously**): loops run → publish
→ wait, streaming a fresh result after *every* run — the sustained/thermal data —
until **Stop**. With auto-publish on it connects to Axona **once** and reuses the
connection for all iterations. The `gap` field sets the pause between runs.

> Note: GitHub Pages can't set COOP/COEP headers, so `crossOriginIsolated` is
> false there — the core metrics (mint/verify timing + `wasmMemory.byteLength`
> peak) work fine; only `measureUserAgentSpecificMemory()` and WASM threads need
> the `collector.js` path below.

## Collect the results on a local node (Axona pub/sub)

Subscribe to the topic from any machine — your laptop becomes the data sink:

```bash
cd axona-relay
node src/cli.js sub "pow-bench/results" --region useast --for 3600   # prints each result as it arrives
```

Every tester's device publishes its result JSON to that topic; the `--region
useast` anchor matches the page's reporter, so you receive them all. Pipe to a
file and tabulate however you like. This *is* the automated data-gathering loop —
Axona relaying its own benchmark telemetry.

**Comparison feedback (closed loop).** The collector also **publishes a compact
leaderboard back** to `pow-bench/leaderboard` every 15 s (per device × candidate ×
difficulty, sorted by mint). The app subscribes and shows a **"how you compare"**
panel — your mint p50, your rank N/M, and fastest/median/slowest — so each tester
sees where their device stands, live.

## Run it now (SHA-256 baseline — works today)

```bash
# from the repo root (axona-protocol/)
node bench/pow-wasm/collector.js          # serves the page (COOP/COEP) + collects results
# → open  http://localhost:8099/bench/pow-wasm/   (desktop)
# → open  http://<your-PC-LAN-IP>:8099/bench/pow-wasm/   on a phone
```

Pick the `sha256-baseline` candidate, set a difficulty (leading-zero bits), and
Run. The baseline is **not** memory-hard — it's the compute-bound / ~0-memory
reference the real candidates must beat at the phone floor.

> You can also serve with any static server (`python3 -m http.server` from the
> repo root), but then `crossOriginIsolated` is false, so the accurate
> `measureUserAgentSpecificMemory()` and WASM threads are unavailable. The
> included `collector.js` sets COOP/COEP so isolation is on.

## Candidates in the suite

- **`sha256-baseline`** — the not-memory-hard reference (difficulty = leading-zero
  bits). Peak mem ~0 by design.
- **`argon2id`** — a **real memory-hard** function (the named *symmetric* Stage-4
  option) via `hash-wasm` (vetted WASM, lazy-loaded from a CDN). Difficulty =
  **memory in MB** (you can't leading-zero-search a fn whose every eval costs
  ~10–100 ms), so the suite sweeps `16/32/64/128/256 MB` — which is what finds the
  phone floor. `verify` recomputes (symmetric — the harness measures that
  expensive-verify cost). *This candidate carries a CDN dependency; the kernel
  stays zero-dep, and if the CDN can't load the harness just skips it.*
- *next:* **Equihash / Cuckoo** — the **asymmetric** (cheap-verify) family we'd
  actually ship. No off-the-shelf browser-WASM build, so these need a compiled
  reference solver (Emscripten / wasm-pack) dropped in as `candidates/*.js`.

Each candidate declares `suiteDifficulties` + `difficultyLabel`, so the suite
runs candidate × that candidate's own sweep.

## Add a memory-hard candidate

1. Compile the reference solver to **single-threaded WASM** (Emscripten for
   Tromp's Cuckoo C/C++; wasm-pack for a Rust Equihash). Size the memory
   parameter to the phone floor (~256–512 MB). Tune difficulty by **search
   effort, not memory.**
2. Write `candidates/<name>.js` implementing the contract in
   `candidates/template.js` (`mint` / `verify` / `peakMemoryBytes` / `reset`).
   `peakMemoryBytes()` should track `wasmMemory.buffer.byteLength` — it's the
   headline metric vs the floor.
3. Register it in the `CANDIDATES` map in `bench.js`. It appears in the dropdown.

## Collecting results across devices

Put the collector's URL (e.g. `http://<PC-LAN-IP>:8099`) in the page's
**collector URL** box; each run POSTs its result JSON to `results.jsonl` and
prints a live tally. Or just use **Copy / Download JSON** per device.

**Axona dogfood (optional):** forward each result to a topic and aggregate from
anywhere with the relay CLI —

```bash
tail -f results.jsonl | while read l; do \
  node ../../axona-relay/src/cli.js pub "axona/pow-bench" "$l"; done
node ../../axona-relay/src/cli.js sub "axona/pow-bench" --for 600   # aggregate
```

## Phased plan (see the Stage-4 record §6)

1. **Node baseline** — param→cost curve, no browser.
2. **Desktop browser** — catch WASM issues (memory growth, SIMD, threads); DevTools
   CPU-throttle for dev iteration (simulates CPU, *not* memory/thermal).
3. **Real phones** — the decision data (your phone + PC + a low-end Android), plus
   optionally a cloud device lab (BrowserStack / AWS Device Farm) for breadth.
   Non-negotiable: emulators don't reproduce mobile memory limits or thermal.
4. **Passive field data** — once a candidate is flagged in, the shipped
   `powCalibrate` + relay logging gather real cross-device numbers over time.

`results.jsonl` is gitignored.
