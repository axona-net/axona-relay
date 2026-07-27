// =====================================================================
// Candidate contract — implement these to add a memory-hard PoW candidate
// (Equihash, Cuckoo Cycle, …). Drop the file in candidates/ and register it in
// bench.js's CANDIDATES map. The benchmark loads it inside the Web Worker.
//
// Design rules (from Stage4-MemoryHard-PoW-v0.1.md):
//   • SOLVE is memory-hard; VERIFY is cheap (a few hashes, no big memory).
//   • Size the memory parameter to the phone floor (~256–512 MB), NOT difficulty.
//   • Tune difficulty by search EFFORT (extra zero bits / K solutions), not memory.
//   • Compile the reference solver to SINGLE-THREADED WASM first (most portable).
//
//   export const name: string
//   export const suiteDifficulties: number[]   // values the suite cycles for THIS
//        // candidate. The integer is candidate-interpreted: SHA = leading-zero
//        // bits; a memory-hard fn = MEMORY in MB (the phone-floor axis); Cuckoo
//        // = edge-bits (graph size). Pick a range that sweeps the device floor.
//   export const difficultyLabel: string        // e.g. 'zero-bits' | 'mem MB' | 'edge-bits'
//   export async function mint(pubkeyHex, difficulty): Promise<witness>
//        // search until a witness meets `difficulty`. The witness is the small
//        // proof (Equihash: indices; Cuckoo: cycle edges) — serialise to a
//        // compact string (hex/base64) so the harness can size + sync it.
//   export async function verify(pubkeyHex, witness, difficulty): Promise<boolean>
//   export function peakMemoryBytes(): number
//        // peak WASM linear memory observed during the last mint — track
//        // wasmMemory.buffer.byteLength. THIS is the headline metric vs the floor.
//   export function reset(): void   // clear the peak counter between trials
// =====================================================================
export {};
