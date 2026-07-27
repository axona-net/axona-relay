// Argon2id candidate — a REAL memory-hard function (the named *symmetric*
// Stage-4 option). Uses hash-wasm (vetted WASM) loaded from a CDN, lazily so
// reading this module's suite metadata stays cheap.
//
// Memory-hard funcs aren't leading-zero-searched (each eval is too expensive),
// so here DIFFICULTY = MEMORY in MB — the axis that actually decides the phone
// floor. `mint` runs ONE argon2id at that memory (the primitive cost a real PoW
// would multiply by its search), `verify` recomputes it (SYMMETRIC — this is the
// expensive-verify cost that makes the asymmetric schemes attractive; the
// harness measures it). Reports the configured memory as peak.
//
// DEPENDENCY: this candidate fetches hash-wasm from a CDN. If it can't load
// (offline/blocked), the harness's fault-tolerance skips it with a note — the
// kernel itself stays zero-dependency; only this benchmark candidate has one.
const HASH_WASM = 'https://esm.sh/hash-wasm@4.12.0';
// 16-byte fixed salt ("axona-pow-bench!") — argon2 needs salt ≥ 8 bytes.
const SALT = new Uint8Array([0x61,0x78,0x6f,0x6e,0x61,0x2d,0x70,0x6f,0x77,0x2d,0x62,0x65,0x6e,0x63,0x68,0x21]);
const ITERATIONS = 3, PARALLELISM = 1, HASH_LEN = 32;

let _hw = null, _peak = 0;
async function hw() { if (!_hw) _hw = await import(HASH_WASM); return _hw; }
const params = (pubkeyHex, memMB) => ({
  password: pubkeyHex, salt: SALT, parallelism: PARALLELISM, iterations: ITERATIONS,
  memorySize: memMB * 1024,            // hash-wasm wants KiB
  hashLength: HASH_LEN, outputType: 'hex',
});

export const name = 'argon2id (memory-hard, symmetric verify)';
export const version = '0.1.0';
// DEMOTED — the symmetric fallback, not the production pick. Empty suite ⇒ NOT
// cycled in continuous mode (so the fleet doesn't grind a fn we won't ship);
// still selectable for a manual single run to get fallback data if ever needed.
export const suiteDifficulties = [];
export const difficultyLabel = 'mem MB';

export async function mint(pubkeyHex, memMB) {
  const { argon2id } = await hw();
  _peak = memMB * 1024 * 1024;
  return argon2id(params(pubkeyHex, memMB));               // witness = the 32-byte hash (hex)
}
export async function verify(pubkeyHex, witness, memMB) {
  const { argon2id } = await hw();
  return (await argon2id(params(pubkeyHex, memMB))) === witness;
}
export function peakMemoryBytes() { return _peak; }
export function estimateMemMB(memMB) { return memMB; }   // difficulty IS the memory
export function reset() {}
