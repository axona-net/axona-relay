// Baseline candidate: the shipped SHA-256 Hashcash PoW (NOT memory-hard).
// Runnable TODAY — establishes the compute-bound / tiny-memory baseline that the
// memory-hard candidates (Equihash, Cuckoo) must beat at the phone floor.
// Implements the candidate contract — see template.js.
import { powMint, powVerify } from '../../../src/pow/pow.js';

export const name = 'sha256-hashcash (baseline — NOT memory-hard)';
export const version = '0.1.0';
export const suiteDifficulties = [12, 16, 18, 20];   // leading-zero bits (search effort)
export const difficultyLabel = 'zero-bits';

export async function mint(pubkeyHex, difficulty) {
  // returns the nonce string == the witness
  return powMint({ pubkeyHex, role: 'publish', difficulty, maxTries: 1e9 });
}

export async function verify(pubkeyHex, witness, difficulty) {
  return powVerify({ pubkeyHex, nonce: witness, role: 'publish', difficulty });
}

// SHA-256's working set is a few hundred bytes — report ~0 to make the contrast
// with a real memory-hard candidate stark.
export function peakMemoryBytes() { return 0; }
export function estimateMemMB() { return 0; }     // not memory-hard — never gated
export function reset() {}
