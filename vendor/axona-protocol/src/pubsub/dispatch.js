// dispatch.js — how the pub/sub planes read what the TRANSPORT said.
//
// One classifier, one definition, imported by every plane that acts on a routing
// outcome. It lived inside repairPlane.js until v4.58.0, when the read path
// needed it too; a second copy would have been two semantics wearing one name.
//
// CAPABILITY IS DECLARED, NEVER INFERRED. This replaces a v4.57.0 classifier
// that read a void return as 'unreported' and CREDITED it, which let test
// doubles set a production durability semantic. Aster and Orion both rejected
// the inference itself (council 2026-08-01).
//
//   {consumed:true} / 'consumed'   → 'consumed'      verified dispatch
//   {consumed:false}               → 'failed'        a routing verdict: it did not
//   adapter declares NO verdicts   → 'unsupported'   honest; credits nothing
//   adapter CLAIMS verdicts, void  → 'violation'     contract breach; LOUD; fails closed
//
// FAIL-CLOSED MEANS "MAKE NO UNEVIDENCED STATE CHANGE", and which direction that
// points depends on the caller, so read the two uses together:
//
//   repairPlane._replicateRole  only 'consumed' CREDITS a replica.
//   AxonaManager._sendSubscribe only 'failed'   DROPS an upstream pin.
//
// Those look opposite and are the same rule. 'unsupported' and 'violation' both
// mean "I have no evidence", and neither a credit nor an unpin may be made on no
// evidence — crediting would fabricate durability, unpinning would thrash every
// healthy subscriber on every adapter that does not report. Silence is never
// evidence, in either direction.
export const dispatchVerdict = (r, verdictsSupported) => {
  if (r && typeof r === 'object' && typeof r.consumed === 'boolean') {
    return r.consumed ? 'consumed' : 'failed';
  }
  if (r === 'consumed') return 'consumed';
  return verdictsSupported === false ? 'unsupported' : 'violation';
};

// Target attribution: did THIS node consume it? (v4.59.1, Aster seq 149.)
//
// `consumed` alone says somebody handled the message — a via-popped reroute can
// land it on a different node than the one we addressed, and acting on
// unattributed consumption re-creates the corpse-pin _forwardToRoot exists to
// kill. This predicate is the ONE definition of "the named node took it", here
// rather than at a call site, for the same reason dispatchVerdict is: a second
// copy would be two semantics wearing one name.
//
// atNode is RICHER than the declared capability (verdictsSupported's contract
// is only consumed:boolean), so it is opportunistic evidence: production
// adapters report an id value (often bigint), test doubles report hex, and an
// adapter reporting neither is not in breach. Missing or malformed attribution
// is therefore NO EVIDENCE — false — never a violation. Comparison is by
// numeric id value, so padding and case differences cannot fake a mismatch.
export const dispatchAttributedTo = (r, rootHex) => {
  if (!r || typeof r !== 'object' || r.atNode == null) return false;
  let want; try { want = BigInt(`0x${String(rootHex)}`); } catch { return false; }
  const a = r.atNode;
  try {
    if (typeof a === 'bigint') return a === want;
    if (typeof a === 'string') return BigInt(`0x${a}`) === want;
  } catch { /* malformed → no evidence */ }
  return false;
};

export default dispatchVerdict;
