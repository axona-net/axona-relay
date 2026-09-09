// =============================================================================
// healthdump.js — build the `health-dump` payload from peer.health().
//
// WHY THIS IS ITS OWN MODULE. This logic used to live inside the SIGUSR1
// handler in src/index.js, where nothing could reach it: importing the relay's
// entry point starts a relay. So it shipped once emitting `roles:0 seated:[]`
// unconditionally — it read `h.roles` and `r.topicId` when the contract is
// `h.axonRoles` and `r.topic` — and that empty output was then misread as "the
// relay just restarted" rather than as a broken reader. Vega caught it.
//
// A pure function over the health object can be tested against real and
// degraded shapes, which is what fence_health_dump_capacity.mjs does.
//
// THE CONTRACT, verified at AxonaPeer.js:3246-3258 and AxonaManager.js:712-723:
//   health() -> { synaptomeSize, peers[], subscriptions, axonRoles[], admission, … }
//   axonRoles[i] -> { topic, isRoot, children:<count>, cacheSize }
//   admission    -> { roles, maxRoles, seated, saturated, …, capacity }
//   capacity     -> { servicePressure, helloPressure, tickLagMaxMs, … }
// Nothing here assumes any of it is present. Absence prints null; it never
// throws, because this runs inside a signal handler on a production relay.
// =============================================================================

/**
 * @param {any} h  the object returned by peer.health()
 * @returns {object} the health-dump payload
 */
export function buildHealthDump(h) {
  const roles = Array.isArray(h?.axonRoles) ? h.axonRoles : [];

  // CAPACITY — the reason a starved relay reads as healthy without it.
  //
  // 2026-09-09, diagnosing #36: three prod droplets each run SIX relays on ONE
  // core at load 15-19 (node CPU 91-96% of that core). One relay shed peers
  // 74 -> 19 in 25 minutes on `pc-closed` evictions — 49 in 30 minutes against
  // 1-6 for its siblings on the same box — and was the only node in the fleet
  // emitting replicate-all-failed. Starvation had to be inferred from `ps` and
  // load average, while the kernel had been MEASURING it since 4.47.0 and this
  // dump was discarding the measurement.
  //
  //   helloPressure   = rolling tick lag / HELLO_DEADLINE_MS (5s)
  //                     "fraction of the way to being kicked off the bridge"
  //   servicePressure = worst obligation age / that obligation's OWN deadline
  //                     "fraction of the way to silently rotting a role"
  //
  // At 1.0 the thing has already happened. Both are OBSERVED wall-clock ratios,
  // never functions of the role count — which is precisely why 4.47.0 replaced
  // `axonRoles.size >= MAX_ROLES` with them.
  //
  // Read SEPARATELY from roles: an older kernel exposes no admission.capacity,
  // and a reader that throws there would lose the role data as well.
  const cap = h?.admission?.capacity ?? null;

  return {
    peers: Array.isArray(h?.peers) ? h.peers.length : (h?.synaptomeSize ?? null),
    synaptome: h?.synaptomeSize ?? null,
    subscriptions: h?.subscriptions ?? null,   // this node's OWN subs — NOT seated downstream
    roles: roles.length,
    rooted: roles.filter((r) => r.isRoot).length,
    // Admission verdict, then the two pressures that decide it.
    saturated:       h?.admission?.saturated ?? null,
    helloPressure:   cap?.helloPressure ?? null,
    servicePressure: cap?.servicePressure ?? null,
    tickLagMaxMs:    cap?.tickLagMaxMs ?? null,   // rolling window — what pressure reads
    tickLagPeakMs:   cap?.tickLagPeakMs ?? null,  // all-time; DIAGNOSIS ONLY, drives nothing
    tickDurMs:       cap?.tickDurMs ?? null,
    tickStalls:      cap?.tickStalls ?? null,     // ticks whose lag exceeded the hello deadline
    worstObligation: cap?.worstObligation ?? null,
    overdueFrac:     cap?.overdueFrac ?? null,
    // EMIT-SIDE LOOKAHEAD (kernel 4.81.0). The receive-side census showed
    // lookahead_probe is 83% of all inbound mesh frames; this says whether the
    // fan-out that produces it earns its traffic. It cannot be read from a
    // browser — a browser is a leaf and emits zero probes — so a relay is the
    // only place the answer exists.
    //
    // Emitted as a nested object rather than flattened: the fields are only
    // meaningful together (a rate without its denominator is not a rate), and
    // logctx.js renders a structured ctx whole.
    lookahead:       h?.lookahead ?? null,
    seated: roles.map((r) => ({
      topic: String(r.topic ?? '').slice(0, 12),
      isRoot: !!r.isRoot,
      kids: typeof r.children === 'number' ? r.children : null,
      cache: r.cacheSize ?? null,
    })),
  };
}
