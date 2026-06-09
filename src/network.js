// network.js — pick which Axona network (bridge) to bootstrap from.
//
// Production went live on the `axona/5` line at the 2026-06-08 flag-day, so the
// **default network is production**. Selection precedence, highest first:
//
//   1. an explicit URL      — BRIDGE_URL env, or the CLI `--bridge <url>` flag
//   2. a named network      — RELAY_NETWORK env, or the CLI `--network <name>`
//   3. the default          — production (`wss://bridge.axona.net`)
//
// Both deployments run the same kernel/epoch today; testnet is the staging line
// ahead of `main`. Target it with `RELAY_NETWORK=testnet` (or a `--bridge` URL).

export const BRIDGES = Object.freeze({
  prod:    'wss://bridge.axona.net',
  testnet: 'wss://testnet.axona.net',
});

export const DEFAULT_NETWORK = 'prod';

const ALIASES = {
  prod: 'prod', production: 'prod', main: 'prod', mainnet: 'prod', live: 'prod',
  testnet: 'testnet', test: 'testnet', staging: 'testnet', stage: 'testnet', sf: 'testnet',
};

/** Normalize a network name to 'prod' | 'testnet', or null if unrecognized. */
export function resolveNetwork(name) {
  if (!name) return null;
  return ALIASES[String(name).trim().toLowerCase()] ?? null;
}

/** Bridge URL for a named network; throws on an unknown name. */
export function bridgeForNetwork(name) {
  const net = resolveNetwork(name);
  if (!net) throw new Error(`unknown network "${name}" (use: ${Object.keys(BRIDGES).join(' | ')})`);
  return BRIDGES[net];
}

/**
 * Resolve the bridge URL from (optional) explicit override → BRIDGE_URL env →
 * RELAY_NETWORK env → default (production).
 * @param {{override?: string, network?: string}} [opts]
 */
export function resolveBridgeUrl({ override, network } = {}) {
  if (override) return override;                                   // --bridge / explicit URL
  if (process.env.BRIDGE_URL) return process.env.BRIDGE_URL;       // explicit URL via env
  if (network) return bridgeForNetwork(network);                  // --network flag
  const fromEnv = resolveNetwork(process.env.RELAY_NETWORK);       // RELAY_NETWORK env
  return BRIDGES[fromEnv ?? DEFAULT_NETWORK];
}
