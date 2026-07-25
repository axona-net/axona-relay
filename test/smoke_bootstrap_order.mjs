// smoke_bootstrap_order.mjs — startRelay's ORDER is the fix, so the order is what
// this test pins. Stub peer/transport, record every lifecycle call, assert the
// sequence and the awaiting. No network, no kernel — pure ordering contract.
//
// Guards the 2026-07-25 defect: integrate() fired BEFORE any peer was known (no
// peer.ready() in between) and was never awaited, so it queried a near-empty
// routing table, did nothing, and let a pub() go out from a node its neighbours
// had not yet adopted — the singleton-root / stranded-publish failure mode.
import { startRelay } from '../src/relay.js';

let failed = 0;
const ok = (label, cond) => { console.log(`${cond ? 'PASS' : 'FAIL'}  ${label}`); if (!cond) failed++; };
const tick = () => new Promise((r) => setTimeout(r, 5));

// A stub pair that logs calls in order. `integrateMs` makes integration slow so
// "was it awaited?" is observable rather than a coin flip.
function stubs({ integrateMs = 20, integrateThrows = false, readyResult } = {}) {
  const calls = [];
  const transport = { start: async () => { calls.push('transport.start'); await tick(); } };
  const peer = {
    start:  async () => { calls.push('peer.start'); await tick(); },
    ready:  async (opts) => { calls.push('peer.ready'); await tick(); return readyResult ?? { ready: true, peers: 4, ms: 12, reason: 'minPeers', opts }; },
    integrate: async () => {
      calls.push('integrate.begin');
      await new Promise((r) => setTimeout(r, integrateMs));
      calls.push('integrate.end');
      if (integrateThrows) throw new Error('integration failed');
      return { opened: 3 };
    },
  };
  return { calls, peer, transport };
}

// ── the canonical order ─────────────────────────────────────────────────────
{
  const { calls, peer, transport } = stubs();
  const status = await startRelay({ peer, transport });
  ok('order is transport.start → peer.start → peer.ready → integrate',
    calls.slice(0, 4).join(',') === 'transport.start,peer.start,peer.ready,integrate.begin');
  // THE REGRESSION GUARD: ready() must come before integration begins. If
  // integrate runs first it queries an unseeded table and accomplishes nothing.
  ok('peer.ready() happens BEFORE integrate() begins',
    calls.indexOf('peer.ready') < calls.indexOf('integrate.begin'));
  // AWAITED: integration must have COMPLETED before startRelay resolved.
  ok('integrate() is awaited (completes before startRelay resolves)',
    calls.includes('integrate.end'));
  ok('readiness status is returned to the caller', status.ready === true && status.peers === 4);
  ok('integration result is reported, not swallowed', status.integrated?.opened === 3 && status.integrateError === null);
}

// ── ready:false — connect()'s background-heal semantics ─────────────────────
{
  const { calls, peer, transport } = stubs();
  const status = await startRelay({ peer, transport, ready: false });
  ok('ready:false skips the warm-up', !calls.includes('peer.ready') && status.ready === undefined);
  ok('ready:false still KICKS OFF integration', calls.includes('integrate.begin'));
  ok('ready:false does NOT await integration (heals in background)', !calls.includes('integrate.end'));
}

// ── tuning passes through ───────────────────────────────────────────────────
{
  const { peer, transport } = stubs();
  const status = await startRelay({ peer, transport, ready: { minPeers: 2, timeoutMs: 500 } });
  ok('ready options reach peer.ready()', status.opts?.minPeers === 2 && status.opts?.timeoutMs === 500);
}

// ── failure handling: non-fatal, but no longer invisible ────────────────────
{
  const { peer, transport } = stubs({ integrateThrows: true });
  let threw = false;
  let status;
  try { status = await startRelay({ peer, transport }); } catch { threw = true; }
  ok('a failing integrate() does NOT fail start-up', !threw);
  ok('the integration error is surfaced on the status', status?.integrateError instanceof Error);
}

// ── a ready() timeout must not wedge start-up ──────────────────────────────
{
  const { peer, transport } = stubs({ readyResult: { ready: false, peers: 0, ms: 10_000, reason: 'timeout' } });
  const status = await startRelay({ peer, transport });
  ok('a ready() timeout is reported, not thrown', status.ready === false && status.reason === 'timeout');
}

// ── a peer without integrate() (older kernel) still starts ─────────────────
{
  const { peer, transport } = stubs();
  delete peer.integrate;
  const status = await startRelay({ peer, transport });
  ok('a peer lacking integrate() still bootstraps', status.ready === true && status.integrated === null);
}

console.log(failed ? `\n${failed} check(s) failed` : '\nsmoke_bootstrap_order: all checks passed');
process.exit(failed ? 1 : 0);
