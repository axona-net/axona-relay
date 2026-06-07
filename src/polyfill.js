// polyfill.js — install the browser globals the kernel's web transport
// expects, so it runs unchanged under Node.
//
// IMPORTANT: this module is imported FIRST in src/index.js (before the
// kernel), so the globals exist before any kernel module evaluates. The
// mesh layer reads `globalThis.RTCPeerConnection` at *connection* time, so
// strictly it only needs to be set before transport.start(); we set it at
// import time to be safe and obvious.
//
//   • RTCPeerConnection / RTCSessionDescription / RTCIceCandidate
//        ← node-datachannel/polyfill (libdatachannel — real ICE/DTLS/SCTP)
//   • WebSocket
//        ← ws  (used to dial the bridge for bootstrap + signaling)
//   • crypto / crypto.subtle
//        ← Node ≥ 20 already exposes globalThis.crypto (WebCrypto)

import * as ndc from 'node-datachannel/polyfill';
import { WebSocket as WsWebSocket } from 'ws';

function def(name, value) {
  if (!globalThis[name]) globalThis[name] = value;
}

def('RTCPeerConnection',   ndc.RTCPeerConnection);
def('RTCSessionDescription', ndc.RTCSessionDescription);
def('RTCIceCandidate',     ndc.RTCIceCandidate);
def('WebSocket',           WsWebSocket);

if (!globalThis.crypto || !globalThis.crypto.subtle) {
  throw new Error(
    'axona-relay requires Node ≥ 20 with global WebCrypto (globalThis.crypto.subtle). ' +
    'Detected an environment without it.');
}

// Exported so callers can pass it explicitly to webTransport({ WebSocketImpl })
// instead of relying on the global, and so node-datachannel can be cleanly
// torn down on shutdown.
export const WebSocketImpl = WsWebSocket;
export function cleanupWebRTC() {
  try { ndc.RTCPeerConnection?.cleanup?.(); } catch { /* best-effort */ }
}
