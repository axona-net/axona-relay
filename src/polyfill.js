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

// ICE PAIR OBSERVATION (2026-10-03, council 5a2cde6c). axona-linux's channels
// formed during a bridge window died within seconds of that window closing,
// while M1's lived ~25 min, and nothing recorded WHICH path a channel rode:
// direct LAN (host), the router's public address (srflx), or the bridge's TURN
// server (relay). Each RTCPeerConnection now logs its selected candidate pair
// when it connects and again, with its age, when it fails or closes. Read-only:
// it observes state the connection already has and changes no behaviour.
// Lines match the relay log format so they interleave with kernel events:
//   [YYYY-MM-DD HH:MM:SS] ice-pair {"pc":N,"ev":"connected","local":"host",...}
let pcSeq = 0;
const ts = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const pairOf = (pc) => {
  try {
    const p = pc.selectedCandidatePair();
    if (!p) return null;
    return { local: p.local?.type, remote: p.remote?.type,
             proto: p.local?.transportType || p.local?.protocol || null,
             lan: /^(10|192\.168|172\.(1[6-9]|2\d|3[01]))\./.test(p.remote?.address || '') };
  } catch { return null; }
};
class ObservedRTCPeerConnection extends ndc.RTCPeerConnection {
  constructor(...args) {
    super(...args);
    const id = ++pcSeq; const born = Date.now(); let pair = null; let ended = false;
    this.addEventListener('connectionstatechange', () => {
      const st = this.connectionState;
      if (st === 'connected') {
        pair = pairOf(this);
        console.log(`[${ts()}] ice-pair ${JSON.stringify({ pc: id, ev: 'connected', ms: Date.now() - born, ...(pair || { pair: null }) })}`);
      } else if ((st === 'failed' || st === 'closed' || st === 'disconnected') && !ended) {
        if (st !== 'disconnected') ended = true;
        console.log(`[${ts()}] ice-pair ${JSON.stringify({ pc: id, ev: st, ageS: Math.round((Date.now() - born) / 1000), ...(pair || { pair: null }) })}`);
      }
    });
  }
}

def('RTCPeerConnection',   ObservedRTCPeerConnection);
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
