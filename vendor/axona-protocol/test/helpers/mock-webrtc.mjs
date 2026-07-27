// =====================================================================
// mock-webrtc.mjs — a fault-injectable RTCPeerConnection / RTCDataChannel
// pair, faithful to exactly the surface MeshManager (src/transport/web/
// mesh.js) touches. Installed as globalThis.RTCPeerConnection so the REAL
// _attachPc / _initiateTo / _handleOffer / onSignal paths run, then the
// test drives the PC into any state the real transports can't reach under
// loopback (node-datachannel never fails ICE): never-open, failed, closed,
// ice-disconnected, send-throw, glare.
//
// Surface MeshManager uses (audited):
//   pc: onicecandidate, onconnectionstatechange, oniceconnectionstatechange,
//       ondatachannel, connectionState, iceConnectionState, localDescription,
//       remoteDescription, createDataChannel, createOffer, createAnswer,
//       setLocalDescription, setRemoteDescription, addIceCandidate, getStats,
//       close
//   dc: onopen, onclose, onerror, onmessage, readyState, send, close
//
// Test hooks are the `sim*` methods. `MockRTCPeerConnection.created` is a
// registry of every PC the code-under-test constructed (reset per test).
// =====================================================================

let _fpSeq = 0;
function fakeSdp(kind) {
  // Include a parseable DTLS fingerprint line so any fingerprint extraction
  // path doesn't choke; the value is unique per PC.
  const fp = Array.from({ length: 32 }, (_, i) => ((_fpSeq + i) % 256).toString(16).padStart(2, '0')).join(':');
  _fpSeq += 1;
  return `v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=-\r\na=fingerprint:sha-256 ${fp.toUpperCase()}\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`;
}

export class MockDataChannel {
  constructor(label = 'axona') {
    this.label = label;
    this.readyState = 'connecting';
    this.onopen = null;
    this.onclose = null;
    this.onerror = null;
    this.onmessage = null;
    this._throwOnSend = false;
    this.sent = [];
  }
  send(data) {
    if (this._throwOnSend) throw new Error('InvalidStateError: mock dc send refused');
    if (this.readyState !== 'open') throw new Error(`InvalidStateError: dc not open (${this.readyState})`);
    this.sent.push(data);
  }
  close() {
    if (this.readyState === 'closed') return;
    this.readyState = 'closed';
    this.onclose?.({});
  }
  // ── test hooks ──
  simOpen()  { if (this.readyState === 'connecting') { this.readyState = 'open'; this.onopen?.({}); } }
  simError(msg = 'mock-dc-error') { this.onerror?.({ error: { message: msg } }); }
  simRemoteMessage(obj) { this.onmessage?.({ data: typeof obj === 'string' ? obj : JSON.stringify(obj) }); }
  failSends(on = true) { this._throwOnSend = on; }
}

export class MockRTCPeerConnection {
  static created = [];
  static reset() { MockRTCPeerConnection.created = []; }

  constructor(config = {}) {
    this.config = config;
    this.connectionState = 'new';
    this.iceConnectionState = 'new';
    this.signalingState = 'stable';
    this.localDescription = null;
    this.remoteDescription = null;
    this.onicecandidate = null;
    this.onconnectionstatechange = null;
    this.oniceconnectionstatechange = null;
    this.ondatachannel = null;
    this._dc = null;             // offerer's channel (createDataChannel)
    this._closed = false;
    MockRTCPeerConnection.created.push(this);
  }

  createDataChannel(label = 'axona') {
    this._dc = new MockDataChannel(label);
    return this._dc;
  }
  async createOffer()  { return { type: 'offer',  sdp: fakeSdp('offer')  }; }
  async createAnswer() { return { type: 'answer', sdp: fakeSdp('answer') }; }
  async setLocalDescription(d)  { this.localDescription  = d; }
  async setRemoteDescription(d) { this.remoteDescription = d; }
  async addIceCandidate(_c)     { /* accepted, no-op */ }
  async getStats() { return new Map(); }   // _refreshPath only does stats.forEach
  close() {
    this._closed = true;
    this.connectionState = 'closed';
    // NB: a real pc.close() does NOT fire onconnectionstatechange; _teardown
    // relies on that. The remote-initiated close is simClose() below.
  }

  // ── test hooks (fault injection) ──
  _setConnState(s) { this.connectionState = s; this.onconnectionstatechange?.({}); }
  _setIceState(s)  { this.iceConnectionState = s; this.oniceconnectionstatechange?.({}); }

  /** Drive a full successful connect: ICE connected + connectionState connected. */
  simConnected() { this._setIceState('connected'); this._setConnState('connected'); }
  /** connectionState → failed (ICE gave up). */
  simFail()  { this.iceConnectionState = 'failed'; this._setConnState('failed'); }
  /** connectionState → closed out from under us (remote close / abrupt drop). */
  simClose() { if (this._closed) return; this.connectionState = 'closed'; this.onconnectionstatechange?.({}); }
  /** ICE consent lost but not yet failed. */
  simIceDisconnect() { this._setIceState('disconnected'); }
  /** Open the offerer's data channel (fires dc.onopen → mesh marks 'open'). */
  simDcOpen() { this._dc?.simOpen(); }
  /** Responder side: the remote created a data channel (fires ondatachannel). */
  simRemoteDataChannel() { const dc = new MockDataChannel('axona'); this.ondatachannel?.({ channel: dc }); return dc; }
  /** Emit a local ICE candidate (fires onicecandidate → mesh relays it). */
  emitIceCandidate(over = {}) {
    const cand = { type: 'host', protocol: 'udp', address: '10.0.0.1', port: 5000, ...over,
                   toJSON() { return { candidate: 'mock', sdpMid: '0', sdpMLineIndex: 0 }; } };
    this.onicecandidate?.({ candidate: cand });
  }
  /** Emit the end-of-candidates null (mesh ignores it). */
  emitIceComplete() { this.onicecandidate?.({ candidate: null }); }
}

/** Install MockRTCPeerConnection as the global; returns an uninstall fn. */
export function installMockWebRTC() {
  const prev = globalThis.RTCPeerConnection;
  globalThis.RTCPeerConnection = MockRTCPeerConnection;
  MockRTCPeerConnection.reset();
  return () => { globalThis.RTCPeerConnection = prev; };
}
