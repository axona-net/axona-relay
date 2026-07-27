// Bench worker — runs mint/verify trials for one candidate, OFF the main thread
// (matches the real mint path; keeps the UI responsive; isolates an OOM so the
// page can report it instead of dying silently). Module worker.
let candidate = null;

self.onmessage = async (e) => {
  const msg = e.data;
  try {
    if (msg.type === 'load') {
      candidate = await import(msg.candidateUrl);
      self.postMessage({ type: 'loaded', name: candidate.name ?? msg.candidateUrl });
      return;
    }
    if (msg.type === 'run') {
      const { pubkeyHex, difficulty, trials } = msg;
      for (let i = 0; i < trials; i++) {
        candidate.reset?.();
        const t0 = performance.now();
        const witness = await candidate.mint(pubkeyHex, difficulty);
        const t1 = performance.now();
        const ok = await candidate.verify(pubkeyHex, witness, difficulty);
        const t2 = performance.now();
        self.postMessage({
          type: 'trial',
          i,
          mintMs:       t1 - t0,
          verifyMs:     t2 - t1,
          ok,
          witnessLen:   typeof witness === 'string' ? witness.length : -1,
          peakMemBytes: candidate.peakMemoryBytes?.() ?? 0,
        });
      }
      self.postMessage({ type: 'done' });
    }
  } catch (err) {
    self.postMessage({ type: 'error', message: String((err && err.message) || err) });
  }
};
