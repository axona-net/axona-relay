// sparkline.js — a tiny hand-rolled canvas line+area chart (no charting lib).
// Ring buffer of recent samples, DPR-scaled backing store, redraw on demand.
// Exposes window.makeSparkline (loaded as a classic script before renderer.js).

(function () {
  function cssVar(name, fallback) {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  }

  function makeSparkline(canvas, opts) {
    opts = opts || {};
    const capacity = opts.capacity || 120;
    const ctx = canvas.getContext('2d');
    const buf = [];
    let W = 0, H = 0, color = opts.color || cssVar('--green', '#50c060');

    function resize() {
      const dpr = window.devicePixelRatio || 1;
      const r = canvas.getBoundingClientRect();
      W = Math.max(1, Math.round(r.width));
      H = Math.max(1, Math.round(r.height));
      canvas.width = Math.round(W * dpr);
      canvas.height = Math.round(H * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }

    function push(v) {
      buf.push(Number.isFinite(+v) ? +v : 0);
      if (buf.length > capacity) buf.shift();
    }

    function draw() {
      ctx.clearRect(0, 0, W, H);
      const n = buf.length;
      if (n < 2) return;
      let max = 1;
      for (const v of buf) if (v > max) max = v;
      const x = (i) => (i / (capacity - 1)) * W;
      const y = (v) => H - (v / max) * (H - 4) - 2;

      ctx.beginPath();
      for (let i = 0; i < n; i++) (i ? ctx.lineTo : ctx.moveTo).call(ctx, x(i), y(buf[i]));
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.5;
      ctx.lineJoin = 'round';
      ctx.stroke();

      // area fill under the line
      ctx.lineTo(x(n - 1), H);
      ctx.lineTo(x(0), H);
      ctx.closePath();
      ctx.globalAlpha = 0.12;
      ctx.fillStyle = color;
      ctx.fill();
      ctx.globalAlpha = 1;
    }

    resize();
    window.addEventListener('resize', () => { resize(); draw(); });

    return {
      push, draw, resize,
      setColor(c) { if (c && c !== color) { color = c; draw(); } },
    };
  }

  window.makeSparkline = makeSparkline;
})();
