// image.js — browser-only image downsample + JPEG compression for Axona apps.
//
// Extracted from the retired file-transport.js: chunking now lives entirely in
// the kernel's @axona/protocol/std/chunk (15 KB reliable-floor aware). Image
// compression is an APP concern (canvas/DOM), so it stays here. It runs BEFORE
// chunking, so the transport itself stays byte-exact and format-agnostic.

/**
 * Downsample + JPEG-compress an image File/Blob to <= maxBytes. Returns a Blob.
 * Lossy by design — only for images; never used for documents.
 */
export async function compressImage(fileOrBlob, { maxBytes = 1_000_000, maxDim = 2048, mime = 'image/jpeg' } = {}) {
  // Use an <img> element + a plain <canvas> + toBlob — the most broadly supported
  // path (works on iOS Safari, where OffscreenCanvas/createImageBitmap/convertToBlob
  // are inconsistent). All steps reject loudly so failures surface, not silently die.
  const img = await loadImageEl(fileOrBlob);
  const baseW = img.naturalWidth || img.width, baseH = img.naturalHeight || img.height;
  if (!baseW || !baseH) throw new Error('could not read image dimensions');
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  let scale = Math.min(1, maxDim / Math.max(baseW, baseH));
  let quality = 0.9;
  const render = () => new Promise((resolve, reject) => {
    canvas.width = Math.max(1, Math.round(baseW * scale));
    canvas.height = Math.max(1, Math.round(baseH * scale));
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    canvas.toBlob((b) => (b ? resolve(b) : reject(new Error('canvas.toBlob returned null'))), mime, quality);
  });
  let blob = await render();
  for (let attempt = 0; attempt < 9 && blob.size > maxBytes; attempt++) {
    if (quality > 0.45) quality -= 0.15; else scale *= 0.8;   // drop quality first, then dimensions
    blob = await render();
  }
  return blob;
}

function loadImageEl(blob) {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => { resolve(img); setTimeout(() => URL.revokeObjectURL(url), 5000); };
    img.onerror = () => { URL.revokeObjectURL(url); reject(new Error('image failed to load (unsupported format?)')); };
    img.src = url;
  });
}
