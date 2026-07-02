#!/usr/bin/env bash
# =====================================================================
# sync-protocol.sh — refresh vendor/axona-protocol/ from the canonical
# @axona/protocol source (github.com/axona-net/axona-protocol), with the
# relay's test suite as a hard gate.
#
# Mirrors axona-peer's vendoring approach: the kernel src/ tree is copied
# WHOLE into vendor/axona-protocol/src/. No hand-maintained file lists —
# the previous per-directory/per-file lists silently dropped connect.js
# when the kernel grew a new top-level module (fixed 610faa8); a full-tree
# copy plus the diff -r completeness check below makes that class of rot
# structurally impossible.
#
# Run from the repo root:  npm run sync:protocol
# Override the source:     PROTOCOL_SRC=/path/to/axona-protocol/src npm run sync:protocol
#
# After a green run, commit the changed vendor/ files (+ version bump).
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROTOCOL_SRC="${PROTOCOL_SRC:-${REPO_ROOT}/../axona-protocol/src}"
VENDOR_DST="${REPO_ROOT}/vendor/axona-protocol/src"

if [ ! -d "${PROTOCOL_SRC}" ]; then
  echo "✗ Source not found at ${PROTOCOL_SRC}"
  echo "  Clone github.com/axona-net/axona-protocol as a sibling directory,"
  echo "  or set PROTOCOL_SRC=path/to/src"
  exit 1
fi

echo "→ Syncing from ${PROTOCOL_SRC}"
echo "  to            ${VENDOR_DST}"

rm -rf "${VENDOR_DST}"
mkdir -p "$(dirname "${VENDOR_DST}")"
cp -R "${PROTOCOL_SRC}" "${VENDOR_DST}"

echo "→ Completeness check (vendored tree must mirror the kernel src/ exactly)"
if ! diff -rq "${PROTOCOL_SRC}" "${VENDOR_DST}" > /dev/null; then
  echo "✗ vendored tree differs from the kernel source after copy:"
  diff -rq "${PROTOCOL_SRC}" "${VENDOR_DST}" | head -20
  exit 1
fi
echo "  ✓ trees identical"

KV=$(grep -oE "KERNEL_VERSION = '[^']+'" "${VENDOR_DST}/transport/handshake.js" || true)
echo "  ✓ vendored ${KV:-(kernel version unknown)}"

echo "→ npm test (re-vendor gate: syntax + import-load of the vendored graph)"
if ! (cd "${REPO_ROOT}" && npm test); then
  echo ""
  echo "✗ TEST FAILED against the freshly vendored kernel — do NOT commit."
  echo "  vendor/ is left in the failing state for inspection; restore with:"
  echo "    git checkout vendor/"
  exit 1
fi

echo ""
echo "✓ sync + gate green. Commit the vendor/ changes (+ relay version bump):"
( cd "${REPO_ROOT}" && git status --porcelain vendor/axona-protocol/ 2>/dev/null | head -20 || true )
