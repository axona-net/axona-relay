#!/usr/bin/env bash
# =====================================================================
# sync-protocol.sh — refresh vendor/axona-protocol/ from the canonical
# @axona/protocol source (github.com/axona-net/axona-protocol).
#
# Mirrors axona-peer's vendoring approach: a plain copy of the kernel
# src/ tree into vendor/axona-protocol/src/. The relay imports the kernel
# from there, so it ships a pinned, self-contained copy.
#
# Run from the repo root:  npm run sync:protocol
# Override the source:     PROTOCOL_SRC=/path/to/axona-protocol/src npm run sync:protocol
#
# After running, commit the changed vendor/ files.
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

mkdir -p "${VENDOR_DST}"
for dir in contracts crypto dht identity persistence pow pubsub transport utils; do
  if [ -d "${PROTOCOL_SRC}/${dir}" ]; then
    rm -rf "${VENDOR_DST}/${dir}"
    cp -r "${PROTOCOL_SRC}/${dir}" "${VENDOR_DST}/${dir}"
    echo "  ✓ ${dir}/"
  fi
done
for f in index.js errors.js; do
  if [ -f "${PROTOCOL_SRC}/${f}" ]; then
    cp "${PROTOCOL_SRC}/${f}" "${VENDOR_DST}/${f}"
    echo "  ✓ ${f}"
  fi
done

KV=$(grep -oE "KERNEL_VERSION = '[^']+'" "${VENDOR_DST}/transport/handshake.js" || true)
echo
echo "Vendored ${KV:-(kernel version unknown)}"
echo "Diff:"
( cd "${REPO_ROOT}" && git status --porcelain vendor/axona-protocol/ 2>/dev/null || true )
