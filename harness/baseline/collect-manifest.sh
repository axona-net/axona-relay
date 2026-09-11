#!/usr/bin/env bash
# Thin wrapper so the path named to council exists; the collector is the .mjs
# beside it (JSON assembly in bash is where quoting bugs live). Read-only.
set -euo pipefail
exec node "$(dirname "$0")/collect-manifest.mjs" "$@"
