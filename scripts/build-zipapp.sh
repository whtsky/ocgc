#!/usr/bin/env bash
#
# Build ocgc as a self-contained zipapp that bundles its dependencies
# (click, rich). The result runs anywhere a python3 is on PATH -- no venv,
# pip, or uv is needed at runtime -- so it is safe to drop into a synced
# ~/bin that is shared across machines.
#
# Usage:
#   scripts/build-zipapp.sh [OUTPUT]
#
#   OUTPUT   where to write the executable (default: dist/ocgc).
#            Pass an install path to build straight into place, e.g.
#            scripts/build-zipapp.sh ~/.bin/ocgc

set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

output="${1:-dist/ocgc}"

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is required -- see https://docs.astral.sh/uv/" >&2
  exit 1
fi

mkdir -p "$(dirname "$output")"

uvx shiv \
  --console-script ocgc \
  --python "/usr/bin/env python3" \
  --output-file "$output" \
  .

chmod +x "$output"

printf 'Built %s (%s)\n' "$output" "$(du -h "$output" | cut -f1)"
