#!/usr/bin/env bash
# Run the OpenEvolve loop against etcd-io/raft.
# Usage: ./run.sh [iterations]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ITERS="${1:-20}"

# Load API key from project .env (gitignored).
if [ -f "$REPO/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO/.env"
  set +a
fi

if [ -z "${MODEL_ACCESS_KEY:-}" ]; then
  echo "MODEL_ACCESS_KEY is not set; refusing to run." >&2
  exit 2
fi

# Always start from the pinned baseline.
cp "$REPO/raft-upstream/raft.go.baseline" "$REPO/raft-upstream/raft.go"

mkdir -p "$HERE/out"

"$REPO/.venv/bin/openevolve-run" \
  "$HERE/initial_program.go" \
  "$HERE/evaluator.py" \
  -c "$HERE/config.yaml" \
  -o "$HERE/out" \
  -i "$ITERS" \
  -l INFO
