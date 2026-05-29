#!/usr/bin/env bash
# Validate a single Go-emitted NDJSON Raft trace against the upstream
# Traceetcdraft.tla specification using the locally-vendored TLC.
#
# Usage:
#   tla-validate.sh <trace.ndjson>
#
# Returns 0 on success, non-zero on validation failure or tooling
# error. Designed to be Mac/BSD-sed compatible (the upstream
# validate.sh assumes GNU sed/nproc).
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <trace.ndjson>" >&2
  exit 64
fi

TRACE="$1"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
TLA_DIR="$REPO/raft-upstream/tla"
JARS="$TLA_DIR/jars"
SPEC="$TLA_DIR/Traceetcdraft.tla"
CONFIG="$TLA_DIR/Traceetcdraft.cfg"

if [ ! -f "$JARS/tla2tools.jar" ] || [ ! -f "$JARS/CommunityModules-deps.jar" ]; then
  echo "TLA+ jars missing under $JARS" >&2
  exit 65
fi
if [ ! -f "$SPEC" ] || [ ! -f "$CONFIG" ]; then
  echo "spec/config missing in $TLA_DIR" >&2
  exit 66
fi
if [ ! -f "$TRACE" ]; then
  echo "trace file not found: $TRACE" >&2
  exit 67
fi

# Stage trace into a temp copy so we don't mutate the source.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp "$TRACE" "$WORK/trace.ndjson"

# Preprocess (BSD-sed compatible: -i '' takes empty backup ext):
#   - strip any leading garbage before the first {
#   - sort by tick (third colon-separated token in each line)
sed -i '' -E 's/^[^{]+//' "$WORK/trace.ndjson"
sort -t":" -k 3 "$WORK/trace.ndjson" -o "$WORK/trace.ndjson"

# Run TLC with the spec.
cd "$TLA_DIR"
env JSON="$WORK/trace.ndjson" java \
  -XX:+UseParallelGC \
  -cp "$JARS/tla2tools.jar:$JARS/CommunityModules-deps.jar" \
  tlc2.TLC \
  -config "$CONFIG" "$SPEC" \
  -lncheck final \
  -metadir "$WORK/state" \
  -fpmem 0.9
