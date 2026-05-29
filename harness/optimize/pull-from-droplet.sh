#!/usr/bin/env bash
# Pull a tagged run's results back to your Mac for paper integration.
#   ./pull-from-droplet.sh root@DROPLET_IP harness/optimize/out-run-20260530-013022
set -euo pipefail

TARGET="${1:?usage: $0 <user@host> <remote-out-dir>}"
REMOTE_OUT="${2:?usage: $0 <user@host> <remote-out-dir>}"
REMOTE_DIR="${REMOTE_DIR:-/root/caisc2026}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
LOCAL_DEST="$REPO/$REMOTE_OUT"

mkdir -p "$LOCAL_DEST"
rsync -avz "$TARGET:$REMOTE_DIR/$REMOTE_OUT/" "$LOCAL_DEST/"
echo ""
echo "results in: $LOCAL_DEST"
echo "best:       $LOCAL_DEST/best/best_program.go"
echo "metrics:    $LOCAL_DEST/best/best_program_info.json"
echo "log:        $LOCAL_DEST/run.log"
