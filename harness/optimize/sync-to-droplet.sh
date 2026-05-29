#!/usr/bin/env bash
# Sync the project to a DO droplet. Run on your Mac.
#   ./sync-to-droplet.sh root@DROPLET_IP
#
# Excludes: Python venv, OpenEvolve output, raft-upstream .git history,
# LaTeX build artefacts, the .env (you must scp that separately so the
# key never sits in rsync transcripts).
set -euo pipefail

TARGET="${1:?usage: $0 <user@host>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
REMOTE_DIR="${REMOTE_DIR:-/root/caisc2026}"

echo "syncing $REPO/  →  $TARGET:$REMOTE_DIR/"

rsync -avz --delete \
  --exclude '.venv/' \
  --exclude 'raft-upstream/.git/' \
  --exclude 'harness/optimize/out/' \
  --exclude 'harness/optimize/.cache/' \
  --exclude 'oe-smoke/' \
  --exclude '.env' \
  --exclude 'paper-format/*.aux' \
  --exclude 'paper-format/*.log' \
  --exclude 'paper-format/*.pdf' \
  --exclude 'paper-format/*.out' \
  --exclude 'paper-format/*.bbl' \
  --exclude 'paper-format/*.blg' \
  --exclude '*.swp' \
  --exclude '.DS_Store' \
  "$REPO/" "$TARGET:$REMOTE_DIR/"

echo ""
echo "synced. next steps:"
echo "  1. one-time bootstrap on droplet:"
echo "     ssh $TARGET 'bash $REMOTE_DIR/harness/optimize/setup-droplet.sh'"
echo "  2. install the API key (once):"
echo "     scp $REPO/.env $TARGET:$REMOTE_DIR/.env"
echo "  3. calibrate the droplet's bench:"
echo "     ssh $TARGET 'bash $REMOTE_DIR/harness/optimize/calibrate-on-droplet.sh'"
echo "  4. drive a run:"
echo "     bash $HERE/run-on-droplet.sh $TARGET 30"
