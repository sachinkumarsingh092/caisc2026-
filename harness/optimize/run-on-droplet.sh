#!/usr/bin/env bash
# Launch a full OpenEvolve sweep on the droplet inside tmux so the SSH
# session can disconnect. Run on your Mac:
#   ./run-on-droplet.sh root@DROPLET_IP 30          # 30-iter rerun
#   ./run-on-droplet.sh root@DROPLET_IP 100 explore # 100-iter explore
set -euo pipefail

TARGET="${1:?usage: $0 <user@host> [iters] [tag]}"
ITERS="${2:-30}"
TAG="${3:-run}"
REMOTE_DIR="${REMOTE_DIR:-/root/caisc2026}"
STAMP=$(date +%Y%m%d-%H%M%S)
SESSION="oe-${TAG}-${STAMP}"
OUT_DIR="harness/optimize/out-${TAG}-${STAMP}"

ssh "$TARGET" bash -s <<EOF
set -euo pipefail
cd $REMOTE_DIR
export PATH=/usr/local/go/bin:\$HOME/.local/bin:\$PATH
/bin/cp raft-upstream/raft.go.baseline raft-upstream/raft.go
mkdir -p $OUT_DIR

# Symlink so run.sh's hardcoded out/ path lands inside our tagged dir
rm -rf harness/optimize/out
ln -sf \$PWD/$OUT_DIR harness/optimize/out

# Launch inside tmux; tee to a log file the Mac can poll
tmux kill-session -t $SESSION 2>/dev/null || true
tmux new-session -d -s $SESSION "cd $REMOTE_DIR && ./harness/optimize/run.sh $ITERS 2>&1 | tee $OUT_DIR/run.log"
echo "launched tmux session: $SESSION"
echo "output dir:            $OUT_DIR"
EOF

echo ""
echo "================================================================"
echo "  session:    $SESSION"
echo "  iters:      $ITERS"
echo "  output:     $REMOTE_DIR/$OUT_DIR"
echo "================================================================"
echo ""
echo "follow live:    ssh $TARGET 'tail -f $REMOTE_DIR/$OUT_DIR/run.log'"
echo "attach tmux:    ssh $TARGET 'tmux attach -t $SESSION'"
echo "list sessions:  ssh $TARGET 'tmux ls'"
echo "pull results:   bash $(dirname "$0")/pull-from-droplet.sh $TARGET $OUT_DIR"
