#!/usr/bin/env bash
# Measure the droplet's clean baseline ns/op distribution. Run ON the droplet
# (or via: ssh DROPLET 'bash /root/caisc2026/harness/optimize/calibrate-on-droplet.sh').
#
# Runs 20 back-to-back -count=5 benches with nothing else competing and prints
# the resulting distribution, plus a recommended BASELINE_NS_OP for evaluator.py.
set -euo pipefail

REPO="${REPO:-/root/caisc2026}"
export PATH=/usr/local/go/bin:$PATH

cd "$REPO"
/bin/cp raft-upstream/raft.go.baseline raft-upstream/raft.go

echo "running 20 paired bench cycles on $(nproc) cores..."
TMP=$(mktemp)
trap 'rm -f $TMP' EXIT

for i in $(seq 1 20); do
  (cd raft-upstream && \
   go test -bench=BenchmarkProposal3Nodes -benchmem \
           -benchtime=1s -count=5 -run='^$' ./rafttest 2>&1) | \
    grep -oE '[0-9]+\s+ns/op' | awk '{print $1}' >> "$TMP"
done

echo ""
echo "samples collected: $(wc -l < "$TMP")"
sort -n "$TMP" | awk '
  BEGIN { n=0 }
  { a[n++] = $1+0; sum += $1 }
  END {
    printf "min:    %d ns/op\n", a[0]
    printf "p10:    %d ns/op\n", a[int(n*0.10)]
    printf "p25:    %d ns/op\n", a[int(n*0.25)]
    printf "median: %d ns/op\n", a[int(n*0.50)]
    printf "p75:    %d ns/op\n", a[int(n*0.75)]
    printf "p90:    %d ns/op\n", a[int(n*0.90)]
    printf "max:    %d ns/op\n", a[n-1]
    printf "mean:   %.1f ns/op\n", sum/n
  }
'

echo ""
echo "recommended BASELINE_NS_OP for evaluator.py: use the min."
echo "  (low value is hard to fake; high values are background noise.)"
