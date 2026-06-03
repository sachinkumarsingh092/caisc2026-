"""OpenEvolve evaluator for the etcd-io/raft maybeSendAppend evolution.

For each candidate raft.go file produced by the LLM:

  1. Copy the candidate into raft-upstream/raft.go.
  2. Verify it compiles (go build ./... in raft-upstream).
  3. Build the deterministic-simulation harness binary.
  4. Run N seeds of the harness; if ANY seed reports an invariant
     violation, reject the candidate (combined_score = 0).
  5. Measure throughput via the in-tree BenchmarkProposal3Nodes.
  6. Report combined_score = baseline_ns_op / candidate_ns_op, so
     values > 1 indicate a real improvement over the pinned baseline.

The evaluator is intentionally noisy-friendly: a single high outlier in
the bench will not score well because BENCH_COUNT=2 (best of two).

All paths are computed relative to this file so the script can be run
from any working directory.
"""

from __future__ import annotations

import os
import re
import shutil
import statistics
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RAFT_UPSTREAM = REPO / "raft-upstream"
DST_DIR = REPO / "harness" / "dst"
CACHE = HERE / ".cache"
DST_BIN = CACHE / "dst-candidate"
RAFT_GO = RAFT_UPSTREAM / "raft.go"
RAFT_BASELINE = RAFT_UPSTREAM / "raft.go.baseline"

N_SEEDS = 8
TICKS = 1500
NODES = 3
SWEEP_TIMEOUT_S = 30
BUILD_TIMEOUT_S = 90
BENCH_TIMEOUT_S = 60
BENCH_TIME = "1s"
BENCH_COUNT = 5

# Pinned baseline (3-node Propose+Apply cycle). Take the *minimum* ns/op
# across BENCH_COUNT runs to discount transient system load. The constant
# scales combined_score so the unmodified baseline scores ~1.0; what
# matters for evolution is monotonicity, not absolute calibration.
#
# Calibration values measured by harness/optimize/calibrate-on-droplet.sh:
#   darwin/arm64 (M-series Mac):  2400 ns/op
#   linux/amd64  (DO c-16):       4688 ns/op
import platform
BASELINE_NS_OP = 4688.0 if platform.machine() == 'x86_64' else 2400.0

BENCH_LINE_RE = re.compile(
    r"^BenchmarkProposal3Nodes(?:-\d+)?\s+\d+\s+(\d+(?:\.\d+)?)\s+ns/op",
    re.MULTILINE,
)

_BENCH_ANY_LINE_RE = re.compile(
    r"^\s*\d+\s+(\d+(?:\.\d+)?)\s+ns/op", re.MULTILINE
)


def _run(cmd, cwd, timeout, extra_env=None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def _restore_baseline():
    if RAFT_BASELINE.exists():
        shutil.copyfile(RAFT_BASELINE, RAFT_GO)


def _parse_ns_op(stdout: str):
    """Return min ns/op across all BenchmarkProposal3Nodes lines.

    Minimum across runs is more robust than mean/median to background system
    load: a low value is hard to fake (it has to actually achieve that rate
    for benchtime). Higher values are typically system-noise excursions.
    """
    matches = BENCH_LINE_RE.findall(stdout)
    if not matches:
        matches = _BENCH_ANY_LINE_RE.findall(stdout)
    if not matches:
        return None
    return min(float(m) for m in matches)


def _parse_ns_op_list(stdout: str):
    """Return every BenchmarkProposal3Nodes ns/op sample, in run order.

    Recorded alongside the min so that min- vs median-ranking can be
    compared post hoc on identical candidates (the in-loop selection
    metric below is left unchanged at min-of-N).
    """
    matches = BENCH_LINE_RE.findall(stdout)
    if not matches:
        matches = _BENCH_ANY_LINE_RE.findall(stdout)
    return [float(m) for m in matches]


def evaluate(program_path: str) -> dict:
    CACHE.mkdir(parents=True, exist_ok=True)
    base = {"combined_score": 0.0, "stage": "init"}

    # 1. Stage the candidate.
    try:
        shutil.copyfile(program_path, RAFT_GO)
    except Exception as e:
        return {**base, "stage": "copy_candidate", "err": str(e)}

    try:
        # 2. Upstream package compiles?
        r = _run(["go", "build", "./..."], cwd=RAFT_UPSTREAM, timeout=BUILD_TIMEOUT_S)
        if r.returncode != 0:
            return {**base, "stage": "go_build_upstream", "err": r.stderr[:400]}

        # 3. Harness builds against the candidate?
        r = _run(
            ["go", "build", "-buildvcs=false", "-o", str(DST_BIN), "./cmd/dst"],
            cwd=DST_DIR,
            timeout=BUILD_TIMEOUT_S,
        )
        if r.returncode != 0:
            return {**base, "stage": "go_build_dst", "err": r.stderr[:400]}

        # 4. DST sweep.
        t0 = time.time()
        violations = 0
        for seed in range(1, N_SEEDS + 1):
            try:
                r = _run(
                    [str(DST_BIN), f"-seed={seed}", f"-ticks={TICKS}", f"-nodes={NODES}"],
                    cwd=DST_DIR,
                    timeout=SWEEP_TIMEOUT_S,
                )
            except subprocess.TimeoutExpired:
                violations += 1
                continue
            if r.returncode != 0 or "OK" not in r.stdout:
                violations += 1
        sweep_secs = time.time() - t0
        if violations > 0:
            return {
                **base,
                "stage": "dst_sweep",
                "violations": violations,
                "sweep_secs": sweep_secs,
            }

        # 5. Benchmark.
        r = _run(
            [
                "go", "test",
                "-bench=BenchmarkProposal3Nodes",
                "-benchmem",
                f"-benchtime={BENCH_TIME}",
                f"-count={BENCH_COUNT}",
                "-run=^$",
                "./rafttest",
            ],
            cwd=RAFT_UPSTREAM,
            timeout=BENCH_TIMEOUT_S,
        )
        if r.returncode != 0:
            return {**base, "stage": "bench", "err": r.stderr[:400]}

        ns_op = _parse_ns_op(r.stdout)
        if ns_op is None or ns_op <= 0:
            return {**base, "stage": "bench_parse", "raw": r.stdout[-400:]}

        ns_op_all = _parse_ns_op_list(r.stdout)
        ns_op_median = statistics.median(ns_op_all) if ns_op_all else ns_op

        # combined_score stays min-based so this run is a faithful
        # reproducibility replicate. The full distribution + median are
        # recorded only for post-hoc paired analysis; they do not steer
        # selection.
        speedup = BASELINE_NS_OP / ns_op
        return {
            "combined_score": float(speedup),
            "speedup": float(speedup),
            "ns_op": float(ns_op),
            "ns_op_median": float(ns_op_median),
            "ns_op_all": [float(x) for x in ns_op_all],
            "baseline_ns_op": float(BASELINE_NS_OP),
            "violations": 0,
            "sweep_secs": float(sweep_secs),
            "stage": "ok",
        }
    finally:
        # Always restore baseline so a broken candidate doesn't poison
        # subsequent iterations or interactive runs.
        _restore_baseline()
