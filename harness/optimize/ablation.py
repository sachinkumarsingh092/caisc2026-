#!/usr/bin/env python3
"""Planted-bug ablation for the verification harness.

We deliberately break ``maybeSendAppend`` in several ways and push each
broken variant through every harness layer, recording which layer first
rejects it. This converts the claim "the harness gate is load-bearing"
from an assertion into a measurement.

Layers, in increasing cost / order of application:

  L0  compile         go build ./...                      (upstream package)
  L1  invariants      dst -seed=1..N                       (LogPrefix /
                                                            leader uniqueness)
  L2  linearizability dst -seed=1..N -lin-check            (Porcupine)
  L3  upstream tests  go test -count=1 .                   (etcd-io/raft suite)

A variant is "caught" by the first layer that rejects it. ``baseline`` is a
control that must pass every layer.

Run on the droplet (has Go + the patched raft-upstream):

    ssh root@HOST 'cd /root/caisc2026 && \
        PATH=/usr/local/go/bin:$PATH python3 harness/optimize/ablation.py'

Writes ``ablation_results.json`` next to this script and prints a table.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
RAFT_UPSTREAM = REPO / "raft-upstream"
DST_DIR = REPO / "harness" / "dst"
CACHE = HERE / ".cache"
DST_BIN = CACHE / "dst-ablation"
RAFT_GO = RAFT_UPSTREAM / "raft.go"
RAFT_BASELINE = RAFT_UPSTREAM / "raft.go.baseline"
RESULTS = HERE / "ablation_results.json"

N_SEEDS = 12
TICKS = 1500
NODES = 3
SWEEP_TIMEOUT_S = 40
BUILD_TIMEOUT_S = 120
TEST_TIMEOUT_S = 180

# Each variant is a list of (old, new) string substitutions applied to the
# pinned baseline raft.go. The substrings are verified unique before use.
VARIANTS = [
    {
        "name": "baseline",
        "contract": "(control) unmodified pinned baseline",
        "edits": [],
    },
    {
        "name": "method_invention",
        "contract": "signature/API: calls nonexistent pr.IsHealthy()",
        "edits": [("if pr.IsPaused() {\n\t\treturn false\n\t}",
                   "if pr.IsHealthy() {\n\t\treturn false\n\t}")],
    },
    {
        "name": "drop_progress",
        "contract": "progress accounting: omits SentEntries/SentCommit",
        "edits": [("\tpr.SentEntries(len(ents), uint64(payloadsSize(ents)))\n"
                   "\tpr.SentCommit(r.raftLog.committed)\n", "")],
    },
    {
        "name": "lie_commit",
        "contract": "safety: advertises commit=lastIndex, not committed",
        "edits": [("Commit:  new(r.raftLog.committed),",
                   "Commit:  new(r.raftLog.lastIndex()),")],
    },
    {
        "name": "skip_pause",
        "contract": "throttle short-circuit: ignores pr.IsPaused()",
        "edits": [("if pr.IsPaused() {\n\t\treturn false\n\t}",
                   "if false {\n\t\treturn false\n\t}")],
    },
    {
        "name": "off_by_one_prev",
        "contract": "log matching: prevIndex = pr.Next (off by one)",
        "edits": [("prevIndex := pr.Next - 1", "prevIndex := pr.Next")],
    },
]

_FAIL_RE = re.compile(r"^FAIL\s+seed=\d+\s+(.*)$", re.MULTILINE)
_LIN_RE = re.compile(r"^LIN\s+seed=\d+\s+(\w+)", re.MULTILINE)


def _run(cmd, cwd, timeout, extra_env=None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        cmd, cwd=str(cwd), capture_output=True, text=True,
        timeout=timeout, env=env,
    )


def _apply_edits(text: str, edits) -> str:
    for old, new in edits:
        n = text.count(old)
        if n != 1:
            raise ValueError(f"substring not unique (count={n}): {old!r}")
        text = text.replace(old, new)
    return text


def _restore_baseline():
    shutil.copyfile(RAFT_BASELINE, RAFT_GO)


def _invariant_sweep():
    """Return (caught, detail). caught is the 1-based seed that first FAILs,
    or 0 if all seeds pass."""
    for seed in range(1, N_SEEDS + 1):
        try:
            r = _run([str(DST_BIN), f"-seed={seed}", f"-ticks={TICKS}",
                      f"-nodes={NODES}"], DST_DIR, SWEEP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return seed, f"timeout@seed{seed}"
        if r.returncode != 0 or "OK" not in r.stdout:
            m = _FAIL_RE.search(r.stdout)
            detail = m.group(1) if m else (r.stdout.strip()[:120] or
                                           r.stderr.strip()[:120])
            return seed, detail
    return 0, ""


def _lin_sweep():
    """Return dict of LIN outcome tallies across seeds."""
    tally = {"OK": 0, "FAIL": 0, "UNKNOWN": 0, "other": 0}
    for seed in range(1, N_SEEDS + 1):
        try:
            r = _run([str(DST_BIN), f"-seed={seed}", f"-ticks={TICKS}",
                      f"-nodes={NODES}", "-lin-check"], DST_DIR,
                     SWEEP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            tally["UNKNOWN"] += 1
            continue
        m = _LIN_RE.search(r.stdout)
        if m and m.group(1) in tally:
            tally[m.group(1)] += 1
        else:
            tally["other"] += 1
    return tally


def _upstream_tests():
    """Run the upstream root-package test suite. Return (passed, detail)."""
    try:
        r = _run(["go", "test", "-count=1", f"-timeout={TEST_TIMEOUT_S}s", "."],
                 RAFT_UPSTREAM, TEST_TIMEOUT_S + 20)
    except subprocess.TimeoutExpired:
        return False, "test-timeout"
    if r.returncode == 0:
        return True, "ok"
    m = re.search(r"^--- FAIL: (\S+)", r.stdout, re.MULTILINE)
    detail = m.group(1) if m else (r.stdout.strip().splitlines() or
                                    ["fail"])[-1][:120]
    return False, detail


def evaluate_variant(v) -> dict:
    out = {"name": v["name"], "contract": v["contract"]}
    baseline_text = RAFT_BASELINE.read_text()
    try:
        candidate = _apply_edits(baseline_text, v["edits"])
    except ValueError as e:
        return {**out, "caught_by": "PATCH_ERROR", "detail": str(e)}

    RAFT_GO.write_text(candidate)

    # L0: compile.
    r = _run(["go", "build", "./..."], RAFT_UPSTREAM, BUILD_TIMEOUT_S)
    if r.returncode != 0:
        return {**out, "caught_by": "L0_compile",
                "detail": r.stderr.strip().splitlines()[-1][:160]
                if r.stderr.strip() else "build failed"}

    # Build the harness against this candidate.
    r = _run(["go", "build", "-buildvcs=false", "-o", str(DST_BIN), "./cmd/dst"],
             DST_DIR, BUILD_TIMEOUT_S)
    if r.returncode != 0:
        return {**out, "caught_by": "L0_compile_dst",
                "detail": r.stderr.strip().splitlines()[-1][:160]
                if r.stderr.strip() else "dst build failed"}

    # L1: invariant sweep.
    t0 = time.time()
    caught_seed, inv_detail = _invariant_sweep()
    out["inv_secs"] = round(time.time() - t0, 1)
    out["inv_caught_seed"] = caught_seed

    # L2: linearizability sweep (always run, to record the floor too).
    t0 = time.time()
    lin = _lin_sweep()
    out["lin_secs"] = round(time.time() - t0, 1)
    out["lin"] = lin

    # L3: upstream tests.
    tests_pass, test_detail = _upstream_tests()
    out["upstream_tests"] = "pass" if tests_pass else f"FAIL:{test_detail}"

    # Attribute to the first layer that rejected the candidate.
    if caught_seed > 0:
        out["caught_by"] = "L1_invariants"
        out["detail"] = f"seed {caught_seed}: {inv_detail}"
    elif lin["FAIL"] > 0:
        out["caught_by"] = "L2_linearizability"
        out["detail"] = f"{lin['FAIL']}/{N_SEEDS} seeds non-linearizable"
    elif not tests_pass:
        out["caught_by"] = "L3_upstream_tests"
        out["detail"] = test_detail
    else:
        out["caught_by"] = "NOT_CAUGHT"
        out["detail"] = "passed every layer"
    return out


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    results = []
    try:
        for v in VARIANTS:
            print(f"\n=== {v['name']}: {v['contract']}")
            res = evaluate_variant(v)
            print(f"    caught_by = {res['caught_by']}  "
                  f"({res.get('detail','')})")
            results.append(res)
    finally:
        _restore_baseline()

    RESULTS.write_text(json.dumps(results, indent=2))

    print("\n" + "=" * 78)
    print(f"{'variant':<18}{'contract':<42}{'caught by'}")
    print("-" * 78)
    for r in results:
        print(f"{r['name']:<18}{r['contract'][:40]:<42}{r['caught_by']}")
    print("=" * 78)
    print(f"\nwrote {RESULTS}")


if __name__ == "__main__":
    main()
