#!/usr/bin/env python3
"""Post-hoc min- vs median-ranking analysis for an evolution run.

Parses a run.log, extracts every evaluated candidate's full bench
distribution (recorded by the augmented evaluator), and compares the
in-loop min-of-N selection metric against a median-of-N metric on the
*same* candidates. Demonstrates whether min-of-N misranks the field.

Usage:
    python3 analyze_rerun.py <run.log> [min_baseline_ns] [median_baseline_ns]

Baselines default to the droplet calibration (min 4905, median 8972).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

EVAL_RE = re.compile(
    r"Evaluated program (\S+) in [\d.]+s: "
    r"combined_score=([\d.]+), speedup=([\d.]+), "
    r"ns_op=([\d.]+), ns_op_median=([\d.]+), "
    r"ns_op_all=\[([^\]]*)\], baseline_ns_op=([\d.]+), "
    r"violations=([\d.]+), sweep_secs=[\d.]+, stage=(\w+)"
)


def main():
    log = Path(sys.argv[1])
    min_base = float(sys.argv[2]) if len(sys.argv) > 2 else 4905.0
    med_base = float(sys.argv[3]) if len(sys.argv) > 3 else 8972.0

    cands = {}  # id -> record (last wins)
    for line in log.read_text().splitlines():
        m = EVAL_RE.search(line)
        if not m:
            continue
        pid = m.group(1)
        ns_min = float(m.group(4))
        ns_med = float(m.group(5))
        cands[pid] = {
            "id": pid,
            "ns_min": ns_min,
            "ns_median": ns_med,
            "violations": float(m.group(8)),
            "stage": m.group(9),
            # speedups (>1 == faster than baseline)
            "min_speedup": min_base / ns_min,
            "median_speedup": med_base / ns_med,
        }

    recs = list(cands.values())
    n = len(recs)
    accepted = [r for r in recs if r["stage"] == "ok" and r["violations"] == 0]

    by_min = sorted(recs, key=lambda r: r["min_speedup"], reverse=True)
    by_med = sorted(recs, key=lambda r: r["median_speedup"], reverse=True)

    best_min = by_min[0]
    best_med = by_med[0]
    med_rank_of_min_best = by_med.index(best_min) + 1
    min_rank_of_med_best = by_min.index(best_med) + 1

    n_min_improve = sum(1 for r in recs if r["min_speedup"] >= 1.0)
    n_med_improve = sum(1 for r in recs if r["median_speedup"] >= 1.0)

    print(f"candidates evaluated : {n}")
    print(f"accepted by gate     : {len(accepted)}/{n} "
          f"(stage=ok, 0 invariant violations)")
    print(f"baselines            : min={min_base:.0f}  median={med_base:.0f} ns/op")
    print()
    print(f"apparent improvements (speedup >= 1.0):")
    print(f"   under min-of-N     : {n_min_improve}/{n}")
    print(f"   under median-of-N  : {n_med_improve}/{n}")
    print()
    print("best candidate by MIN-of-N (the in-loop metric):")
    print(f"   {best_min['id'][:8]}  min_speedup={best_min['min_speedup']:.4f}"
          f"  median_speedup={best_min['median_speedup']:.4f}"
          f"  (median rank #{med_rank_of_min_best} of {n})")
    print("best candidate by MEDIAN-of-N:")
    print(f"   {best_med['id'][:8]}  median_speedup={best_med['median_speedup']:.4f}"
          f"  min_speedup={best_med['min_speedup']:.4f}"
          f"  (min rank #{min_rank_of_med_best} of {n})")
    print()
    print("top 5 by min-of-N      ->  (median_speedup in parens):")
    for r in by_min[:5]:
        print(f"   {r['id'][:8]}  min={r['min_speedup']:.4f}  "
              f"(med={r['median_speedup']:.4f})")
    print("top 5 by median-of-N   ->  (min_speedup in parens):")
    for r in by_med[:5]:
        print(f"   {r['id'][:8]}  med={r['median_speedup']:.4f}  "
              f"(min={r['min_speedup']:.4f})")

    out = {
        "n_candidates": n,
        "n_accepted": len(accepted),
        "min_baseline_ns": min_base,
        "median_baseline_ns": med_base,
        "n_apparent_improve_min": n_min_improve,
        "n_apparent_improve_median": n_med_improve,
        "best_by_min": best_min,
        "best_by_median": best_med,
        "median_rank_of_min_best": med_rank_of_min_best,
        "min_rank_of_median_best": min_rank_of_med_best,
    }
    dest = log.parent / "rerun_ranking_analysis.json"
    dest.write_text(json.dumps(out, indent=2))
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
