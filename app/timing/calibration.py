"""Calibrate the finish-line estimator against ground truth.

Every finish-line event stores the raw observations behind it
(``GET /api/v1/events/{id}/observations``, table ``event_observations``).  Record
some passes with an independent reference (photo-finish, video with a synced
clock, a manual timing device), then run::

    python -m app.timing.calibration --db data/otreader.db --truth truth.csv

``truth.csv`` columns: ``event_id,truth`` or ``epc,truth`` (``truth`` = ISO 8601
with timezone).  With ``epc`` the event nearest to the truth time is used.

The tool re-runs the estimator over a grid of settings and prints, for each, the
signed bias and absolute error statistics, then recommends settings and the
``timing.finish_time_offset_ms`` that removes the median bias.  Treat results from
fewer than ~30 passes with caution.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import sqlite3
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from statistics import mean, median
from typing import Any

from app.clock import datetime_to_us
from app.timing.finish_estimator import EstimatorSettings, Point, estimate_crossing


@dataclass
class CalibrationPass:
    key: str
    points: list[Point]
    truth_us: int


def points_from_compact(obs: list[dict[str, Any]]) -> list[Point]:
    return [
        Point(
            t_us=o["t"], rssi=o.get("r"), antenna=o.get("a"), doppler=o.get("d"),
            host_timestamp=o.get("src") == "host_receive", aggregated=(o.get("n") or 1) > 1,
        )
        for o in obs
    ]


def evaluate(passes: list[CalibrationPass], settings: EstimatorSettings) -> dict[str, Any]:
    errors = []
    for p in passes:
        est = estimate_crossing(p.points, settings)
        errors.append((est.timestamp_us - p.truth_us) / 1000.0)
    if not errors:
        return {"n": 0}
    abs_err = sorted(abs(e) for e in errors)
    p95 = abs_err[min(len(abs_err) - 1, int(round(0.95 * (len(abs_err) - 1))))]
    return {
        "n": len(errors),
        "bias_mean_ms": round(mean(errors), 2),
        "bias_median_ms": round(median(errors), 2),
        "abs_median_ms": round(median(abs_err), 2),
        "abs_p95_ms": round(p95, 2),
        "abs_max_ms": round(abs_err[-1], 2),
    }


def grid_search(passes: list[CalibrationPass], base: EstimatorSettings | None = None) -> list[dict[str, Any]]:
    base = base or EstimatorSettings()
    results = []
    for top, minpts, gap in itertools.product((6.0, 10.0, 15.0), (4, 5, 7), (0.5, 1.0, 2.0)):
        s = replace(base, top_window_db=top, min_fit_points=minpts, cluster_gap_s=gap, time_offset_ms=0.0)
        stats = evaluate(passes, s)
        # Error after removing the constant bias (what a calibrated offset would achieve).
        corrected = evaluate(passes, replace(s, time_offset_ms=-stats["bias_median_ms"])) if stats["n"] else stats
        results.append({"top_window_db": top, "min_fit_points": minpts, "cluster_gap_s": gap, "raw": stats,
                        "with_offset": corrected, "suggested_offset_ms": -stats.get("bias_median_ms", 0.0)})
    results.sort(key=lambda r: (r["with_offset"].get("abs_p95_ms", 1e18), r["with_offset"].get("abs_median_ms", 1e18)))
    return results


def load_passes(db: str, truth_csv: str) -> list[CalibrationPass]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    passes = []
    with open(truth_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            truth_us = datetime_to_us(datetime.fromisoformat(row["truth"].replace("Z", "+00:00")))
            if row.get("event_id"):
                eid = row["event_id"]
            else:
                r = conn.execute(
                    "SELECT event_id FROM events WHERE epc=? AND mode='finish_line'"
                    " ORDER BY ABS(timestamp_us - ?) LIMIT 1",
                    (row["epc"].upper(), truth_us),
                ).fetchone()
                if r is None:
                    print(f"warning: no event for {row['epc']}", file=sys.stderr)
                    continue
                eid = r[0]
            o = conn.execute("SELECT observations FROM event_observations WHERE event_id=?", (eid,)).fetchone()
            if o is None:
                print(f"warning: no observations for event {eid}", file=sys.stderr)
                continue
            passes.append(CalibrationPass(eid, points_from_compact(json.loads(o[0])), truth_us))
    conn.close()
    return passes


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Calibrate the finish-line estimator")
    ap.add_argument("--db", required=True, help="path to otreader.db (read-only access)")
    ap.add_argument("--truth", required=True, help="CSV with event_id|epc and truth columns")
    ap.add_argument("--top", type=int, default=5, help="show the N best settings")
    args = ap.parse_args(argv)
    passes = load_passes(args.db, args.truth)
    if not passes:
        sys.exit("no calibration passes loaded")
    print(f"Loaded {len(passes)} passes. Current defaults: {evaluate(passes, EstimatorSettings())}")
    results = grid_search(passes)
    for r in results[: args.top]:
        print(json.dumps(r))
    best = results[0]
    print(
        "\nRecommended timing settings:\n"
        f"  finish_top_window_db: {best['top_window_db']}\n"
        f"  finish_min_fit_points: {best['min_fit_points']}\n"
        f"  finish_cluster_gap_seconds: {best['cluster_gap_s']}\n"
        f"  finish_time_offset_ms: {best['suggested_offset_ms']}"
    )
    if len(passes) < 30:
        print("\nNOTE: fewer than 30 passes - treat these recommendations as provisional.")


if __name__ == "__main__":
    main()
