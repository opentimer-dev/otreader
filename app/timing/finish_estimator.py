"""Finish-line crossing-time estimation from RSSI profiles.

Physical model
--------------
As a tag passes an antenna, received signal strength rises, peaks near the point
of closest approach (usually when the tag is over/in front of the antenna) and
falls again.  Near the peak the RSSI-vs-time curve (in dB) is approximately a
downward parabola.  The estimator finds the time of that maximum.

This is an *estimate* of when the tag was closest to the antenna, not an exact
physical crossing time.  Antenna placement, body shadowing (tag on the far side
of the runner), multipath, reader frequency hopping and RSSI quantisation (1 dB
for standard LLRP PeakRSSI, 0.01 dB for Impinj PeakRSSI) all bias or blur the
result.  A constant geometric bias can be removed with ``finish_time_offset_ms``
determined by calibration (see :mod:`app.timing.calibration`).

Algorithm (per pass)
--------------------
1. Sort observations by reader timestamp; split into *clusters* wherever the gap
   between consecutive reads exceeds ``cluster_gap_s`` (a tag leaving and
   re-entering the field).  Choose the cluster with the strongest peak; clusters
   within ``peak_tie_db`` of it are treated as ties and the earliest wins (the
   first genuine crossing).  Multiple candidate clusters lower the confidence.
2. Within the chosen cluster, estimate per antenna (antennas have different gains
   and positions, so their profiles must not be mixed).  The antenna with the
   strongest peak is primary; disagreement between antennas lowers confidence.
3. Per-antenna estimate, in order of preference:
   * ``rssi_quadratic_fit`` - least-squares parabola through the reads within
     ``top_window_db`` of the peak level (needs >= ``min_fit_points``, concave fit,
     vertex inside the fitted span; retried once with a doubled window).  The peak
     level comes from a 3-point smoothed series because the raw maximum is biased
     upwards by noise.  Robust to noise and to 1 dB quantisation.
   * ``rssi_plateau_center`` - several reads tie at the maximum: centre of the tie.
   * ``rssi_peak_interpolation`` - three-point parabolic interpolation through the
     raw reads around the smoothed maximum (non-uniform spacing), clamped to the
     neighbouring reads.
   * ``rssi_peak_at_edge`` - the maximum is the first or last read: the true peak
     may lie outside the observed window; the edge timestamp is used.
   * ``strongest_of_two`` / ``two_point_midpoint`` - two reads.
   * ``single_observation`` - one read; the timestamp is used as-is.
   * ``timestamp_median`` - no RSSI available.
4. Optional Doppler corroboration (Impinj): the radial-velocity sign change nearest
   the estimate is reported in ``details``; it does not alter the estimate.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from statistics import median
from typing import Any

from app.models import Confidence, EstimationMethod

_CONF_ORDER = [Confidence.LOW, Confidence.MEDIUM, Confidence.HIGH]


def _downgrade(c: Confidence, steps: int = 1) -> Confidence:
    return _CONF_ORDER[max(0, _CONF_ORDER.index(c) - steps)]


@dataclass(frozen=True)
class EstimatorSettings:
    top_window_db: float = 10.0
    min_fit_points: int = 5
    cluster_gap_s: float = 1.0
    peak_tie_db: float = 3.0
    time_offset_ms: float = 0.0
    antenna_agreement_ms: float = 250.0
    doppler_agreement_ms: float = 150.0


@dataclass(slots=True)
class Point:
    t_us: int
    rssi: float | None
    antenna: int | None = None
    doppler: float | None = None
    host_timestamp: bool = False
    aggregated: bool = False  # reader merged several reads into this report


@dataclass
class Estimate:
    timestamp_us: int
    method: EstimationMethod
    confidence: Confidence
    estimated: bool
    antenna: int | None
    antennas: list[int]
    peak_rssi: float | None
    first_seen_us: int
    last_seen_us: int
    observation_count: int
    uncertainty_ms: float | None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class _GroupResult:
    t_us: float
    method: EstimationMethod
    confidence: Confidence
    uncertainty_ms: float | None
    n: int
    peak: float | None
    info: dict[str, Any] = field(default_factory=dict)


def _solve3(m: list[list[float]], v: list[float]) -> list[float] | None:
    """Solve a 3x3 linear system by Gaussian elimination with partial pivoting."""
    a = [row[:] + [v[i]] for i, row in enumerate(m)]
    for col in range(3):
        piv = max(range(col, 3), key=lambda r: abs(a[r][col]))
        if abs(a[piv][col]) < 1e-15:
            return None
        a[col], a[piv] = a[piv], a[col]
        for r in range(3):
            if r != col:
                f = a[r][col] / a[col][col]
                for c in range(col, 4):
                    a[r][c] -= f * a[col][c]
    return [a[i][3] / a[i][i] for i in range(3)]


def _inv3(m: list[list[float]]) -> list[list[float]] | None:
    cols = []
    for i in range(3):
        e = [1.0 if j == i else 0.0 for j in range(3)]
        s = _solve3(m, e)
        if s is None:
            return None
        cols.append(s)
    return [[cols[j][i] for j in range(3)] for i in range(3)]


def quadratic_fit(xs: list[float], ys: list[float]) -> tuple[float, float, float, float, list[list[float]]] | None:
    """Least-squares y = a x^2 + b x + c.  Returns (a, b, c, rms, covariance) or None."""
    n = len(xs)
    if n < 3:
        return None
    s = [sum(x**k for x in xs) for k in range(5)]
    t = [sum((x**k) * y for x, y in zip(xs, ys, strict=True)) for k in range(3)]
    xtx = [[s[4], s[3], s[2]], [s[3], s[2], s[1]], [s[2], s[1], s[0]]]
    sol = _solve3(xtx, [t[2], t[1], t[0]])
    if sol is None:
        return None
    a, b, c = sol
    ssr = sum((y - (a * x * x + b * x + c)) ** 2 for x, y in zip(xs, ys, strict=True))
    dof = max(1, n - 3)
    sigma2 = ssr / dof
    inv = _inv3(xtx)
    if inv is None:
        return None
    cov = [[sigma2 * inv[i][j] for j in range(3)] for i in range(3)]
    return a, b, c, math.sqrt(ssr / n), cov


def _smooth(values: list[float]) -> list[float]:
    """Centred 3-point moving average (2-point at the ends)."""
    n = len(values)
    return [sum(values[max(0, i - 1) : i + 2]) / len(values[max(0, i - 1) : i + 2]) for i in range(n)]


def _median_interval_ms(ts: list[int]) -> float | None:
    if len(ts) < 2:
        return None
    return median((b - a) / 1000.0 for a, b in zip(ts, ts[1:], strict=False))


def _estimate_group(pts: list[Point], s: EstimatorSettings) -> _GroupResult:
    ts = [p.t_us for p in pts]
    rssi_pts = [p for p in pts if p.rssi is not None]
    n = len(pts)
    if not rssi_pts:
        return _GroupResult(float(median(ts)), EstimationMethod.TIMESTAMP_MEDIAN, Confidence.LOW,
                            (ts[-1] - ts[0]) / 2000.0 if n > 1 else None, n, None)
    pts = rssi_pts
    n = len(pts)
    rs = [p.rssi for p in pts]  # type: ignore[misc]
    ts = [p.t_us for p in pts]
    rmax = max(rs)  # type: ignore[type-var]
    if n == 1:
        return _GroupResult(ts[0], EstimationMethod.SINGLE_OBSERVATION, Confidence.LOW, None, 1, rmax)
    if n == 2:
        if abs(rs[0] - rs[1]) >= 1.0:  # type: ignore[operator]
            i = 0 if rs[0] > rs[1] else 1  # type: ignore[operator]
            return _GroupResult(ts[i], EstimationMethod.STRONGEST_OF_TWO, Confidence.LOW,
                                (ts[1] - ts[0]) / 2000.0, 2, rmax)
        return _GroupResult((ts[0] + ts[1]) / 2, EstimationMethod.TWO_POINT_MIDPOINT, Confidence.LOW,
                            (ts[1] - ts[0]) / 2000.0, 2, rmax)

    med_int = _median_interval_ms(ts) or 0.0
    # The raw maximum is biased upwards by noise, so the peak *level* used to define
    # the fit window comes from a lightly smoothed series.
    sm = _smooth(rs) if n >= 5 else list(rs)  # type: ignore[arg-type]
    r_ref = max(sm)
    i_max = max(range(n), key=lambda i: (sm[i], -i))  # earliest (smoothed) maximum
    t_ref = ts[i_max]

    # 1. Quadratic fit over the top window; widen once if the fit is not usable.
    for window in (s.top_window_db, s.top_window_db * 2):
        top = [(p.t_us, p.rssi) for p in pts if p.rssi >= r_ref - window]  # type: ignore[operator]
        if len(top) < s.min_fit_points or top[-1][0] <= top[0][0]:
            continue
        xs = [(t - t_ref) / 1e6 for t, _ in top]
        ys = [float(r) for _, r in top]  # type: ignore[arg-type]
        fit = quadratic_fit(xs, ys)
        if fit is None:
            continue
        a, b, _c, rms, cov = fit
        if a >= 0:
            continue
        xv = -b / (2 * a)
        if not xs[0] <= xv <= xs[-1]:
            continue
        # Delta-method variance of the vertex position.
        da, db = b / (2 * a * a), -1 / (2 * a)
        var = da * da * cov[0][0] + 2 * da * db * cov[0][1] + db * db * cov[1][1]
        se_ms = math.sqrt(max(var, 0.0)) * 1000
        unc = max(1.96 * se_ms, med_int / 2)
        left = sum(1 for x in xs if x < xv)
        right = len(xs) - left
        conf = (
            Confidence.HIGH
            if len(top) >= 7 and rms <= 3.0 and left >= 2 and right >= 2 and unc <= 100
            else Confidence.MEDIUM
        )
        return _GroupResult(
            t_ref + xv * 1e6, EstimationMethod.RSSI_QUADRATIC_FIT, conf, round(unc, 2), n, rmax,
            {"fit_points": len(top), "fit_window_db": window, "fit_rms_db": round(rms, 3),
             "curvature_db_per_s2": round(a, 3), "points_before_peak": left, "points_after_peak": right},
        )
    # 2. Plateau of equal raw maxima (common with 1 dB quantisation).
    tied = [i for i in range(n) if rs[i] >= rmax - 1e-9]  # type: ignore[operator]
    if len(tied) > 1:
        t_c = sum(ts[i] for i in tied) / len(tied)
        span_ms = (ts[tied[-1]] - ts[tied[0]]) / 1000.0
        conf = Confidence.MEDIUM if n >= 5 and 0 < tied[0] and tied[-1] < n - 1 else Confidence.LOW
        return _GroupResult(t_c, EstimationMethod.RSSI_PLATEAU_CENTER, conf, round(max(span_ms / 2, med_int / 2), 2),
                            n, rmax, {"tied_points": len(tied)})

    # 3. Maximum (of the smoothed series) at an edge of the observed window.
    if i_max == 0 or i_max == n - 1:
        nb = ts[1] - ts[0] if i_max == 0 else ts[-1] - ts[-2]
        return _GroupResult(ts[i_max], EstimationMethod.RSSI_PEAK_AT_EDGE, Confidence.LOW, round(nb / 1000.0, 2), n,
                            rmax, {"edge": "first" if i_max == 0 else "last"})

    # 4. Three-point parabolic interpolation (non-uniform spacing) through the raw
    #    readings around the smoothed maximum.
    t0, t1, t2 = ts[i_max - 1], ts[i_max], ts[i_max + 1]
    r0, r1, r2 = rs[i_max - 1], rs[i_max], rs[i_max + 1]
    x0, x2 = (t0 - t1) / 1e6, (t2 - t1) / 1e6
    denom = x0 * x2 * (x0 - x2)
    tv = float(t1)
    if abs(denom) > 1e-18:
        a = (x2 * (r0 - r1) - x0 * (r2 - r1)) / denom  # type: ignore[operator]
        b = (x0 * x0 * (r2 - r1) - x2 * x2 * (r0 - r1)) / denom  # type: ignore[operator]
        if a < 0:
            tv = t1 + (-b / (2 * a)) * 1e6
            tv = min(max(tv, t0), t2)
    conf = Confidence.MEDIUM if n >= 5 else Confidence.LOW
    return _GroupResult(tv, EstimationMethod.RSSI_PEAK_INTERPOLATION, conf, round((t2 - t0) / 4000.0, 2), n, rmax)


def _clusters(pts: list[Point], gap_us: int) -> list[list[Point]]:
    out: list[list[Point]] = [[pts[0]]]
    for p in pts[1:]:
        if p.t_us - out[-1][-1].t_us > gap_us:
            out.append([p])
        else:
            out[-1].append(p)
    return out


def _cluster_peak(c: list[Point]) -> float:
    vals = [p.rssi for p in c if p.rssi is not None]
    return max(vals) if vals else -math.inf


def _doppler_crossing(pts: list[Point], near_us: float) -> float | None:
    dp = [(p.t_us, p.doppler) for p in pts if p.doppler is not None]
    if len(dp) < 4:
        return None
    vals = [d for _, d in dp]
    sm = [median(vals[max(0, i - 1) : i + 2]) for i in range(len(vals))]
    crossings = []
    for i in range(1, len(sm)):
        a, b = sm[i - 1], sm[i]
        if (a > 0 >= b) or (a < 0 <= b):
            ta, tb = dp[i - 1][0], dp[i][0]
            frac = a / (a - b) if a != b else 0.5
            crossings.append(ta + frac * (tb - ta))
    if not crossings:
        return None
    return min(crossings, key=lambda c: abs(c - near_us))


def estimate_crossing(points: list[Point], settings: EstimatorSettings | None = None) -> Estimate:
    """Estimate the crossing time for one pass.  *points* must be non-empty."""
    if not points:
        raise ValueError("no observations")
    s = settings or EstimatorSettings()
    pts = sorted(points, key=lambda p: p.t_us)
    dedup: list[Point] = []
    seen: set[tuple] = set()
    for p in pts:
        key = (p.t_us, p.antenna, p.rssi)
        if key not in seen:
            seen.add(key)
            dedup.append(p)
    pts = dedup

    clusters = _clusters(pts, int(s.cluster_gap_s * 1e6))
    best_peak = max(_cluster_peak(c) for c in clusters)
    candidates = [c for c in clusters if _cluster_peak(c) >= best_peak - s.peak_tie_db]
    chosen = candidates[0]

    by_ant: dict[int | None, list[Point]] = defaultdict(list)
    for p in chosen:
        by_ant[p.antenna].append(p)
    results = {a: _estimate_group(g, s) for a, g in by_ant.items()}
    primary_ant = max(
        results, key=lambda a: (results[a].peak if results[a].peak is not None else -math.inf, results[a].n)
    )
    primary = results[primary_ant]
    conf = primary.confidence
    details: dict[str, Any] = dict(primary.info)
    reasons: list[str] = []

    if len(clusters) > 1:
        details["clusters"] = len(clusters)
        details["candidate_clusters"] = len(candidates)
        if len(candidates) > 1:
            conf = _downgrade(conf)
            reasons.append("multiple comparable signal peaks (tag re-entered the field)")

    if len(results) > 1:
        per_ant = {}
        disagree = False
        for a, r in results.items():
            per_ant[str(a)] = {"timestamp_offset_ms": round((r.t_us - primary.t_us) / 1000.0, 1),
                               "method": r.method.value, "observations": r.n, "peak_rssi": r.peak}
            if a != primary_ant and r.n >= 3 and abs(r.t_us - primary.t_us) > s.antenna_agreement_ms * 1000:
                disagree = True
        details["per_antenna"] = per_ant
        if disagree:
            conf = _downgrade(conf)
            reasons.append("antennas disagree on peak time")

    if any(p.host_timestamp for p in chosen):
        conf = _downgrade(conf)
        reasons.append("host receive timestamps (reader timestamps unavailable)")
    if any(p.aggregated for p in chosen) and primary.method not in (EstimationMethod.SINGLE_OBSERVATION,):
        details["aggregated_reports"] = True

    dop = _doppler_crossing(by_ant[primary_ant], primary.t_us)
    if dop is not None:
        delta = (dop - primary.t_us) / 1000.0
        details["doppler_zero_crossing_offset_ms"] = round(delta, 1)
        details["doppler_agrees"] = abs(delta) <= s.doppler_agreement_ms

    if reasons:
        details["confidence_reasons"] = reasons
    t_final = int(round(primary.t_us + s.time_offset_ms * 1000))
    if s.time_offset_ms:
        details["calibration_offset_ms"] = s.time_offset_ms

    rssi_vals = [p.rssi for p in chosen if p.rssi is not None]
    return Estimate(
        timestamp_us=t_final,
        method=primary.method,
        confidence=conf,
        estimated=primary.method != EstimationMethod.SINGLE_OBSERVATION,
        antenna=primary_ant,
        antennas=sorted(a for a in {p.antenna for p in pts} if a is not None),
        peak_rssi=max(rssi_vals) if rssi_vals else None,
        first_seen_us=pts[0].t_us,
        last_seen_us=pts[-1].t_us,
        observation_count=len(points),
        uncertainty_ms=primary.uncertainty_ms,
        details=details,
    )
