import random
from statistics import median

import pytest

from app.models import Confidence, EstimationMethod
from app.timing.finish_estimator import EstimatorSettings, Point, estimate_crossing, quadratic_fit

T0 = 1_790_000_000_000_000


def profile(peak_us, n=40, interval_ms=25.0, peak_rssi=-40.0, curv=60.0, noise=0.0, quant=None, antenna=1,
            rng=None, jitter_ms=0.0, doppler=False):
    rng = rng or random.Random(0)
    pts = []
    start = peak_us - int(n / 2 * interval_ms * 1000)
    for k in range(n + 1):
        t = start + int(k * interval_ms * 1000 + (rng.uniform(-jitter_ms, jitter_ms) * 1000 if jitter_ms else 0))
        dt = (t - peak_us) / 1e6
        r = peak_rssi - curv * dt * dt + (rng.gauss(0, noise) if noise else 0.0)
        if quant:
            r = round(r / quant) * quant
        pts.append(Point(t_us=t, rssi=r, antenna=antenna, doppler=(-dt * 40.0) if doppler else None))
    return pts


def err_ms(est, truth_us):
    return (est.timestamp_us - truth_us) / 1000.0


def test_quadratic_fit_recovers_parabola():
    xs = [i / 10 for i in range(-5, 6)]
    ys = [-3 * (x - 0.123) ** 2 + 7 for x in xs]
    a, b, c, rms, _ = quadratic_fit(xs, ys)
    assert a == pytest.approx(-3)
    assert -b / (2 * a) == pytest.approx(0.123)
    assert rms < 1e-9


def test_ideal_profile_peak_between_samples():
    truth = T0 + 12_345  # deliberately not on a sample instant
    est = estimate_crossing(profile(truth))
    assert est.method == EstimationMethod.RSSI_QUADRATIC_FIT
    assert abs(err_ms(est, truth)) < 1.0
    assert est.confidence == Confidence.HIGH
    assert est.estimated is True
    assert est.observation_count == 41


def test_non_uniform_sampling():
    truth = T0
    est = estimate_crossing(profile(truth, jitter_ms=10.0, rng=random.Random(3)))
    assert abs(err_ms(est, truth)) < 3.0


def test_noisy_profiles_statistics():
    rng = random.Random(42)
    errs = []
    for i in range(200):
        truth = T0 + i * 10_000_000 + rng.randint(-20_000, 20_000)
        est = estimate_crossing(profile(truth, noise=2.0, rng=rng, jitter_ms=5.0))
        errs.append(abs(err_ms(est, truth)))
    assert median(errs) < 30.0
    assert sorted(errs)[int(0.95 * len(errs))] < 90.0


def test_one_db_quantisation_standard_llrp_rssi():
    rng = random.Random(7)
    errs = []
    for i in range(100):
        truth = T0 + i * 10_000_000 + rng.randint(-12_000, 12_000)
        est = estimate_crossing(profile(truth, quant=1.0, noise=0.5, rng=rng))
        errs.append(abs(err_ms(est, truth)))
    assert median(errs) < 25.0


def test_single_observation():
    est = estimate_crossing([Point(t_us=T0, rssi=-50.0, antenna=2)])
    assert est.method == EstimationMethod.SINGLE_OBSERVATION
    assert est.timestamp_us == T0
    assert est.estimated is False
    assert est.confidence == Confidence.LOW
    assert est.first_seen_us == est.last_seen_us == T0
    assert est.uncertainty_ms is None


def test_two_observations_strongest_wins():
    est = estimate_crossing([Point(T0, -60.0, 1), Point(T0 + 80_000, -48.0, 1)])
    assert est.method == EstimationMethod.STRONGEST_OF_TWO
    assert est.timestamp_us == T0 + 80_000


def test_two_equal_observations_midpoint():
    est = estimate_crossing([Point(T0, -50.0, 1), Point(T0 + 80_000, -50.4, 1)])
    assert est.method == EstimationMethod.TWO_POINT_MIDPOINT
    assert est.timestamp_us == T0 + 40_000
    assert est.uncertainty_ms == 40.0


def test_plateau_of_equal_maxima():
    rs = [-60, -55, -50, -50, -50, -55, -60]
    pts = [Point(T0 + i * 50_000, float(r), 1) for i, r in enumerate(rs)]
    est = estimate_crossing(pts, EstimatorSettings(min_fit_points=99))  # force fallback path
    assert est.method == EstimationMethod.RSSI_PLATEAU_CENTER
    assert est.timestamp_us == T0 + 150_000


def test_three_point_interpolation():
    # Peak at +10 ms relative to the max sample; fit disabled to exercise interpolation.
    truth = T0 + 10_000
    pts = [Point(t, -40 - 100 * ((t - truth) / 1e6) ** 2, 1) for t in (T0 - 50_000, T0, T0 + 50_000)]
    pts = [Point(T0 - 100_000, -60.0, 1)] + pts + [Point(T0 + 100_000, -60.0, 1)]
    est = estimate_crossing(pts, EstimatorSettings(min_fit_points=99))
    assert est.method == EstimationMethod.RSSI_PEAK_INTERPOLATION
    assert abs(err_ms(est, truth)) < 1.0


def test_peak_at_edge_is_low_confidence():
    pts = [Point(T0 + i * 30_000, -40.0 - 3 * i, 1) for i in range(6)]  # monotonically falling
    est = estimate_crossing(pts, EstimatorSettings(min_fit_points=99))
    assert est.method == EstimationMethod.RSSI_PEAK_AT_EDGE
    assert est.timestamp_us == T0
    assert est.confidence == Confidence.LOW


def test_strongly_fluctuating_rssi_does_not_crash_and_is_bounded():
    rng = random.Random(5)
    truth = T0
    pts = profile(truth, noise=8.0, rng=rng)
    est = estimate_crossing(pts)
    assert pts[0].t_us <= est.timestamp_us <= pts[-1].t_us
    assert est.confidence in (Confidence.LOW, Confidence.MEDIUM, Confidence.HIGH)


def test_multiple_antennas_primary_is_strongest_and_agreement_checked():
    truth = T0
    a1 = profile(truth, antenna=1, peak_rssi=-45.0)
    a2 = profile(truth + 5_000, antenna=2, peak_rssi=-38.0)
    est = estimate_crossing(a1 + a2)
    assert est.antenna == 2
    assert est.antennas == [1, 2]
    assert abs(err_ms(est, truth + 5_000)) < 1.0
    assert est.details["per_antenna"]["1"]["timestamp_offset_ms"] == pytest.approx(-5.0, abs=1.0)
    assert est.confidence == Confidence.HIGH


def test_antenna_disagreement_lowers_confidence():
    a1 = profile(T0, antenna=1, peak_rssi=-45.0)
    a2 = profile(T0 + 600_000, antenna=2, peak_rssi=-38.0)
    est = estimate_crossing(a1 + a2, EstimatorSettings(cluster_gap_s=5.0))
    assert est.confidence == Confidence.MEDIUM
    assert "antennas disagree" in " ".join(est.details["confidence_reasons"])


def test_disappear_and_reappear_selects_strongest_pass():
    weak_early = profile(T0, peak_rssi=-62.0, n=10)  # tag seen weakly while approaching
    strong = profile(T0 + 3_000_000, peak_rssi=-40.0)
    est = estimate_crossing(weak_early + strong)
    assert abs(err_ms(est, T0 + 3_000_000)) < 1.0
    assert est.details["clusters"] == 2
    assert est.first_seen_us == weak_early[0].t_us  # first/last cover the whole pass


def test_reappearance_with_comparable_peak_prefers_first_and_downgrades():
    first = profile(T0, peak_rssi=-40.0)
    second = profile(T0 + 3_000_000, peak_rssi=-41.0)
    est = estimate_crossing(first + second)
    assert abs(err_ms(est, T0)) < 1.0
    assert est.confidence == Confidence.MEDIUM
    assert est.details["candidate_clusters"] == 2


def test_no_rssi_uses_median_timestamp():
    pts = [Point(T0 + i * 10_000, None, 1) for i in range(5)]
    est = estimate_crossing(pts)
    assert est.method == EstimationMethod.TIMESTAMP_MEDIAN
    assert est.timestamp_us == T0 + 20_000


def test_host_timestamps_downgrade_confidence():
    pts = profile(T0)
    for p in pts:
        p.host_timestamp = True
    est = estimate_crossing(pts)
    assert est.confidence == Confidence.MEDIUM


def test_calibration_offset_applied():
    est = estimate_crossing(profile(T0), EstimatorSettings(time_offset_ms=-35.0))
    assert abs(err_ms(est, T0) + 35.0) < 1.0
    assert est.details["calibration_offset_ms"] == -35.0


def test_doppler_corroboration_reported():
    est = estimate_crossing(profile(T0, doppler=True))
    assert est.details["doppler_agrees"] is True
    assert abs(est.details["doppler_zero_crossing_offset_ms"]) < 30


def test_duplicate_observations_ignored():
    pts = profile(T0)
    est = estimate_crossing(pts + pts[:5])
    assert abs(err_ms(est, T0)) < 1.0


def test_calibration_grid_search_finds_bias():
    from app.timing.calibration import CalibrationPass, evaluate, grid_search

    rng = random.Random(1)
    passes = []
    for i in range(30):
        truth = T0 + i * 10_000_000
        # Systematic +40 ms bias: peak RSSI occurs 40 ms after the true line crossing.
        passes.append(CalibrationPass(str(i), profile(truth + 40_000, noise=1.0, rng=rng), truth))
    stats = evaluate(passes, EstimatorSettings())
    assert stats["bias_median_ms"] == pytest.approx(40.0, abs=8.0)
    best = grid_search(passes)[0]
    assert best["suggested_offset_ms"] == pytest.approx(-40.0, abs=8.0)
    assert abs(best["with_offset"]["bias_median_ms"]) < 3.0
