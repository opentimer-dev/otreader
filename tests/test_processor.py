import asyncio

import pytest

from app.clock import ManualClock, TimestampSource
from app.config import OperatingMode, TimingConfig
from app.models import EstimationMethod, Observation, ReportPurpose
from app.timing.cooldown import CooldownTracker
from app.timing.inventory import Inventory
from app.timing.processor import TimingEngine
from app.timing.test_mode import TestReportBuffer

T0 = 1_790_000_000_000_000


def obs(epc, t, rssi=-50.0, antenna=1, source=TimestampSource.READER_UTC):
    return Observation(epc=epc, antenna=antenna, rssi=rssi, rssi_source="impinj_peak_rssi", timestamp_us=t,
                       timestamp_source=source, first_seen_us=t, last_seen_us=t, seen_count=1, channel_index=1,
                       phase_angle_deg=None, doppler_hz=None, rospec_id=None, host_received_us=t,
                       host_received_mono=0.0)


def pass_obs(epc, peak, n=21, interval=25_000):
    return [obs(epc, peak + (k - n // 2) * interval, rssi=-40 - 60 * (((k - n // 2) * interval) / 1e6) ** 2)
            for k in range(n)]


class Harness:
    def __init__(self, cooldown=300.0, window=2.0, fail_commits=0):
        self.clock = ManualClock()
        self.inventory = Inventory(CooldownTracker(cooldown, self.clock), self.clock)
        self.committed = []
        self.notified = []
        self.fail_commits = fail_commits
        self.test_buffer = TestReportBuffer(100)
        self.engine = TimingEngine(
            TimingConfig(cooldown_seconds=cooldown, finish_estimation_window_seconds=window),
            self.inventory, self.clock, self.commit, self.notified.append, self.test_buffer,
        )

    async def commit(self, event, observations, row):
        if self.fail_commits > 0:
            self.fail_commits -= 1
            raise OSError("disk full")
        self.committed.append((event, observations, row))
        return len(self.committed)


async def drain(h: Harness):
    await asyncio.wait_for(h.engine._wait_drained(), 5)


async def test_start_line_emits_immediately_with_reader_timestamp_and_cooldown():
    h = Harness()
    await h.engine.start()
    h.engine.handle([obs("A", T0), obs("A", T0 + 1000), obs("B", T0 + 5)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    await drain(h)
    events = [e for e, _, _ in h.committed]
    assert [e.epc for e in events] == ["A", "B"]
    assert events[0].timestamp_us == T0
    assert events[0].estimation_method == EstimationMethod.FIRST_OBSERVATION
    assert events[0].sequence == 1 and h.notified[0] is events[0]
    assert h.engine.counters["suppressed_cooldown"] == 1
    await h.engine.stop()


async def test_finish_line_pass_finalised_after_quiet_window():
    h = Harness(window=2.0)
    await h.engine.start()
    h.engine.handle(pass_obs("A", T0), ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
    assert h.engine.finalize_due() == 0
    assert h.inventory.view(h.inventory.get("A"))["status"] == "tracking"
    h.clock.advance(1.9)
    assert h.engine.finalize_due() == 0
    h.clock.advance(0.2)
    assert h.engine.finalize_due() == 1
    await drain(h)
    (event, observations, row), = h.committed
    assert abs(event.timestamp_us - T0) < 1000
    assert event.estimation_method == EstimationMethod.RSSI_QUADRATIC_FIT
    assert len(observations) == 21 and row["cooldown_until_us"] is not None
    # Tag lingering near the line during cooldown: no new event.
    h.engine.handle(pass_obs("A", T0 + 10_000_000), ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
    h.clock.advance(3)
    h.engine.finalize_due()
    await drain(h)
    assert len(h.committed) == 1
    await h.engine.stop()


async def test_cooldown_expiry_allows_new_event():
    h = Harness(cooldown=10, window=0.5)
    await h.engine.start()
    h.engine.handle([obs("A", T0)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    h.clock.advance(11)
    h.engine.handle([obs("A", T0 + 11_000_000)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    await drain(h)
    assert len(h.committed) == 2
    await h.engine.stop()


async def test_remove_tag_makes_it_eligible_again():
    h = Harness()
    await h.engine.start()
    h.engine.handle([obs("A", T0)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    assert h.engine.remove_tag("A")
    h.engine.handle([obs("A", T0 + 1_000_000)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    await drain(h)
    assert len(h.committed) == 2
    await h.engine.stop()


async def test_remove_tag_discards_open_pass():
    h = Harness()
    await h.engine.start()
    h.engine.handle(pass_obs("A", T0), ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
    h.engine.remove_tag("A")
    h.clock.advance(5)
    assert h.engine.finalize_due() == 0
    await h.engine.stop()
    assert h.committed == []


async def test_test_observations_never_touch_timing():
    h = Harness()
    await h.engine.start()
    h.engine.handle([obs("A", T0), obs("B", T0)], ReportPurpose.TEST, None)
    h.clock.advance(5)
    h.engine.finalize_due()
    await h.engine.stop()
    assert h.committed == [] and len(h.inventory) == 0
    assert h.test_buffer.list()["summary"]["unique_tags"] == 2


async def test_pass_open_during_mode_switch_cannot_duplicate():
    h = Harness()
    await h.engine.start()
    h.engine.handle(pass_obs("A", T0), ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
    h.engine.handle([obs("A", T0 + 2_000_000)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    h.clock.advance(5)
    h.engine.finalize_due()
    await drain(h)
    assert len(h.committed) == 1
    assert h.engine.counters["passes_discarded_cooldown"] == 1
    await h.engine.stop()


async def test_max_pass_duration_forces_finalisation():
    h = Harness(window=2.0)
    h.engine.config = h.engine.config.model_copy(update={"finish_max_pass_seconds": 5.0})
    await h.engine.start()
    for i in range(12):
        h.engine.handle([obs("A", T0 + i * 500_000, rssi=-50 + (i % 3))], ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
        h.clock.advance(0.5)
    h.engine.finalize_due()
    await drain(h)
    assert len(h.committed) == 1
    assert h.committed[0][0].details["finalized_by"] == "max_duration"
    await h.engine.stop()


async def test_commit_failures_are_retried_not_lost():
    h = Harness(fail_commits=3)
    await h.engine.start()
    h.engine.handle([obs("A", T0), obs("B", T0 + 1)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    await asyncio.wait_for(h.engine._wait_drained(), 10)
    assert [e.epc for e, _, _ in h.committed] == ["A", "B"]  # order preserved
    assert h.engine.counters["commit_failures"] == 3
    await h.engine.stop()


async def test_stop_flushes_open_passes():
    h = Harness()
    await h.engine.start()
    h.engine.handle(pass_obs("A", T0), ReportPurpose.TIMING, OperatingMode.FINISH_LINE)
    await h.engine.stop()
    assert len(h.committed) == 1 and h.committed[0][0].details["finalized_by"] == "flush"


async def test_host_timestamp_events_are_labelled():
    h = Harness()
    await h.engine.start()
    h.engine.handle([obs("A", T0, source=TimestampSource.HOST_RECEIVE)], ReportPurpose.TIMING, OperatingMode.START_LINE)
    await drain(h)
    ev = h.committed[0][0]
    assert ev.timestamp_source == TimestampSource.HOST_RECEIVE and ev.confidence.value == "low"
    await h.engine.stop()


@pytest.mark.parametrize("n", [1, 2])
async def test_sparse_finish_passes(n):
    h = Harness()
    await h.engine.start()
    h.engine.handle([obs("A", T0 + i * 50_000, rssi=-50 - i * 5) for i in range(n)], ReportPurpose.TIMING,
                    OperatingMode.FINISH_LINE)
    h.clock.advance(3)
    h.engine.finalize_due()
    await drain(h)
    ev = h.committed[0][0]
    assert ev.estimation_method == (EstimationMethod.SINGLE_OBSERVATION if n == 1 else EstimationMethod.STRONGEST_OF_TWO)
    assert ev.estimated is (n > 1)
    await h.engine.stop()
