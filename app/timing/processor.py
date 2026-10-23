"""Timing engine: turns normalised observations into timing events.

Processing is synchronous and runs on the event loop (it is pure in-memory work);
durable commits happen on a single ordered committer task, and callbacks are only
queued *after* the event has been committed to the event log.

* Start line: an event is emitted on the first observation of a tag that is not
  in cooldown, timestamped with the reader's observation time.
* Finish line: observations accumulate in a per-tag *pass*; the pass is finalised
  when the tag has been silent for ``finish_estimation_window_seconds`` (or the
  pass exceeds ``finish_max_pass_seconds``), then the estimator produces the
  crossing time.
* Cooldown is checked when a pass opens *and* again when it is finalised, so a
  pass straddling an accepted event (e.g. after a mode switch) cannot produce a
  duplicate.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from app.clock import Clock, TimestampSource
from app.config import OperatingMode, TimingConfig
from app.models import Confidence, EstimationMethod, Observation, ReportPurpose, TimingEvent
from app.timing.finish_estimator import EstimatorSettings, Point, estimate_crossing
from app.timing.inventory import Inventory
from app.timing.test_mode import TestReportBuffer

log = logging.getLogger(__name__)

CommitFn = Callable[[TimingEvent, list[dict[str, Any]], dict[str, Any] | None], Awaitable[int | None]]
MAX_STORED_OBSERVATIONS = 2000


def new_event_id() -> str:
    return uuid.uuid4().hex


def settings_from_config(cfg: TimingConfig) -> EstimatorSettings:
    return EstimatorSettings(
        top_window_db=cfg.finish_top_window_db,
        min_fit_points=cfg.finish_min_fit_points,
        cluster_gap_s=cfg.finish_cluster_gap_seconds,
        peak_tie_db=cfg.finish_peak_tie_db,
        time_offset_ms=cfg.finish_time_offset_ms,
    )


def observations_to_points(observations: list[Observation]) -> list[Point]:
    return [
        Point(
            t_us=o.timestamp_us,
            rssi=o.rssi,
            antenna=o.antenna,
            doppler=o.doppler_hz,
            host_timestamp=o.timestamp_source == TimestampSource.HOST_RECEIVE,
            aggregated=(o.seen_count or 1) > 1,
        )
        for o in observations
    ]


@dataclass
class PassTrack:
    epc: str
    mode: OperatingMode
    opened_mono: float
    last_mono: float
    observations: list[Observation] = field(default_factory=list)


class TimingEngine:
    def __init__(
        self,
        config: TimingConfig,
        inventory: Inventory,
        clock: Clock,
        commit: CommitFn,
        on_committed: Callable[[TimingEvent], None],
        test_buffer: TestReportBuffer,
        id_factory: Callable[[], str] = new_event_id,
    ) -> None:
        self.config = config
        self.inventory = inventory
        self._clock = clock
        self._commit = commit
        self._on_committed = on_committed
        self.test_buffer = test_buffer
        self._id_factory = id_factory
        self.settings = settings_from_config(config)
        self._passes: dict[str, PassTrack] = {}
        self._queue: asyncio.Queue[tuple[TimingEvent, list[dict[str, Any]], dict[str, Any] | None]] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self.counters: Counter[str] = Counter()
        self.last_commit_error: str | None = None
        self._uncommitted = 0
        self._uncommitted_epcs: Counter[str] = Counter()

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._ticker(), name="timing-ticker"),
            asyncio.create_task(self._committer(), name="timing-committer"),
        ]

    async def stop(self, drain_timeout: float = 10.0) -> None:
        """Finalise open passes and wait for pending commits."""
        self.flush_all()
        try:
            await asyncio.wait_for(self._wait_drained(), timeout=drain_timeout)
        except TimeoutError:
            log.error("%d timing event(s) could not be committed before shutdown", self._uncommitted)
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def _wait_drained(self) -> None:
        while self._uncommitted > 0:
            await asyncio.sleep(0.02)

    # ------------------------------------------------------------ ingestion

    def handle(self, observations: list[Observation], purpose: ReportPurpose, mode: OperatingMode | None) -> None:
        if purpose == ReportPurpose.TEST:
            self.test_buffer.add(observations)
            self.counters["test_observations"] += len(observations)
            return
        assert mode is not None
        for obs in observations:
            self.counters["observations"] += 1
            suppressed = self.inventory.in_cooldown(obs.epc)
            self.inventory.record_observation(obs, mode, suppressed)
            if suppressed:
                self.counters["suppressed_cooldown"] += 1
                continue
            if mode == OperatingMode.START_LINE:
                self._accept_start(obs)
            else:
                self._track_finish(obs, mode)

    def _accept_start(self, obs: Observation) -> None:
        source = obs.timestamp_source
        conf = Confidence.LOW if source == TimestampSource.HOST_RECEIVE else Confidence.HIGH
        event = TimingEvent(
            event_id=self._id_factory(),
            epc=obs.epc,
            mode=OperatingMode.START_LINE,
            timestamp_us=obs.timestamp_us,
            antenna=obs.antenna,
            antennas=[obs.antenna] if obs.antenna is not None else [],
            first_seen_us=obs.timestamp_us,
            last_seen_us=obs.timestamp_us,
            peak_rssi=obs.rssi,
            observation_count=1,
            estimation_method=EstimationMethod.FIRST_OBSERVATION,
            confidence=conf,
            estimated=False,
            timestamp_source=source,
            uncertainty_ms=obs.timestamp_uncertainty_ms,
            created_at_us=self._clock.now_us(),
            details={"rssi_source": obs.rssi_source},
        )
        self._accept(event, [obs])

    def _track_finish(self, obs: Observation, mode: OperatingMode) -> None:
        now = self._clock.monotonic()
        tr = self._passes.get(obs.epc)
        if tr is None:
            tr = PassTrack(epc=obs.epc, mode=mode, opened_mono=now, last_mono=now)
            self._passes[obs.epc] = tr
            self.inventory.set_tracking(obs.epc, True)
            self.counters["passes_opened"] += 1
        tr.observations.append(obs)
        tr.last_mono = now
        if len(tr.observations) >= self.config.finish_max_observations_per_pass:
            self._finalize(tr, reason="max_observations")

    # ------------------------------------------------------------ finalisation

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            try:
                self.finalize_due()
            except Exception:
                log.exception("Error finalising finish-line passes")

    def finalize_due(self) -> int:
        now = self._clock.monotonic()
        window = self.config.finish_estimation_window_seconds
        max_pass = self.config.finish_max_pass_seconds
        due = [
            (tr, "max_duration" if now - tr.opened_mono >= max_pass else "quiet_window")
            for tr in self._passes.values()
            if now - tr.last_mono >= window or now - tr.opened_mono >= max_pass
        ]
        for tr, reason in due:
            self._finalize(tr, reason)
        return len(due)

    def flush_all(self) -> int:
        trs = list(self._passes.values())
        for tr in trs:
            self._finalize(tr, reason="flush")
        return len(trs)

    def _finalize(self, tr: PassTrack, reason: str) -> None:
        self._passes.pop(tr.epc, None)
        self.inventory.set_tracking(tr.epc, False)
        if self.inventory.in_cooldown(tr.epc):
            self.counters["passes_discarded_cooldown"] += 1
            return
        obs = tr.observations
        try:
            est = estimate_crossing(observations_to_points(obs), self.settings)
        except Exception:
            log.exception("Estimator failed for %s; using first observation", tr.epc)
            self.counters["estimator_errors"] += 1
            est = None
        sources = {o.timestamp_source for o in obs}
        source = TimestampSource.HOST_RECEIVE if TimestampSource.HOST_RECEIVE in sources else obs[0].timestamp_source
        if est is None:
            first = min(obs, key=lambda o: o.timestamp_us)
            event = TimingEvent(
                event_id=self._id_factory(), epc=tr.epc, mode=tr.mode, timestamp_us=first.timestamp_us,
                antenna=first.antenna, antennas=sorted({o.antenna for o in obs if o.antenna is not None}),
                first_seen_us=first.timestamp_us, last_seen_us=max(o.timestamp_us for o in obs),
                peak_rssi=max((o.rssi for o in obs if o.rssi is not None), default=None),
                observation_count=len(obs), estimation_method=EstimationMethod.SINGLE_OBSERVATION,
                confidence=Confidence.LOW, estimated=False, timestamp_source=source, uncertainty_ms=None,
                created_at_us=self._clock.now_us(), details={"estimator_error": True, "finalized_by": reason},
            )
        else:
            details = dict(est.details)
            details["finalized_by"] = reason
            details["rssi_source"] = next((o.rssi_source for o in obs if o.rssi_source), None)
            event = TimingEvent(
                event_id=self._id_factory(),
                epc=tr.epc,
                mode=tr.mode,
                timestamp_us=est.timestamp_us,
                antenna=est.antenna,
                antennas=est.antennas,
                first_seen_us=est.first_seen_us,
                last_seen_us=est.last_seen_us,
                peak_rssi=est.peak_rssi,
                observation_count=est.observation_count,
                estimation_method=est.method,
                confidence=est.confidence,
                estimated=est.estimated,
                timestamp_source=source,
                uncertainty_ms=est.uncertainty_ms,
                created_at_us=self._clock.now_us(),
                details=details,
            )
        self._accept(event, obs)

    def _accept(self, event: TimingEvent, observations: list[Observation]) -> None:
        # Cooldown starts at acceptance, before the asynchronous commit, so no
        # duplicate can be created while the commit is in flight.
        self.inventory.accept_event(event)
        row = self.inventory.row(event.epc)
        compact = [o.compact() for o in observations[:MAX_STORED_OBSERVATIONS]]
        self._uncommitted += 1
        self._uncommitted_epcs[event.epc] += 1
        self._queue.put_nowait((event, compact, row))
        self.counters[f"events_{event.mode.value}"] += 1
        log.info(
            "Timing event %s epc=%s mode=%s t=%s method=%s conf=%s obs=%d",
            event.event_id, event.epc, event.mode.value, event.timestamp_us, event.estimation_method.value,
            event.confidence.value, event.observation_count,
        )

    async def _committer(self) -> None:
        while True:
            event, compact, row = await self._queue.get()
            delay = 0.1
            while True:
                try:
                    seq = await self._commit(event, compact, row)
                    self.last_commit_error = None
                    break
                except Exception as exc:
                    self.last_commit_error = f"{type(exc).__name__}: {exc}"
                    self.counters["commit_failures"] += 1
                    log.error("Failed to commit event %s (will retry): %s", event.event_id, exc)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 5.0)
            self._uncommitted -= 1
            self._uncommitted_epcs[event.epc] -= 1
            if self._uncommitted_epcs[event.epc] <= 0:
                del self._uncommitted_epcs[event.epc]
            if seq is None:
                self.counters["duplicate_events_ignored"] += 1
                continue
            event.sequence = seq
            try:
                self._on_committed(event)
            except Exception:
                log.exception("on_committed handler failed for %s", event.event_id)

    # ------------------------------------------------------------ admin

    @property
    def uncommitted_epcs(self) -> frozenset[str]:
        return frozenset(self._uncommitted_epcs)

    def remove_tag(self, epc: str) -> bool:
        had_pass = self._passes.pop(epc, None) is not None
        return self.inventory.remove(epc) or had_pass

    def clear(self) -> int:
        self._passes.clear()
        return self.inventory.clear()

    def open_passes(self) -> list[dict[str, Any]]:
        now = self._clock.monotonic()
        out = []
        for tr in self._passes.values():
            ants: dict[int | None, int] = defaultdict(int)
            for o in tr.observations:
                ants[o.antenna] += 1
            out.append({"epc": tr.epc, "observations": len(tr.observations), "age_seconds": round(now - tr.opened_mono, 3),
                        "silent_seconds": round(now - tr.last_mono, 3), "antennas": dict(ants)})
        return out

    def stats(self) -> dict[str, Any]:
        return {
            "open_passes": len(self._passes),
            "uncommitted_events": self._uncommitted,
            "last_commit_error": self.last_commit_error,
            "counters": dict(self.counters),
        }
