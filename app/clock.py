"""Time sources and reader-clock mapping.

All timestamps inside the application are integer **microseconds since the Unix
epoch, UTC** (``*_us`` fields).  Externally they are rendered as ISO 8601 with an
explicit ``Z`` suffix.

Elapsed intervals (cooldowns, pass windows, backoff) use a monotonic clock.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def now_us(self) -> int: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def now_us(self) -> int:
        return time.time_ns() // 1000


class ManualClock:
    """Deterministic clock for tests."""

    def __init__(self, start_us: int = 1_790_000_000_000_000, start_mono: float = 1000.0) -> None:
        self._now_us = start_us
        self._mono = start_mono
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        return self._mono

    def now_us(self) -> int:
        return self._now_us

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._mono += seconds
            self._now_us += int(round(seconds * 1_000_000))


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def us_to_datetime(us: int) -> datetime:
    # Integer arithmetic avoids float rounding at second boundaries.
    return _EPOCH + timedelta(microseconds=us)


def datetime_to_us(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("naive datetimes are not accepted; include a timezone (e.g. 'Z')")
    delta = dt.astimezone(UTC) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def iso_us(us: int | None) -> str | None:
    """ISO 8601 UTC with microsecond precision and an explicit 'Z' suffix."""
    if us is None:
        return None
    return us_to_datetime(us).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def host_clock_status() -> dict:
    """Kernel NTP status of this host's clock (Linux ``adjtimex``; same source as ``timedatectl``).

    Returns {"synchronized": bool | None, "estimated_error_ms": float | None, "max_error_ms": float | None};
    ``synchronized`` is None when the status cannot be determined (non-Linux).
    """
    import ctypes
    import ctypes.util

    class _Timex(ctypes.Structure):
        _fields_ = [
            ("modes", ctypes.c_uint), ("offset", ctypes.c_long), ("freq", ctypes.c_long),
            ("maxerror", ctypes.c_long), ("esterror", ctypes.c_long), ("status", ctypes.c_int),
            ("constant", ctypes.c_long), ("precision", ctypes.c_long), ("tolerance", ctypes.c_long),
            ("time_sec", ctypes.c_long), ("time_usec", ctypes.c_long), ("tick", ctypes.c_long),
            ("ppsfreq", ctypes.c_long), ("jitter", ctypes.c_long), ("shift", ctypes.c_int),
            ("stabil", ctypes.c_long), ("jitcnt", ctypes.c_long), ("calcnt", ctypes.c_long),
            ("errcnt", ctypes.c_long), ("stbcnt", ctypes.c_long), ("tai", ctypes.c_int),
            ("pad", ctypes.c_int * 11),
        ]

    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
        tx = _Timex()
        rc = libc.adjtimex(ctypes.byref(tx))
    except (OSError, AttributeError):
        return {"synchronized": None, "estimated_error_ms": None, "max_error_ms": None}
    if rc < 0:
        return {"synchronized": None, "estimated_error_ms": None, "max_error_ms": None}
    unsync = rc == 5 or bool(tx.status & 0x40)  # TIME_ERROR or STA_UNSYNC
    return {"synchronized": not unsync, "estimated_error_ms": round(tx.esterror / 1000, 1),
            "max_error_ms": round(tx.maxerror / 1000, 1)}


class TimestampSource(StrEnum):
    READER_UTC = "reader_utc"  # reader UTC clock, used as-is
    READER_UTC_CORRECTED = "reader_utc_corrected"  # reader UTC + measured offset to host clock
    READER_UPTIME_MAPPED = "reader_uptime_mapped"  # reader uptime mapped to host UTC
    HOST_RECEIVE = "host_receive"  # host clock at report receipt (fallback)


@dataclass(slots=True)
class MappedTime:
    us: int
    source: TimestampSource
    uncertainty_ms: float | None


class ReaderClockMapper:
    """Maps reader timestamps to UTC and tracks the reader/host clock relationship.

    For every report we observe ``host_receive - reader_timestamp = offset + latency``
    where ``latency >= 0``.  The running minimum is therefore the tightest available
    estimate of the true offset (reader clock behind host => positive offset); the
    latency floor is not observable, so the estimate is biased high by the minimum
    one-way latency (typically < 5 ms on a LAN).

    Modes:
      * ``reader``            - always use reader UTC timestamps unchanged.
      * ``reader_corrected``  - always add the measured offset (maps into host time).
      * ``auto``              - use reader UTC unless |offset| exceeds the configured
                                limit (reader clock evidently unsynchronised), then correct.
      * ``host``              - ignore reader timestamps; use host receive time.

    Uptime timestamps are always mapped via the offset measured against uptime.
    """

    def __init__(self, mode: str = "auto", max_offset_s: float = 2.0) -> None:
        self.mode = mode
        self.max_offset_us = int(max_offset_s * 1_000_000)
        self.reset()

    def reset(self) -> None:
        self.utc_offset_us: int | None = None  # host - reader (min filter)
        self.uptime_offset_us: int | None = None
        self.samples = 0
        self.connect_offset_us: int | None = None

    def observe_utc(self, reader_us: int, host_us: int) -> None:
        off = host_us - reader_us
        self.samples += 1
        if self.utc_offset_us is None or off < self.utc_offset_us:
            self.utc_offset_us = off

    def observe_uptime(self, reader_uptime_us: int, host_us: int) -> None:
        off = host_us - reader_uptime_us
        if self.uptime_offset_us is None or off < self.uptime_offset_us:
            self.uptime_offset_us = off

    def on_connect(self, kind: str | None, reader_us: int | None, host_us: int) -> None:
        self.reset()
        if kind == "utc" and reader_us is not None:
            self.observe_utc(reader_us, host_us)
            self.connect_offset_us = host_us - reader_us
        elif kind == "uptime" and reader_us is not None:
            self.observe_uptime(reader_us, host_us)

    @property
    def correcting(self) -> bool:
        if self.mode == "reader_corrected":
            return self.utc_offset_us is not None
        if self.mode == "auto":
            return self.utc_offset_us is not None and abs(self.utc_offset_us) > self.max_offset_us
        return False

    def map_utc(self, reader_us: int, host_us: int) -> MappedTime:
        if self.mode == "host":
            return MappedTime(host_us, TimestampSource.HOST_RECEIVE, None)
        self.observe_utc(reader_us, host_us)
        if self.correcting:
            assert self.utc_offset_us is not None
            return MappedTime(reader_us + self.utc_offset_us, TimestampSource.READER_UTC_CORRECTED, 10.0)
        return MappedTime(reader_us, TimestampSource.READER_UTC, None)

    def map_uptime(self, reader_uptime_us: int, host_us: int) -> MappedTime:
        if self.mode == "host":
            return MappedTime(host_us, TimestampSource.HOST_RECEIVE, None)
        self.observe_uptime(reader_uptime_us, host_us)
        assert self.uptime_offset_us is not None
        return MappedTime(reader_uptime_us + self.uptime_offset_us, TimestampSource.READER_UPTIME_MAPPED, 10.0)

    def status(self) -> dict:
        return {
            "mode": self.mode,
            "reader_minus_host_ms": None if self.utc_offset_us is None else round(-self.utc_offset_us / 1000, 3),
            "correction_active": self.correcting,
            "uptime_mapping_active": self.uptime_offset_us is not None,
            "samples": self.samples,
            "note": (
                "reader_minus_host_ms is an upper-bound estimate that includes the minimum "
                "network/report latency; values beyond a few ms indicate the reader and host "
                "clocks are not synchronised (configure NTP on both)."
            ),
        }
