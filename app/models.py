"""Internal domain models (hot-path dataclasses).

API/callback representations live in :mod:`app.api.schemas`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from app.clock import TimestampSource, iso_us
from app.config import OperatingMode


class ReaderState(StrEnum):
    DISCONNECTED = "disconnected"
    WAITING_FOR_ADDRESS = "waiting_for_address"  # reader IP not (yet) available in reader.host_file
    SYNCING_TIME = "syncing_time"  # setting the reader clock over SSH before connecting
    CONNECTING = "connecting"
    CONFIGURING = "configuring"
    IDLE = "idle"  # connected + configured, not inventorying
    STARTING = "starting"
    INVENTORYING = "inventorying"
    STOPPING = "stopping"
    DISCONNECTING = "disconnecting"
    RECONNECT_WAIT = "reconnect_wait"


class ReaderActivity(StrEnum):
    NONE = "none"
    TIMING = "timing"
    TEST = "test"


class ReportPurpose(StrEnum):
    TIMING = "timing"
    TEST = "test"


class EstimationMethod(StrEnum):
    FIRST_OBSERVATION = "first_observation"  # start line
    SINGLE_OBSERVATION = "single_observation"
    STRONGEST_OF_TWO = "strongest_of_two"
    TWO_POINT_MIDPOINT = "two_point_midpoint"
    RSSI_QUADRATIC_FIT = "rssi_quadratic_fit"
    RSSI_PEAK_INTERPOLATION = "rssi_peak_interpolation"
    RSSI_PLATEAU_CENTER = "rssi_plateau_center"
    RSSI_PEAK_AT_EDGE = "rssi_peak_at_edge"
    TIMESTAMP_MEDIAN = "timestamp_median"  # no RSSI available


class Confidence(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(slots=True)
class Observation:
    """One normalised tag observation (a TagReportData entry)."""

    epc: str
    antenna: int | None
    rssi: float | None  # dBm
    rssi_source: str | None  # "impinj_peak_rssi" (0.01 dB) | "llrp_peak_rssi" (1 dB)
    timestamp_us: int  # best UTC estimate of the observation instant
    timestamp_source: TimestampSource
    first_seen_us: int | None
    last_seen_us: int | None
    seen_count: int | None
    channel_index: int | None
    phase_angle_deg: float | None
    doppler_hz: float | None
    rospec_id: int | None
    host_received_us: int
    host_received_mono: float
    reader_first_seen_raw_us: int | None = None
    timestamp_uncertainty_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp_source"] = str(self.timestamp_source)
        d["timestamp"] = iso_us(self.timestamp_us)
        d["host_received_at"] = iso_us(self.host_received_us)
        d.pop("host_received_mono", None)
        return d

    def compact(self) -> dict[str, Any]:
        """Compact form stored alongside events for diagnostics / calibration."""
        return {
            "t": self.timestamp_us,
            "a": self.antenna,
            "r": self.rssi,
            "rs": self.rssi_source,
            "n": self.seen_count,
            "ph": self.phase_angle_deg,
            "d": self.doppler_hz,
            "ch": self.channel_index,
            "src": str(self.timestamp_source),
            "fs": self.first_seen_us,
            "ls": self.last_seen_us,
            "hr": self.host_received_us,
        }


@dataclass(slots=True)
class TimingEvent:
    event_id: str
    epc: str
    mode: OperatingMode
    timestamp_us: int  # estimated crossing (finish) / first observation (start)
    antenna: int | None
    antennas: list[int]
    first_seen_us: int
    last_seen_us: int
    peak_rssi: float | None
    observation_count: int
    estimation_method: EstimationMethod
    confidence: Confidence
    estimated: bool
    timestamp_source: TimestampSource
    uncertainty_ms: float | None
    created_at_us: int
    details: dict[str, Any] = field(default_factory=dict)
    sequence: int | None = None  # assigned by the event log

    def payload(self) -> dict[str, Any]:
        """Stable JSON representation (also the callback body)."""
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "epc": self.epc,
            "timestamp": iso_us(self.timestamp_us),
            "mode": str(self.mode),
            "antenna": self.antenna,
            "antennas": self.antennas,
            "peak_rssi": self.peak_rssi,
            "first_seen": iso_us(self.first_seen_us),
            "last_seen": iso_us(self.last_seen_us),
            "observation_count": self.observation_count,
            "estimation_method": str(self.estimation_method),
            "confidence": str(self.confidence),
            "estimated": self.estimated,
            "timestamp_source": str(self.timestamp_source),
            "uncertainty_ms": self.uncertainty_ms,
            "created_at": iso_us(self.created_at_us),
            "details": self.details,
        }
