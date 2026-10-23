"""Pydantic request/response schemas.  These define the documented, stable API.

All timestamps are ISO 8601 UTC strings with a ``Z`` suffix and microsecond
precision, e.g. ``2026-09-30T13:30:15.123456Z``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.config import OperatingMode


class ErrorBody(BaseModel):
    code: str = Field(examples=["reader_unavailable"])
    message: str
    details: Any = None


class ErrorResponse(BaseModel):
    error: ErrorBody


# ----------------------------------------------------------------- events


class TimingEventOut(BaseModel):
    """A timing event.  This is also exactly the JSON body POSTed to the callback URL."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "event_id": "4f1c2b7e9a0d4e61b2c3d4e5f6a7b8c9",
                "sequence": 42,
                "epc": "300833B2DDD9014000000001",
                "timestamp": "2026-09-30T13:30:15.123456Z",
                "mode": "finish_line",
                "antenna": 1,
                "antennas": [1, 2],
                "peak_rssi": -42.5,
                "first_seen": "2026-09-30T13:30:14.900112Z",
                "last_seen": "2026-09-30T13:30:15.200871Z",
                "observation_count": 8,
                "estimation_method": "rssi_quadratic_fit",
                "confidence": "medium",
                "estimated": True,
                "timestamp_source": "reader_utc",
                "uncertainty_ms": 18.5,
                "created_at": "2026-09-30T13:30:17.310200Z",
                "details": {"fit_points": 6, "fit_rms_db": 0.8, "finalized_by": "quiet_window"},
            }
        }
    )

    event_id: str = Field(description="Unique, stable ID. Receivers must de-duplicate on it.")
    sequence: int | None = Field(description="Position in the append-only event log (creation order).")
    epc: str = Field(description="Tag EPC, upper-case hex.")
    timestamp: str = Field(
        description="Estimated crossing time (finish_line) or first observation time (start_line). "
        "An estimate, not an exact physical crossing time; see estimation_method/confidence."
    )
    mode: OperatingMode
    antenna: int | None = Field(description="Primary antenna used for the estimate.")
    antennas: list[int] = Field(description="All antennas that observed the tag during the pass.")
    peak_rssi: float | None = Field(description="Strongest RSSI in dBm.")
    first_seen: str
    last_seen: str
    observation_count: int
    estimation_method: str = Field(
        description="first_observation | single_observation | strongest_of_two | two_point_midpoint | "
        "rssi_quadratic_fit | rssi_peak_interpolation | rssi_plateau_center | rssi_peak_at_edge | timestamp_median"
    )
    confidence: Literal["high", "medium", "low"]
    estimated: bool = Field(description="False when the timestamp is a single raw observation.")
    timestamp_source: str = Field(
        description="reader_utc | reader_utc_corrected | reader_uptime_mapped | host_receive"
    )
    uncertainty_ms: float | None = Field(description="Heuristic ± uncertainty of the estimate (not a guarantee).")
    created_at: str = Field(description="When the middleware accepted the event (not the crossing time).")
    details: dict[str, Any] = Field(default_factory=dict, description="Diagnostic information; not a stable contract.")


class EventOut(TimingEventOut):
    replayed: bool = Field(default=False, description="True when delivered through a rewind/replay cursor.")


class EventPage(BaseModel):
    total: int
    offset: int
    limit: int
    events: list[EventOut]
    last_sequence: int | None = Field(description="Highest sequence in this page (use as after_sequence to poll).")


class RewindRequest(BaseModel):
    event_id: str | None = Field(default=None, description="Replay from this event (inclusive).")
    timestamp: datetime | None = Field(
        default=None, description="Replay events whose timestamp is >= this instant (inclusive). Must include a timezone."
    )
    ttl_seconds: float = Field(default=86400, gt=0, le=7 * 86400)

    @model_validator(mode="after")
    def _exactly_one(self) -> "RewindRequest":
        if (self.event_id is None) == (self.timestamp is None):
            raise ValueError("provide exactly one of 'event_id' or 'timestamp'")
        if self.timestamp is not None and self.timestamp.tzinfo is None:
            raise ValueError("timestamp must include a timezone (e.g. '2026-09-30T13:30:00Z')")
        return self


class ReplaySession(BaseModel):
    cursor_id: str
    start_sequence: int
    end_sequence: int = Field(description="Replay covers events up to this sequence (log head at rewind time).")
    position: int
    total_events: int
    from_event_id: str | None
    from_timestamp: str | None
    expires_at: str
    semantics: str
    next_url: str
    live_continuation: str


class ReplayPage(BaseModel):
    cursor_id: str
    events: list[EventOut]
    position: int
    has_more: bool
    end_sequence: int
    total_events: int
    live_continuation: str


# ----------------------------------------------------------------- tags


class TagOut(BaseModel):
    epc: str
    first_seen: str | None
    last_seen: str | None
    estimated_crossing: str | None
    peak_rssi: float | None
    last_rssi: float | None
    antennas: list[int]
    antenna_counts: dict[int, int]
    last_antenna: int | None
    observation_count: int
    suppressed_count: int
    mode: str
    event_id: str | None
    event_count: int
    estimation_method: str | None
    confidence: str | None
    status: Literal["tracking", "cooldown", "eligible", "eligible_after_cooldown"]
    cooldown_remaining_seconds: float
    cooldown_until: str | None


class TagPage(BaseModel):
    total: int
    offset: int
    limit: int
    tags: list[TagOut]


class OperationResult(BaseModel):
    ok: bool = True
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


# ----------------------------------------------------------------- reader


class ModeRequest(BaseModel):
    mode: OperatingMode


class ReaderStatusOut(BaseModel):
    model_config = ConfigDict(extra="allow")
    connection_state: str = Field(
        description="waiting_for_address | syncing_time | disconnected | connecting | configuring | idle | "
        "starting | inventorying | stopping | disconnecting | reconnect_wait"
    )
    connected: bool
    host: str | None = Field(description="Reader IP in use (null until reader.host_file holds a valid address).")
    address_source: str = Field(description="'file:<path>' or 'config'.")
    address_problem: str | None = Field(description="Why the address file is not usable right now, if it isn't.")
    port: int
    manufacturer: str | None
    model: str | None
    serial_number: str | None
    firmware_version: str | None
    impinj_extensions: bool | None
    mode: OperatingMode
    inventorying: bool
    test_mode_active: bool
    last_connected_at: str | None
    last_error: dict[str, Any] | None


# ----------------------------------------------------------------- callbacks


class CallbackURLRequest(BaseModel):
    url: str = Field(min_length=1, max_length=2048, examples=["http://192.168.8.20:8080/rfid/events"])


class CallbackRetryRequest(BaseModel):
    event_ids: list[str] | None = Field(default=None, description="Limit the retry to these events.")
    include_pending: bool = Field(default=False, description="Also reset pending (backing-off) callbacks to retry now.")


class CallbackQueueItem(BaseModel):
    event_id: str
    sequence: int
    epc: str
    status: Literal["pending", "failed", "delivered"]
    attempts: int
    next_attempt_at: str | None
    last_attempt_at: str | None
    delivered_at: str | None
    last_error: str | None
    last_status_code: int | None


class CallbackQueuePage(BaseModel):
    total: int
    offset: int
    limit: int
    items: list[CallbackQueueItem]
