"""Active tag inventory (in memory, O(1) lookup by EPC) with cooldown state."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from app.clock import Clock, iso_us
from app.config import OperatingMode
from app.models import Observation, TimingEvent
from app.timing.cooldown import CooldownTracker


@dataclass(slots=True)
class TagRecord:
    epc: str
    first_seen_us: int
    last_seen_us: int
    mode: str
    observation_count: int = 0
    suppressed_count: int = 0  # observations ignored because of cooldown
    peak_rssi: float | None = None
    last_rssi: float | None = None
    antenna_counts: dict[str, int] = field(default_factory=dict)  # str keys for JSON
    last_antenna: int | None = None
    event_id: str | None = None
    event_count: int = 0
    estimated_crossing_us: int | None = None
    last_event_created_us: int | None = None
    estimation_method: str | None = None
    confidence: str | None = None
    cooldown_until_us: int | None = None
    tracking: bool = False  # finish-line pass currently open

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TagRecord":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in d.items() if k in known})


class Inventory:
    def __init__(self, cooldown: CooldownTracker, clock: Clock) -> None:
        self.cooldown = cooldown
        self._clock = clock
        self._tags: dict[str, TagRecord] = {}
        self.version = 0  # bumped on every mutation (dirty tracking for snapshots)

    def __len__(self) -> int:
        return len(self._tags)

    def __contains__(self, epc: str) -> bool:
        return epc in self._tags

    def get(self, epc: str) -> TagRecord | None:
        return self._tags.get(epc)

    def in_cooldown(self, epc: str) -> bool:
        return self.cooldown.active(epc)

    def record_observation(self, obs: Observation, mode: OperatingMode, suppressed: bool) -> TagRecord:
        rec = self._tags.get(obs.epc)
        if rec is None:
            rec = TagRecord(epc=obs.epc, first_seen_us=obs.timestamp_us, last_seen_us=obs.timestamp_us, mode=mode.value)
            self._tags[obs.epc] = rec
        rec.observation_count += 1
        if suppressed:
            rec.suppressed_count += 1
        if obs.timestamp_us > rec.last_seen_us:
            rec.last_seen_us = obs.timestamp_us
        if obs.timestamp_us < rec.first_seen_us:
            rec.first_seen_us = obs.timestamp_us
        if obs.rssi is not None:
            rec.last_rssi = obs.rssi
            if rec.peak_rssi is None or obs.rssi > rec.peak_rssi:
                rec.peak_rssi = obs.rssi
        if obs.antenna is not None:
            key = str(obs.antenna)
            rec.antenna_counts[key] = rec.antenna_counts.get(key, 0) + 1
            rec.last_antenna = obs.antenna
        rec.mode = mode.value
        self.version += 1
        return rec

    def set_tracking(self, epc: str, tracking: bool) -> None:
        rec = self._tags.get(epc)
        if rec is not None and rec.tracking != tracking:
            rec.tracking = tracking
            self.version += 1

    def accept_event(self, event: TimingEvent) -> TagRecord:
        """Mark an accepted timing event and start the tag's cooldown."""
        rec = self._tags.get(event.epc)
        if rec is None:
            rec = TagRecord(
                epc=event.epc, first_seen_us=event.first_seen_us, last_seen_us=event.last_seen_us, mode=event.mode.value
            )
            self._tags[event.epc] = rec
        entry = self.cooldown.start(event.epc)
        rec.event_id = event.event_id
        rec.event_count += 1
        rec.estimated_crossing_us = event.timestamp_us
        rec.last_event_created_us = event.created_at_us
        rec.estimation_method = event.estimation_method.value
        rec.confidence = event.confidence.value
        rec.cooldown_until_us = entry.until_utc_us
        rec.tracking = False
        self.version += 1
        return rec

    def remove(self, epc: str) -> bool:
        existed = self._tags.pop(epc, None) is not None
        cleared = self.cooldown.clear(epc)
        if existed or cleared:
            self.version += 1
        return existed or cleared

    def clear(self) -> int:
        n = len(self._tags)
        self._tags.clear()
        self.cooldown.clear_all()
        self.version += 1
        return n

    def status(self, rec: TagRecord) -> str:
        if rec.tracking:
            return "tracking"
        if self.cooldown.active(rec.epc):
            return "cooldown"
        if rec.event_id:
            return "eligible_after_cooldown"
        return "eligible"

    def view(self, rec: TagRecord) -> dict[str, Any]:
        antennas = sorted(int(a) for a in rec.antenna_counts)
        return {
            "epc": rec.epc,
            "first_seen": iso_us(rec.first_seen_us),
            "last_seen": iso_us(rec.last_seen_us),
            "estimated_crossing": iso_us(rec.estimated_crossing_us),
            "peak_rssi": rec.peak_rssi,
            "last_rssi": rec.last_rssi,
            "antennas": antennas,
            "antenna_counts": {int(k): v for k, v in rec.antenna_counts.items()},
            "last_antenna": rec.last_antenna,
            "observation_count": rec.observation_count,
            "suppressed_count": rec.suppressed_count,
            "mode": rec.mode,
            "event_id": rec.event_id,
            "event_count": rec.event_count,
            "estimation_method": rec.estimation_method,
            "confidence": rec.confidence,
            "status": self.status(rec),
            "cooldown_remaining_seconds": round(self.cooldown.remaining(rec.epc), 3),
            "cooldown_until": iso_us(rec.cooldown_until_us) if self.cooldown.active(rec.epc) else None,
        }

    def query(
        self,
        epc: str | None = None,
        antenna: int | None = None,
        seen_after_us: int | None = None,
        seen_before_us: int | None = None,
        status: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[int, list[dict[str, Any]]]:
        needle = epc.upper() if epc else None
        items = []
        for rec in self._tags.values():
            if needle and needle not in rec.epc:
                continue
            if antenna is not None and str(antenna) not in rec.antenna_counts:
                continue
            if seen_after_us is not None and rec.last_seen_us < seen_after_us:
                continue
            if seen_before_us is not None and rec.first_seen_us > seen_before_us:
                continue
            if status is not None and self.status(rec) != status:
                continue
            items.append(rec)
        items.sort(key=lambda r: (r.first_seen_us, r.epc))
        return len(items), [self.view(r) for r in items[offset : offset + limit]]

    def snapshot(self, uncommitted: set[str] | frozenset[str] = frozenset()) -> list[dict[str, Any]]:
        """Serialise all records.  Tags in *uncommitted* (event accepted but not yet in the
        event log) are written without their cooldown: persisting a cooldown before its
        event would, after a crash, suppress the tag with no event recorded."""
        self.cooldown.purge_expired()
        out = []
        for rec in self._tags.values():
            d = rec.to_dict()
            entry = self.cooldown.get(rec.epc)
            d["cooldown_until_us"] = entry.until_utc_us if entry and rec.epc not in uncommitted else None
            out.append(d)
        return out

    def row(self, epc: str) -> dict[str, Any] | None:
        rec = self._tags.get(epc)
        if rec is None:
            return None
        d = rec.to_dict()
        entry = self.cooldown.get(epc)
        d["cooldown_until_us"] = entry.until_utc_us if entry else None
        return d

    def restore(self, rows: list[dict[str, Any]]) -> tuple[int, int]:
        """Load persisted records.  Returns (records, active cooldowns restored)."""
        self._tags.clear()
        self.cooldown.clear_all()
        cooldowns = 0
        for d in rows:
            rec = TagRecord.from_dict(d)
            rec.tracking = False  # open passes are not persisted
            self._tags[rec.epc] = rec
            if rec.cooldown_until_us and self.cooldown.restore(rec.epc, rec.cooldown_until_us):
                cooldowns += 1
        self.version += 1
        return len(self._tags), cooldowns

    def restore_cooldown(self, epc: str, until_utc_us: int) -> bool:
        ok = self.cooldown.restore(epc, until_utc_us)
        if ok:
            rec = self._tags.get(epc)
            if rec is not None:
                rec.cooldown_until_us = until_utc_us
            self.version += 1
        return ok

    def stats(self) -> dict[str, int]:
        return {
            "tags": len(self._tags),
            "cooldowns_active": sum(1 for epc in list(self._tags) if self.cooldown.active(epc)),
            "tracking": sum(1 for r in self._tags.values() if r.tracking),
        }
