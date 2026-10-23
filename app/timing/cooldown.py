"""Per-tag cooldown tracking.

A cooldown starts when a timing event is *accepted* for a tag (not when the tag
was last observed).  While active, observations of that tag produce no events.

During operation elapsed time is measured with the monotonic clock, so wall-clock
adjustments (NTP steps) cannot shorten or extend a cooldown.  For persistence the
expiry is also recorded as a UTC wall-clock time; on recovery the remaining
duration is recomputed from the wall clock and re-anchored to the monotonic clock.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.clock import Clock


@dataclass(slots=True)
class CooldownEntry:
    until_mono: float
    until_utc_us: int
    started_utc_us: int


class CooldownTracker:
    def __init__(self, seconds: float, clock: Clock) -> None:
        self.seconds = seconds
        self._clock = clock
        self._entries: dict[str, CooldownEntry] = {}

    def start(self, epc: str) -> CooldownEntry:
        now_mono = self._clock.monotonic()
        now_us = self._clock.now_us()
        entry = CooldownEntry(
            until_mono=now_mono + self.seconds,
            until_utc_us=now_us + int(self.seconds * 1_000_000),
            started_utc_us=now_us,
        )
        self._entries[epc] = entry
        return entry

    def active(self, epc: str) -> bool:
        e = self._entries.get(epc)
        if e is None:
            return False
        if self._clock.monotonic() >= e.until_mono:
            del self._entries[epc]
            return False
        return True

    def remaining(self, epc: str) -> float:
        e = self._entries.get(epc)
        if e is None:
            return 0.0
        return max(0.0, e.until_mono - self._clock.monotonic())

    def get(self, epc: str) -> CooldownEntry | None:
        return self._entries.get(epc) if self.active(epc) else None

    def clear(self, epc: str) -> bool:
        return self._entries.pop(epc, None) is not None

    def clear_all(self) -> int:
        n = len(self._entries)
        self._entries.clear()
        return n

    def restore(self, epc: str, until_utc_us: int, started_utc_us: int | None = None) -> bool:
        """Re-establish a persisted cooldown.  Returns False if it already expired."""
        remaining_s = (until_utc_us - self._clock.now_us()) / 1_000_000
        if remaining_s <= 0:
            return False
        # Never extend beyond the configured duration (guards against wall-clock jumps backwards).
        remaining_s = min(remaining_s, self.seconds) if self.seconds > 0 else remaining_s
        self._entries[epc] = CooldownEntry(
            until_mono=self._clock.monotonic() + remaining_s,
            until_utc_us=until_utc_us,
            started_utc_us=started_utc_us or (until_utc_us - int(self.seconds * 1_000_000)),
        )
        return True

    def purge_expired(self) -> int:
        now = self._clock.monotonic()
        expired = [k for k, e in self._entries.items() if now >= e.until_mono]
        for k in expired:
            del self._entries[k]
        return len(expired)

    def __len__(self) -> int:
        return len(self._entries)
