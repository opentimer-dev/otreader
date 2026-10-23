"""Bounded in-memory buffer of raw reports captured in antenna test mode.

Test observations never touch the timing inventory, cooldowns, the event log or
callbacks: the reader manager routes them here by ROSpecID.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from app.clock import iso_us
from app.models import Observation


class TestReportBuffer:
    __test__ = False  # not a pytest test class

    def __init__(self, size: int) -> None:
        self._buf: deque[dict[str, Any]] = deque(maxlen=size)
        self._seq = 0
        self._antenna: dict[int | None, dict[str, Any]] = {}
        self._tags: dict[int | None, set[str]] = {}
        self.session_started_us: int | None = None

    def reset(self, now_us: int) -> None:
        self._buf.clear()
        self._antenna.clear()
        self._tags.clear()
        self.session_started_us = now_us

    def add(self, observations: list[Observation]) -> None:
        for o in observations:
            self._seq += 1
            d = o.to_dict()
            d["sequence"] = self._seq
            self._buf.append(d)
            st = self._antenna.setdefault(
                o.antenna, {"reads": 0, "rssi_min": None, "rssi_max": None, "rssi_sum": 0.0, "rssi_n": 0,
                            "last_read_us": None}
            )
            st["reads"] += 1
            st["last_read_us"] = o.timestamp_us
            if o.rssi is not None:
                st["rssi_min"] = o.rssi if st["rssi_min"] is None else min(st["rssi_min"], o.rssi)
                st["rssi_max"] = o.rssi if st["rssi_max"] is None else max(st["rssi_max"], o.rssi)
                st["rssi_sum"] += o.rssi
                st["rssi_n"] += 1
            self._tags.setdefault(o.antenna, set()).add(o.epc)

    @property
    def last_sequence(self) -> int:
        return self._seq

    def summary(self) -> dict[str, Any]:
        per = []
        for ant, st in sorted(self._antenna.items(), key=lambda kv: (kv[0] is None, kv[0] or 0)):
            per.append(
                {
                    "antenna": ant,
                    "reads": st["reads"],
                    "unique_tags": len(self._tags.get(ant, ())),
                    "rssi_min": st["rssi_min"],
                    "rssi_max": st["rssi_max"],
                    "rssi_avg": round(st["rssi_sum"] / st["rssi_n"], 2) if st["rssi_n"] else None,
                    "last_read_at": iso_us(st["last_read_us"]),
                }
            )
        return {
            "session_started_at": iso_us(self.session_started_us),
            "total_reads": sum(s["reads"] for s in self._antenna.values()),
            "unique_tags": len(set().union(*self._tags.values())) if self._tags else 0,
            "per_antenna": per,
        }

    def list(self, after_sequence: int = 0, limit: int = 100) -> dict[str, Any]:
        items = [r for r in self._buf if r["sequence"] > after_sequence][:limit]
        oldest = self._buf[0]["sequence"] if self._buf else None
        return {
            "reports": items,
            "next_after_sequence": items[-1]["sequence"] if items else after_sequence,
            "last_sequence": self._seq,
            "oldest_buffered_sequence": oldest,
            "buffer_capacity": self._buf.maxlen,
            "truncated": oldest is not None and after_sequence + 1 < oldest,
            "summary": self.summary(),
        }
